package s3test

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"runtime/pprof"
	//"strconv"
	"sort"
	"sync"
	"testing"
	"time"

	rpc "github.com/keybase/go-framed-msgpack-rpc/rpc"
	context "golang.org/x/net/context"
)

var (
	nOps     = flag.Int("nOps", 100, "Number of test operations issued per client thread")
	barrier  = flag.Bool("barrier", false, "Do a barrier after sending nThreads ops")
	nBuckets = flag.Int("nBuckets", 1, "Number of buckets used")
	//	bucketPrefix   = flag.String("bucketPrefix", "pperf-test-", "Prefix of the testing buckets")
	bucketPrefix   = flag.String("bucketPrefix", "bservertest", "Prefix of the testing buckets")
	nThreads       = flag.Int("nThreads", 10, "Number of client threads to use")
	bDist          = flag.Int("bdist", 80, "fraction of large block")
	useOfficialSDK = flag.Bool("sdk", true, "Use Amazon's official SDK")
	accelerate     = flag.Bool("acc", true, "Use Amazon's accelerate option")
	cpuprofile     = flag.String("cpuprofile", "", "write cpu profile to file")
	srvAddr        = flag.String("srvAddr", "", "server address")
	//createBuckets = flag.Bool("createBuckets", false, "Create buckets")

	letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
)

type nullLogOutput struct{}

func (nullLogOutput) Info(fmt string, args ...interface{})                  {}
func (nullLogOutput) Error(fmt string, args ...interface{})                 {}
func (nullLogOutput) Debug(fmt string, args ...interface{})                 {}
func (nullLogOutput) Warning(fmt string, args ...interface{})               {}
func (nullLogOutput) Profile(fmt string, args ...interface{})               {}
func (n nullLogOutput) CloneWithAddedDepth(int) rpc.LogOutputWithDepthAdder { return n }

type CloudBlobStore interface {
	GetBucketName() string
	Get(string) ([]byte, error)
	Put(string, []byte) error
	ReadAllKeys(int, int) ([]string, error)
}

type LoadGenerator interface {
	DoWork(generator *rand.Rand) (sz int, err error)
}

type S3TestOp func(store CloudBlobStore, generator *rand.Rand) error

var stores []CloudBlobStore
var allKeys [][]string

type Sample struct {
	Start time.Time
	End   time.Time
	Size  int
	Lat   int64
}

type ByEnd []Sample

func (s ByEnd) Len() int           { return len(s) }
func (s ByEnd) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s ByEnd) Less(i, j int) bool { return s[i].End.Before(s[j].End) }

type ByLat []Sample

func (s ByLat) Len() int           { return len(s) }
func (s ByLat) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s ByLat) Less(i, j int) bool { return s[i].Lat < s[j].Lat }

var allSamples []Sample
var randBlock []byte

var maxBlockSize int = 500000
var minBlockSize int = 300

/*
func initRandomBlock() {
	small := make([]byte, 16)
	generator := rand.New(rand.NewSource(time.Now().UnixNano()))
	r, err := generator.Read(small)
	if r != 16 || err != nil {
		log.Fatalf("returned %d random bytes, expected %d err=%v\n", r, 16, err)
	}
	buf := make([]byte, maxBlockSize)
	for i := 0; i < *bSize; i++ {
		buf[i] = small[i%16]
	}
	randBlock = buf
}
*/

func getSizeFromDistribution(generator *rand.Rand) int {
	if generator.Intn(100) < *bDist {
		return maxBlockSize
	}
	return minBlockSize
}

func genRandBlock(generator *rand.Rand) (block []byte, err error) {
	sz := getSizeFromDistribution(generator)
	block = make([]byte, sz)
	r, err := generator.Read(block)
	if r != sz || err != nil {
		return nil, err
	}
	return block, nil
}

func genRandKey(n int, generator *rand.Rand) string {
	if n < 4 {
		log.Fatal("too small")
	}
	buf := make([]rune, n)
	i := 0
	for i < n {
		buf[i] = letterRunes[generator.Intn(len(letterRunes))]
		i += 1
		if i == 2 {
			buf[i] = '/'
			i += 1
		} else if i == 5 {
			buf[i] = '/'
			i += 1
		}
	}
	return string(buf)
}

func calculateThroughput() (throughput float32, startTime time.Time, count int, dur time.Duration) {
	totalSize := 0
	startTime = time.Now()
	for i := 0; i < *nOps; i++ {
		totalSize += allSamples[i].Size
		if allSamples[i].Start.Before(startTime) {
			startTime = allSamples[i].Start
		}
	}
	sort.Sort(ByEnd(allSamples))
	cutoff := 10
	if len(allSamples) < cutoff {
		log.Fatal("Not enough requests")
	}
	endTime := allSamples[len(allSamples)-cutoff].End
	for i := len(allSamples) - 1; i >= len(allSamples)-cutoff; i-- {
		totalSize -= allSamples[i].Size
	}
	fmt.Printf("full duration %d total bytes %d\n", allSamples[len(allSamples)-1].End.Sub(startTime)/time.Millisecond,
		totalSize)
	//throughput = 1000.0 * float32(len(allSamples)) / float32(endTime.Sub(startTime)/time.Millisecond)
	duration := endTime.Sub(startTime)
	throughput = float32(totalSize) / (1000.0 * float32(duration/time.Millisecond))
	return throughput, startTime, count, duration
}

func measure(t *testing.T, loadGen LoadGenerator) {
	var wg sync.WaitGroup

	inputChan := make(chan int, *nOps)
	outputChan := make(chan Sample, *nOps)
	errChan := make(chan error)

	for i := 0; i < *nThreads; i++ {
		wg.Add(1)
		go func() {
			generator := rand.New(rand.NewSource(time.Now().UnixNano()))
			for range inputChan {
				var s Sample
				s.Start = time.Now()
				sz, err := loadGen.DoWork(generator)
				if err != nil {
					errChan <- err
					break
				}
				s.Size = sz
				s.End = time.Now()
				s.Lat = s.End.Sub(s.Start).Nanoseconds() / int64(time.Millisecond)
				outputChan <- s
			}
			wg.Done()
		}()
	}

	for i := 0; i < *nOps; i++ {
		inputChan <- i
	}
	close(inputChan)
	wg.Wait()

	i := 0
	for i < *nOps {
		select {
		case err := <-errChan:
			log.Fatal("request error %v\n", err)
		case s := <-outputChan:
			allSamples[i] = s
			i++
		}
	}
	// report overall statistics
	throughput, startTime, count, dur := calculateThroughput()
	fmt.Printf("# throughput %.2f ops/sec %d requests duration %d\n", throughput, count, dur/time.Millisecond)

	sort.Sort(ByLat(allSamples))
	for i, v := range allSamples {
		fmt.Printf("%d %d %f\n", v.Start.Sub(startTime).Nanoseconds()/int64(time.Millisecond),
			v.Lat, float32((i+1))/float32(len(allSamples)))
	}
}

func measureWithBarrier(t *testing.T, loadGen LoadGenerator) {

	if (*nOps % *nThreads) != 0 {
		log.Fatal("total ops %d must be a multiple of total threads %d\n", *nOps, *nThreads)
	}
	for k := 0; k < *nOps; k += *nThreads {
		var wg sync.WaitGroup

		errChan := make(chan error, *nThreads)
		for i := 0; i < *nThreads; i++ {
			wg.Add(1)
			go func(tid int, indStart int) {
				generator := rand.New(rand.NewSource(time.Now().UnixNano()))
				var s Sample
				s.Start = time.Now()
				sz, err := loadGen.DoWork(generator)
				if err != nil {
					errChan <- err
				}
				s.Size = sz
				s.End = time.Now()
				s.Lat = s.End.Sub(s.Start).Nanoseconds() / int64(time.Millisecond)
				allSamples[indStart+tid] = s
				wg.Done()
			}(i, k)
		}
		wg.Wait()
		close(errChan)

		for err := range errChan {
			log.Fatal("request error %v\n", err)
		}
	}
	// report overall statistics
	throughput, startTime, count, dur := calculateThroughput()
	fmt.Printf("# throughput %.2f ops/sec %d requests duration %d\n", throughput, count, dur/time.Millisecond)

	sort.Sort(ByLat(allSamples))
	for i, v := range allSamples {
		fmt.Printf("%d %d %f\n", v.Start.Sub(startTime).Nanoseconds()/int64(time.Millisecond),
			v.Lat, float32((i+1))/float32(len(allSamples)))
	}
}

type PutGenerator struct {
	store CloudBlobStore
}

func (p *PutGenerator) DoWork(generator *rand.Rand) (sz int, err error) {
	// do a random put operation
	key := genRandKey(40, generator)
	val, err := genRandBlock(generator)
	if err != nil {
		return 0, err
	}
	err = p.store.Put(key, val)
	return len(val), err
}

func TestS3PutPerformance(t *testing.T) {
	p := &PutGenerator{
		store: stores[0],
	}
	if *barrier {
		measureWithBarrier(t, p)
	} else {
		measure(t, p)
	}
}

type GetGenerator struct {
	keys  []string
	store CloudBlobStore
}

func (p *GetGenerator) DoWork(generator *rand.Rand) (sz int, err error) {
	// do a random put operation
	key := p.keys[rand.Intn(len(p.keys))]
	b, err := p.store.Get(key)
	return len(b), err
}

func TestS3GetPerformance(t *testing.T) {
	keys, err := stores[0].ReadAllKeys(*nOps*(*nThreads), maxBlockSize)
	if err != nil || len(keys) == 0 {
		t.Fatal("bucket(%s) err %v tuples in bucket %d\n",
			stores[0].GetBucketName(), err, len(keys))
	}
	log.Printf("Retrieved %d keys\n", len(keys))
	g := &GetGenerator{
		keys:  keys,
		store: stores[0],
	}
	if *barrier {
		measureWithBarrier(t, g)
	} else {
		measure(t, g)
	}

}

type RPCConnectHandler struct {
}

func (c *RPCConnectHandler) OnConnect(ctx context.Context,
	conn *rpc.Connection, client rpc.GenericClient, _ *rpc.Server) error {
	log.Printf("OnConnect\n")
	return nil
}

func (c *RPCConnectHandler) OnDoCommandError(err error, nextTime time.Duration) {
	log.Printf("OnDoCommandError\n")
}

func (c *RPCConnectHandler) OnDisconnected(ctx context.Context, status rpc.DisconnectStatus) {
	log.Printf("OnDisconnected\n")
}

func (c *RPCConnectHandler) OnConnectError(err error, dur time.Duration) {
	log.Printf("OnConnectError\n")
}

func (c *RPCConnectHandler) ShouldRetry(name string, err error) bool {
	log.Printf("ShouldRetry\n")
	return true
}

func (c *RPCConnectHandler) ShouldRetryOnConnect(err error) bool {
	log.Printf("ShouldRetryOnConnect\n")
	return true
}

func (c *RPCConnectHandler) HandlerName() string {
	return "RPCConnectHandler"
}

type ServerGetGenerator struct {
	keys   []string
	client BlockProtocolClient
}

func (g *ServerGetGenerator) DoWork(generator *rand.Rand) (sz int, err error) {
	key := g.keys[rand.Intn(len(g.keys))]
	arg := GetArg{
		Key:  key,
		Size: 0,
	}
	res, err := g.client.Get(context.Background(), arg)
	return len(res.Value), err
}

type ServerPutGenerator struct {
	client BlockProtocolClient
}

func (g *ServerPutGenerator) DoWork(generator *rand.Rand) (sz int, err error) {
	block, err := genRandBlock(generator)
	if err != nil {
		return 0, err
	}
	arg := PutArg{
		Key:   genRandKey(40, generator),
		Value: block,
	}
	_, err = g.client.Put(context.Background(), arg)
	return len(block), err
}

func makeClient() BlockProtocolClient {
	cert, err := ioutil.ReadFile("server/selfsigned.crt")
	if err != nil {
		log.Fatal(err)
	}
	//srvAddr must be s3test.news.cs.nyu.edu specify that in /etc/hosts
	opts := rpc.ConnectionOpts{
		DontConnectNow: true,
	}
	handler := &RPCConnectHandler{}
	logOpts := rpc.NewStandardLogOptions("", nil)
	conn := rpc.NewTLSConnection(rpc.NewFixedRemote(*srvAddr), cert, nil,
		handler, rpc.NewSimpleLogFactory(nil, logOpts), nullLogOutput{}, rpc.DefaultMaxFrameLength, opts)
	client := BlockProtocolClient{Cli: conn.GetClient()}
	return client
}

func TestServerGetPerformance(t *testing.T) {
	client := makeClient()
	store, err := NewOfficialS3Store("us-east-1", *bucketPrefix, *accelerate)
	if err != nil {
		log.Fatal(err)
	}
	keys, err := store.ReadAllKeys(*nOps*(*nThreads), maxBlockSize)
	if err != nil || len(keys) == 0 {
		t.Fatal("bucket(%s) err %v tuples in bucket %d\n",
			stores[0].GetBucketName(), err, len(keys))
	}
	log.Printf("Retrieved %d keys\n", len(keys))

	g := &ServerGetGenerator{
		client: client,
		keys:   keys,
	}
	if *barrier {
		measureWithBarrier(t, g)
	} else {
		measure(t, g)
	}
}

func TestServerPutPerformance(t *testing.T) {
	client := makeClient()
	g := &ServerPutGenerator{
		client: client,
	}
	if *barrier {
		measureWithBarrier(t, g)
	} else {
		measure(t, g)
	}
}

type ServerNullPutGenerator struct {
	client BlockProtocolClient
}

func (g *ServerNullPutGenerator) DoWork(generator *rand.Rand) (sz int, err error) {
	val, err := genRandBlock(generator)
	if err != nil {
		return 0, err
	}
	arg := PutArg{
		Key:   genRandKey(40, generator),
		Value: val,
		NoS3:  true,
	}
	_, err = g.client.Put(context.Background(), arg)
	return len(val), err
}

func TestServerNullPutPerformance(t *testing.T) {
	client := makeClient()
	g := &ServerNullPutGenerator{
		client: client,
	}
	if *barrier {
		measureWithBarrier(t, g)
	} else {
		measure(t, g)
	}
}

func TestMain(m *testing.M) {
	flag.Parse()
	//initRandomBlock()

	allSamples = make([]Sample, *nOps)

	//create all nBuckets
	stores = make([]CloudBlobStore, *nBuckets)
	/*
		for i := 0; i < *nBuckets; i++ {
			name := *bucketPrefix
			if i != 0 {
				name += strconv.Itoa(i)
			}
			var err error
			if *useOfficialSDK {
				stores[i], err = NewOfficialS3Store("us-east-1", name, *accelerate)
			} else {
				stores[i], err = NewGoamzS3Store("us-east-1", name)
			}
			if err != nil {
				log.Fatal(err)
			}
		}
	*/
	if *nBuckets != 1 {
		log.Fatal(fmt.Errorf("nBuckets must be 1"))
	}
	store, err := NewOfficialS3Store("us-east-1", *bucketPrefix, *accelerate)
	if err != nil {
		log.Fatal(err)
	}
	stores[0] = store
	log.Printf("created %d buckets\n", *nBuckets)

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()
		fmt.Printf("starting profiling\n")
		pprof.StartCPUProfile(f)
		defer func() {
			fmt.Printf("stopping profiling\n")
			pprof.StopCPUProfile()
		}()
	}
	m.Run()
}
