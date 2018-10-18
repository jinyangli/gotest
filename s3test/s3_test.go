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
	nBuckets = flag.Int("nBuckets", 1, "Number of buckets used")
	//	bucketPrefix   = flag.String("bucketPrefix", "pperf-test-", "Prefix of the testing buckets")
	bucketPrefix   = flag.String("bucketPrefix", "bservertest", "Prefix of the testing buckets")
	nThreads       = flag.Int("nThreads", 10, "Number of client threads to use")
	bSize          = flag.Int("bSize", 500000, "Default block size")
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
	ReadAllKeys(int) ([]string, error)
}

type LoadGenerator interface {
	DoWork(generator *rand.Rand) error
}

type S3TestOp func(store CloudBlobStore, generator *rand.Rand) error

var stores []CloudBlobStore
var allKeys [][]string

type Sample struct {
	Start time.Time
	End   time.Time
	Lat   int64
}

type ByStart []Sample

func (s ByStart) Len() int           { return len(s) }
func (s ByStart) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s ByStart) Less(i, j int) bool { return s[i].Start.Before(s[j].Start) }

type ByLat []Sample

func (s ByLat) Len() int           { return len(s) }
func (s ByLat) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s ByLat) Less(i, j int) bool { return s[i].Lat < s[j].Lat }

var allSamples []Sample
var randBlock []byte

func init() {
	small := make([]byte, 16)
	generator := rand.New(rand.NewSource(time.Now().UnixNano()))
	r, err := generator.Read(small)
	if r != 16 || err != nil {
		log.Fatalf("returned %d random bytes, expected %d err=%v\n", r, 16, err)
	}
	n := *bSize
	buf := make([]byte, n)
	for i := 0; i < n; i++ {
		buf[i] = small[i%16]
	}

	randBlock = buf
}

func genRandBytes(n int, generator *rand.Rand) []byte {
	return randBlock
}

func genRandString(n int, generator *rand.Rand) string {
	buf := make([]rune, n)
	for i := range buf {
		buf[i] = letterRunes[generator.Intn(len(letterRunes))]
	}
	return string(buf)
}

func calculateThroughput() (throughput float32, samples []Sample, startTime time.Time) {
	startTime = time.Now()
	for i := 0; i < *nThreads; i++ {
		ind := i*(*nOps) + 0
		if allSamples[ind].Start.Before(startTime) {
			startTime = allSamples[ind].Start
		}
	}
	endTime := time.Now()
	for i := 0; i < *nThreads; i++ {
		ind := i*(*nOps) + (*nOps - 1)
		if allSamples[ind].End.Before(endTime) {
			endTime = allSamples[ind].End
		}
	}
	for i := 0; i < *nThreads; i++ {
		for j := 0; j < *nOps; j++ {
			ind := i*(*nOps) + j
			if allSamples[ind].End.Before(endTime) {
				samples = append(samples, allSamples[ind])
			} else {
				break
			}
		}
	}
	throughput = float32(len(samples)) / float32(endTime.Sub(startTime).Seconds())
	return throughput, samples, startTime
}

func measureS3(t *testing.T, loadGen LoadGenerator) {
	var wg sync.WaitGroup

	for i := 0; i < *nThreads; i++ {
		wg.Add(1)
		go func(loadGen LoadGenerator, whichTh int) {
			generator := rand.New(rand.NewSource(time.Now().UnixNano()))
			for j := 0; j < *nOps; j++ {
				ind := whichTh*(*nOps) + j
				allSamples[ind].Start = time.Now()
				err := loadGen.DoWork(generator)
				if err != nil {
					t.Logf("thread %d request %d err %v\n", whichTh, j, err)
				}
				allSamples[ind].End = time.Now()
				allSamples[ind].Lat = allSamples[ind].End.Sub(allSamples[ind].Start).Nanoseconds() / int64(time.Millisecond)
			}
			wg.Done()
		}(loadGen, i)
	}
	wg.Wait()

	// report overall statistics
	throughput, samples, startTime := calculateThroughput()
	fmt.Printf("# throughput %.2f ops/sec %d requests\n", throughput, len(samples))

	// sort.Sort(ByStart(samples))
	sort.Sort(ByLat(samples))
	for _, v := range samples {
		fmt.Printf("%d %d\n", v.Start.Sub(startTime).Nanoseconds()/int64(time.Millisecond), v.Lat)
	}
}

type PutGenerator struct {
	store CloudBlobStore
}

func (p *PutGenerator) DoWork(generator *rand.Rand) error {
	// do a random put operation
	key := genRandString(40, generator)
	val := genRandBytes(*bSize, generator)
	return p.store.Put(key, val)
}

func TestS3PutPerformance(t *testing.T) {
	p := &PutGenerator{
		store: stores[0],
	}
	measureS3(t, p)
}

type GetGenerator struct {
	keys  []string
	store CloudBlobStore
}

func (p *GetGenerator) DoWork(generator *rand.Rand) error {
	// do a random put operation
	key := p.keys[rand.Intn(len(p.keys))]
	_, err := p.store.Get(key)
	return err
}

func TestS3GetPerformance(t *testing.T) {
	keys, err := stores[0].ReadAllKeys(*nOps * (*nThreads))
	if err != nil || len(keys) == 0 {
		t.Fatal("bucket(%s) err %v tuples in bucket %d\n",
			stores[0].GetBucketName(), err, len(keys))
	}
	loadGen := GetGenerator{
		keys:  keys,
		store: stores[0],
	}
	measureS3(t, &loadGen)
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
	client BlockProtocolClient
}

func (g *ServerGetGenerator) DoWork(generator *rand.Rand) error {
	arg := GetArg{
		Key:  "hello",
		Size: *bSize,
	}
	_, err := g.client.Get(context.Background(), arg)
	return err
}

type ServerPutGenerator struct {
	client BlockProtocolClient
}

func (g *ServerPutGenerator) DoWork(generator *rand.Rand) error {
	arg := PutArg{
		Key:   genRandString(40, generator),
		Value: genRandBytes(*bSize, generator),
	}
	_, err := g.client.Put(context.Background(), arg)
	return err
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
	g := &ServerGetGenerator{
		client: client,
	}
	measureS3(t, g)
}

func TestServerPutPerformance(t *testing.T) {
	client := makeClient()
	g := &ServerPutGenerator{
		client: client,
	}
	measureS3(t, g)
}

type ServerNullPutGenerator struct {
	client BlockProtocolClient
}

func (g *ServerNullPutGenerator) DoWork(generator *rand.Rand) error {
	arg := PutArg{
		Key:   genRandString(40, generator),
		Value: genRandBytes(*bSize, generator),
		NoS3:  true,
	}
	_, err := g.client.Put(context.Background(), arg)
	return err
}

func TestServerNullPutPerformance(t *testing.T) {
	client := makeClient()
	g := &ServerNullPutGenerator{
		client: client,
	}
	measureS3(t, g)
}

func TestMain(m *testing.M) {
	flag.Parse()

	allSamples = make([]Sample, *nThreads*(*nOps))

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
