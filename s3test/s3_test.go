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

type sample struct {
	end time.Time
	lat int
}

var sampleStats []sample

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

func calculateStats(dur time.Duration, latencies []int) (throughput float32, min int, max int, avg float32) {
	milli := dur.Nanoseconds() / 1000000
	throughput = float32(len(latencies)) * 1000.0 / float32(milli)
	var total int
	min, max = latencies[0], latencies[0]
	for _, v := range latencies {
		if v > max {
			max = v
		}
		if v < min {
			min = v
		}
		total += v
	}
	avg = float32(total) / float32(len(latencies))
	return
}

func measureS3(t *testing.T, loadGen LoadGenerator) {
	var wg sync.WaitGroup

	startTime := time.Now()
	for i := 0; i < *nThreads; i++ {
		wg.Add(1)
		go func(loadGen LoadGenerator, whichTh int) {
			generator := rand.New(rand.NewSource(time.Now().UnixNano()))
			for j := 0; j < *nOps; j++ {
				start := time.Now()
				err := loadGen.DoWork(generator)
				if err != nil {
					t.Logf("thread %d request %d err %v\n", whichTh, j, err)
				}
				end := time.Now()
				sampleStats[whichTh*(*nOps)+j].end = end
				lat := end.Sub(start).Nanoseconds() / int64(time.Millisecond)
				sampleStats[whichTh*(*nOps)+j].lat = int(lat)
			}
			wg.Done()
		}(loadGen, i)
	}
	wg.Wait()

	// report statistics
	// per X second breakdown and total
	finalTime := time.Now()
	endTime := startTime.Add(10 * time.Second)
	if endTime.After(finalTime) {
		endTime = finalTime
	}
	startPos := make([]int, *nThreads)
	for {
		var latencies []int
		for i := 0; i < (*nThreads); i++ {
			var j int
			for j = startPos[i]; j < (*nOps); j++ {
				if endTime.Before(sampleStats[i*(*nOps)+j].end) {
					break
				}
				latencies = append(latencies, sampleStats[i*(*nOps)+j].lat)
			}
			startPos[i] = j
		}
		if len(latencies) == 0 {
			break
		}
		throughput, min, max, avg := calculateStats(endTime.Sub(startTime), latencies)
		log.Printf("endTime %v: throughput %.2f latency (min, avg, max) %d %.2f %d\n", endTime, throughput, min, avg, max)
		startTime = endTime
		endTime = endTime.Add(10 * time.Second)
		if endTime.After(finalTime) {
			endTime = finalTime
		}
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
	fmt.Printf("OnConnect\n")
	return nil
}

func (c *RPCConnectHandler) OnDoCommandError(err error, nextTime time.Duration) {
	fmt.Printf("OnDoCommandError\n")
}

func (c *RPCConnectHandler) OnDisconnected(ctx context.Context, status rpc.DisconnectStatus) {
	fmt.Printf("OnDisconnected\n")
}

func (c *RPCConnectHandler) OnConnectError(err error, dur time.Duration) {
	fmt.Printf("OnConnectError\n")
}

func (c *RPCConnectHandler) ShouldRetry(name string, err error) bool {
	fmt.Printf("ShouldRetry\n")
	return true
}

func (c *RPCConnectHandler) ShouldRetryOnConnect(err error) bool {
	fmt.Printf("ShouldRetryOnConnect\n")
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

	sampleStats = make([]sample, *nThreads*(*nOps))

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
