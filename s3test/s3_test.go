package s3test

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"runtime/pprof"
	//"strconv"
	"sync"
	"testing"
	"time"
)

var (
	nOps     = flag.Int("nOps", 100, "Number of test operations issued per client thread")
	nBuckets = flag.Int("nBuckets", 1, "Number of buckets used")
	//	bucketPrefix   = flag.String("bucketPrefix", "pperf-test-", "Prefix of the testing buckets")
	bucketPrefix   = flag.String("bucketPrefix", "bservertest", "Prefix of the testing buckets")
	nThreads       = flag.Int("nThreads", 10, "Number of client threads to use")
	bSize          = flag.Int("bSize", 500000, "Default block size")
	useOfficialSDK = flag.Bool("sdk", true, "Use Amazon's official SDK")
	accelerate     = flag.Bool("acc", false, "Use Amazon's accelerate option")
	cpuprofile     = flag.String("cpuprofile", "", "write cpu profile to file")
	//createBuckets = flag.Bool("createBuckets", false, "Create buckets")

	letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
)

type CloudBlobStore interface {
	GetBucketName() string
	GetRandomKey(*rand.Rand) string
	Get(string) ([]byte, error)
	Put(string, []byte) error
	ReadAllKeys(int) (int, error)
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

func measureS3(t *testing.T, op S3TestOp) {
	var wg sync.WaitGroup

	startTime := time.Now()
	for i := 0; i < *nThreads; i++ {
		wg.Add(1)
		b := i % (*nBuckets)
		go func(store CloudBlobStore, whichTh int) {
			generator := rand.New(rand.NewSource(time.Now().UnixNano()))
			for j := 0; j < *nOps; j++ {
				start := time.Now()
				err := op(store, generator)
				if err != nil {
					t.Logf("thread %d request %d err %v\n", whichTh, j, err)
				}
				end := time.Now()
				sampleStats[whichTh*(*nOps)+j].end = end
				lat := end.Sub(start).Nanoseconds() / int64(time.Millisecond)
				sampleStats[whichTh*(*nOps)+j].lat = int(lat)
			}
			wg.Done()
		}(stores[b], i)
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

func TestS3PutPerformance(t *testing.T) {
	measureS3(t, func(store CloudBlobStore, generator *rand.Rand) error {
		// do a random put operation
		key := genRandString(40, generator)
		val := genRandBytes(*bSize, generator)
		return store.Put(key, val)
	})
}

func TestS3GetPerformance(t *testing.T) {
	var nRead int
	var err error
	for i := 0; i < *nBuckets; i++ {
		nRead, err = stores[i].ReadAllKeys(*nOps * (*nThreads))
		if err != nil || nRead == 0 {
			t.Fatal("bucket(%s) err %v tuples in bucket %d\n",
				stores[i].GetBucketName(), err, nRead)
		}
	}
	measureS3(t, func(store CloudBlobStore, generator *rand.Rand) error {
		// do a random get operation
		key := store.GetRandomKey(generator)
		_, err := store.Get(key)
		return err
	})
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
