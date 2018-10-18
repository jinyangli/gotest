package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"time"

	"github.com/jinyangli/gotest/s3test"
	"github.com/keybase/go-framed-msgpack-rpc/rpc"
	context "golang.org/x/net/context"
)

var (
	verbose = flag.Bool("v", false, "verbose printing")
)

type MyServer struct {
	s3store    *s3test.OfficialS3Store
	randSource *rand.Rand
	buf        []byte
}

func NewMyServer() (*MyServer, error) {
	s3store, err := s3test.NewOfficialS3Store("us-east-1", "bservertest", true)
	if err != nil {
		return nil, err
	}
	return &MyServer{
		s3store:    s3store,
		randSource: rand.New(rand.NewSource(time.Now().UnixNano())),
	}, nil
}

func (f *MyServer) CreateServerAndRegister(rpcs *rpc.Server) {
	rpcs.Register(s3test.BlockProtocol(f))
}

func (f *MyServer) Shutdown() {
}

func (f *MyServer) Get(ctx context.Context, arg s3test.GetArg) (res s3test.GetRes, err error) {
	if arg.Size > 0 && len(f.buf) < arg.Size {
		f.buf = make([]byte, arg.Size)
		f.randSource.Read(f.buf)
	} else if arg.Size > 0 {
		res.Value = f.buf[0:arg.Size]
	}
	buf, err := f.s3store.Get(arg.Key)
	if err != nil {
		log.Fatal(err)
	}
	res.Value = buf
	return res, nil
}

func (f *MyServer) Put(ctx context.Context, arg s3test.PutArg) (res s3test.PutRes, err error) {
	startTime := time.Now()
	if !arg.NoS3 {
		err := f.s3store.Put(arg.Key, arg.Value)
		if err != nil {
			log.Fatal(err)
		}
	}
	endTime := time.Now()
	if *verbose {
		fmt.Printf("Put %d NoS3 %v Key size %d Value size %d\n", endTime.Sub(startTime).Nanoseconds()/int64(time.Millisecond), arg.NoS3, len(arg.Key), len(arg.Value))
	}
	res.Size = len(arg.Value)
	return res, nil
}

func main() {
	flag.Parse()

	cert, err := ioutil.ReadFile("selfsigned.crt")
	if err != nil {
		log.Fatal(err)
	}
	key, err := ioutil.ReadFile("selfsigned.key")
	if err != nil {
		log.Fatal(err)
	}
	srv, err := NewMyServer()
	if err != nil {
		log.Fatal(err)
	}
	d, err := s3test.NewDaemon(srv, cert, key)
	if err != nil {
		log.Fatal(err)
	}
	err = d.AcceptLoop()
	//wait for shutdown to be complete
	d.WaitForShutdown()
}
