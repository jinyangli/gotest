package main

import (
	//"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"time"

	"github.com/jinyangli/gotest/s3test"
	"github.com/keybase/go-framed-msgpack-rpc/rpc"
	context "golang.org/x/net/context"
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
	if len(f.buf) < arg.Size {
		f.buf = make([]byte, arg.Size)
		f.randSource.Read(f.buf)
	}
	res.Value = f.buf[0:arg.Size]
	return res, nil
}

func main() {
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
	d, err := s3test.NewDaemon(srv, nil, cert, key)
	if err != nil {
		log.Fatal(err)
	}
	err = d.AcceptLoop()
	//wait for shutdown to be complete
	d.WaitForShutdown()
}