package main

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"sync/atomic"
	"time"

	simple1 "github.com/jinyangli/gotest/testrpc/protocol"
	rpc "github.com/keybase/go-framed-msgpack-rpc"
	context "golang.org/x/net/context"
)

var (
	isServer   = flag.Bool("s", false, "server mode")
	serverAddr = flag.String("h", "127.0.0.1:8888", "server address")
	bSize      = flag.Int("bSize", 512000, "Block Size")
)

type ServerRPCHandlers struct {
	totalReceived uint64
	totalSent     uint64
}

func NewServerRPCHandlers() *ServerRPCHandlers {
	return &ServerRPCHandlers{}
}

func (h *ServerRPCHandlers) Put(ctx context.Context, args simple1.PutArg) error {
	atomic.AddUint64(&h.totalReceived, uint64(len(args.Buf)))
	return nil
}

func (h *ServerRPCHandlers) Get(ctx context.Context, args simple1.GetArg) (res simple1.GetRes, err error) {
	res.Buf = make([]byte, args.Len)
	atomic.AddUint64(&h.totalSent, uint64(args.Len))
	return res, nil
}

func RunServer(bindAddr string, cacert []byte, key []byte) {
	cert, err := tls.X509KeyPair(cacert, key)
	if err != nil {
		log.Fatal(err)
	}
	conf := tls.Config{Certificates: []tls.Certificate{cert}}
	listener, err := tls.Listen("tcp", bindAddr, &conf)
	if err != nil {
		log.Fatal(err)
	}

	for {
		c, err := listener.Accept()
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Accepted remote %s\n", c.RemoteAddr())
		go func() {
			xp := rpc.NewTransport(c, nil, nil)
			rpcsrv := rpc.NewServer(xp, nil)
			rpcsrv.Register(simple1.SimpleProtocol(NewServerRPCHandlers()))
			<-rpcsrv.Run()
			err := rpcsrv.Err()
			if err != io.EOF {
				fmt.Printf("Server connection handler failed %v\n", err)
			} else {
				fmt.Printf("Server connection handler finished successfuly\n")
			}
		}()
	}
}

/*-------------- client ---------------*/
type Client struct {
}

func NewClient() *Client {
	return &Client{}
}

// HandlerName implements the ConnectionHandler interface.
func (Client) HandlerName() string {
	return "SimpleClient"
}

// OnConnect implements the ConnectionHandler interface.
func (ut *Client) OnConnect(context.Context, *rpc.Connection, rpc.GenericClient, *rpc.Server) error {
	return nil
}

// OnConnectError implements the ConnectionHandler interface.
func (ut *Client) OnConnectError(error, time.Duration) {
}

// OnDoCommandError implements the ConnectionHandler interace
func (ut *Client) OnDoCommandError(error, time.Duration) {
}

// OnDisconnected implements the ConnectionHandler interface.
func (ut *Client) OnDisconnected(context.Context, rpc.DisconnectStatus) {
}

// ShouldRetry implements the ConnectionHandler interface.
func (ut *Client) ShouldRetry(name string, err error) bool {
	return true
}

var errCanceled = errors.New("Canceled!")

// ShouldRetryOnConnect implements the ConnectionHandler interface.
func (ut *Client) ShouldRetryOnConnect(err error) bool {
	return err != errCanceled
}

func RunClient(srvAddr string, cacert []byte) {
	certs := x509.NewCertPool()
	if !certs.AppendCertsFromPEM(cacert) {
		log.Fatal("Unable to load root certificates")
	}
	config := &tls.Config{RootCAs: certs}
	conn := rpc.NewTLSConnectionWithTLSConfig(srvAddr, config, nil, NewClient(), true, nil, nil, nil, nil)
	client := simple1.SimpleClient{Cli: conn.GetClient()}

	//generate workload
	var arg simple1.PutArg
	arg.Buf = make([]byte, *bSize)
	arg.Key = "aaa"
	err := client.Put(context.Background(), arg)
	fmt.Printf("Put err=%v\n", err)
}

func main() {
	flag.Parse()
	cert := os.Getenv("TEST_CERT")
	key := os.Getenv("TEST_KEY")
	if *isServer {
		RunServer("0.0.0.0:8888", []byte(cert), []byte(key))
	} else {
		RunClient(*serverAddr, []byte(cert))
	}
}
