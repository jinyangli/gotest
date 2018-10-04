package s3test

import (
	"crypto/tls"
	"fmt"
	"io"
	"log"
	"net"
	"sync"

	"github.com/keybase/go-framed-msgpack-rpc/rpc"
)

// RPCSFactory interace creates and registers RPC servers. Its functions must be
// goroutine-safe.
type RPCSFactory interface {
	CreateServerAndRegister(rpcs *rpc.Server)
	Shutdown()
}

// Daemon handles incoming connections to KBFS servers.
type Daemon struct {
	rpcsFactory RPCSFactory
	logFactory  rpc.LogFactory
	port        string
	cacert      []byte
	tlsKey      []byte
	listener    net.Listener

	connsMtx sync.RWMutex
	conns    map[net.Conn]struct{}

	wg sync.WaitGroup

	shutdownOnce sync.Once
	shutdownCh   chan struct{}
	doneCh       chan struct{}
}

// NewDaemon constructs a new, named Daemon instance.
func NewDaemon(f RPCSFactory, logFactory rpc.LogFactory, cacert []byte, key []byte) (
	*Daemon, error) {
	cert, err := tls.X509KeyPair(cacert, key)
	if err != nil {
		return nil, err
	}
	tlsConfig := tls.Config{Certificates: []tls.Certificate{cert}}
	listener, err := tls.Listen("tcp", ":8888", &tlsConfig)
	if err != nil {
		return nil, err
	}
	d := &Daemon{
		rpcsFactory: f,
		logFactory:  logFactory,
		listener:    listener,
		conns:       make(map[net.Conn]struct{}),
		shutdownCh:  make(chan struct{}),
		doneCh:      make(chan struct{}),
	}
	return d, nil
}

// Handle receives an incoming connection.
func (d *Daemon) Handle(c net.Conn) {
	// cleanup
	defer func() {
		func() {
			d.connsMtx.Lock()
			defer d.connsMtx.Unlock()
			delete(d.conns, c)
		}()
		c.Close()
		d.wg.Done()
	}()

	xp := rpc.NewTransport(c, d.logFactory, nil)
	srv := rpc.NewServer(xp, nil)
	d.rpcsFactory.CreateServerAndRegister(srv)

	<-srv.Run()
	// Always non-nil after the channel returned by Run() is closed.
	err := srv.Err()

	if err != io.EOF {
		fmt.Printf("daemon error %s\n", err.Error())
	}
}

// GetBindAddr returns the address for which the driver is bound.
func (d *Daemon) GetBindAddr() string {
	return d.listener.Addr().String()
}

// AcceptLoop accepts incoming connections.
func (d *Daemon) AcceptLoop() error {
	fmt.Printf("daemon bindAddr %s\n", d.GetBindAddr())
OUTER:
	for {
		c, err := d.listener.Accept()
		if err != nil {
			select {
			case <-d.shutdownCh:
				break OUTER
			default:
			}
			log.Fatal(err)
		} else {
			fmt.Printf("daemon %s accepted\n", c.RemoteAddr())
			func() {
				d.connsMtx.Lock()
				defer d.connsMtx.Unlock()
				d.conns[c] = struct{}{}
			}()
			d.wg.Add(1)
			go d.Handle(c)
		}
	}

	// Kill all live connections.
	func() {
		d.connsMtx.Lock()
		defer d.connsMtx.Unlock()
		for c := range d.conns {
			c.Close()
		}
		d.conns = make(map[net.Conn]struct{})
	}()

	d.wg.Wait()
	close(d.doneCh)
	return nil
}

// Shutdown cleanly stops a Daemon instance.
func (d *Daemon) Shutdown() {
	fmt.Printf("daemon to be shut down\n")

	d.shutdownOnce.Do(func() {
		close(d.shutdownCh)
		d.listener.Close()
		d.rpcsFactory.Shutdown()
	})
}

// WaitForShutdown blocks for shutdown to complete
func (d *Daemon) WaitForShutdown() {
	//wait for all connections to shutdown and rpcsfactory to clean up
	<-d.doneCh
}
