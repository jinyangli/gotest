package main

import (
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"time"
)

var (
	isServer   = flag.Bool("s", false, "server mode")
	serverAddr = flag.String("h", "127.0.0.1:8888", "server address")
)

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
		go DoPong(c)
	}
}

func RunClient(svrAddr string, cacert []byte) {
	certs := x509.NewCertPool()
	if !certs.AppendCertsFromPEM(cacert) {
		log.Fatal("Unable to load root certificates")
	}
	config := &tls.Config{RootCAs: certs}
	conn, err := tls.DialWithDialer(&net.Dialer{
		KeepAlive: 10 * time.Second,
	}, "tcp", svrAddr, config)
	if err != nil {
		log.Fatal(err)
	}
	DoPing(conn)
}

func DoPing(c net.Conn) {
	for i := 0; i < 10; i++ {
		str := fmt.Sprintf("ping %d\n", i)
		buf := []byte(str)
		n, err := c.Write(buf)
		if err != nil {
			log.Fatal(err)
		} else if n != len(buf) {
			log.Fatal(fmt.Errorf("Short write %d!=%d\n", n, len(buf)))
		}
		fmt.Printf("Written %s\n", string(buf))
		buf2 := make([]byte, 100)
		n, err = c.Read(buf2)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%s\n", string(buf2))
	}
	c.Close()
}

func DoPong(c net.Conn) {
	buf := make([]byte, 100)
	for {
		_, err := c.Read(buf)
		if err != nil {
			if err == io.EOF {
				fmt.Printf("Connection closed\n")
				return
			}
			log.Fatal(err)
		}
		fmt.Printf("%s\n", string(buf))
		n, err := c.Write(buf)
		if err != nil {
			log.Fatal(err)
		} else if n != len(buf) {
			log.Fatal("Short write\n")
		}
		fmt.Printf("echoed back\n")
	}
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
