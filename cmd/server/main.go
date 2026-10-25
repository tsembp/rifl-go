package main

import (
	"flag"
	"log"
	"net"
	"net/rpc"

	riflrpc "github.com/tsembp/rifl-go"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:9000", "TCP listen address")
	flag.Parse()
	server := rpc.NewServer()
	if err := server.RegisterName("Counter", &riflrpc.Counter{}); err != nil {
		log.Fatal(err)
	}
	listener, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()
	log.Printf("in-memory counter RPC listening on %s", listener.Addr())
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Fatal(err)
		}
		go server.ServeConn(conn)
	}
}
