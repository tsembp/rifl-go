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
	dbPath := flag.String("db", "rifl.db", "durable counter database path")
	flag.Parse()
	counter, err := riflrpc.OpenCounter(*dbPath)
	if err != nil {
		log.Fatal(err)
	}
	defer counter.Close()
	server := rpc.NewServer()
	if err := server.RegisterName("Counter", counter); err != nil {
		log.Fatal(err)
	}
	listener, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()
	log.Printf("durable counter RPC listening on %s (database %s)", listener.Addr(), *dbPath)
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Fatal(err)
		}
		go server.ServeConn(conn)
	}
}
