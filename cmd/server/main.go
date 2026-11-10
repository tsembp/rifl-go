package main

import (
	"flag"
	"log"
	"net"
	"net/rpc"
	"strings"

	riflrpc "github.com/tsembp/rifl-go"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:9000", "TCP listen address")
	dbPath := flag.String("db", "rifl.db", "durable counter database path")
	mode := flag.String("mode", "protected", "service mode: protected or baseline")
	flag.Parse()
	var service interface{}
	var closeService func() error
	switch strings.ToLower(*mode) {
	case "protected":
		counter, err := riflrpc.OpenCounter(*dbPath)
		if err != nil {
			log.Fatal(err)
		}
		service, closeService = counter, counter.Close
	case "baseline":
		counter, err := riflrpc.OpenUnprotectedCounter(*dbPath)
		if err != nil {
			log.Fatal(err)
		}
		service, closeService = counter, counter.Close
	default:
		log.Fatalf("unknown mode %q: use protected or baseline", *mode)
	}
	defer closeService()
	server := rpc.NewServer()
	if err := server.RegisterName("Counter", service); err != nil {
		log.Fatal(err)
	}
	listener, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()
	log.Printf("%s durable counter RPC listening on %s (database %s)", *mode, listener.Addr(), *dbPath)
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Fatal(err)
		}
		go server.ServeConn(conn)
	}
}
