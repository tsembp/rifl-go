package main

import (
	"flag"
	"log"
	"net"
	"net/rpc"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"strings"
	"syscall"

	riflrpc "github.com/tsembp/rifl-go"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:9000", "TCP listen address")
	dbPath := flag.String("db", "rifl.db", "durable counter database path")
	mode := flag.String("mode", "protected", "service mode: protected or baseline")
	cpuProfile := flag.String("cpuprofile", "", "diagnostics only: write a CPU profile here until SIGINT/SIGTERM")
	memProfile := flag.String("memprofile", "", "diagnostics only: write an allocation profile here on SIGINT/SIGTERM")
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
	if *cpuProfile != "" || *memProfile != "" {
		stopProfiling := startProfiling(*cpuProfile, *memProfile)
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
		go func() {
			<-signals
			stopProfiling()
			closeService()
			os.Exit(0)
		}()
	}
	log.Printf("%s durable counter RPC listening on %s (database %s)", *mode, listener.Addr(), *dbPath)
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Fatal(err)
		}
		go server.ServeConn(conn)
	}
}

// startProfiling is opt-in. Uninstrumented runs keep the default shutdown path.
func startProfiling(cpuPath, memPath string) func() {
	var cpuFile *os.File
	if cpuPath != "" {
		file, err := os.Create(cpuPath)
		if err != nil {
			log.Fatal(err)
		}
		if err := pprof.StartCPUProfile(file); err != nil {
			log.Fatal(err)
		}
		cpuFile = file
	}
	return func() {
		if cpuFile != nil {
			pprof.StopCPUProfile()
			cpuFile.Close()
		}
		if memPath != "" {
			file, err := os.Create(memPath)
			if err != nil {
				log.Print(err)
				return
			}
			defer file.Close()
			runtime.GC()
			if err := pprof.Lookup("allocs").WriteTo(file, 0); err != nil {
				log.Print(err)
			}
		}
	}
}
