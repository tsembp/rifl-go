// Command shmserver serves RIFL counter requests through a shared-memory region
// (CXL-inspired model). Start several on the same region: the first owns it,
// the others wait as standbys and take over when the owner process dies.
//
//	-store shm    RIFL state lives in the region (configuration C)
//	-store bbolt  slots in front of the durable bbolt counter (configuration B)
package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	riflrpc "github.com/tsembp/rifl-go"
	"github.com/tsembp/rifl-go/shm"
)

func main() {
	path := flag.String("region", "/dev/shm/rifl-region", "shared-memory region file")
	create := flag.Bool("create", false, "create the region if it does not exist")
	slots := flag.Int("slots", 64, "request/reply slots (with -create)")
	clients := flag.Int("clients", 4096, "logical client capacity (with -create)")
	records := flag.Int("records", 64, "unacknowledged completion records per client (with -create)")
	store := flag.String("store", "shm", "RIFL state: shm (in the region) or bbolt (durable file)")
	dbPath := flag.String("db", "rifl.db", "bbolt database for -store bbolt")
	flag.Parse()
	log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.LUTC)

	cfg := shm.Config{Slots: *slots, Clients: *clients, Records: *records}
	region, err := shm.Open(*path)
	if errors.Is(err, os.ErrNotExist) && *create {
		region, err = shm.Create(*path, cfg)
	}
	if err != nil {
		log.Fatal(err)
	}
	defer region.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	log.Printf("pid %d waiting for region %s", os.Getpid(), *path)
	epoch, err := region.Acquire(ctx)
	if err != nil {
		log.Fatal(err)
	}
	acquired := time.Now()

	var handler shm.Handler
	switch *store {
	case "shm":
		state := shm.NewState(region, epoch)
		state.Recover()
		handler = state
	case "bbolt":
		counter, err := riflrpc.OpenCounter(*dbPath)
		if err != nil {
			log.Fatal(err)
		}
		defer counter.Close()
		handler = shm.CounterHandler{Counter: counter}
	default:
		log.Fatalf("unknown -store %q: use shm or bbolt", *store)
	}
	log.Printf("owner epoch %d (store %s, %+v): acquired at %s, ready after %v",
		epoch, *store, region.Config(), acquired.UTC().Format(time.RFC3339Nano), time.Since(acquired))
	shm.Serve(ctx, region, handler)
	log.Printf("owner epoch %d stopping", epoch)
}
