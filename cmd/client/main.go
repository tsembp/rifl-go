package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	riflrpc "github.com/tsembp/rifl-go"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:9000", "server TCP address")
	delta := flag.Int64("delta", 10, "amount to add")
	flag.Parse()
	client, err := riflrpc.NewClient(*addr, 500*time.Millisecond, 25*time.Millisecond)
	if err != nil {
		log.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req := client.NewRequest(*delta)
	first, err := client.Add(ctx, req)
	if err != nil {
		log.Fatal(err)
	}
	_, staleErr := client.Add(ctx, req)
	if staleErr == nil {
		log.Fatal("acknowledged request was unexpectedly accepted")
	}
	fmt.Printf("Request %s/%d: result=%d, acknowledged retry=%v\n",
		req.ID.ClientID, req.ID.Sequence, first.Value, staleErr)
}
