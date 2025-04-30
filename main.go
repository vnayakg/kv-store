package main

import (
	"flag"
	"kv-store/server"
	"kv-store/store"
	"log"
)

func main() {
	listenAddress := flag.String("address", ":8000", "Address and port to listen on (e.g. :8000, 127.0.0.1:8000)")
	flag.Parse()
	store := store.CreateNewStore()

	err := server.Start(*listenAddress, store)
	if err != nil {
		log.Fatalf("server error: %v", err)
	}
}
