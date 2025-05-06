package main

import (
	"flag"
	"kv-store/server"
	"kv-store/store"
	"log"
)

const defaultNumDatabases = 16

func main() {
	listenAddress := flag.String("address", ":8000", "Address and port to listen on (e.g. :8000, 127.0.0.1:8000)")
	flag.Parse()

	inMemoryStorage := store.NewMemoryStorage(defaultNumDatabases)
	store := store.CreateNewStore(inMemoryStorage)

	err := server.Start(*listenAddress, store)
	if err != nil {
		log.Fatalf("server error: %v", err)
	}
}
