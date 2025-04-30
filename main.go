package main

import (
	"flag"
	"kv-store/server"
	"log"
)

func main() {
	listenAddress := flag.String("address", ":8000", "Address and port to listen on (e.g. :8000, 127.0.0.1:8000)")
	flag.Parse()

	err := server.Start(*listenAddress)
	if err != nil {
		log.Fatalf("server error: %v", err)
	}
}
