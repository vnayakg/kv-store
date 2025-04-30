package server

import (
	"kv-store/store"
	"log"
	"net"
)

func Start(address string, store *store.Store) error {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		log.Printf("Failed to bind to address %s: %v", address, err)
		return err
	}
	log.Printf("Server listening on %s", address)

	for {
		connection, err := listener.Accept()
		if err != nil {
			log.Printf("Failed to accept connection: %v", err)
			continue
		}

		go handleConnection(connection, store)
	}
}
