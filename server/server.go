package server

import (
	"bufio"
	"fmt"
	"log"
	"net"
)

func Start(address string) error {
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

		writer := bufio.NewWriter(connection)
		writer.WriteString(fmt.Sprintf("hello %v", connection.RemoteAddr()))
		writer.Flush()
	}
}
