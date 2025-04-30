package server

import (
	"bufio"
	"kv-store/parser"
	"kv-store/store"
	"log"
	"net"
)

func handleConnection(conn net.Conn, store *store.Store) {
	log.Printf("Accepted connection from %s", conn.RemoteAddr())

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			log.Printf("Error reading from %s: %v", conn.RemoteAddr(), err)
			return
		}

		command, args, parseErr := parser.ParseCommandLine(line)
		if parseErr != nil {
			_, err := writer.WriteString(parseErr.Error() + "\n")
			if err != nil {
				log.Printf("Error writing response: %v", err)
			}
			writer.Flush()
			continue
		}

		switch command {
		case "SET":
			if len(args) != 2 {
				_, err := writer.WriteString("wrong number of arguments for SET command\n")
				if err != nil {
					log.Printf("Error writing response: %v", err)
				}
				writer.Flush()
				continue
			}
			store.Set(args[0], args[1])
			_, err := writer.WriteString("OK\n")
			if err != nil {
				log.Printf("Error writing response: %v", err)
			}
			writer.Flush()

		case "GET":
			if len(args) != 1 {
				_, err := writer.WriteString("wrong number of arguments for GET command\n")
				if err != nil {
					log.Printf("Error writing response: %v", err)
				}
				writer.Flush()
				continue
			}
			value, ok := store.Get(args[0])
			if !ok {
				_, err := writer.WriteString("nil\n")
				if err != nil {
					log.Printf("Error writing response: %v", err)
				}
				writer.Flush()
			}
			_, err := writer.WriteString(value + "\n")
			if err != nil {
				log.Printf("Error writing response: %v", err)
			}
			writer.Flush()

		default:
			_, err := writer.WriteString("command not supported\n")
			if err != nil {
				log.Printf("Error writing response: %v", err)
			}
			writer.Flush()
		}
	}
}
