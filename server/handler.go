package server

import (
	"bufio"
	"fmt"
	"kv-store/parser"
	"kv-store/store"
	"log"
	"net"
	"strconv"
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
			writeResponse(writer, parseErr.Error())
			continue
		}

		switch command {
		case "SET":
			if len(args) != 2 {
				writeResponse(writer, "wrong number of arguments for SET command")
				continue
			}
			store.Set(args[0], args[1])
			writeResponse(writer, "OK")

		case "GET":
			if len(args) != 1 {
				writeResponse(writer, "wrong number of arguments for GET command")
				continue
			}
			value, ok := store.Get(args[0])
			if !ok {
				writeResponse(writer, "nil")
			}
			writeResponse(writer, value)

		case "DEL":
			if len(args) != 1 {
				writeResponse(writer, "wrong number of arguments for DEL command")
				continue
			}
			result := store.Del(args[0])
			writeResponse(writer, fmt.Sprint(result))
		
		case "INCR":
			if len(args) != 1 {
				writeResponse(writer, "wrong number of arguments for INCR command")
				continue
			}
			result, err := store.Incr(args[0])
			if err != nil {
				writeResponse(writer, err.Error())
				continue
			}
			writeResponse(writer, fmt.Sprint(result))
		case "INCRBY":
			if len(args) != 2 {
				writeResponse(writer, "wrong number of arguments for INCRBY command")
				continue
			}

			increment, err := strconv.ParseInt(args[1], 10, 64)
			if err != nil {
				writeResponse(writer, "increment must be an integer")
				continue
			}

			result, err := store.IncrBy(args[0], increment)
			if err != nil {
				writeResponse(writer, err.Error())
				continue
			}
			writeResponse(writer, fmt.Sprint(result))
		default:
			writeResponse(writer, "command not supported")
		}
	}
}

func writeResponse(writer *bufio.Writer, input string) {
	_, err := writer.WriteString(input + "\n")
	if err != nil {
		log.Printf("Error writing response: %v", err)
	}
	writer.Flush()
}
