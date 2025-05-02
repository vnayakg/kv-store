package server

import (
	"bufio"
	"fmt"
	"kv-store/parser"
	"kv-store/store"
	"log"
	"net"
	"strconv"
	"strings"
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

		if command == "MULTI" {
			handleMulti(writer, store)
			continue
		} else if command == "EXEC" {
			handleExec(writer, store)
			continue
		} else if command == "DISCARD" {
			handleDiscard(writer, store)
			continue
		}

		if store.InTransaction() {
			validationErr := validateCommand(command, args)
			if validationErr != nil {
				store.ReportTransactionError()
				writeResponse(writer, validationErr.Error())
				continue
			}
			err := store.QueueCommand(command, args)
			if err != nil {
				writeResponse(writer, err.Error())
				continue
			}
			writeResponse(writer, "QUEUED")
			continue
		}

		result, err := executeCommand(store, command, args)
		if err != nil {
			writeResponse(writer, err.Error())
			continue
		}

		writeResponse(writer, fmt.Sprint(result))
	}
}

func writeResponse(writer *bufio.Writer, input string) {
	_, err := writer.WriteString(input + "\n")
	if err != nil {
		log.Printf("Error writing response: %v", err)
	}
	writer.Flush()
}

func handleMulti(writer *bufio.Writer, store *store.Store) {
	err := store.StartTransaction()
	if err != nil {
		writeResponse(writer, err.Error())
		return
	}
	writeResponse(writer, "OK")
}

func handleExec(writer *bufio.Writer, store *store.Store) {
	hasError := store.HasTransactionError()
	if hasError {
		writeResponse(writer, "discarding transaction due to above errors")
		return
	}
	results, err := store.ExecuteTransaction()
	if err != nil {
		writeResponse(writer, err.Error())
		return
	}

	var formattedResults []string
	for i, result := range results {
		formattedResults = append(formattedResults, fmt.Sprintf("%d) %s", i+1, result))
	}
	writeResponse(writer, strings.Join(formattedResults, "\n"))
}

func handleDiscard(writer *bufio.Writer, store *store.Store) {
	err := store.DiscardTransaction()
	if err != nil {
		writeResponse(writer, err.Error())
		return
	}
	writeResponse(writer, "OK")
}

func executeCommand(store *store.Store, command string, args []string) (interface{}, error) {
	err := validateCommand(command, args)
	if err != nil {
		return nil, err
	}

	switch command {
	case "SET":
		store.Set(args[0], args[1])
		return "OK", nil

	case "GET":
		value, ok := store.Get(args[0])
		if !ok {
			return nil, nil
		}
		return value, nil

	case "DEL":
		return store.Del(args[0]), nil

	case "INCR":
		return store.Incr(args[0])

	case "INCRBY":
		increment, _ := strconv.ParseInt(args[1], 10, 64)
		return store.IncrBy(args[0], increment)

	default:
		return nil, fmt.Errorf("ERR unknown command")
	}
}

func validateCommand(command string, args []string) error {
	switch command {
	case "SET":
		if len(args) != 2 {
			return fmt.Errorf("wrong number of arguments for SET command")
		}
		return nil

	case "GET":
		if len(args) != 1 {
			return fmt.Errorf("wrong number of arguments for GET command")
		}
		return nil

	case "DEL":
		if len(args) != 1 {
			return fmt.Errorf("wrong number of arguments for DEL command")
		}
		return nil

	case "INCR":
		if len(args) != 1 {
			return fmt.Errorf("wrong number of arguments for INCR command")
		}
		return nil

	case "INCRBY":
		if len(args) != 2 {
			return fmt.Errorf("wrong number of arguments for INCRBY command")
		}

		_, err := strconv.ParseInt(args[1], 10, 64)
		if err != nil {
			return fmt.Errorf("increment must be an integer")
		}
		return nil

	default:
		return fmt.Errorf("ERR unknown command")
	}
}
