package server

import (
	"bufio"
	"errors"
	"fmt"
	"kv-store/parser"
	"kv-store/store"
	"log"
	"net"
	"strconv"
	"strings"
)

var (
	ErrNotInteger        = errors.New("err value is not an integer or out of range")
	ErrWrongNumberOfArgs = func(commandName string) error {
		return fmt.Errorf("wrong number of arguments for %v command", commandName)
	}
	ErrUnknownCommand    = func(commandName string) error { return fmt.Errorf("err unknown command: %s", commandName) }
	ErrDbIndexOutOfRange = errors.New("err DB index is out of range")
)

var (
	ResQueued             = "QUEUED"
	ResOk                 = "OK"
	ResDiscardTransaction = "discarding transaction due to above errors"
)

func handleConnection(conn net.Conn, store *store.Store) {
	clientId := fmt.Sprintf("%s-%p", conn.RemoteAddr(), conn)
	log.Printf("Accepted connection from %s (ID: %s)", conn.RemoteAddr(), clientId)

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	store.SetClientDBIndex(clientId, 0)

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err.Error() == "EOF" {
				log.Printf("Connection closed for client %s", conn.RemoteAddr())

				if store.InTransaction(clientId) {
					store.DiscardTransaction(clientId)
					log.Printf("Discarded transaction for client %s", conn.RemoteAddr())
				}
				return
			}
			log.Printf("Error reading from %s: %v", conn.RemoteAddr(), err)
		}

		command, args, parseErr := parser.ParseCommandLine(line)
		if parseErr != nil {
			writeResponse(writer, parseErr.Error())
			continue
		}

		if command == "MULTI" {
			handleMulti(clientId, writer, store)
			continue
		} else if command == "EXEC" {
			handleExec(clientId, writer, store)
			continue
		} else if command == "DISCARD" {
			handleDiscard(clientId, writer, store)
			continue
		}

		if store.InTransaction(clientId) {
			validationErr := validateCommand(command, args)
			if validationErr != nil {
				store.ReportTransactionError(clientId)
				writeResponse(writer, validationErr.Error())
				continue
			}
			err := store.QueueCommand(clientId, command, args)
			if err != nil {
				writeResponse(writer, err.Error())
				continue
			}
			writeResponse(writer, ResQueued)
			continue
		}

		result, err := executeCommand(store, clientId, command, args)
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

func handleMulti(transactionId string, writer *bufio.Writer, store *store.Store) {
	err := store.StartTransaction(transactionId)
	if err != nil {
		writeResponse(writer, err.Error())
		return
	}
	writeResponse(writer, ResOk)
}

func handleExec(transactionId string, writer *bufio.Writer, store *store.Store) {
	results, err := store.ExecuteTransaction(transactionId)
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

func handleDiscard(transactionId string, writer *bufio.Writer, store *store.Store) {
	err := store.DiscardTransaction(transactionId)
	if err != nil {
		writeResponse(writer, err.Error())
		return
	}
	writeResponse(writer, ResOk)
}

func executeCommand(store *store.Store, clientId string, command string, args []string) (any, error) {
	err := validateCommand(command, args)
	if err != nil {
		return nil, err
	}
	dbIndex := store.GetClientDBIndex(clientId)
	switch command {
	case "SET":
		store.Set(dbIndex, args[0], args[1])
		return ResOk, nil

	case "GET":
		value, ok := store.Get(dbIndex, args[0])
		if !ok {
			return nil, nil
		}
		return value, nil

	case "DEL":
		return store.Del(dbIndex, args[0]), nil

	case "INCR":
		return store.Incr(dbIndex, args[0])

	case "INCRBY":
		increment, _ := strconv.ParseInt(args[1], 10, 64)
		return store.IncrBy(dbIndex, args[0], increment)
	case "COMPACT":
		return store.Compact(dbIndex), nil
	case "SELECT":
		dbIndex, err := strconv.ParseInt(args[0], 10, 64)
		if err != nil {
			return nil, ErrNotInteger
		}
		if dbIndex < 0 || dbIndex >= int64(store.GetDatabasesCount()) {
			return nil, errors.New("err DB index is out of range")
		}
		store.SetClientDBIndex(clientId, int(dbIndex))
		return ResOk, nil
	default:
		return nil, ErrUnknownCommand(command)
	}
}

func validateCommand(command string, args []string) error {
	switch command {
	case "SET":
		if len(args) != 2 {
			return ErrWrongNumberOfArgs("SET")
		}
		return nil

	case "GET":
		if len(args) != 1 {
			return ErrWrongNumberOfArgs("GET")
		}
		return nil

	case "DEL":
		if len(args) != 1 {
			return ErrWrongNumberOfArgs("DEL")
		}
		return nil

	case "INCR":
		if len(args) != 1 {
			return ErrWrongNumberOfArgs("INCR")
		}
		return nil

	case "INCRBY":
		if len(args) != 2 {
			return ErrWrongNumberOfArgs("INCRBY")
		}

		_, err := strconv.ParseInt(args[1], 10, 64)
		if err != nil {
			return ErrNotInteger
		}
		return nil
	case "COMPACT":
		if len(args) != 0 {
			return ErrWrongNumberOfArgs("COMPACT")
		}
		return nil
	case "SELECT":
		if len(args) != 1 {
			return ErrWrongNumberOfArgs("SELECT")
		}
		_, err := strconv.ParseInt(args[0], 10, 64)
		if err != nil {
			return ErrNotInteger
		}
		return nil
	default:
		return ErrUnknownCommand(command)
	}
}
