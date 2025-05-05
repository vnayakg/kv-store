package store

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
)

var (
	ErrIntOverflow             = errors.New("err increment or decrement would overflow")
	ErrNoTransactionInProgress = errors.New("err no transaction in progress")
	ErrTransactionInProgress   = errors.New("err transaction already in progress")
	ErrNotInteger              = errors.New("err value is not an integer or out of range")
	ErrUnknownCommand          = func(cmdName string) error { return fmt.Errorf("err unknown command: %s", cmdName) }
)

type Store struct {
	data             map[string]string
	dataMutex        sync.RWMutex
	transactions     map[string]*Transaction
	transactionMutex sync.Mutex
}

type Transaction struct {
	commands       []Command
	originalValues map[string]*string
	hasErrors      bool
}

type Command struct {
	name string
	args []string
}

func CreateNewStore() *Store {
	return &Store{
		data:         make(map[string]string),
		transactions: make(map[string]*Transaction)}
}

func (s *Store) Set(key, value string) {
	s.dataMutex.Lock()
	defer s.dataMutex.Unlock()
	s.data[key] = value
}

func (s *Store) Get(key string) (string, bool) {
	s.dataMutex.RLock()
	defer s.dataMutex.RUnlock()
	value, ok := s.data[key]
	return value, ok
}

func (s *Store) Del(key string) int {
	_, ok := s.data[key]
	if !ok {
		return 0
	}
	delete(s.data, key)
	return 1
}

func (s *Store) Incr(key string) (int64, error) {
	return s.IncrBy(key, 1)
}

func (s *Store) IncrBy(key string, increment int64) (int64, error) {
	s.dataMutex.Lock()
	defer s.dataMutex.Unlock()

	value, ok := s.data[key]

	var currentValue int64 = 0
	var err error

	if ok {
		currentValue, err = strconv.ParseInt(value, 10, 64)
		if err != nil {
			return 0, ErrNotInteger
		}
	}
	if err := checkIntegerOverflow(currentValue, increment); err != nil {
		return 0, err
	}
	currentValue += increment
	s.data[key] = strconv.FormatInt(currentValue, 10)

	return currentValue, nil
}

func (s *Store) Compact() string {
	s.dataMutex.RLock()
	defer s.dataMutex.RUnlock()

	var result []string
	for k, v := range s.data {
		result = append(result, fmt.Sprintf("SET %s %s", k, v))
	}
	return strings.Join(result, "\n")
}

func checkIntegerOverflow(currentValue, increment int64) error {
	if increment > 0 && currentValue > math.MaxInt64-increment {
		return ErrIntOverflow
	}
	if increment < 0 && currentValue < math.MinInt64-increment {
		return ErrIntOverflow
	}
	return nil
}

func (s *Store) StartTransaction(transactionId string) error {
	s.transactionMutex.Lock()
	defer s.transactionMutex.Unlock()

	if _, exists := s.transactions[transactionId]; exists {
		return ErrTransactionInProgress
	}

	s.transactions[transactionId] = &Transaction{
		commands:       make([]Command, 0),
		originalValues: make(map[string]*string),
	}
	return nil
}

func (s *Store) InTransaction(transactionId string) bool {
	_, exists := s.transactions[transactionId]
	return exists
}

func (s *Store) QueueCommand(transactionId, name string, args []string) error {
	s.transactionMutex.Lock()
	defer s.transactionMutex.Unlock()

	transaction, exists := s.transactions[transactionId]
	if !exists {
		return ErrNoTransactionInProgress
	}
	transaction.commands = append(transaction.commands,
		Command{
			name: name,
			args: args,
		})
	return nil
}

func (s *Store) DiscardTransaction(transactionId string) error {
	s.transactionMutex.Lock()
	defer s.transactionMutex.Unlock()

	if _, exists := s.transactions[transactionId]; !exists {
		return ErrNoTransactionInProgress
	}

	delete(s.transactions, transactionId)
	return nil
}

func (s *Store) ExecuteTransaction(transactionId string) ([]string, error) {
	s.transactionMutex.Lock()
	transaction, exists := s.transactions[transactionId]
	if !exists {
		s.transactionMutex.Unlock()
		return nil, ErrNoTransactionInProgress
	}

	commands := make([]Command, len(transaction.commands))
	copy(commands, transaction.commands)
	s.transactionMutex.Unlock()

	results := make([]string, 0, len(commands))

	for _, cmd := range commands {
		var result string
		var err error

		switch cmd.name {
		case "SET":
			s.saveOriginalValue(transaction, cmd.args[0])
			s.Set(cmd.args[0], cmd.args[1])
			result = "OK"

		case "GET":
			val, ok := s.Get(cmd.args[0])
			if !ok {
				result = "nil"
			} else {
				result = val
			}

		case "DEL":
			s.saveOriginalValue(transaction, cmd.args[0])
			result = strconv.FormatInt(int64(s.Del(cmd.args[0])), 10)

		case "INCR":
			s.saveOriginalValue(transaction, cmd.args[0])

			var intResult int64
			intResult, err = s.Incr(cmd.args[0])
			if err != nil {
				s.rollbackSelective(transactionId, transaction.originalValues)
				return nil, err
			}
			result = strconv.FormatInt(int64(intResult), 10)

		case "INCRBY":
			var increment int64
			increment, err = strconv.ParseInt(cmd.args[1], 10, 64)
			if err != nil {
				s.rollbackSelective(transactionId, transaction.originalValues)
				return nil, ErrNotInteger
			}

			s.saveOriginalValue(transaction, cmd.args[0])
			var intResult int64
			intResult, err = s.IncrBy(cmd.args[0], increment)
			if err != nil {
				s.rollbackSelective(transactionId, transaction.originalValues)
				return nil, err
			}
			result = strconv.FormatInt(int64(intResult), 10)
		case "COMPACT":
			result = s.Compact()

		default:
			s.rollbackSelective(transactionId, transaction.originalValues)
			return nil, ErrUnknownCommand(cmd.name)
		}

		results = append(results, result)
	}

	s.transactions[transactionId] = nil
	return results, nil
}

func (s *Store) saveOriginalValue(transaction *Transaction, key string) {
	if _, exists := transaction.originalValues[key]; !exists {
		s.dataMutex.Lock()
		value, exists := s.data[key]
		s.dataMutex.Unlock()

		if exists {
			valueCopy := value
			transaction.originalValues[key] = &valueCopy
		} else {
			transaction.originalValues[key] = nil
		}
	}
}

func (s *Store) rollbackSelective(transactionId string, originalValues map[string]*string) {
	s.dataMutex.Lock()
	defer s.dataMutex.Unlock()

	for key, originalValuePtr := range originalValues {
		if originalValuePtr == nil {
			delete(s.data, key)
		} else {
			s.data[key] = *originalValuePtr
		}
	}

	s.transactionMutex.Lock()
	delete(s.transactions, transactionId)
	s.transactionMutex.Unlock()
}

func (s *Store) ReportTransactionError(transactionId string) {
	s.transactions[transactionId].hasErrors = true
}

func (s *Store) HasTransactionError(transactionId string) bool {
	return s.transactions[transactionId].hasErrors
}
