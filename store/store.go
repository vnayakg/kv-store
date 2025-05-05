package store

import (
	"fmt"
	"math"
	"strconv"
	"sync"
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
			return 0, fmt.Errorf("value is not an integer or out of range")
		}
	}
	if err := checkIntegerOverflow(currentValue, increment); err != nil {
		return 0, err
	}
	currentValue += increment
	s.data[key] = strconv.FormatInt(currentValue, 10)

	return currentValue, nil
}

func checkIntegerOverflow(currentValue, increment int64) error {
	if increment > 0 && currentValue > math.MaxInt64-increment {
		return fmt.Errorf("increment or decrement would overflow")
	}
	if increment < 0 && currentValue < math.MinInt64-increment {
		return fmt.Errorf("increment or decrement would overflow")
	}
	return nil
}

func (s *Store) StartTransaction(transactionId string) error {
	s.transactionMutex.Lock()
	defer s.transactionMutex.Unlock()

	if _, exists := s.transactions[transactionId]; exists {
		return fmt.Errorf("transaction already in progress")
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
		return fmt.Errorf("no transaction in progress")
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
		return fmt.Errorf("no transaction in progress")
	}

	delete(s.transactions, transactionId)
	return nil
}

func (s *Store) ExecuteTransaction(transactionId string) ([]string, error) {
	s.transactionMutex.Lock()
	transaction, exists := s.transactions[transactionId]
	if !exists {
		s.transactionMutex.Unlock()
		return nil, fmt.Errorf("no transaction in progress")
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
				return nil, fmt.Errorf("increment must be an integer")
			}

			s.saveOriginalValue(transaction, cmd.args[0])
			var intResult int64
			intResult, err = s.IncrBy(cmd.args[0], increment)
			if err != nil {
				s.rollbackSelective(transactionId, transaction.originalValues)
				return nil, err
			}
			result = strconv.FormatInt(int64(intResult), 10)

		default:
			s.rollbackSelective(transactionId, transaction.originalValues)
			return nil, fmt.Errorf("unknown command: %s", cmd.name)
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
