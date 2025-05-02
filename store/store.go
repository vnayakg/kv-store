package store

import (
	"fmt"
	"math"
	"strconv"
)

type Store struct {
	data        map[string]string
	transaction *Transaction
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
	return &Store{data: make(map[string]string)}
}

func (s *Store) Set(key, value string) {
	s.data[key] = value
}

func (s *Store) Get(key string) (string, bool) {
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

func (s *Store) StartTransaction() error {
	if s.InTransaction() {
		return fmt.Errorf("transaction already in progress")
	}
	s.transaction = &Transaction{
		commands:       make([]Command, 0),
		originalValues: make(map[string]*string),
	}
	return nil
}

func (s *Store) InTransaction() bool {
	return s.transaction != nil
}

func (s *Store) QueueCommand(name string, args []string) error {
	if !s.InTransaction() {
		return fmt.Errorf("no transaction in progress")
	}
	s.transaction.commands = append(s.transaction.commands,
		Command{
			name: name,
			args: args,
		})
	return nil
}

func (s *Store) DiscardTransaction() error {
	if !s.InTransaction() {
		return fmt.Errorf("no transaction in progress")
	}
	s.transaction = nil
	return nil
}

func (s *Store) ExecuteTransaction() ([]string, error) {
	if !s.InTransaction() {
		return nil, fmt.Errorf("no transaction in progress")
	}

	results := make([]string, 0, len(s.transaction.commands))
	transaction := s.transaction

	for _, cmd := range s.transaction.commands {
		var result string
		var err error

		switch cmd.name {
		case "SET":
			s.saveOriginalValue(cmd.args[0])
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
			s.saveOriginalValue(cmd.args[0])
			result = strconv.FormatInt(int64(s.Del(cmd.args[0])), 10)

		case "INCR":
			s.saveOriginalValue(cmd.args[0])

			var intResult int64
			intResult, err = s.Incr(cmd.args[0])
			if err != nil {
				s.rollbackSelective(transaction.originalValues)
				return nil, err
			}
			result = strconv.FormatInt(int64(intResult), 10)

		case "INCRBY":
			var increment int64
			increment, err = strconv.ParseInt(cmd.args[1], 10, 64)
			if err != nil {
				s.rollbackSelective(transaction.originalValues)
				return nil, fmt.Errorf("increment must be an integer")
			}

			s.saveOriginalValue(cmd.args[0])
			var intResult int64
			intResult, err = s.IncrBy(cmd.args[0], increment)
			if err != nil {
				s.rollbackSelective(transaction.originalValues)
				return nil, err
			}
			result = strconv.FormatInt(int64(intResult), 10)

		default:
			s.rollbackSelective(transaction.originalValues)
			return nil, fmt.Errorf("unknown command: %s", cmd.name)
		}

		results = append(results, result)
	}

	s.transaction = nil
	return results, nil
}

func (s *Store) saveOriginalValue(key string) {
	if _, exists := s.transaction.originalValues[key]; !exists {
		value, exists := s.data[key]
		if exists {
			valueCopy := value
			s.transaction.originalValues[key] = &valueCopy
		} else {
			s.transaction.originalValues[key] = nil
		}
	}
}

func (s *Store) rollbackSelective(originalValues map[string]*string) {
	for key, originalValuePtr := range originalValues {
		if originalValuePtr == nil {
			delete(s.data, key)
		} else {
			s.data[key] = *originalValuePtr
		}
	}

	s.transaction = nil
}

func (s *Store) ReportTransactionError() {
	s.transaction.hasErrors = true
}

func (s *Store) HasTransactionError() bool {
	return s.transaction.hasErrors
}
