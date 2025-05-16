package store

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"sync"
)

var (
	ErrIntOverflow             = errors.New("err increment or decrement would overflow")
	ErrNoTransactionInProgress = errors.New("err no transaction in progress")
	ErrTransactionInProgress   = errors.New("err transaction already in progress")
	ErrNotInteger              = errors.New("err value is not an integer or out of range")
	ErrUnknownCommand          = func(cmdName string) error { return fmt.Errorf("err unknown command: %s", cmdName) }
	ErrSelectInMulti           = errors.New("err SELECT command cannot be used in a transaction")
	ErrSelectInTransaction     = errors.New("err SELECT is not allowed in transactions")
)

type Storage interface {
	Set(dbIndex int, key, value string)
	Get(dbIndex int, key string) (string, bool)
	Del(dbIndex int, key string) int
	IncrBy(dbIndex int, key string, increment int64) (int64, error)
	Compact(dbIndex int) string
	numDatabases() int
}

type Store struct {
	storage          Storage
	transactions     map[string]*Transaction
	transactionMutex sync.Mutex
	clientDBIndices  map[string]int
	clientMutex      sync.RWMutex
}

type Transaction struct {
	commands       []Command
	originalValues map[string]*string
	hasErrors      bool
	dbIndex        int
}

type Command struct {
	name string
	args []string
}

func CreateNewStore(storage Storage) *Store {
	return &Store{
		storage:         storage,
		transactions:    make(map[string]*Transaction),
		clientDBIndices: make(map[string]int),
	}
}

func (s *Store) GetDatabasesCount() int {
	return s.storage.numDatabases()
}

func (s *Store) SetClientDBIndex(clientId string, dbIndex int) {
	s.clientMutex.Lock()
	defer s.clientMutex.Unlock()
	s.clientDBIndices[clientId] = dbIndex
}

func (s *Store) GetClientDBIndex(clientId string) int {
	s.clientMutex.RLock()
	defer s.clientMutex.RUnlock()
	dbIndex, exists := s.clientDBIndices[clientId]
	if !exists {
		return 0
	}
	return dbIndex
}

func (s *Store) RemoveClient(clientId string) {
	s.clientMutex.Lock()
	defer s.clientMutex.Unlock()
	delete(s.clientDBIndices, clientId)
}

func (s *Store) Set(dbIndex int, key, value string) {
	s.storage.Set(dbIndex, key, value)
}

func (s *Store) Get(dbIndex int, key string) (string, bool) {
	return s.storage.Get(dbIndex, key)
}

func (s *Store) Del(dbIndex int, key string) int {
	return s.storage.Del(dbIndex, key)
}

func (s *Store) Incr(dbIndex int, key string) (int64, error) {
	return s.storage.IncrBy(dbIndex, key, 1)
}

func (s *Store) IncrBy(dbIndex int, key string, increment int64) (int64, error) {
	return s.storage.IncrBy(dbIndex, key, increment)
}

func (s *Store) Compact(dbIndex int) string {
	return s.storage.Compact(dbIndex)
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

	if s.InTransaction(transactionId) {
		return ErrTransactionInProgress
	}

	s.transactions[transactionId] = &Transaction{
		commands:       make([]Command, 0),
		originalValues: make(map[string]*string),
		dbIndex:        s.GetClientDBIndex(transactionId),
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

	if !s.InTransaction(transactionId) {
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
	if transaction.hasErrors {
		return nil, fmt.Errorf("err Transaction discarded because of previous errors")
	}

	commands := make([]Command, len(transaction.commands))
	copy(commands, transaction.commands)
	dbIndex := transaction.dbIndex
	s.transactionMutex.Unlock()

	results := make([]string, 0, len(commands))

	for _, cmd := range commands {
		var result string
		var err error

		switch cmd.name {
		case "SET":
			s.saveOriginalValue(transaction, cmd.args[0])
			s.Set(dbIndex, cmd.args[0], cmd.args[1])
			result = "OK"

		case "GET":
			val, ok := s.Get(dbIndex, cmd.args[0])
			if !ok {
				result = "nil"
			} else {
				result = val
			}

		case "DEL":
			s.saveOriginalValue(transaction, cmd.args[0])
			result = strconv.FormatInt(int64(s.Del(dbIndex, cmd.args[0])), 10)

		case "INCR":
			s.saveOriginalValue(transaction, cmd.args[0])

			var intResult int64
			intResult, err = s.Incr(dbIndex, cmd.args[0])
			if err != nil {
				s.rollbackSelective(transactionId, transaction.originalValues, dbIndex)
				return nil, err
			}
			result = strconv.FormatInt(int64(intResult), 10)

		case "INCRBY":
			var increment int64
			increment, err = strconv.ParseInt(cmd.args[1], 10, 64)
			if err != nil {
				s.rollbackSelective(transactionId, transaction.originalValues, dbIndex)
				return nil, ErrNotInteger
			}

			s.saveOriginalValue(transaction, cmd.args[0])
			var intResult int64
			intResult, err = s.IncrBy(dbIndex, cmd.args[0], increment)
			if err != nil {
				s.rollbackSelective(transactionId, transaction.originalValues, dbIndex)
				return nil, err
			}
			result = strconv.FormatInt(int64(intResult), 10)
		case "COMPACT":
			result = s.Compact(dbIndex)
		case "SELECT":
			s.rollbackSelective(transactionId, transaction.originalValues, dbIndex)
			return nil, ErrSelectInTransaction
		default:
			s.rollbackSelective(transactionId, transaction.originalValues, dbIndex)
			return nil, ErrUnknownCommand(cmd.name)
		}

		results = append(results, result)
	}

	s.transactions[transactionId] = nil
	return results, nil
}

func (s *Store) saveOriginalValue(transaction *Transaction, key string) {
	if _, exists := transaction.originalValues[key]; !exists {
		value, exists := s.storage.Get(transaction.dbIndex, key)
		if exists {
			valueCopy := value
			transaction.originalValues[key] = &valueCopy
		} else {
			transaction.originalValues[key] = nil
		}
	}
}

func (s *Store) rollbackSelective(transactionId string, originalValues map[string]*string, dbIndex int) {
	for key, originalValuePtr := range originalValues {
		if originalValuePtr == nil {
			s.Del(dbIndex, key)
		} else {
			s.storage.Set(dbIndex, key, *originalValuePtr)
		}
	}

	s.transactionMutex.Lock()
	delete(s.transactions, transactionId)
	s.transactionMutex.Unlock()
}

func (s *Store) ReportTransactionError(transactionId string) {
	s.transactionMutex.Lock()
	defer s.transactionMutex.Unlock()
	if transaction, exists := s.transactions[transactionId]; exists {
		transaction.hasErrors = true
	}
}
