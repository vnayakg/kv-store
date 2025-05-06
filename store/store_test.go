package store

import (
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
)

const defaultNumDatabases = 16

func getInMemoryStore(t *testing.T) *Store {
	t.Helper()
	inMemoryStorage := NewMemoryStorage(defaultNumDatabases)
	return CreateNewStore(inMemoryStorage)
}

func TestCreateNewStore(t *testing.T) {
	store := getInMemoryStore(t)

	if store == nil {
		t.Fatal("CreateNewStore() returned nil")
	}
	if store.storage == nil {
		t.Fatalf("CreateNewStore() did not initialize data map")
	}
}

func TestSetGet(t *testing.T) {
	key := "name"
	value := "batman"
	store := getInMemoryStore(t)

	store.Set(0, key, value)
	retrievedValue, ok := store.Get(0, key)

	if !ok {
		t.Errorf("Get(%q) failed, expected key to exist", key)
	}
	if retrievedValue != value {
		t.Errorf("Get(%q) = %q; expected %q", key, retrievedValue, value)
	}
}

func TestSetGet_OverwriteValue(t *testing.T) {
	key := "name"
	value := "batman"
	valueToOverwrite := "superman"
	store := getInMemoryStore(t)

	store.Set(0, key, value)
	store.Set(0, key, valueToOverwrite)
	retrievedValue, ok := store.Get(0, key)

	if !ok {
		t.Errorf("Get(%q) failed, expected key to exist", key)
	}
	if retrievedValue != valueToOverwrite {
		t.Errorf("Get(%q) = %q; expected %q", key, retrievedValue, valueToOverwrite)
	}
}

func TestStoreGet_NotFound(t *testing.T) {
	store := getInMemoryStore(t)
	key := "non-existent"

	_, ok := store.Get(0, key)
	if ok {
		t.Errorf("Get(%q) succeeded, expected key not to exist", key)
	}
}

func TestDel(t *testing.T) {
	store := getInMemoryStore(t)
	key := "name"
	store.Set(0, key, "superman")

	result := store.Del(0, key)

	_, ok := store.Get(0, key)
	if result != 1 {
		t.Errorf("Del(%q) = %q, expected 1", key, result)
	}
	if ok {
		t.Errorf("expected: %q should be deleted, got: it is still present", key)
	}
}

func TestDel_ForNonExistentKey(t *testing.T) {
	store := getInMemoryStore(t)
	key := "surname"

	result := store.Del(0, key)

	if result != 0 {
		t.Errorf("Del(%q) = %q, expected 0", key, result)
	}
}

func TestIncr_ForExistingKey(t *testing.T) {
	store := getInMemoryStore(t)
	key := "counter"
	value := "1"
	store.Set(0, key, value)

	updatedValue, err := store.Incr(0, key)

	if err != nil {
		t.Errorf("expected to increment counter, got error: %v", err)
	}
	if updatedValue != 2 {
		t.Errorf("Incr(%q) = %q, expected 2", key, updatedValue)
	}
}

func TestIncr_ForExistingNonIntegerKey(t *testing.T) {
	store := getInMemoryStore(t)
	key := "counter"
	value := "abc"
	store.Set(0, key, value)

	updatedValue, err := store.Incr(0, key)

	expectedError := ErrNotInteger
	if err.Error() != expectedError.Error() {
		t.Errorf("expected: %q, got: %q", expectedError, err)
	}
	if updatedValue != 0 {
		t.Errorf("expected: 0, got: %q", updatedValue)
	}
}

func TestIncr_ForNonExistingKey(t *testing.T) {
	store := getInMemoryStore(t)
	key := "counter"

	updatedValue, err := store.Incr(0, key)

	if err != nil {
		t.Errorf("expected to increment counter, got error: %v", err)
	}
	if updatedValue != 1 {
		t.Errorf("Incr(%q) = %q, expected 2", key, updatedValue)
	}
}

func TestIncr_ForOverflow(t *testing.T) {
	store := getInMemoryStore(t)
	key := "counter"
	value := strconv.FormatInt(math.MaxInt64, 10)
	store.Set(0, key, value)

	updatedValue, err := store.Incr(0, key)

	expectedError := ErrIntOverflow
	if err.Error() != expectedError.Error() {
		t.Errorf("expected: %q, got: %q", expectedError, err)
	}
	if updatedValue != 0 {
		t.Errorf("expected: 0, got: %q", updatedValue)
	}
}

func TestIncrBy_ForExistingKey(t *testing.T) {
	store := getInMemoryStore(t)
	key := "counter"
	value := "1"
	store.Set(0, key, value)

	updatedValue, err := store.IncrBy(0, key, 9)

	if err != nil {
		t.Errorf("expected to increment counter, got error: %v", err)
	}
	if updatedValue != 10 {
		t.Errorf("Incr(%q) = %q, expected 2", key, updatedValue)
	}
}

func TestIncrBy_ForExistingNonIntegerKey(t *testing.T) {
	store := getInMemoryStore(t)
	key := "counter"
	value := "abc"
	store.Set(0, key, value)

	updatedValue, err := store.IncrBy(0, key, 10)

	expectedError := ErrNotInteger
	if err.Error() != expectedError.Error() {
		t.Errorf("expected: %q, got: %q", expectedError, err)
	}
	if updatedValue != 0 {
		t.Errorf("expected: 0, got: %q", updatedValue)
	}
}

func TestIncrBy_ForNonExistingKey(t *testing.T) {
	store := getInMemoryStore(t)
	key := "counter"

	updatedValue, err := store.IncrBy(0, key, 10)

	if err != nil {
		t.Errorf("expected to increment counter, got error: %v", err)
	}
	if updatedValue != 10 {
		t.Errorf("Incr(%q) = %q, expected 2", key, updatedValue)
	}
}

func TestIncrBy_ForOverflow(t *testing.T) {
	store := getInMemoryStore(t)
	key := "counter"
	value := strconv.FormatInt(math.MinInt64, 10)
	store.Set(0, key, value)

	updatedValue, err := store.IncrBy(0, key, -10)

	expectedError := ErrIntOverflow
	if err.Error() != expectedError.Error() {
		t.Errorf("expected: %q, got: %q", expectedError, err)
	}
	if updatedValue != 0 {
		t.Errorf("expected: 0, got: %q", updatedValue)
	}
}

func TestStartTransaction_NoOnGoingTransaction(t *testing.T) {
	store := getInMemoryStore(t)
	transactionId := "1"
	err := store.StartTransaction(transactionId)

	if err != nil {
		t.Errorf("expected: should start transaction, got: %v", err)
	}
}

func TestStartTransaction_OnGoingTransactionPresent(t *testing.T) {
	store := getInMemoryStore(t)
	transactionId := "1"
	store.transactions[transactionId] = &Transaction{}

	err := store.StartTransaction(transactionId)

	expectedError := ErrTransactionInProgress
	if err.Error() != expectedError.Error() {
		t.Errorf("expected: %v, got: %v", expectedError, err)
	}
}

func TestQueueCommand_OnGoingTransactionPresent(t *testing.T) {
	store := getInMemoryStore(t)
	transactionId := "1"
	store.transactions[transactionId] = &Transaction{}
	commandName := "SET"
	args := []string{"a", "2"}

	err := store.QueueCommand(transactionId, commandName, args)

	if err != nil {
		t.Errorf("expected: should queue command, got: %v", err)
	}
	expectedCommand := Command{commandName, args}
	if store.transactions[transactionId].commands[0].name != expectedCommand.name &&
		!reflect.DeepEqual(store.transactions[transactionId].commands[0].args, expectedCommand.args) {
		t.Errorf("expected: %v, got: %v", expectedCommand, store.transactions[transactionId].commands[0])
	}
}

func TestQueueCommand_NoOnGoingTransactionPresent(t *testing.T) {
	store := getInMemoryStore(t)
	transactionId := "1"
	commandName := "SET"
	args := []string{"a", "2"}

	err := store.QueueCommand(transactionId, commandName, args)

	expectedError := ErrNoTransactionInProgress
	if err.Error() != expectedError.Error() {
		t.Errorf("expected: %v, got: %v", expectedError, err)
	}
}

func TestDiscardTransaction_OnGoingTransactionPresent(t *testing.T) {
	store := getInMemoryStore(t)
	transactionId := "1"
	store.transactions[transactionId] = &Transaction{}

	err := store.DiscardTransaction(transactionId)

	if err != nil {
		t.Errorf("expected: should discard transaction, got: %v", err)
	}
	if store.transactions[transactionId] != nil {
		t.Errorf("expected: %v, got %v", nil, store.transactions[transactionId])
	}
}

func TestDiscardTransaction_NoOnGoingTransactionPresent(t *testing.T) {
	store := getInMemoryStore(t)
	transactionId := "1"

	err := store.DiscardTransaction(transactionId)

	expectedError := ErrNoTransactionInProgress
	if err.Error() != expectedError.Error() {
		t.Errorf("expected: %v, got: %v", expectedError, err)
	}
}

func TestExecuteTransaction_OnGoingTransactionPresent(t *testing.T) {
	store := getInMemoryStore(t)
	transactionId := "1"
	store.transactions[transactionId] = &Transaction{
		commands: []Command{
			{name: "GET", args: []string{"a"}},
			{name: "SET", args: []string{"a", "1"}},
			{name: "GET", args: []string{"a"}},
			{name: "DEL", args: []string{"a"}},
			{name: "INCR", args: []string{"a"}},
			{name: "INCRBY", args: []string{"a", "9"}},
		},
		originalValues: make(map[string]*string),
	}

	result, err := store.ExecuteTransaction(transactionId)

	expectedResult := []string{"nil", "OK", "1", "1", "1", "10"}
	if err != nil {
		t.Errorf("expected: should execute transaction, got: %v", err)
	}
	if !reflect.DeepEqual(expectedResult, result) {
		t.Errorf("expected: %v, got: %v", expectedResult, result)
	}
}

func TestExecuteTransaction_NoOnGoingTransactionPresent(t *testing.T) {
	store := getInMemoryStore(t)
	transactionId := "1"

	_, err := store.ExecuteTransaction(transactionId)

	expectedError := ErrNoTransactionInProgress
	if err.Error() != expectedError.Error() {
		t.Errorf("expected: %v, got: %v", expectedError, err)
	}
}

func TestExecuteTransaction_ShouldRollbackOnError(t *testing.T) {
	store := getInMemoryStore(t)
	store.Set(0, "a", "1")
	transactionId := "1"
	store.transactions[transactionId] = &Transaction{
		commands: []Command{
			{name: "GET", args: []string{"a"}},
			{name: "INCR", args: []string{"a"}},
			{name: "SET", args: []string{"b", "b"}},
			{name: "INCR", args: []string{"b"}},
		},
		originalValues: make(map[string]*string),
	}

	result, err := store.ExecuteTransaction(transactionId)

	if err == nil {
		t.Errorf("expected: should execute transaction, got: %v", err)
	}
	if result != nil {
		t.Errorf("expected: nil, got: %v", result)
	}
	value, _ := store.Get(0, "a")
	if value != "1" {
		t.Errorf("expected: Get('a') = 1, got: %v", 1)
	}
}

func TestExecuteTransaction_ShouldRollbackForUnknownCommand(t *testing.T) {
	store := getInMemoryStore(t)
	transactionId := "1"
	unknownCommand := "UNKNOWN"
	store.transactions[transactionId] = &Transaction{
		commands: []Command{
			{name: unknownCommand, args: []string{"a"}},
		},
		originalValues: make(map[string]*string),
	}

	result, err := store.ExecuteTransaction(transactionId)

	if result != nil {
		t.Errorf("expected: %v, got: %v", nil, result)
	}
	if err.Error() != ErrUnknownCommand(unknownCommand).Error() {
		t.Errorf("expected: %v, got: %v", ErrUnknownCommand(unknownCommand), err)
	}
}

func TestInTransaction(t *testing.T) {
	store := getInMemoryStore(t)
	transactionId := "1"
	store.StartTransaction(transactionId)

	result := store.InTransaction(transactionId)

	if result != true {
		t.Errorf("expected: %v, got: %v", true, false)
	}
}

func TestCompact_EmptyStore(t *testing.T) {
	s := getInMemoryStore(t)

	output := s.Compact(0)
	if output != "" {
		t.Errorf("Expected empty string for empty store, got: %q", output)
	}
}

func TestCompact_WithMultipleKeys(t *testing.T) {
	s := getInMemoryStore(t)
	s.Set(0, "counter", "13")
	s.Incr(0, "counter")
	s.Set(0, "foo", "bar")

	output := s.Compact(0)

	expectedLines := []string{
		"SET counter 14",
		"SET foo bar",
	}
	for _, line := range expectedLines {
		if !strings.Contains(output, line) {
			t.Errorf("Expected line %q in output, but not found. Got:\n%s", line, output)
		}
	}
}

func TestCompact_AfterDelete(t *testing.T) {
	s := getInMemoryStore(t)
	s.Set(0, "key1", "val1")
	s.Set(0, "key2", "val2")
	s.Del(0, "key1")

	output := s.Compact(0)

	if strings.Contains(output, "key1") {
		t.Errorf("Expected key1 to be deleted, but found in output: %q", output)
	}
	if !strings.Contains(output, "SET key2 val2") {
		t.Errorf("Expected remaining key2 in output, got: %q", output)
	}
}

func TestCompact_HandlesOverwrites(t *testing.T) {
	s := getInMemoryStore(t)
	s.Set(0, "x", "1")
	s.Set(0, "x", "2")

	output := s.Compact(0)

	if !strings.Contains(output, "SET x 2") {
		t.Errorf("Expected latest value of x to be 2, got: %q", output)
	}
	if strings.Contains(output, "SET x 1") {
		t.Errorf("Should not contain old value of x: %q", output)
	}
}

func TestStore_SetClientDBIndex(t *testing.T) {
	store := getInMemoryStore(t)
	clientId := "client1"

	store.SetClientDBIndex(clientId, 5)
	if dbIndex := store.GetClientDBIndex(clientId); dbIndex != 5 {
		t.Errorf("Expected DB index 5, got %d", dbIndex)
	}

	store.SetClientDBIndex(clientId, 0)
	if dbIndex := store.GetClientDBIndex(clientId); dbIndex != 0 {
		t.Errorf("Expected DB index 0, got %d", dbIndex)
	}

	clientId2 := "client2"
	if dbIndex := store.GetClientDBIndex(clientId2); dbIndex != 0 {
		t.Errorf("Expected default DB index 0 for new client, got %d", dbIndex)
	}
}

func TestStore_DatabaseIsolation(t *testing.T) {
	store := getInMemoryStore(t)
	clientId := "client1"

	store.SetClientDBIndex(clientId, 1)
	store.Set(1, "key1", "value1")
	if value, ok := store.Get(1, "key1"); !ok || value != "value1" {
		t.Errorf("Expected key1=value1 in DB 1, got ok=%v, value=%s", ok, value)
	}
	if value, ok := store.Get(2, "key1"); ok {
		t.Errorf("Expected key1 to be absent in DB 2, got value=%s", value)
	}

	store.SetClientDBIndex(clientId, 2)
	store.Set(2, "key1", "value2")
	if value, ok := store.Get(2, "key1"); !ok || value != "value2" {
		t.Errorf("Expected key1=value2 in DB 2, got ok=%v, value=%s", ok, value)
	}
	if value, ok := store.Get(1, "key1"); !ok || value != "value1" {
		t.Errorf("Expected key1=value1 in DB 1, got ok=%v, value=%s", ok, value)
	}
}

func TestStore_TransactionOnSetDBIndex(t *testing.T) {
	store := getInMemoryStore(t)
	clientId := "client1"

	store.SetClientDBIndex(clientId, 1)
	if err := store.StartTransaction(clientId); err != nil {
		t.Fatalf("Failed to start transaction: %v", err)
	}

	if err := store.QueueCommand(clientId, "SET", []string{"key1", "value1"}); err != nil {
		t.Fatalf("Failed to queue SET: %v", err)
	}

	results, err := store.ExecuteTransaction(clientId)
	if err != nil {
		t.Fatalf("Transaction execution failed: %v", err)
	}
	if len(results) != 1 || results[0] != "OK" {
		t.Errorf("Expected results=[OK], got %v", results)
	}

	if value, ok := store.Get(1, "key1"); !ok || value != "value1" {
		t.Errorf("Expected key1=value1 in DB 1, got ok=%v, value=%s", ok, value)
	}

	if value, ok := store.Get(0, "key1"); ok {
		t.Errorf("Expected key1 to be absent in DB 0, got value=%s", value)
	}
}

func TestStore_ConcurrentDBAccess(t *testing.T) {
	store := getInMemoryStore(t)
	var wg sync.WaitGroup
	numClients := 100

	for i := range numClients {
		wg.Add(1)
		go func(clientNum, dbIndex int) {
			defer wg.Done()
			clientId := fmt.Sprintf("client%d", clientNum)
			store.SetClientDBIndex(clientId, dbIndex)
			key := fmt.Sprintf("key%d", clientNum)
			value := fmt.Sprintf("value%d", clientNum)

			store.Set(dbIndex, key, value)
			if v, ok := store.Get(dbIndex, key); !ok || v != value {
				t.Errorf("Client %d: Expected %s=%s in DB %d, got ok=%v, value=%s", clientNum, key, value, dbIndex, ok, v)
			}
		}(i, i%defaultNumDatabases)
	}

	wg.Wait()

	for i := range numClients {
		dbIndex := i % defaultNumDatabases
		key := fmt.Sprintf("key%d", i)
		value := fmt.Sprintf("value%d", i)
		if v, ok := store.Get(dbIndex, key); !ok || v != value {
			t.Errorf("Verification: Expected %s=%s in DB %d, got ok=%v, value=%s", key, value, dbIndex, ok, v)
		}
	}
}
