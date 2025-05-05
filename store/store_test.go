package store

import (
	"math"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

func TestCreateNewStore(t *testing.T) {
	store := CreateNewStore()

	if store == nil {
		t.Fatal("CreateNewStore() returned nil")
	}
	if store.data == nil {
		t.Fatalf("CreateNewStore() did not initialize data map")
	}
}

func TestSetGet(t *testing.T) {
	key := "name"
	value := "batman"
	store := CreateNewStore()

	store.Set(key, value)
	retrievedValue, ok := store.Get(key)

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
	store := CreateNewStore()

	store.Set(key, value)
	store.Set(key, valueToOverwrite)
	retrievedValue, ok := store.Get(key)

	if !ok {
		t.Errorf("Get(%q) failed, expected key to exist", key)
	}
	if retrievedValue != valueToOverwrite {
		t.Errorf("Get(%q) = %q; expected %q", key, retrievedValue, valueToOverwrite)
	}
}

func TestStoreGet_NotFound(t *testing.T) {
	store := CreateNewStore()
	key := "non-existent"

	_, ok := store.Get(key)
	if ok {
		t.Errorf("Get(%q) succeeded, expected key not to exist", key)
	}
}

func TestDel(t *testing.T) {
	store := CreateNewStore()
	key := "name"
	store.Set(key, "superman")

	result := store.Del(key)

	_, ok := store.Get(key)
	if result != 1 {
		t.Errorf("Del(%q) = %q, expected 1", key, result)
	}
	if ok {
		t.Errorf("expected: %q should be deleted, got: it is still present", key)
	}
}

func TestDel_ForNonExistentKey(t *testing.T) {
	store := CreateNewStore()
	key := "surname"

	result := store.Del(key)

	if result != 0 {
		t.Errorf("Del(%q) = %q, expected 0", key, result)
	}
}

func TestIncr_ForExistingKey(t *testing.T) {
	store := CreateNewStore()
	key := "counter"
	value := "1"
	store.Set(key, value)

	updatedValue, err := store.Incr(key)

	if err != nil {
		t.Errorf("expected to increment counter, got error: %v", err)
	}
	if updatedValue != 2 {
		t.Errorf("Incr(%q) = %q, expected 2", key, updatedValue)
	}
}

func TestIncr_ForExistingNonIntegerKey(t *testing.T) {
	store := CreateNewStore()
	key := "counter"
	value := "abc"
	store.Set(key, value)

	updatedValue, err := store.Incr(key)

	expectedError := ErrNotInteger
	if err.Error() != expectedError.Error() {
		t.Errorf("expected: %q, got: %q", expectedError, err)
	}
	if updatedValue != 0 {
		t.Errorf("expected: 0, got: %q", updatedValue)
	}
}

func TestIncr_ForNonExistingKey(t *testing.T) {
	store := CreateNewStore()
	key := "counter"

	updatedValue, err := store.Incr(key)

	if err != nil {
		t.Errorf("expected to increment counter, got error: %v", err)
	}
	if updatedValue != 1 {
		t.Errorf("Incr(%q) = %q, expected 2", key, updatedValue)
	}
}

func TestIncr_ForOverflow(t *testing.T) {
	store := CreateNewStore()
	key := "counter"
	value := strconv.FormatInt(math.MaxInt64, 10)
	store.Set(key, value)

	updatedValue, err := store.Incr(key)

	expectedError := ErrIntOverflow
	if err.Error() != expectedError.Error() {
		t.Errorf("expected: %q, got: %q", expectedError, err)
	}
	if updatedValue != 0 {
		t.Errorf("expected: 0, got: %q", updatedValue)
	}
}

func TestIncrBy_ForExistingKey(t *testing.T) {
	store := CreateNewStore()
	key := "counter"
	value := "1"
	store.Set(key, value)

	updatedValue, err := store.IncrBy(key, 9)

	if err != nil {
		t.Errorf("expected to increment counter, got error: %v", err)
	}
	if updatedValue != 10 {
		t.Errorf("Incr(%q) = %q, expected 2", key, updatedValue)
	}
}

func TestIncrBy_ForExistingNonIntegerKey(t *testing.T) {
	store := CreateNewStore()
	key := "counter"
	value := "abc"
	store.Set(key, value)

	updatedValue, err := store.IncrBy(key, 10)

	expectedError := ErrNotInteger
	if err.Error() != expectedError.Error() {
		t.Errorf("expected: %q, got: %q", expectedError, err)
	}
	if updatedValue != 0 {
		t.Errorf("expected: 0, got: %q", updatedValue)
	}
}

func TestIncrBy_ForNonExistingKey(t *testing.T) {
	store := CreateNewStore()
	key := "counter"

	updatedValue, err := store.IncrBy(key, 10)

	if err != nil {
		t.Errorf("expected to increment counter, got error: %v", err)
	}
	if updatedValue != 10 {
		t.Errorf("Incr(%q) = %q, expected 2", key, updatedValue)
	}
}

func TestIncrBy_ForOverflow(t *testing.T) {
	store := CreateNewStore()
	key := "counter"
	value := strconv.FormatInt(math.MinInt64, 10)
	store.Set(key, value)

	updatedValue, err := store.IncrBy(key, -10)

	expectedError := ErrIntOverflow
	if err.Error() != expectedError.Error() {
		t.Errorf("expected: %q, got: %q", expectedError, err)
	}
	if updatedValue != 0 {
		t.Errorf("expected: 0, got: %q", updatedValue)
	}
}

func TestStartTransaction_NoOnGoingTransaction(t *testing.T) {
	store := CreateNewStore()
	transactionId := "1"
	err := store.StartTransaction(transactionId)

	if err != nil {
		t.Errorf("expected: should start transaction, got: %v", err)
	}
}

func TestStartTransaction_OnGoingTransactionPresent(t *testing.T) {
	store := CreateNewStore()
	transactionId := "1"
	store.transactions[transactionId] = &Transaction{}

	err := store.StartTransaction(transactionId)

	expectedError := ErrTransactionInProgress
	if err.Error() != expectedError.Error() {
		t.Errorf("expected: %v, got: %v", expectedError, err)
	}
}

func TestQueueCommand_OnGoingTransactionPresent(t *testing.T) {
	store := CreateNewStore()
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
	store := CreateNewStore()
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
	store := CreateNewStore()
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
	store := CreateNewStore()
	transactionId := "1"

	err := store.DiscardTransaction(transactionId)

	expectedError := ErrNoTransactionInProgress
	if err.Error() != expectedError.Error() {
		t.Errorf("expected: %v, got: %v", expectedError, err)
	}
}

func TestExecuteTransaction_OnGoingTransactionPresent(t *testing.T) {
	store := CreateNewStore()
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
	store := CreateNewStore()
	transactionId := "1"

	_, err := store.ExecuteTransaction(transactionId)

	expectedError := ErrNoTransactionInProgress
	if err.Error() != expectedError.Error() {
		t.Errorf("expected: %v, got: %v", expectedError, err)
	}
}

func TestExecuteTransaction_ShouldRollbackOnError(t *testing.T) {
	store := CreateNewStore()
	store.Set("a", "1")
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
	value, _ := store.Get("a")
	if value != "1" {
		t.Errorf("expected: Get('a') = 1, got: %v", 1)
	}
}

func TestExecuteTransaction_ShouldRollbackForUnknownCommand(t *testing.T) {
	store := CreateNewStore()
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

func TestReportTransactionError_HasTransactionError(t *testing.T) {
	store := CreateNewStore()
	transactionId := "1"
	store.StartTransaction(transactionId)

	store.ReportTransactionError(transactionId)
	result := store.HasTransactionError(transactionId)

	if result != true {
		t.Errorf("expected: %v, got: %v", true, false)
	}
}

func TestInTransaction(t *testing.T) {
	store := CreateNewStore()
	transactionId := "1"
	store.StartTransaction(transactionId)

	result := store.InTransaction(transactionId)

	if result != true {
		t.Errorf("expected: %v, got: %v", true, false)
	}
}

func TestCompact_EmptyStore(t *testing.T) {
	s := CreateNewStore()

	output := s.Compact()
	if output != "" {
		t.Errorf("Expected empty string for empty store, got: %q", output)
	}
}

func TestCompact_WithMultipleKeys(t *testing.T) {
	s := CreateNewStore()
	s.Set("counter", "13")
	s.Incr("counter")
	s.Set("foo", "bar")

	output := s.Compact()

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
	s := CreateNewStore()
	s.Set("key1", "val1")
	s.Set("key2", "val2")
	s.Del("key1")

	output := s.Compact()

	if strings.Contains(output, "key1") {
		t.Errorf("Expected key1 to be deleted, but found in output: %q", output)
	}
	if !strings.Contains(output, "SET key2 val2") {
		t.Errorf("Expected remaining key2 in output, got: %q", output)
	}
}

func TestCompact_HandlesOverwrites(t *testing.T) {
	s := CreateNewStore()
	s.Set("x", "1")
	s.Set("x", "2")

	output := s.Compact()

	if !strings.Contains(output, "SET x 2") {
		t.Errorf("Expected latest value of x to be 2, got: %q", output)
	}
	if strings.Contains(output, "SET x 1") {
		t.Errorf("Should not contain old value of x: %q", output)
	}
}
