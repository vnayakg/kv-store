package store

import (
	"fmt"
	"math"
	"reflect"
	"strconv"
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

	expectedError := fmt.Errorf("value is not an integer or out of range")
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

	expectedError := fmt.Errorf("increment or decrement would overflow")
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

	expectedError := fmt.Errorf("value is not an integer or out of range")
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

	expectedError := fmt.Errorf("increment or decrement would overflow")
	if err.Error() != expectedError.Error() {
		t.Errorf("expected: %q, got: %q", expectedError, err)
	}
	if updatedValue != 0 {
		t.Errorf("expected: 0, got: %q", updatedValue)
	}
}

func TestStartTransaction_NoOnGoingTransaction(t *testing.T) {
	store := CreateNewStore()

	err := store.StartTransaction()

	if err != nil {
		t.Errorf("expected: should start transaction, got: %v", err)
	}
}

func TestStartTransaction_OnGoingTransactionPresent(t *testing.T) {
	store := CreateNewStore()
	store.transaction = &Transaction{}

	err := store.StartTransaction()

	expectedError := fmt.Errorf("transaction already in progress")
	if err.Error() != expectedError.Error() {
		t.Errorf("expected: %v, got: %v", expectedError, err)
	}
}

func TestQueueCommand_OnGoingTransactionPresent(t *testing.T) {
	store := CreateNewStore()
	store.transaction = &Transaction{}
	commandName := "SET"
	args := []string{"a", "2"}

	err := store.QueueCommand(commandName, args)

	if err != nil {
		t.Errorf("expected: should queue command, got: %v", err)
	}
	expectedCommand := Command{commandName, args}
	if store.transaction.commands[0].name != expectedCommand.name &&
		!reflect.DeepEqual(store.transaction.commands[0].args, expectedCommand.args) {
		t.Errorf("expected: %v, got: %v", expectedCommand, store.transaction.commands[0])
	}
}

func TestQueueCommand_NoOnGoingTransactionPresent(t *testing.T) {
	store := CreateNewStore()
	commandName := "SET"
	args := []string{"a", "2"}

	err := store.QueueCommand(commandName, args)

	expectedError := fmt.Errorf("no transaction in progress")
	if err.Error() != expectedError.Error() {
		t.Errorf("expected: %v, got: %v", expectedError, err)
	}
}

func TestDiscardTransaction_OnGoingTransactionPresent(t *testing.T) {
	store := CreateNewStore()
	store.transaction = &Transaction{}

	err := store.DiscardTransaction()

	if err != nil {
		t.Errorf("expected: should discard transaction, got: %v", err)
	}
	if store.transaction != nil {
		t.Errorf("expected: %v, got %v", nil, store.transaction)
	}
}

func TestDiscardTransaction_NoOnGoingTransactionPresent(t *testing.T) {
	store := CreateNewStore()

	err := store.DiscardTransaction()

	expectedError := fmt.Errorf("no transaction in progress")
	if err.Error() != expectedError.Error() {
		t.Errorf("expected: %v, got: %v", expectedError, err)
	}
}

func TestExecuteTransaction_OnGoingTransactionPresent(t *testing.T) {
	store := CreateNewStore()
	store.transaction = &Transaction{
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

	result, err := store.ExecuteTransaction()

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

	_, err := store.ExecuteTransaction()

	expectedError := fmt.Errorf("no transaction in progress")
	if err.Error() != expectedError.Error() {
		t.Errorf("expected: %v, got: %v", expectedError, err)
	}
}

func TestExecuteTransaction_ShouldRollbackOnError(t *testing.T) {
	store := CreateNewStore()
	store.Set("a", "1")
	store.transaction = &Transaction{
		commands: []Command{
			{name: "GET", args: []string{"a"}},
			{name: "INCR", args: []string{"a"}},
			{name: "SET", args: []string{"b", "b"}},
			{name: "INCR", args: []string{"b"}},
		},
		originalValues: make(map[string]*string),
	}

	result, err := store.ExecuteTransaction()

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
