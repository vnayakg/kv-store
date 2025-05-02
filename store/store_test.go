package store

import (
	"fmt"
	"math"
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
