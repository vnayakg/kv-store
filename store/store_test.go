package store

import "testing"

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
