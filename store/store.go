package store

import (
	"fmt"
	"math"
	"strconv"
)

type Store struct {
	data map[string]string
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
