package store

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
)

type MemoryStorage struct {
	data      []map[string]string
	dataMutex sync.RWMutex
}

func NewMemoryStorage(numDatabases int) *MemoryStorage {
	data := make([]map[string]string, numDatabases)
	for i := range numDatabases {
		data[i] = make(map[string]string)
	}
	return &MemoryStorage{
		data: data,
	}
}

func (ms *MemoryStorage) numDatabases() int {
	return len(ms.data)
}

func (ms *MemoryStorage) Set(dbIndex int, key, value string) {
	ms.dataMutex.Lock()
	defer ms.dataMutex.Unlock()
	ms.data[dbIndex][key] = value
}

func (ms *MemoryStorage) Get(dbIndex int, key string) (string, bool) {
	ms.dataMutex.RLock()
	defer ms.dataMutex.RUnlock()
	value, ok := ms.data[dbIndex][key]
	return value, ok
}

func (ms *MemoryStorage) Del(dbIndex int, key string) int {
	ms.dataMutex.Lock()
	defer ms.dataMutex.Unlock()
	_, ok := ms.data[dbIndex][key]
	if !ok {
		return 0
	}
	delete(ms.data[dbIndex], key)
	return 1
}

func (ms *MemoryStorage) IncrBy(dbIndex int, key string, increment int64) (int64, error) {
	ms.dataMutex.Lock()
	defer ms.dataMutex.Unlock()

	value, ok := ms.data[dbIndex][key]
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
	ms.data[dbIndex][key] = strconv.FormatInt(currentValue, 10)
	return currentValue, nil
}

func (ms *MemoryStorage) Compact(dbIndex int) string {
	ms.dataMutex.RLock()
	defer ms.dataMutex.RUnlock()

	var result []string
	for k, v := range ms.data[dbIndex] {
		result = append(result, fmt.Sprintf("SET %s %s", k, v))
	}
	return strings.Join(result, "\n")
}
