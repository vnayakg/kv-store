package store

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
