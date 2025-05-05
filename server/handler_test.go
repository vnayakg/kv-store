package server

import (
	"bufio"
	"kv-store/store"
	"net"
	"strings"
	"testing"
	"time"
)

func TestHandleConnection(t *testing.T) {
	testCases := []struct {
		name          string
		commands      []string
		wantResponses []string
		storeSetup    func(s *store.Store)
	}{
		{
			name: "Simple SET and GET",
			commands: []string{
				"SET name gandalf",
				"GET name",
			},
			wantResponses: []string{
				"OK\n",
				"gandalf\n",
			},
		},
		{
			name: "SET with spaces and GET",
			commands: []string{
				`SET wizard "gandalf the grey"`,
				"GET wizard",
			},
			wantResponses: []string{
				"OK\n",
				"gandalf the grey\n",
			},
		},
		{
			name: "GET non-existent key",
			commands: []string{
				"GET missingkey",
			},
			wantResponses: []string{
				"<nil>\n",
			},
		},
		{
			name: "Overwrite key with SET",
			commands: []string{
				"SET fruit apple",
				"GET fruit",
				"SET fruit banana",
				"GET fruit",
			},
			wantResponses: []string{
				"OK\n",
				"apple\n",
				"OK\n",
				"banana\n",
			},
		},
		{
			name: "Unknown command",
			commands: []string{
				"FOOBAR arg1 arg2",
			},
			wantResponses: []string{
				"err unknown command: FOOBAR\n",
			},
		},
		{
			name: "Wrong number of arguments SET",
			commands: []string{
				"SET one",
				"SET one two three",
			},
			wantResponses: []string{
				"wrong number of arguments for SET command\n",
				"wrong number of arguments for SET command\n",
			},
		},
		{
			name: "Wrong number of arguments GET",
			commands: []string{
				"GET",
				"GET one two",
			},
			wantResponses: []string{
				"wrong number of arguments for GET command\n",
				"wrong number of arguments for GET command\n",
			},
		},
		{
			name: "Empty command line",
			commands: []string{
				"",
			},
			wantResponses: []string{
				"ERR empty command\n",
			},
		},
		{
			name: "Parser error unmatched quote",
			commands: []string{
				`SET key "unterminated`,
			},
			wantResponses: []string{
				"ERR syntax, mismatched quotes\n",
			},
		},
		{
			name: "Should Delete key",
			commands: []string{
				`DEL wizard`,
			},
			wantResponses: []string{
				"1\n",
			},
			storeSetup: func(s *store.Store) { s.Set(0, "wizard", "gandalf the white") },
		},
		{
			name: "Return 0 for deleting non-existent key",
			commands: []string{
				`DEL this-is-random-key`,
				`DEL a b`,
			},
			wantResponses: []string{
				"0\n",
				"wrong number of arguments for DEL command\n",
			},
		},
		{
			name: "INCR non-existent key",
			commands: []string{
				"INCR counter",
			},
			wantResponses: []string{
				"1\n",
			},
		},
		{
			name: "INCR existing key",
			storeSetup: func(s *store.Store) {
				s.Set(0, "counter", "5")
			},
			commands: []string{
				"INCR counter",
				"INCR counter",
			},
			wantResponses: []string{
				"6\n",
				"7\n",
			},
		},
		{
			name: "INCR non-integer value",
			storeSetup: func(s *store.Store) {
				s.Set(0, "key", "hello")
			},
			commands: []string{
				"INCR key",
			},
			wantResponses: []string{
				"err value is not an integer or out of range\n",
			},
		},
		{
			name: "INCR wrong number of args",
			commands: []string{
				"INCR",
				"INCR key1 key2",
			},
			wantResponses: []string{
				"wrong number of arguments for INCR command\n",
				"wrong number of arguments for INCR command\n",
			},
		},
		{
			name: "INCRBY non-existent key",
			commands: []string{
				"INCRBY visits 10",
			},
			wantResponses: []string{
				"10\n",
			},
		},
		{
			name: "INCRBY existing key positive",
			storeSetup: func(s *store.Store) {
				s.Set(0, "visits", "100")
			},
			commands: []string{
				"INCRBY visits 25",
			},
			wantResponses: []string{
				"125\n",
			},
		},
		{
			name: "INCRBY existing key negative (decrement)",
			storeSetup: func(s *store.Store) {
				s.Set(0, "visits", "50")
			},
			commands: []string{
				"INCRBY visits -10",
			},
			wantResponses: []string{
				"40\n",
			},
		},
		{
			name: "INCRBY non-integer value",
			storeSetup: func(s *store.Store) {
				s.Set(0, "key", "world")
			},
			commands: []string{
				"INCRBY key 5",
			},
			wantResponses: []string{
				"err value is not an integer or out of range\n",
			},
		},
		{
			name: "INCRBY non-integer increment",
			commands: []string{
				"INCRBY key abc",
			},
			wantResponses: []string{
				"err value is not an integer or out of range\n",
			},
		},
		{
			name: "INCRBY wrong number of args",
			commands: []string{
				"INCRBY",
				"INCRBY key",
				"INCRBY key 10 extra",
			},
			wantResponses: []string{
				"wrong number of arguments for INCRBY command\n",
				"wrong number of arguments for INCRBY command\n",
				"wrong number of arguments for INCRBY command\n",
			},
		},
		{
			name: "MULTI EXEC success",
			commands: []string{
				"MULTI",
				"SET counter 10",
				"INCR counter",
				"EXEC",
			},
			wantResponses: []string{
				"OK\n",
				"QUEUED\n",
				"QUEUED\n",
				"1) OK\n",
			},
		},
		{
			name: "MULTI DISCARD success",
			commands: []string{
				"MULTI",
				"SET counter 10",
				"INCR counter",
				"DISCARD",
			},
			wantResponses: []string{
				"OK\n",
				"QUEUED\n",
				"QUEUED\n",
				"OK\n",
			},
		},
		{
			name: "COMPACT",
			commands: []string{
				"COMPACT",
				"COMPACT hello",
			},
			wantResponses: []string{
				"\n",
				"wrong number of arguments for COMPACT command\n",
			},
		},
		{
			name: "Unknown command",
			commands: []string{
				"UNKNOWN",
			},
			wantResponses: []string{
				"err unknown command: UNKNOWN\n",
			},
		},
		{
			name: "SELECT with invalid database index",
			commands: []string{
				"SELECT -1",
				"SELECT 16",
				"SELECT 17",
				"SELECT",
				"SELECT hi",
			},
			wantResponses: []string{
				"err DB index is out of range\n",
				"err DB index is out of range\n",
				"err DB index is out of range\n",
				"wrong number of arguments for SELECT command\n",
				"err value is not an integer or out of range\n",
			},
		}, {
			name: "SELECT success",
			commands: []string{
				"SELECT 1",
				"SET key1 value1",
				"SELECT 2",
				"GET key1",
			},
			wantResponses: []string{
				"OK\n",
				"OK\n",
				"OK\n",
				"<nil>\n",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {

			store := store.CreateNewStore()

			if tc.storeSetup != nil {
				tc.storeSetup(store)
			}

			clientConn, serverConn := net.Pipe()
			defer clientConn.Close()

			go func() {
				handleConnection(serverConn, store)
			}()

			clientReader := bufio.NewReader(clientConn)
			clientWriter := bufio.NewWriter(clientConn)

			for index, command := range tc.commands {
				t.Logf("Client sending: %q", command)
				clientWriter.WriteString(command + "\n")
				clientWriter.Flush()

				response, err := clientReader.ReadString('\n')
				clientConn.SetReadDeadline(time.Time{})

				if err != nil {
					t.Fatalf("Error reading response for command %d %q: %v", index, command, err)
				}

				trimmedResponse := strings.TrimSuffix(response, "\r\n")
				t.Logf("Client received: %q", trimmedResponse)

				if trimmedResponse != tc.wantResponses[index] {
					t.Errorf("Response mismatch for command %d (%q):\n got: %q\nwant: %q",
						index, command, trimmedResponse, tc.wantResponses[index])
				}
			}
			clientConn.Close()
		})
	}
}
