package parser

import (
	"fmt"
	"reflect"
	"testing"
)

func TestParseCommandLine(t *testing.T) {
	tests := []struct {
		input string
		cmd   string
		args  []string
		err   error
	}{
		{``, "", nil, nil},
		{`set name foo`, "SET", []string{"name", "foo"}, nil},
		{`SET surname "foo bar"`, "SET", []string{"surname", "foo bar"}, nil},
		{`SET name "foo bar baz"`, "SET", []string{"name", "foo bar baz"}, nil},
		{`GET name`, "GET", []string{"name"}, nil},
		{`SET key "val\"ue"`, "SET", []string{"key", `val"ue`}, nil},
		{`SET key \"bad`, "SET", []string{`key`, `"bad`}, nil},
		{`SET key "bad`, "", nil, fmt.Errorf("ERR syntax, mismatched quotes")},
		{``, "", nil, fmt.Errorf("ERR empty command")},
	}

	for _, tt := range tests {
		cmd, args, _ := ParseCommandLine(tt.input)
		if cmd != tt.cmd || !reflect.DeepEqual(args, tt.args) {
			t.Errorf("input=%q => got (%q, %v), expected (%q, %v)", tt.input, cmd, args, tt.cmd, tt.args)
		}
	}
}
