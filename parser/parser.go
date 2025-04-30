package parser

import (
	"fmt"
	"strings"
	"unicode"
)

func ParseCommandLine(line string) (string, []string, error) {
	var args []string
	var curr strings.Builder
	inQuotes := false
	escaped := false

	for _, char := range line {
		switch {
		case escaped:
			curr.WriteRune(char)
			escaped = false
		case char == '\\':
			escaped = true
		case char == '"':
			inQuotes = !inQuotes
		case unicode.IsSpace(char) && !inQuotes:
			if curr.Len() > 0 {
				args = append(args, curr.String())
				curr.Reset()
			}

		default:
			curr.WriteRune(char)
		}
	}

	if curr.Len() > 0 {
		args = append(args, curr.String())
	}
	if inQuotes {
		return "", nil, fmt.Errorf("ERR syntax, mismatched quotes")
	}
	if len(args) == 0{
		return "", nil, fmt.Errorf("ERR empty command")
	}
	if len(args) == 1 {
		return "", nil, fmt.Errorf("ERR missing args")
	}
	return strings.ToUpper(args[0]), args[1:], nil
}
