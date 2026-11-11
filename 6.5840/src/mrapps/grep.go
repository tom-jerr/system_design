package main

// Distributed Grep plugin:
// go build -buildmode=plugin grep.go
// Set MR_GREP_PATTERN to a Go regular expression on every worker.

import (
	"fmt"
	"os"
	"regexp"
	"strings"

	"6.5840/mr"
)

// Map emits every matching line, including repeated lines at different locations.
// A location key lets the framework's single-string Reduce interface preserve
// each match without grouping identical text into one output record.
func Map(filename string, contents string) []mr.KeyValue {
	pattern, set := os.LookupEnv("MR_GREP_PATTERN")
	if !set {
		pattern = "Sherlock"
	}
	re := regexp.MustCompile(pattern)
	lines := strings.Split(contents, "\n")
	var matches []mr.KeyValue
	for i, line := range lines {
		// A trailing newline terminates the last line; it does not add another line.
		// This also makes an empty input file contain no lines.
		if i == len(lines)-1 && line == "" {
			break
		}
		if re.MatchString(line) {
			matches = append(matches, mr.KeyValue{
				Key:   fmt.Sprintf("%s:%d", filename, i+1),
				Value: line,
			})
		}
	}
	return matches
}

// Reduce copies the matched contents without counting or deduplicating them.
func Reduce(_ string, values []string) string {
	return strings.Join(values, "\n")
}
