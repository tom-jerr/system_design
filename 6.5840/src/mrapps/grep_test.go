package main

import (
	"reflect"
	"strings"
	"testing"

	"6.5840/mr"
)

func TestGrepMap(t *testing.T) {
	longLine := "ERROR " + strings.Repeat("x", 70*1024)
	tests := []struct {
		name     string
		pattern  string
		contents string
		want     []mr.KeyValue
	}{
		{"regex and duplicates", "^(ERROR|WARN) ", "ok\nERROR repeated\nERROR repeated\nWARN 最后一行", []mr.KeyValue{
			{Key: "input.log:2", Value: "ERROR repeated"},
			{Key: "input.log:3", Value: "ERROR repeated"},
			{Key: "input.log:4", Value: "WARN 最后一行"},
		}},
		{"no match", "ERROR", "ok\nwarning\n", nil},
		{"empty file", "", "", nil},
		{"blank lines and final newline", "^$", "\nhello\n\n", []mr.KeyValue{
			{Key: "input.log:1", Value: ""},
			{Key: "input.log:3", Value: ""},
		}},
		{"long line", "^ERROR", longLine + "\n", []mr.KeyValue{{Key: "input.log:1", Value: longLine}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv("MR_GREP_PATTERN", test.pattern)
			if got := Map("input.log", test.contents); !reflect.DeepEqual(got, test.want) {
				t.Fatalf("matches differ: got %d records, want %d", len(got), len(test.want))
			}
		})
	}
}

func TestGrepReduce(t *testing.T) {
	if got := Reduce("input.log:1", []string{"ERROR repeated", "ERROR repeated"}); got != "ERROR repeated\nERROR repeated" {
		t.Fatalf("Reduce changed matches: %q", got)
	}
}
