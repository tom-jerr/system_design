package mr

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

func TestGrep(t *testing.T) {
	mkOut()
	t.Cleanup(func() {
		if !t.Failed() {
			cleanup()
		}
	})
	// Child workers inherit the same immutable application configuration.
	t.Setenv("MR_GREP_PATTERN", "^(ERROR|WARN) ")
	inputs := []string{
		"INFO started\nERROR repeated\nERROR repeated\n",
		"WARN 中文\nINFO fine\nERROR final",
	}
	var files, want []string
	for i, contents := range inputs {
		filename, err := filepath.Abs(filepath.Join(tmp, fmt.Sprintf("mr-grep-input-%d.txt", i)))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filename, []byte(contents), 0600); err != nil {
			t.Fatal(err)
		}
		files = append(files, filename)
	}
	want = append(want,
		files[0]+":2 ERROR repeated",
		files[0]+":3 ERROR repeated",
		files[1]+":1 WARN 中文",
		files[1]+":3 ERROR final",
	)
	sort.Strings(want)
	runMR(files, "../../mrapps/grep.so", 3)
	mergeOutput("mr-grep-all.txt")
	got, err := os.ReadFile(filepath.Join(tmp, "mr-grep-all.txt"))
	if err != nil {
		t.Fatal(err)
	}
	// Compare to explicit expected records, rather than running the same plugin
	// sequentially, so a filtering or duplicate-handling bug cannot pass both sides.
	if string(got) != strings.Join(want, "\n")+"\n" {
		t.Fatalf("unexpected grep output:\n%s\nwant:\n%s", got, strings.Join(want, "\n"))
	}
}
