package lock

import (
	"testing"

	"6.5840/kvsrv1/rpc"
)

// The script describes application-visible results, including both possible
// outcomes behind ErrMaybe. An extra write or premature return fails the test.
type lockStep struct {
	method  string
	value   string
	version rpc.Tversion
	err     rpc.Err
}

type scriptedClerk struct {
	t     *testing.T
	name  string
	steps []lockStep
	next  int
}

func (ck *scriptedClerk) step(method, key string) lockStep {
	ck.t.Helper()
	if key != ck.name || ck.next >= len(ck.steps) {
		ck.t.Fatalf("unexpected %s(%q) at step %d", method, key, ck.next)
	}
	s := ck.steps[ck.next]
	ck.next++
	if s.method != method {
		ck.t.Fatalf("step %d: got %s, want %s", ck.next-1, method, s.method)
	}
	return s
}

func (ck *scriptedClerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	s := ck.step("Get", key)
	return s.value, s.version, s.err
}

func (ck *scriptedClerk) Put(key, value string, version rpc.Tversion) rpc.Err {
	s := ck.step("Put", key)
	if value != s.value || version != s.version {
		ck.t.Fatalf("Put(%q, %q, %d), want value %q version %d", key, value, version, s.value, s.version)
	}
	return s.err
}

func TestAcquireUncertain(t *testing.T) {
	for _, committed := range []bool{true, false} {
		name := "not-committed"
		if committed {
			name = "committed"
		}
		t.Run(name, func(t *testing.T) {
			ck := &scriptedClerk{t: t, name: "lock"}
			lk := MakeLock(ck, ck.name)
			ck.steps = []lockStep{
				{"Get", "", 0, rpc.ErrNoKey},
				{"Put", lk.owner, 0, rpc.ErrMaybe},
			}
			if committed {
				ck.steps = append(ck.steps, lockStep{"Get", lk.owner, 1, rpc.OK})
			} else {
				ck.steps = append(ck.steps,
					lockStep{"Get", "another-owner", 1, rpc.OK},
					lockStep{"Get", "", 2, rpc.OK},
					lockStep{"Put", lk.owner, 2, rpc.OK},
				)
			}
			lk.Acquire()
			if ck.next != len(ck.steps) {
				t.Fatalf("Acquire returned after %d of %d steps", ck.next, len(ck.steps))
			}
		})
	}
}

func TestReleaseUncertain(t *testing.T) {
	for _, transferred := range []bool{true, false} {
		name := "still-owned"
		if transferred {
			name = "transferred"
		}
		t.Run(name, func(t *testing.T) {
			ck := &scriptedClerk{t: t, name: "lock"}
			lk := MakeLock(ck, ck.name)
			ck.steps = []lockStep{
				{"Get", lk.owner, 7, rpc.OK},
				{"Put", "", 7, rpc.ErrMaybe},
			}
			if transferred {
				ck.steps = append(ck.steps, lockStep{"Get", "new-owner", 9, rpc.OK})
			} else {
				ck.steps = append(ck.steps,
					lockStep{"Get", lk.owner, 7, rpc.OK},
					lockStep{"Put", "", 7, rpc.OK},
				)
			}
			lk.Release()
			if ck.next != len(ck.steps) {
				t.Fatalf("Release returned after %d of %d steps", ck.next, len(ck.steps))
			}
		})
	}
}
