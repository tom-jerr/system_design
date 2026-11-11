package lock

import (
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck    kvtest.IKVClerk
	name  string
	owner string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// This interface supports multiple locks by means of the
// lockname argument; locks with different names should be
// independent.
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	return &Lock{ck: ck, name: lockname, owner: kvtest.RandValue(8)}
}

func (lk *Lock) Acquire() {
	for {
		value, version, err := lk.ck.Get(lk.name)
		// Only this instance writes its owner ID. After an uncertain Put,
		// observing it confirms ownership before Acquire returns.
		if err == rpc.OK && value == lk.owner {
			return
		}
		if err == rpc.ErrNoKey || (err == rpc.OK && value == "") {
			if err == rpc.ErrNoKey {
				version = 0
			}
			if lk.ck.Put(lk.name, lk.owner, version) == rpc.OK {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (lk *Lock) Release() {
	for {
		value, version, err := lk.ck.Get(lk.name)
		if err == rpc.ErrNoKey || (err == rpc.OK && value != lk.owner) {
			return
		}
		if err == rpc.OK {
			if lk.ck.Put(lk.name, "", version) == rpc.OK {
				return
			}
		}
		// Re-read after ErrMaybe: another client may already own the lock.
		time.Sleep(100 * time.Millisecond)
	}
}
