package kvsrv

import (
	"testing"

	"6.5840/kvsrv1/rpc"
)

func TestServerStateTransitions(t *testing.T) {
	kv := MakeKVServer()
	get := func(key, value string, version rpc.Tversion, err rpc.Err) {
		t.Helper()
		// Start with a nonzero reply to ensure missing keys clear all fields.
		reply := rpc.GetReply{Value: "stale", Version: 99, Err: rpc.OK}
		kv.Get(&rpc.GetArgs{Key: key}, &reply)
		want := rpc.GetReply{Value: value, Version: version, Err: err}
		if reply != want {
			t.Fatalf("Get(%q) = %+v, want %+v", key, reply, want)
		}
	}
	put := func(key, value string, version rpc.Tversion, want rpc.Err) {
		t.Helper()
		var reply rpc.PutReply
		kv.Put(&rpc.PutArgs{Key: key, Value: value, Version: version}, &reply)
		if reply.Err != want {
			t.Fatalf("Put(%q, %q, %d) = %s, want %s", key, value, version, reply.Err, want)
		}
	}

	get("", "", 0, rpc.ErrNoKey)
	put("", "rejected", 1, rpc.ErrNoKey)
	get("", "", 0, rpc.ErrNoKey)
	put("", "", 0, rpc.OK)
	get("", "", 1, rpc.OK)
	put("", "", 0, rpc.ErrVersion) // Exact retransmission.
	put("", "competitor", 0, rpc.ErrVersion)
	put("", "future", 2, rpc.ErrVersion)
	get("", "", 1, rpc.OK)
	put("", "owner-a", 1, rpc.OK)
	put("", "", 2, rpc.OK)
	put("", "owner-b", 3, rpc.OK)
	put("", "owner-a", 1, rpc.ErrVersion) // Late acquire.
	put("", "", 2, rpc.ErrVersion)        // Late release.
	get("", "owner-b", 4, rpc.OK)
	put("other", "independent", 0, rpc.OK)
	get("other", "independent", 1, rpc.OK)
	get("", "owner-b", 4, rpc.OK)
}
