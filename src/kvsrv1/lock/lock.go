package lock

import (
	"6.5840/kvtest1"
	"6.5840/kvsrv1/rpc"
	"log"
	"time"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// get through kvtest.RandValue(8)
	clientID string
	keyl string   // usually the content name be protected/locked
	// You may add code here
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck}
	// You may add code here
	lk.clientID = kvtest.RandValue(8)
	lk.keyl = l
	return lk
}

// 
func (lk *Lock) Acquire() {
	// Your code here
	key := lk.keyl
	cID := lk.clientID

	for {
		oldID, ver, _ := lk.ck.Get(key)

		// be locked by others, wait and continue
		if oldID != "" && oldID != cID {
			time.Sleep(50 * time.Millisecond)
			continue
		}

		//err could be ErrVersion, ErrMaybe
		err := lk.ck.Put(key, cID, ver)

		// success
		if err == rpc.OK {
			return
		}

		nID, _, _ := lk.ck.Get(key)

		// may return erro(ErrVersion, ErrMaybe) but actual success
		if nID == cID {
			return
		}
		// be procced by others, wait and continue
		time.Sleep(50 * time.Millisecond)
	}
}


func (lk *Lock) Release() {
	// Your code here
	key := lk.keyl
	cID := lk.clientID

	for {
		getID, ver, err := lk.ck.Get(key)
		// already unlock, return!
		if cID == "" || getID != cID{
			return 
		}
		// this lock non exsit
		if err == rpc.ErrNoKey {
			log.Printf("Lock error: release a non exist lock!\n")
			return
		}

		// nErr could be ErrVersion or ErrMaybe
		putErr := lk.ck.Put(key, "", ver)

		// put return ok, we success!
		if putErr == rpc.OK {
			return
		}
		nID, _, _ := lk.ck.Get(key)

		// if ID is empty or be locked by other client ID(all represent the lock
		// has been released), though we may get error from put, but they are fake error,
		// we actual success!
		if nID == "" || nID != cID {
			return
		}
		// proceed by other threads maybe, so wait and go on!
		time.Sleep(50 * time.Millisecond)
	}
}
