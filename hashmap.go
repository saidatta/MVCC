package MVCC

import (
	"fmt"
	"sync"
	"sync/atomic"
	"unsafe"
)

type HashMap struct {
	size     int
	maxReach int

	values []*SingleLinkedList

	txnCtr uint64

	// Last Successful Txn
	lsTxn uint64
	// TODO: For mulitple writers
	//txnTable map[uint64]bool

	sync.Mutex
}

// KeyType is the key
type KeyType int

// ValType is the val
type ValType int

// KVType is the keyvalue type
type KVType struct {
	Key KeyType
	Val ValType
}

func newHT(size, maxReach int) *HashMap {
	h := HashMap{
		size:     size,
		maxReach: maxReach,
	}

	h.values = make([]*SingleLinkedList, size)
	for i := range h.values {
		h.values[i] = &SingleLinkedList{}

		err := h.insert(0, &KVType{}, i)
		if err != nil {
			return nil
		}
	}

	h.lsTxn = 0
	h.txnCtr = 0
	return &h
}

// NewDefaultHT returns a new HashMap with sensible defaults
func NewDefaultHT() *HashMap {
	return newHT(512, 16)
}

func (h *HashMap) hash1(i KeyType) int {
	return int(i) % h.size
}

func (h *HashMap) hash2(i KeyType) int {
	return (int(i) / 11) % h.size
}

func (h *HashMap) insert(txn uint64, kv *KVType, idx int) error {
	h.values[idx].insert(txn, unsafe.Pointer(kv))
	return nil
}

// Put puts the kv into the hashtable
// ANNY: This is the key part. See how the rollback happens
func (h *HashMap) Put(kv KVType) error {
	h.Lock()
	defer h.Unlock()
	// NOTE: The rollback will be via the abandoning of the txn
	txn := atomic.AddUint64(&h.txnCtr, 1)
	current := &kv
	idxMod := make([]int, h.maxReach)

	for i := 0; i < h.maxReach; i++ {
		idx := h.hash1(current.Key)
		temp := (*KVType)(h.values[idx].Head())
		if (*temp == KVType{}) {
			_ = h.insert(txn, current, idx)
			h.lsTxn = txn
			return nil
		}

		idx = h.hash2(current.Key)
		temp = (*KVType)(h.values[idx].Head())
		if (*temp == KVType{}) {
			_ = h.insert(txn, current, idx)
			h.lsTxn = txn
			return nil
		}

		// Take the key from the second slot
		_ = h.insert(txn, current, idx)
		idxMod[i] = idx
		current = temp
	}
	// Abandon the txn
	// Delete the elements of the current txn
	for i := 0; i < h.maxReach; i++ {
		h.values[idxMod[i]].delete(txn)
	}

	return fmt.Errorf("Key %d couldn't be inserted due to a tight table", kv.Key)
}

// Get gets the key-value pair back
func (h *HashMap) Get(k KeyType) (bool, ValType) {
	// NOTE: Make this serialised when we deal with multiple
	// writers
	version := h.lsTxn

	idx := h.hash1(k)
	// No nil check needed as we are filling version 0
	val := (*KVType)(h.values[idx].LatestVersion(version))
	// not empty and key is same.
	if (*val != KVType{} && val.Key == k) {
		return true, val.Val
	}

	idx = h.hash2(k)
	val = (*KVType)(h.values[idx].LatestVersion(version))
	if (*val != KVType{} && val.Key == k) {
		return true, val.Val
	}

	return false, 0
}

// Delete deletes all the elements with the key
func (h *HashMap) Delete(k KeyType) (bool, error) {
	h.Lock()
	defer h.Unlock()
	txn := atomic.AddUint64(&h.txnCtr, 1)
	idx := h.hash1(k)
	val := (*KVType)(h.values[idx].LatestVersion(h.lsTxn))
	if (*val != KVType{} && val.Key == k) {
		_ = h.insert(txn, &KVType{}, idx)
		return true, nil
	}

	idx = h.hash2(k)
	val = (*KVType)(h.values[idx].LatestVersion(h.lsTxn))
	if (*val != KVType{} && val.Key == k) {
		_ = h.insert(txn, &KVType{}, idx)
		return true, nil
	}

	return false, nil
}
