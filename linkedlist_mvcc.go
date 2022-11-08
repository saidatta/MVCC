package MVCC

import (
	"sync/atomic"
	"time"
	unsafe "unsafe"
)

type Node struct {
	next *Node
	//prev *Node

	version   uint64
	isDeleted *bool

	object unsafe.Pointer
}

type SingleLinkedList struct {
	head *Node
	//head *Node
}

// insert atomically appends to the Linked list.
func (ll *SingleLinkedList) insert(newVersion uint64, data unsafe.Pointer) *Node {
	currentHead := ll.head
	//currentHead := ll.tai
	isDeleted := false

	if currentHead == nil || newVersion > currentHead.version {
		newLLNode := &Node{
			version:   newVersion,
			isDeleted: &isDeleted,
			next:      currentHead,
			//prev:      currentHead,

			object: data,
		}

		if !atomicallyCASNodes(
			(*unsafe.Pointer)(unsafe.Pointer(&ll.head)),
			unsafe.Pointer(currentHead),
			unsafe.Pointer(newLLNode),
		) {
			// keep retrying until you succeed. duration - 1000 ns.
			time.Sleep(1000)
			return ll.insert(newVersion, data)
		}

		return newLLNode
	}

	currentCursor := ll.head

	for {
		if currentCursor.next == nil || newVersion > currentCursor.next.version {
			nextNode := currentCursor.next

			// if nextNode is deleted then ignore even if its version is in the future.
			if nextNode != nil && *nextNode.isDeleted {
				continue
			}

			newLLNode := &Node{
				version:   newVersion,
				isDeleted: &isDeleted,
				next:      nextNode,
				//prev:      currentHead,

				object: data,
			}

			if !atomicallyCASNodes(
				(*unsafe.Pointer)(unsafe.Pointer(&currentCursor.next)),
				unsafe.Pointer(nextNode),
				unsafe.Pointer(newLLNode),
			) {
				return ll.insert(newVersion, data)
			}

			return newLLNode
		}

		currentCursor = currentCursor.next
	}
}

func (ll *SingleLinkedList) delete(targetVersion uint64) {
	var prevNode *Node
	currentHead := ll.head
	currentCursor := ll.head
	t := new(bool)
	*t = true

	for {
		if currentCursor == nil {
			break
		}

		if currentCursor.version == targetVersion {
			if !atomicallyCASNodes(
				(*unsafe.Pointer)(unsafe.Pointer(&currentCursor.isDeleted)),
				unsafe.Pointer(currentCursor.isDeleted),
				unsafe.Pointer(t),
			) {

				// if not successful retry
				ll.delete(targetVersion)
				return
			}

			rt := false

			if prevNode != nil {
				rt = atomicallyCASNodes(
					(*unsafe.Pointer)(unsafe.Pointer(&(prevNode.next))),
					unsafe.Pointer(prevNode.next),
					unsafe.Pointer(currentCursor.next),
				)
			} else {
				// prevNode is null, then currentCursor is pointing to the head.
				rt = atomicallyCASNodes(
					(*unsafe.Pointer)(unsafe.Pointer(&currentHead)),
					unsafe.Pointer(currentHead),
					unsafe.Pointer(currentCursor.next),
				)
			}

			if !rt {
				// if unsuccessfully cas'd, then retry.
				time.Sleep(1000)
				ll.delete(targetVersion)
			}

			break
		}

		prevNode = currentCursor
		currentCursor = currentCursor.next
	}
}

// Head returns the object stored in the first node
func (ll *SingleLinkedList) Head() unsafe.Pointer {
	return ll.head.object
}

// LatestVersion returns the node that has a version equal to
// or less than the version given
func (ll *SingleLinkedList) LatestVersion(v uint64) unsafe.Pointer {
	cur := ll.head
	for cur != nil && cur.version > v {
		cur = cur.next
	}

	if cur == nil {
		return nil
	}

	return cur.object
}

// Snapshot gets the current Snapshot.
// For debugging only
func (ll *SingleLinkedList) Snapshot() (s []uint64) {
	cursor := ll.head

	for cursor != nil {
		if !*cursor.isDeleted {
			s = append(s, cursor.version)
		}

		cursor = cursor.next
	}

	return
}

// Helper functions
func atomicallyCASNodes(targetNodeAddress *unsafe.Pointer, nextExistingLLNode unsafe.Pointer, newLLNode unsafe.Pointer) bool {

	return !atomic.CompareAndSwapPointer(
		targetNodeAddress,
		unsafe.Pointer(nextExistingLLNode),
		unsafe.Pointer(newLLNode),
	)
}
