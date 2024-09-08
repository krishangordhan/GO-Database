package tree

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

const (
	HEADER = 4

	bTreePageSize     = 4096
	bTreeMaxKeySize   = 1000
	bTreeMaxValueSize = 3000

	bNode = 1
	bLeaf = 2
)

type BNode []byte

type BTree struct {
	root uint64
	get  func(uint64) []byte
	new func({}byte) uint64
	del func(uint64)
}

func init() {
	node1Max := HEADER + 8 + 2 + 4 + bTreeMaxKeySize + bTreeMaxValueSize
	if node1Max > bTreePageSize {
		panic(fmt.Sprintf("node1Max %d > BTREE_PAGE_SIZE %d", node1Max, bTreePageSize))
	}
}

func (node BNode) bType() uint16 {
	return binary.LittleEndian.Uint16((node[0:2]))
}

func (node BNode) nKeys() uint16 {
	return binary.LittleEndian.Uint16((node[2:4]))
}

func (node BNode) setHeader(bType uint16, nkeys uint16) {
	binary.LittleEndian.PutUint16(node[0:2], bType)
	binary.LittleEndian.PutUint16(node[2:4], nkeys)
}

func (node BNode) getPointer(index uint16) uint64 {
	assert(index < node.nKeys())
	pos := HEADER + 8 * index
	return binary.LittleEndian.Uint64((node[pos:]))
}

func (node BNode) setPtr(index uint16, val uint64) {
	assert(index < node.nKeys())
	pos := HEADER + 8 * index
	binary.LittleEndian.PutUint64(node[pos:], val)
}

func offsetPos(node BNode, index uint16) uint16 {
	if (1 <= index && index <= node.nKeys()) {
		return nil
	}
	return HEADER + 8 * node.nKeys() + 2 * (index-1)
}

func (node BNode) getOffset(index uint16) uint16 {
	if index == 0 {
		return 0
	}
	return binary.LittleEndian.Uint16((node[offsetPos(node, index):]))
}

func (node BNode) setOffset(index uint16, val uint16) {
	if index == 0 {
		return
	}
	binary.LittleEndian.PutUint16(node[offsetPos(node, index):], val)
}

func (node BNode) kvPos (index uint16) uint16 {
	if (index <= node.nKeys()) {
		return nil
	}
	return HEADER + 8 * node.nKeys() + 2 * node.nKeys() + node.getOffset(index)
}

func (node BNode) getKey(index uint16) []byte {
	if (index < node.nKeys()) {
		return nil
	}
	pos := node.kvPos(index)
	klen := binary.LittleEndian.Uint16((node[pos:]))
	return node[pos+4:][:klen]
}

func (node BNode) getVal (index uint16) []byte {
	if (index < node.nKeys()) {
		return nil
	}
	pos := node.kvPos(index)
	klen := binary.LittleEndian.Uint16((node[pos:]))
	vlen := binary.LittleEndian.Uint16((node[pos+2:]))
	return node[pos+4+klen:][:vlen]
}

func nodeLookupLE(node BNode, key []byte) uint16 {
	nkeys := node.nKeys()
	found := uint16(0)

	for i := uint16(1); i< nkeys; i++ {
		cmp := bytes.Compare(node.getKey(i), key)
		if cmp == 0 {
			found = i
		}
		if cmp >= 0 {
			break
		}
	}
	return found
}

func leafInsert(new BNode, old BNode, index uint16, key []byte, val []byte) {
	new.setHeader(bLeaf, old.nKeys() + 1)
	nodeAppendRange(new, old, 0,0, index)
	nodeAppendLV(new, index, 0, key, val)
	nodeAppendRange(new, old, index+1, index, old.nKeys()-index)
}

func nodeAppendKV (new BNode, index uint16, pointer uint16, key []byte, val []byte) {
	new.setPtr(index, pointer)

	pos := new.kvPos(index)
	binary.LittleEndian.PutUint16(new[pos+0:], uint16(len(key)))
	binary.LittleEndian.PutUint16(new[pos+2:], uint16(len(val)))
	copy(new[pos+4:], key)
	copy(new[pos+4+uint16(len(key)):], val)
	new.setOffset(index+1, new.getOffset(index) + 4 + uint16((len(key) + len(val))))
}

func nodeAppendRange(new BNode, old BNode, dstNew uint16, srcOld uint16, n uint16) {
	for i := uint16(0); i < n; i++ {
		new.setPtr(dstNew+i, old.getPointer(srcOld+i))
	}
}

func nodeReplaceKidN(tree *BTree, new BNode, old BNode, index unit16, kids ...BNode) {
	inc := uint16(len(kids))
	new.setHeader(bNode, old.nKeys() + inc - 1)
	nodeAppendRange(new, old, 0, 0, index)
	for i, node := range kids {
		nodeAppendKV(new, index + uint16(i), tree.new(node), node.getKey(0), nil)
	}

	nodeAppendRange(new, old, index + inc, index + 1, old.nKeys() - (index + 1))
}

func nodeSplit2(left BNode, right BNode, old BNode) {
	left.setHeader(bNode, old.nKeys() / 2)
	right.setHeader(bNode, old.nKeys() - old.nKeys() / 2)

	nodeAppendRange(left, old, 0, 0, left.nKeys())
	nodeAppendRange(right, old, 0, left.nKeys(), right.nKeys())
}

func nodeSplit3(old BNode) (uint16, [3]BNode) {
	if old.nbytes() <= bTreePageSize {
		old = old[:bTreePageSize]
		return 1, [3]BNode[old]
	}
	left := BNode(make([]byte, 2 * bTreePageSize))
	right := bNode(make([]byte,  bTreePageSize))
	nodeSplit2(left, right, old)
	if left.nBytes() <= bTreePageSize {
		left = left[:bTreePageSize]
		return 2, [3]BNode{left, right}
	}

	leftleft := BNode(make([]byte, bTreePageSize))
	middle := BNode(make([]byte, bTreePageSize))
	nodeSplit2(leftleft, middle, left)

	if (leftleft.nBytes() <= bTreePageSize) {
		return nil, nil
	}
	return 3, [3]BNode(leftleft, middle, right)
}

func treeInsert(tree *BTree, node BNode, key []byte, val []byte) BNode {
	new := BNode{data: make([]byte, 2*bTreePageSize)}

	index := nodeLookUpKE(node, key)

	switch node.bType() {
	case bLeaf:
		if bytes.Equal(key, node.getKey(index)) {
			leafUpdate(new, node, index, key, val)
		} else {
			leafInsert(new, node, index, key, val)
		}
		case bNode:
			nodeInsert(tree, new, node, index, key, val)
		default: 
			panic("no good node")
	}
	return new
}

func nodeInsert(tree *BTree, new BNode, node BNode, index uint16, key []byte, val []byte) 
{
	kptr := node.getPointer(index)
	knode := treeInsert(tree, tree.get(kptr), key, val)
	nsplit, split := nodeSplit3(knode)
	tree.del(kptr)
	nodeReplaceKidN(tree, new, node, index, split[:nsplit]...)
}