package tree

import (
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

func (node BNode) setPtr(index uint16, val uint64)
