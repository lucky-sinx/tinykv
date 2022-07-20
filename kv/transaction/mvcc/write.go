package mvcc

import (
	"encoding/binary"
	"fmt"

	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// Write is a representation of a committed write to backing storage.
// A serialized version is stored in the "write" CF of our engine when a write is committed. That allows MvccTxn to find
// the status of a key at a given timestamp.
// The write CF is accessed using the user key and the commit timestamp of the transaction in which it was written; it stores a Write data structure
// Write 记录有一种类型是 Rollback。这种记录用于标记被回滚的事务，其 commit_ts 被设为与 start_ts 相同。这一做法是 Percolator 论文中没有提到的。
//这样，如果在 rollback 之后收到同一个事务的 prewrite，则会由于 prewrite 的这部分代码而直接返回错误（写写冲突）
type Write struct {
	StartTS uint64
	Kind    WriteKind
}

func (wr *Write) ToBytes() []byte {
	buf := append([]byte{byte(wr.Kind)}, 0, 0, 0, 0, 0, 0, 0, 0)
	binary.BigEndian.PutUint64(buf[1:], wr.StartTS)
	return buf
}

func ParseWrite(value []byte) (*Write, error) {
	if value == nil {
		return nil, nil
	}
	if len(value) != 9 {
		return nil, fmt.Errorf("mvcc/write/ParseWrite: value is incorrect length, expected 9, found %d", len(value))
	}
	kind := value[0]
	startTs := binary.BigEndian.Uint64(value[1:])

	return &Write{startTs, WriteKind(kind)}, nil
}

type WriteKind int

const (
	WriteKindPut      WriteKind = 1
	WriteKindDelete   WriteKind = 2
	WriteKindRollback WriteKind = 3
)

func (wk WriteKind) ToProto() kvrpcpb.Op {
	switch wk {
	case WriteKindPut:
		return kvrpcpb.Op_Put
	case WriteKindDelete:
		return kvrpcpb.Op_Del
	case WriteKindRollback:
		return kvrpcpb.Op_Rollback
	}

	return -1
}

func WriteKindFromProto(op kvrpcpb.Op) WriteKind {
	switch op {
	case kvrpcpb.Op_Put:
		return WriteKindPut
	case kvrpcpb.Op_Del:
		return WriteKindDelete
	case kvrpcpb.Op_Rollback:
		return WriteKindRollback
	default:
		panic("unsupported type")
	}
}
