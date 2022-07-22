package mvcc

import (
	"bytes"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

// Scanner is used for reading multiple sequential key/value pairs from the storage layer. It is aware of the implementation
// of the storage layer and returns results suitable for users.
// Invariant: either the scanner is finished and cannot be used, or it is ready to return a value immediately.
type Scanner struct {
	// Your Data Here (4C).

	StartKey []byte
	Txn      *MvccTxn
	Iter     engine_util.DBIterator
	LastKey  []byte
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	// Your Code Here (4C).
	//return nil
	scanner := &Scanner{Txn: txn, StartKey: startKey, LastKey: nil}
	scanner.Iter = txn.Reader.IterCF(engine_util.CfWrite)
	scanner.Iter.Seek(EncodeKey(startKey, txn.StartTS))
	return scanner
}

func (scan *Scanner) Close() {
	// Your Code Here (4C).
	scan.Iter.Close()
}

// Next returns the next key/value pair from the scanner. If the scanner is exhausted, then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {
	// Your Code Here (4C).
	if !scan.Iter.Valid() {
		return nil, nil, nil
	}
	// 寻找一个与lastKey不同的key
	for bytes.Equal(DecodeUserKey(scan.Iter.Item().Key()), scan.LastKey) {
		scan.Iter.Next()
		if !scan.Iter.Valid() {
			return nil, nil, nil
		}
	}
	// 找到了新的key,继续寻找这个key对应的合适版本
	var key []byte
	var value []byte
	scan.LastKey = DecodeUserKey(scan.Iter.Item().Key())
	for scan.Iter.Valid() {
		item := scan.Iter.Item()
		key = DecodeUserKey(scan.Iter.Item().Key())
		if !bytes.Equal(key, scan.LastKey) {
			scan.LastKey = key
		}
		ts := decodeTimestamp(scan.Iter.Item().Key())
		if ts <= scan.Txn.StartTS {
			wByte, _ := item.Value()
			write, _ := ParseWrite(wByte)
			if write.Kind == WriteKindPut {
				value, _ = scan.Txn.Reader.GetCF(engine_util.CfDefault, EncodeKey(key, write.StartTS))
				break
			} else if write.Kind == WriteKindDelete {
				value = nil
				return scan.Next() //这个key被删除了，重新调用找下一个Pair
			} else if write.Kind == WriteKindRollback {

			}
		}
		scan.Iter.Next()
	}

	return key, value, nil
}
