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
	txn      *MvccTxn
	iter     engine_util.DBIterator
	startKey []byte
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	// Your Code Here (4C).
	scanner := &Scanner{
		txn:      txn,
		iter:     nil,
		startKey: startKey,
	}
	return scanner
}

func (scan *Scanner) Close() {
	// Your Code Here (4C).
	if scan.iter != nil {
		scan.iter.Close()
	}
}

// Next returns the next key/value pair from the scanner. If the scanner is exhausted, then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {
	// Your Code Here (4C).

	if scan.iter == nil {
		// 第一次查找
		scan.iter = scan.txn.Reader.IterCF(engine_util.CfWrite)
		scan.iter.Seek(EncodeKey(scan.startKey, scan.txn.StartTS))
	} else {
		if !scan.iter.Valid() {
			return nil, nil, nil
		}
		// 查找与当前iter所在key不同的下一个key
		nowKey := DecodeUserKey(scan.iter.Item().Key())
		for ; scan.iter.Valid(); scan.iter.Next() {
			item := scan.iter.Item()
			findKey := item.Key()
			// key与开始位置不相同则说明此时的key已经可能可以作为返回值
			if !bytes.Equal(nowKey, DecodeUserKey(findKey)) {
				break
			}
		}
		if !scan.iter.Valid() {
			return nil, nil, nil
		}
	}

	var key []byte
	var value []byte
	// 记录是否后面rollBack了，并保存哪个startTS需要回滚
	shouldRollBack := TsMax
	for ; scan.iter.Valid(); scan.iter.Next() {
		item := scan.iter.Item()
		findKey := item.Key()
		// 未来版本的key
		commitTime := decodeTimestamp(findKey)
		if commitTime > scan.txn.StartTS {
			continue
		}
		// 已找到现有版本的key
		writeByte, err := item.Value()
		if err != nil {
			return nil, nil, err
		}
		write, err1 := ParseWrite(writeByte)
		if err1 != nil {
			return nil, nil, err1
		}
		key = DecodeUserKey(item.Key())
		if write.Kind == WriteKindRollback {
			// rollback了，继续往前寻找，并忽略同startTs的结果
			shouldRollBack = write.StartTS
			continue
		} else if write.StartTS == shouldRollBack {
			// rollback了,忽略本次查找结果
			continue
		} else if write.Kind == WriteKindDelete {
			// key的value已删除，应该忽略该key，退出重新开始找
			value = nil
			break
		} else if write.Kind == WriteKindPut {
			// 去default中查找value
			value, err = scan.txn.Reader.GetCF(engine_util.CfDefault, EncodeKey(key, write.StartTS))
			if err != nil {
				return nil, nil, err
			}
			break
		}
	}
	return key, value, nil
}
