package mvcc

import (
	"bytes"
	"encoding/binary"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/codec"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/tsoutil"
)

// KeyError is a wrapper type so we can implement the `error` interface.
type KeyError struct {
	kvrpcpb.KeyError
}

func (ke *KeyError) Error() string {
	return ke.String()
}

// MvccTxn groups together writes as part of a single transaction. It also provides an abstraction over low-level
// storage, lowering the concepts of timestamps, writes, and locks into plain keys and values.
type MvccTxn struct {
	StartTS uint64
	Reader  storage.StorageReader
	writes  []storage.Modify
}

func NewMvccTxn(reader storage.StorageReader, startTs uint64) *MvccTxn {
	return &MvccTxn{
		Reader:  reader,
		StartTS: startTs,
	}
}

// Writes returns all changes added to this transaction.
func (txn *MvccTxn) Writes() []storage.Modify {
	return txn.writes
}

// PutWrite records a write at key and ts.
func (txn *MvccTxn) PutWrite(key []byte, ts uint64, write *Write) {
	// Your Code Here (4A).
	// 记录Write操作，标记key在ts这个commit timestamp提交
	// The write CF is accessed using the user key and the commit timestamp of the transaction in which it was written; it stores a Write data structure
	txn.writes = append(txn.writes, storage.Modify{Data: storage.Put{
		Key:   EncodeKey(key, ts),
		Value: write.ToBytes(),
		Cf:    engine_util.CfWrite,
	}})

}

// GetLock returns a lock if key is locked. It will return (nil, nil) if there is no lock on key, and (nil, err)
// if an error occurs during lookup.
func (txn *MvccTxn) GetLock(key []byte) (*Lock, error) {
	// Your Code Here (4A).
	value, err := txn.Reader.GetCF(engine_util.CfLock, key)
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, nil
	}
	lock, err1 := ParseLock(value)
	return lock, err1
}

// PutLock adds a key/lock to this transaction.
func (txn *MvccTxn) PutLock(key []byte, lock *Lock) {
	// Your Code Here (4A).
	// 记录Lock操作
	// Lock The lock CF is accessed using the user key; it stores a serialized Lock data structure
	txn.writes = append(txn.writes, storage.Modify{Data: storage.Put{
		Key:   key,
		Value: lock.ToBytes(),
		Cf:    engine_util.CfLock,
	}})
	//EncodeKey(key,txn.StartTS)
}

// DeleteLock adds a delete lock to this transaction.
func (txn *MvccTxn) DeleteLock(key []byte) {
	// Your Code Here (4A).
	txn.writes = append(txn.writes, storage.Modify{Data: storage.Delete{
		Key: key,
		Cf:  engine_util.CfLock,
	}})
}

// GetValue finds the value for key, valid at the start timestamp of this transaction.
// I.e., the most recent value committed before the start of this transaction.
func (txn *MvccTxn) GetValue(key []byte) ([]byte, error) {
	// Your Code Here (4A).
	// 查找已经commit的key中timestamp最大的value，存储中是按照timestamp从大到小排序的
	writeIter := txn.Reader.IterCF(engine_util.CfWrite)
	defer writeIter.Close()
	writeIter.Seek(EncodeKey(key, txn.StartTS))

	var value []byte
	// 记录是否后面rollBack了，并保存哪个startTS需要回滚
	shouldRollBack := TsMax
	for ; writeIter.Valid(); writeIter.Next() {
		item := writeIter.Item()
		findKey := item.Key()
		// 找到了commit的key
		if bytes.Equal(key, DecodeUserKey(findKey)) {
			writeByte, err := item.Value()
			if err != nil {
				return nil, err
			}
			write, err1 := ParseWrite(writeByte)
			if err1 != nil {
				return nil, err1
			}
			if write.Kind == WriteKindRollback {
				// rollback了，继续往前寻找，并忽略同startTs的结果
				shouldRollBack = write.StartTS
				continue
			} else if write.StartTS == shouldRollBack {
				// rollback了,忽略本次查找结果
				continue
			} else if write.Kind == WriteKindDelete {
				// key的value已删除
				value = nil
				break
			} else if write.Kind == WriteKindPut {
				// 去default中查找value
				value, err = txn.Reader.GetCF(engine_util.CfDefault, EncodeKey(key, write.StartTS))
				if err != nil {
					return nil, err
				}
				break
			}
		} else {
			break
		}

	}
	return value, nil
}

// PutValue adds a key/value write to this transaction.
func (txn *MvccTxn) PutValue(key []byte, value []byte) {
	// Your Code Here (4A).
	// 记录preWrite写的value
	// The default CF is accessed using the user key and the start timestamp of the transaction in which it was written; it stores the user value only.
	txn.writes = append(txn.writes, storage.Modify{Data: storage.Put{
		Key:   EncodeKey(key, txn.StartTS),
		Value: value,
		Cf:    engine_util.CfDefault,
	}})
}

// DeleteValue removes a key/value pair in this transaction.
func (txn *MvccTxn) DeleteValue(key []byte) {
	// Your Code Here (4A).
	txn.writes = append(txn.writes, storage.Modify{Data: storage.Delete{
		Key: EncodeKey(key, txn.StartTS),
		Cf:  engine_util.CfDefault,
	}})
}

// CurrentWrite searches for a write with this transaction's start timestamp. It returns a Write from the DB and that
// write's commit timestamp, or an error.
func (txn *MvccTxn) CurrentWrite(key []byte) (*Write, uint64, error) {
	// Your Code Here (4A).
	iter := txn.Reader.IterCF(engine_util.CfWrite)
	defer iter.Close()
	iter.Seek(EncodeKey(key, TsMax))

	for ; iter.Valid(); iter.Next() {
		item := iter.Item()
		findKey := item.Key()
		if !bytes.Equal(DecodeUserKey(findKey), key) {
			break
		}
		writeByte, err := item.Value()
		if err != nil {
			return nil, 0, err
		}
		write, err1 := ParseWrite(writeByte)
		if err1 != nil {
			return nil, 0, err1
		}
		if write.StartTS == txn.StartTS {
			return write, decodeTimestamp(findKey), nil
		} else if decodeTimestamp(findKey) < txn.StartTS {
			// commit timestamp都小了，start timestamp肯定更小
			break
		}
	}
	return nil, 0, nil
}

// MostRecentWrite finds the most recent write with the given key. It returns a Write from the DB and that
// write's commit timestamp, or an error.
func (txn *MvccTxn) MostRecentWrite(key []byte) (*Write, uint64, error) {
	// Your Code Here (4A).
	iter := txn.Reader.IterCF(engine_util.CfWrite)
	defer iter.Close()
	iter.Seek(EncodeKey(key, TsMax))

	for ; iter.Valid(); iter.Next() {
		item := iter.Item()
		findKey := item.Key()
		if !bytes.Equal(DecodeUserKey(findKey), key) {
			break
		}
		writeByte, err := item.Value()
		if err != nil {
			return nil, 0, err
		}
		write, err1 := ParseWrite(writeByte)
		if err1 != nil {
			return nil, 0, err1
		}
		return write, decodeTimestamp(findKey), nil
	}
	return nil, 0, nil
}

// EncodeKey encodes a user key and appends an encoded timestamp to a key. Keys and timestamps are encoded so that
// timestamped keys are sorted first by key (ascending), then by timestamp (descending). The encoding is based on
// https://github.com/facebook/mysql-5.6/wiki/MyRocks-record-format#memcomparable-format.
func EncodeKey(key []byte, ts uint64) []byte {
	encodedKey := codec.EncodeBytes(key)
	newKey := append(encodedKey, make([]byte, 8)...)
	binary.BigEndian.PutUint64(newKey[len(encodedKey):], ^ts)
	return newKey
}

// DecodeUserKey takes a key + timestamp and returns the key part.
func DecodeUserKey(key []byte) []byte {
	_, userKey, err := codec.DecodeBytes(key)
	if err != nil {
		panic(err)
	}
	return userKey
}

// decodeTimestamp takes a key + timestamp and returns the timestamp part.
func decodeTimestamp(key []byte) uint64 {
	left, _, err := codec.DecodeBytes(key)
	if err != nil {
		panic(err)
	}
	return ^binary.BigEndian.Uint64(left)
}

// PhysicalTime returns the physical time part of the timestamp.
func PhysicalTime(ts uint64) uint64 {
	return ts >> tsoutil.PhysicalShiftBits
}
