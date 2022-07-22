package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"

	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4A/4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// Your Code Here (4B).
	resp := &kvrpcpb.GetResponse{}
	startVersion := req.Version

	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}

	txn := mvcc.NewMvccTxn(reader, startVersion)
	keys := [][]byte{req.Key}

	server.Latches.WaitForLatches(keys)
	defer server.Latches.ReleaseLatches(keys)

	// 1.检查[0，startVersion]之间的锁
	lock, _ := txn.GetLock(req.Key)
	if lock != nil {
		if lock.Ts < startVersion { // startVersion之前的事务尚未提交，仍保持着锁，需要等待其完成
			resp.Error = &kvrpcpb.KeyError{Locked: lock.Info(req.Key)}
			return resp, nil
		}
	}
	// 2.获取值
	value, err := txn.GetValue(req.Key)
	resp.Value = value
	if value == nil {
		resp.NotFound = true
	}
	return resp, err
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	startVersion := req.StartVersion
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	txn := mvcc.NewMvccTxn(reader, startVersion)
	keys := [][]byte{}
	for _, mutation := range req.Mutations {
		keys = append(keys, mutation.Key)
	}
	server.Latches.WaitForLatches(keys)
	defer server.Latches.ReleaseLatches(keys)
	for _, mutation := range req.Mutations {
		_, ts, _ := txn.MostRecentWrite(mutation.Key)
		if ts > startVersion { // writeTs>startTs,有一个startTs之后的txn提交了，写写冲突
			return &kvrpcpb.PrewriteResponse{Errors: []*kvrpcpb.KeyError{&kvrpcpb.KeyError{Conflict: &kvrpcpb.
				WriteConflict{StartTs: startVersion, ConflictTs: ts, Key: mutation.GetKey(), Primary: req.PrimaryLock}}}}, nil
		}
		lock, _ := txn.GetLock(mutation.Key)
		if lock != nil && lock.Ts != startVersion { // 已经被其他事务preWrite上锁了
			return &kvrpcpb.PrewriteResponse{Errors: []*kvrpcpb.KeyError{&kvrpcpb.KeyError{Locked: lock.Info(mutation.Key)}}}, nil
		}
		txn.PutValue(mutation.Key, mutation.Value)
		lock = &mvcc.Lock{
			Primary: req.PrimaryLock,
			Ts:      startVersion,
			Ttl:     req.LockTtl}
		switch mutation.Op {
		case kvrpcpb.Op_Put:
			lock.Kind = mvcc.WriteKindPut
		case kvrpcpb.Op_Del:
			lock.Kind = mvcc.WriteKindDelete
		case kvrpcpb.Op_Rollback:
			lock.Kind = mvcc.WriteKindRollback
		}
		txn.PutLock(mutation.Key, lock)
	}
	server.storage.Write(req.Context, txn.Writes())

	return &kvrpcpb.PrewriteResponse{}, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	//server.Latches.AcquireLatches(req.Keys)
	startVersion := req.StartVersion
	server.Latches.WaitForLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)

	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	txn := mvcc.NewMvccTxn(reader, startVersion)
	for _, key := range req.Keys {
		lock, _ := txn.GetLock(key)
		currentWrite, _, _ := txn.CurrentWrite(key)
		if lock == nil { //没有锁了,commit or rollback
			//if currentWrite == nil || currentWrite.Kind == mvcc.WriteKindRollback {
			if currentWrite != nil && currentWrite.Kind == mvcc.WriteKindRollback {
				// rollback 返回错误
				return &kvrpcpb.CommitResponse{Error: &kvrpcpb.KeyError{}}, nil
			}
			return &kvrpcpb.CommitResponse{}, nil // 已经commit了
		}
		if lock.Ts != startVersion {
			// 不是自己的lock，说明还是被rollback了
			return &kvrpcpb.CommitResponse{Error: &kvrpcpb.KeyError{}}, nil
		}
		// 正常commit
		write := &mvcc.Write{
			StartTS: startVersion,
			Kind:    lock.Kind,
		}
		txn.PutWrite(key, req.CommitVersion, write)
		txn.DeleteLock(key)
	}
	server.storage.Write(req.Context, txn.Writes())
	return &kvrpcpb.CommitResponse{}, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.ScanResponse{Pairs: []*kvrpcpb.KvPair{}}
	startVersion := req.Version

	reader, err := server.storage.Reader(req.Context)
	txn := mvcc.NewMvccTxn(reader, startVersion)

	if err != nil {
		return resp, err
	}

	scanner := mvcc.NewScanner(req.StartKey, txn)
	defer scanner.Close()
	key, value, err := scanner.Next()

	for !(key == nil && value == nil && err == nil) && uint32(len(resp.Pairs)) < req.Limit {
		pair := &kvrpcpb.KvPair{Key: key, Value: value}
		// 盘算key是否有锁
		lock, _ := txn.GetLock(key)
		if lock != nil && lock.Ts != startVersion {
			pair.Error = &kvrpcpb.KeyError{Locked: lock.Info(key)}
			pair.Key = nil
			pair.Value = nil
		}
		resp.Pairs = append(resp.Pairs, pair)
		key, value, err = scanner.Next()
	}

	return resp, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).

	// CheckTxnStatus reports on the status of a transaction and may take action to
	// rollback expired locks.
	// If the transaction has previously been rolled back or committed, return that information.
	// If the TTL of the transaction is exhausted, abort that transaction and roll back the primary lock.
	// Otherwise, returns the TTL information.
	resp := &kvrpcpb.CheckTxnStatusResponse{}
	startVersion := req.LockTs
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	primaryKey := req.PrimaryKey
	server.Latches.WaitForLatches([][]byte{primaryKey})
	defer server.Latches.ReleaseLatches([][]byte{primaryKey})
	txn := mvcc.NewMvccTxn(reader, startVersion)

	lock, _ := txn.GetLock(primaryKey)
	if lock == nil || lock.Ts != startVersion {
		// 没有锁或者锁是其他事务的，可能被rollback or commit了
		write, ts, _ := txn.CurrentWrite(primaryKey)
		if write == nil {
			// do rollback  ---> commitTs=lockTs
			txn.PutWrite(primaryKey, startVersion, &mvcc.Write{StartTS: startVersion, Kind: mvcc.WriteKindRollback})
			server.storage.Write(req.Context, txn.Writes())
			resp.LockTtl, resp.CommitVersion, resp.Action = 0, 0, kvrpcpb.Action_LockNotExistRollback
			return resp, nil
		} else if write.Kind == mvcc.WriteKindRollback {
			// already rollback
			resp.LockTtl, resp.CommitVersion, resp.Action = 0, 0, kvrpcpb.Action_NoAction
			return resp, nil
		} else {
			// already commit
			resp.LockTtl, resp.CommitVersion, resp.Action = 0, ts, kvrpcpb.Action_NoAction
			return resp, nil
		}
	} else {
		// 盘算ttl是否过期
		if mvcc.PhysicalTime(req.CurrentTs)-mvcc.PhysicalTime(req.LockTs) >= lock.Ttl {
			// do rollback
			txn.DeleteValue(primaryKey)
			txn.DeleteLock(primaryKey)
			txn.PutWrite(primaryKey, startVersion, &mvcc.Write{StartTS: startVersion, Kind: mvcc.WriteKindRollback})
			server.storage.Write(req.Context, txn.Writes())
			resp.LockTtl, resp.CommitVersion, resp.Action = 0, 0, kvrpcpb.Action_TTLExpireRollback
			return resp, nil
		} else {
			resp.LockTtl, resp.CommitVersion, resp.Action = lock.Ttl, 0, kvrpcpb.Action_NoAction
			return resp, nil
		}
	}
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	// KvBatchRollback checks that a key is locked by the current transaction, and if so removes the lock,
	// deletes any value and leaves a rollback indicator as a write.

	// Rollback an un-committed transaction. Will fail if the transaction has already
	// been committed or keys are locked by a different transaction. If the keys were never
	// locked, no action is needed but it is not an error.  If successful all keys will be
	// unlocked and all uncommitted values removed.
	resp := &kvrpcpb.BatchRollbackResponse{}
	startVersion := req.StartVersion
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	server.Latches.WaitForLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)
	txn := mvcc.NewMvccTxn(reader, startVersion)

	for _, key := range req.Keys {
		lock, _ := txn.GetLock(key)
		if lock == nil || lock.Ts != startVersion {
			// no action is needed but it is not an error
			write, _, _ := txn.CurrentWrite(key)
			if write == nil {
				txn.PutWrite(key, startVersion, &mvcc.Write{StartTS: startVersion, Kind: mvcc.WriteKindRollback})
			} else if write.Kind != mvcc.WriteKindRollback {
				// already commit
				resp.Error = &kvrpcpb.KeyError{}
				return resp, nil
			}
			continue
		}
		// key is locked by the current transaction，
		// deletes any value and leaves a rollback indicator as a write
		txn.DeleteValue(key)
		txn.DeleteLock(key)
		txn.PutWrite(key, startVersion, &mvcc.Write{StartTS: startVersion, Kind: mvcc.WriteKindRollback})
	}
	server.storage.Write(req.Context, txn.Writes())
	return resp, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).

	// Resolve lock will find all locks belonging to the transaction with the given start timestamp.
	// If commit_version is 0, TinyKV will rollback all locks. If commit_version is greater than
	// 0 it will commit those locks with the given commit timestamp.
	// The client will make a resolve lock request for all secondary keys once it has successfully
	// committed or rolled back the primary key.

	// Response==> Empty if the lock is resolved successfully.
	resp := &kvrpcpb.ResolveLockResponse{}
	startVersion := req.StartVersion
	reader, _ := server.storage.Reader(req.Context)
	txn := mvcc.NewMvccTxn(reader, startVersion)

	iterCF := reader.IterCF(engine_util.CfLock)
	defer iterCF.Close()

	for ; iterCF.Valid(); iterCF.Next() {
		item := iterCF.Item()
		value, _ := item.Value()
		lock, _ := mvcc.ParseLock(value)
		if lock.Ts == startVersion {
			key := item.Key()
			if req.CommitVersion > 0 {
				// commit
				txn.PutWrite(key, req.CommitVersion, &mvcc.Write{StartTS: startVersion, Kind: lock.Kind})
				txn.DeleteLock(key)
			} else if req.CommitVersion == 0 {
				// rollback
				txn.DeleteValue(key)
				txn.DeleteLock(key)
				txn.PutWrite(key, startVersion, &mvcc.Write{StartTS: startVersion, Kind: mvcc.WriteKindRollback})
			}
		}
	}
	server.storage.Write(req.Context, txn.Writes())
	return resp, nil
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}
