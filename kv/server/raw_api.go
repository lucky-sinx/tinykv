package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	response := &kvrpcpb.RawGetResponse{}
	reader, err := server.storage.Reader(nil)
	if err != nil {

	}
	value, err := reader.GetCF(req.GetCf(), req.GetKey())
	if value == nil {
		response.NotFound = true
	}
	response.Value = value
	return response, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified

	//return nil, nil
	server.storage.Write(nil, []storage.Modify{
		{
			Data: storage.Put{
				Cf:    req.Cf,
				Key:   req.Key,
				Value: req.Value,
			},
		},
	})
	return nil, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	server.storage.Write(nil, []storage.Modify{
		{
			Data: storage.Delete{
				Cf:  req.Cf,
				Key: req.Key,
			},
		},
	})
	return nil, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	reader, err := server.storage.Reader(nil)
	resp := &kvrpcpb.RawScanResponse{}
	resp.Kvs = []*kvrpcpb.KvPair{}

	if err != nil {
		return resp, err
	}
	iter := reader.IterCF(req.GetCf())
	i := uint32(0)
	for iter.Seek(req.StartKey); iter.Valid() && i < req.Limit; iter.Next() {
		item := iter.Item()
		value, _ := item.Value()
		resp.Kvs = append(resp.Kvs, &kvrpcpb.KvPair{
			Key:   item.Key(),
			Value: value,
		})
		i++
	}
	iter.Close()
	return resp, nil
}
