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
	reader, err := server.storage.Reader(nil)

	if err == nil {
		var value []byte
		value, err = reader.GetCF(req.GetCf(), req.GetKey())
		// 未报错并且找到了value
		if err == nil && value != nil {
			return &kvrpcpb.RawGetResponse{
				Value:    value,
				NotFound: false,
			}, err
		}
	}
	return &kvrpcpb.RawGetResponse{
		Value:    nil,
		NotFound: true,
	}, err
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified

	err := server.storage.Write(nil, []storage.Modify{
		{
			Data: storage.Put{
				Cf:    req.GetCf(),
				Key:   req.GetKey(),
				Value: req.GetValue(),
			},
		},
	})
	return &kvrpcpb.RawPutResponse{}, err
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	err := server.storage.Write(nil, []storage.Modify{
		{
			Data: storage.Delete{
				Cf:  req.GetCf(),
				Key: req.GetKey(),
			},
		},
	})
	return &kvrpcpb.RawDeleteResponse{}, err
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	reader, err := server.storage.Reader(nil)
	if err != nil {
		return &kvrpcpb.RawScanResponse{}, err
	}
	iter := reader.IterCF(req.GetCf())
	iter.Seek(req.GetStartKey())
	cnt := req.GetLimit()
	res := &kvrpcpb.RawScanResponse{}
	res.Kvs = make([]*kvrpcpb.KvPair, 0)
	//res.Kvs = []*kvrpcpb.KvPair{}
	for cnt != 0 && iter.Valid() {
		item := iter.Item()
		value, _ := item.Value()
		res.Kvs = append(res.Kvs, &kvrpcpb.KvPair{
			Key:   item.Key(),
			Value: value,
		})
		iter.Next()
		cnt--
	}
	return res, err

}
