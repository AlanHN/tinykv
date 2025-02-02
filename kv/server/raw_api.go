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
	reader, err := server.storage.Reader(req.Context)
	res := &kvrpcpb.RawGetResponse{}
	if err != nil {
		return res, err
	}
	value, err := reader.GetCF(req.Cf, req.Key)
	if err != nil {
		return res, err
	}
	if value != nil {
		res.Value = value
		res.NotFound = false
	} else {
		res.NotFound = true
	}
	return res, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	res := &kvrpcpb.RawPutResponse{}
	put := storage.Put{
		Key:   req.Key,
		Value: req.Value,
		Cf:    req.Cf,
	}
	batch := storage.Modify{Data: put}

	err := server.storage.Write(req.Context, []storage.Modify{batch})
	if err != nil {
		return res, err
	}
	return res, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	res := &kvrpcpb.RawDeleteResponse{}
	del := storage.Delete{
		Key: req.Key,
		Cf:  req.Cf,
	}
	batch := storage.Modify{Data: del}

	err := server.storage.Write(req.Context, []storage.Modify{batch})
	if err != nil {
		return res, err
	}
	return res, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	res := &kvrpcpb.RawScanResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return &kvrpcpb.RawScanResponse{}, err
	}
	iter := reader.IterCF(req.Cf)
	iter.Seek(req.StartKey)
	var kvPairs []*kvrpcpb.KvPair
	limit := req.Limit
	for ; iter.Valid(); iter.Next() {
		item := iter.Item()
		val, _ := item.Value()
		kvPairs = append(kvPairs, &kvrpcpb.KvPair{
			Key:   item.Key(),
			Value: val,
		})

		limit--
		if limit == 0 {
			break
		}
	}
	res.Kvs = kvPairs
	return res, nil
}
