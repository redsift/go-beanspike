package beanspike

import (
	"errors"
	"fmt"
	"strings"
	"sync"

	as "github.com/aerospike/aerospike-client-go"
	ast "github.com/aerospike/aerospike-client-go/types"
)

var tubesMap = struct {
	sync.RWMutex
	m map[string]*Tube
}{m: make(map[string]*Tube)}

func (conn *Conn) newJobID() (int64, error) {
	key, err := as.NewKey(AerospikeNamespace, AerospikeMetadataSet, "seq")
	if err != nil {
		return 0, err
	}

	bin := as.NewBin("last", as.NewLongValue(1))
	record, err := conn.aerospike.Operate(as.NewWritePolicy(0, 0), key, as.AddOp(bin), as.GetOp())
	if err != nil {
		return 0, err
	}

	// This type conversion seem required as the value appears to be a
	// int instead of an int64
	id := int64(record.Bins[bin.Name].(int))
	return id, nil
}

// Note, index limits may cause this to fail
func (conn *Conn) Use(name string) (*Tube, error) {
	if conn == nil {
		return nil, errors.New("Aerospike connection not established")
	}

	name = strings.Trim(name, " ")
	if name == "" {
		return nil, errors.New("Tube name must not be blank")
	}

	if name == AerospikeMetadataSet {
		return nil, fmt.Errorf("Tube name %v is reserved", name)
	}

	tubesMap.RLock()
	t := tubesMap.m[name]
	if t != nil {
		tubesMap.RUnlock()
		return t, nil
	}
	tubesMap.RUnlock()

	tubesMap.Lock()
	defer tubesMap.Unlock()

	t = tubesMap.m[name]
	if t != nil {
		return t, nil
	}

	task, err := conn.aerospike.CreateIndex(nil, AerospikeNamespace, name, "idx_tube_"+name+"_"+AerospikeNameStatus, AerospikeNameStatus, as.STRING)
	if err != nil {
		if ae, ok := err.(ast.AerospikeError); ok && ae.ResultCode() == ast.INDEX_FOUND {
			// skipping index creation
			// println("Skipping index creation")
		} else {
			return nil, err
		}
	}

	if task == nil {
		//TODO: Check that this is ok
	} else {
		for ierr := range task.OnComplete() {
			if ierr != nil {
				return nil, ierr
			}
		}
	}

	task1, err := conn.aerospike.CreateIndex(nil, AerospikeNamespace, name, "idx_tube_"+name+"_"+AerospikeNameMetadata, AerospikeNameMetadata, as.STRING)
	if err != nil {
		if ae, ok := err.(ast.AerospikeError); ok && ae.ResultCode() == ast.INDEX_FOUND {
			// skipping index creation
			// println("Skipping index creation")
		} else {
			return nil, err
		}
	}

	if task1 == nil {
		//TODO: Check that this is ok
	} else {
		for ierr := range task1.OnComplete() {
			if ierr != nil {
				return nil, ierr
			}
		}
	}
	tube := &Tube{Conn: conn, Name: name, first: true}

	tubesMap.m[name] = tube
	return tube, nil
}

// Any Tubes that reference this name
// should be discarded after this operation
func (conn *Conn) Delete(name string) error {
	if conn == nil {
		return errors.New("Aerospike connection not established")
	}

	client := conn.aerospike

	// No Truncate for sets yet, scan and purge
	recordset, err := client.ScanAll(nil, AerospikeNamespace, name)

	if err != nil {
		return err
	}

	defer func() {
		recordset.Close()
		tubesMap.Lock()
		delete(tubesMap.m, name)
		tubesMap.Unlock()
	}()

	for res := range recordset.Results() {
		if res.Err != nil {
			return res.Err
		}
		key := res.Record.Key

		// nil out body before deleting record to address aerospike limitations.
		// set status to DELETED
		// Ref: https://discuss.aerospike.com/t/expired-deleted-data-reappears-after-server-is-restarted/470
		policy := as.NewWritePolicy(res.Record.Generation, 1) // set a a small ttl so the record gets evicted
		policy.RecordExistsAction = as.UPDATE_ONLY
		policy.SendKey = true
		policy.CommitLevel = as.COMMIT_MASTER
		policy.GenerationPolicy = as.EXPECT_GEN_EQUAL

		binBody := as.NewBin(AerospikeNameBody, nil)
		binCSize := as.NewBin(AerospikeNameCompressedSize, nil)
		binSize := as.NewBin(AerospikeNameSize, 0)
		binStatus := as.NewBin(AerospikeNameStatus, AerospikeSymDeleted)

		err = client.PutBins(policy, key, binBody, binCSize, binSize, binStatus)
		if err != nil {
			return err
		}

		_, err = client.Delete(nil, key)
		if err != nil {
			return err
		}

		conn.stats("tube.delete.count", name, float64(1))
	}

	tk, _ := as.NewKey(AerospikeNamespace, AerospikeMetadataSet, name+":"+AerospikeKeySuffixTtr)
	client.Delete(nil, tk)

	dk, _ := as.NewKey(AerospikeNamespace, AerospikeMetadataSet, name+":"+AerospikeKeySuffixDelayed)
	client.Delete(nil, dk)

	err = client.DropIndex(nil, AerospikeNamespace, name, "idx_tube_"+name+"_"+AerospikeNameStatus)

	if err != nil {
		return err
	}

	return nil
}
