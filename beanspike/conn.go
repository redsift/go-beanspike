package beanspike

import (
	"fmt"
	"errors"
	"strings"
	as "github.com/aerospike/aerospike-client-go"
	ast "github.com/aerospike/aerospike-client-go/types"
)

func (conn *Conn) newJobId() (int64, error)  {
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
		return nil, errors.New(fmt.Sprintf("Tube name %v is reserved", name))
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
	
	tube := new(Tube)
	tube.Conn = conn
	tube.Name = name
		
	return tube, nil
}

// Any Tubes that reference this name
// should be discarded after this operation
func (conn *Conn) Delete(name string) (error) {
	if conn == nil {
		return errors.New("Aerospike connection not established")	
	}
	
	client := conn.aerospike
	
	// No Truncate for sets yet, scan and purge
	recordset, err := client.ScanAll(nil, AerospikeNamespace, name)

	if err != nil {
		return err
	}	
	
	defer recordset.Close()

	for res := range recordset.Results() {
		if res.Err != nil {
			return res.Err
		}
		key := res.Record.Key
		
		_, err = client.Delete(nil, key)
		if err != nil {
			return err
		}
	}
	
	err = client.DropIndex(nil, AerospikeNamespace, name, "idx_tube_"+name+"_"+AerospikeNameStatus)
	
	if err != nil {
		return err
	}	
		
	return nil
}
