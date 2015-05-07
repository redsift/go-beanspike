package beanspike

import (
	"os"
	"strconv"
	"errors"
	"fmt"
	"regexp"
	"time"
	"sync/atomic"
	as "github.com/aerospike/aerospike-client-go"
	ast "github.com/aerospike/aerospike-client-go/types"
	pt "github.com/aerospike/aerospike-client-go/types/particle_type"
)

type Conn struct {
	aerospike	*as.Client
	clientId	string
}

// Note, index limits may cause this to fail
func (conn *Conn) Use(name string) (*Tube, error) {
	if name == "" {
		return nil, errors.New("Tube name must not be blank")
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
	
	if task != nil {
		err = <- task.OnComplete() 
		if err != nil {
			return nil, err
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


type Tube struct {
	Conn 	*Conn
	Name 	string
}

type Stats struct {
	Jobs 		int
	Ready		int
}

func (conn *Conn) newJobId() (int64, error)  {
	key, err := as.NewKey(AerospikeNamespace, "jobmetadata", "seq")
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

func (tube *Tube) Put(body []byte, delay time.Duration, ttr time.Duration) (id int64, err error) {
	id, err = tube.Conn.newJobId()
	if err != nil {
		return 0, err
	}
	
	key, err := as.NewKey(AerospikeNamespace, tube.Name, id)
	if err != nil {
		return 0, err
	}

	binBody := as.NewBin(AerospikeNameBody, body)
	binStatus := as.NewBin(AerospikeNameStatus, AerospikeSymReady)

	policy := as.NewWritePolicy(0, 0)
	policy.RecordExistsAction = as.CREATE_ONLY
	policy.SendKey = true
	policy.CommitLevel = as.COMMIT_MASTER
	
	client := tube.Conn.aerospike
	err = client.PutBins(policy, key, binBody, binStatus)

	if err != nil {
		return 0, err
	}
		
	return id, nil
}

func (tube *Tube) ReserveAndWait(bus *MessageBus, timeout time.Duration) (id int64, body []byte, err error) {
	if bus == nil {
		return 0, nil, errors.New("MessageBus cannot be nil for reservation tasks with wait")
	}
	id, body, err = tube.Reserve()
	if err != nil {
		return 0, nil, err
	}
	if id != 0 {
		return id, body, nil
	}
	
	// Wait state
	return 0, nil, nil
}

func (tube *Tube) attemptJobReservation(record *as.Record) (err error) {
	writePolicy := as.NewWritePolicy(int32(record.Generation), 0)
	writePolicy.GenerationPolicy = as.EXPECT_GEN_EQUAL
	writePolicy.RecordExistsAction = as.UPDATE_ONLY
	
	binStatus := as.NewBin(AerospikeNameStatus, AerospikeSymReserved)
	binBy := as.NewBin(AerospikeNameBy, tube.Conn.clientId)
	return tube.Conn.aerospike.PutBins(writePolicy, record.Key, binStatus, binBy)
}

func (tube *Tube) Delete(id int64) (bool, error) {
	key, err := as.NewKey(AerospikeNamespace, tube.Name, id)
	if err != nil {
		return false, err
	}

	return tube.Conn.aerospike.Delete(nil, key)
}


const statsLua = `
local function aggregate_stats(out, rec)
    local val = 0
    if rec['status'] == 'READY' then
    	val = 1
    end
    
    out['count'] = out['count'] + 1
    out['ready'] = out['ready'] + val
    return out
end

function add_stat_ops(stream)
	local m = map()
	m['count'] = 0
	m['ready'] = 0
    return stream : aggregate(m, aggregate_stats)
end
`
func registerUDFs(client* as.Client) (error) {
	regTask, err := client.RegisterUDF(nil, []byte(statsLua), "beanspikeStats.lua", as.LUA)
	if err != nil {
		return err
	}
	err = <-regTask.OnComplete()
	if err != nil {
		return err
	}
	
	return nil
}

func (tube* Tube) Stats() (s *Stats, err error) {
	stm := as.NewStatement(AerospikeNamespace, tube.Name)
	stm.SetAggregateFunction("beanspikeStats", "add_stat_ops", nil, true)	
	
	recordset, err := tube.Conn.aerospike.Query(nil, stm)

	if err != nil {
		return nil, err
	}	
	
	defer recordset.Close()
	
	s = new(Stats)

	for res := range recordset.Results() {
		if res.Err != nil {
			return nil, res.Err
		}
		results := res.Record.Bins["SUCCESS"].(map[interface{}]interface{})
		
		s.Jobs += results["count"].(int)
		s.Ready += results["ready"].(int)
	}

	return s, nil
}

func (tube *Tube) Reserve() (id int64, body []byte, err error) {
	client := tube.Conn.aerospike
	
	stm := as.NewStatement(AerospikeNamespace, tube.Name, AerospikeNameBody)
	stm.Addfilter(as.NewEqualFilter(AerospikeNameStatus, AerospikeSymReady))
	
	policy := as.NewQueryPolicy()
	policy.RecordQueueSize = AerospikeQueryQueueSize

	recordset, err := client.Query(policy, stm)

	if err != nil {
		return 0, nil, err
	}	
	
	defer recordset.Close()
	
	for res := range recordset.Results() {
		if res.Err != nil {
			if err == nil {
				err = res.Err
			}
		} else {
			body := res.Record.Bins[AerospikeNameBody]
			if key := res.Record.Key.Value(); key != nil && key.GetType() == pt.INTEGER && body != nil {
				job := res.Record.Key.Value().GetObject().(int64)
				
				lockErr := tube.attemptJobReservation(res.Record)
				if lockErr == nil {
					// success, we have this job
					return job, body.([]byte), nil		
				}		
				// else something happened to this job in the way
				fmt.Printf("Job lock failed due to %v", lockErr)
			} else {
				// if the key is nil or not an int something is wrong. WritePolicy is not set
				// correctly. Skip this record and set err if this is the only record
				if err == nil {
					err = errors.New("Missing appropriate entry in job tube")
				}
			}
		}
	}
	
	// Some form of error or no job fall through
	return 0, nil, err	
}

// Parse port check is the port var is actually a Docker ENV
// as this can easily happen
func parsePort(portStr string) (host string, port int, err error) {
	host = ""

	r := regexp.MustCompile(`^tcp:\/\/(\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b):(\d{1,5})$`)
	match := r.FindAllStringSubmatch(portStr, -1)
	if match != nil {
		// Docker style port ENV
		host = match[0][1]
		portStr = match[0][2]
	}

	port, err = strconv.Atoi(portStr)

	return host, port, err			
}

var instanceCount int32 = 0

func genId() (string) {
	count := atomic.AddInt32(&instanceCount, 1)
	
	pid := os.Getpid()
	
	if host, err := os.Hostname(); err == nil {
		return fmt.Sprintf("%v:%v:%v", host, pid, count)
	}
	return fmt.Sprintf("????????:%v:%v", pid, count)	
}

func DialDefault() (*Conn, error) {
	var (
		host string
		port int
	)
	
	if host = os.Getenv(AerospikeHostEnv); host == "" {
		host = AerospikeHost
	}

	if portStr := os.Getenv(AerospikePortEnv); portStr != "" {
		dockerHost, envPort, err := parsePort(portStr)
		if err != nil {
			return nil, errors.New(fmt.Sprintf("invalid environment variable %v. %v", AerospikePortEnv, err))
		} else {
			port = envPort
			if dockerHost != "" {
				// note, this overrides AerospikeHostEnv
				host = dockerHost
			}
		}
	} else {
		port = AerospikePort
	}
	
	return Dial("", host, port)
}

func Dial(id string, host string, port int) (*Conn, error) {
	if id == "" {
		// generate a default Id
		id = genId()
	}
	client, err := as.NewClient(host, port)

	if err != nil {
		return nil, err
	}

	err = registerUDFs(client)
	if err != nil {
		return nil, err
	}

	c := new(Conn)
	c.aerospike = client
	c.clientId = id
	
    return c, nil
}
