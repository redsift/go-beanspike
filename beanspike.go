package beanspike

import (
	"fmt"
	"os"
	"strconv"
	"sync"
	"sync/atomic"

	as "github.com/aerospike/aerospike-client-go"
)

var defaultPolicy = &as.WritePolicy{DurableDelete: true}

type Conn struct {
	aerospike    *as.Client
	clientID     string
	statsHandler func(string, string, float64)
}

func (conn *Conn) stats(event, tube string, count float64) {
	if conn.statsHandler != nil {
		conn.statsHandler(event, tube, count)
	}
}

type Tube struct {
	Conn *Conn
	Name string
	once *sync.Once
}

type Stats struct {
	Jobs        int
	Ready       int
	Buried      int
	Delayed     int
	Reserved    int
	Deleted     int
	JobSize     int
	UsedSize    int
	SkippedSize int
}

func DialDefault(statsHandler func(string, string, float64)) (*Conn, error) {
	host := AerospikeHost
	port := AerospikePort

	if h := os.Getenv(AerospikeHostEnv); h != "" {
		host = h
	}

	if p := os.Getenv(AerospikePortEnv); p != "" {
		var err error
		port, err = strconv.Atoi(p)
		if err != nil {
			return nil, err
		}
	}

	return Dial("", host, port, statsHandler)
}

func Dial(id string, host string, port int, statsHandler func(string, string, float64)) (*Conn, error) {
	if id == "" {
		// generate a default Id
		id = genID()
	}

	policy := as.NewClientPolicy()
	policy.LimitConnectionsToQueueSize = true

	client, err := as.NewClientWithPolicy(policy, host, port)

	if err != nil {
		return nil, err
	}

	err = registerUDFs(client)
	if err != nil {
		return nil, err
	}

	return &Conn{
		aerospike:    client,
		clientID:     id,
		statsHandler: statsHandler,
	}, nil

}

var instanceCount int32

func genID() string {
	count := atomic.AddInt32(&instanceCount, 1)

	pid := os.Getpid()

	if host, err := os.Hostname(); err == nil {
		return fmt.Sprintf("%v:%v:%v", host, pid, count)
	}
	return fmt.Sprintf("????????:%v:%v", pid, count)
}

/*var portRE = regexp.MustCompile(`^tcp:\/\/(\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b):(\d{1,5})$`)

// Parse port check is the port var is actually a Docker ENV
// as this can easily happen
func parsePort(portStr string) (host string, port int, err error) {
	match := portRE.FindAllStringSubmatch(portStr, -1)
	if match != nil {
		// Docker style port ENV
		host = match[0][1]
		portStr = match[0][2]
	}

	port, err = strconv.Atoi(portStr)
	return host, port, err
}*/
