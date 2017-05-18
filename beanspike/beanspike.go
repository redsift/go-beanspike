package beanspike

import (
	"fmt"
	"os"
	"strconv"
	"sync/atomic"

	as "github.com/aerospike/aerospike-client-go"
)

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
	Conn  *Conn
	Name  string
	first bool
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
	client, err := as.NewClient(host, port)

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
