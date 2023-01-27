package beanspike

import (
	"fmt"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	as "github.com/aerospike/aerospike-client-go"
	"github.com/redsift/go-stats/stats"
)

type Conn struct {
	aerospike    *as.Client
	clientID     string
	statsHandler func(string, string, float64)
	collector    stats.Collector
}

func (conn *Conn) stats(event, tube string, count float64) {
	if conn.statsHandler != nil {
		conn.statsHandler(event, tube, count)
	}
}

// SetCollector set the collector to use for stats
// TODO: it is a kludge for collecting scan metrics and should be refactored
func (conn *Conn) SetCollector(collector stats.Collector) {
	conn.collector = collector
}

// timing collects the given time metric
func (conn *Conn) timing(name string, duration time.Duration, tags ...string) {
	if conn.collector == nil {
		return
	}
	conn.collector.Timing(name, duration, tags...)
}

// coounter collects the given counter metric
func (conn *Conn) counter(name string, v float64, tags ...string) {
	if conn.collector == nil {
		return
	}
	conn.collector.Count(name, v, tags...)
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
