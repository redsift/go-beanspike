package beanspike

import (
	"os"
	"strconv"
	"errors"
	"fmt"
	"regexp"
	"sync/atomic"
	as "github.com/aerospike/aerospike-client-go"
)

type Conn struct {
	aerospike	*as.Client
	clientId	string
}

type Tube struct {
	Conn 	*Conn
	Name 	string
}

type Stats struct {
	Jobs 		int
	Ready		int
	JobSize		int
	UsedSize	int
	SkippedSize	int
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

var instanceCount int32 = 0

func genId() (string) {
	count := atomic.AddInt32(&instanceCount, 1)
	
	pid := os.Getpid()
	
	if host, err := os.Hostname(); err == nil {
		return fmt.Sprintf("%v:%v:%v", host, pid, count)
	}
	return fmt.Sprintf("????????:%v:%v", pid, count)	
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

