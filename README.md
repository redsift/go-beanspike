# Beanspike

Beanstalk inspired job queue backed by Aerospike KVS.

## Basic usage

### Put

	conn, _ := DialDefault()
	tube, _ := conn.Use("testtube")
	id, err := tube.Put([]byte("hello"), 0, 0)


### Reserve & Delete
	
	conn, _ := DialDefault()
	tube, _ := conn.Use("testtube")
	id, body, err := tube.Reserve()
	....
	exists, err := tube.Delete(id)

### Stats

	conn, _ := DialDefault()
	tube, _ := conn.Use("testtube")
	stats, err := tube.Stats()
	
### Delete a tube

This is mainly for unit test clean up but there may be a legitimate usecase.
	
	conn, _ := DialDefault()
	conn.Delete("testtube")
	
## Connection information

If `DialDefault()` is used, various reasonable defaults are set.

The client uses environment variables `AEROSPIKE_HOST` and `AEROSPIKE_PORT` to connect to an Aerospike node.

The library will attempt to parse `AEROSPIKE_PORT` either as a port number or a Docker environment variable as might be set if the the application is in a container that has been linked to an Aerospike conatiner with `--link aerospike:aerospike`. 

If nothing is specified, the client assumes `aerospike` and `3000`

The alternative `Dial(id string, host string, port int)` requires everything to be set explicitly.

## Benchmarks

	go test --bench=.
	...
	BenchmarkPut	    	2000	    589034 ns/op
	BenchmarkReserve	     200	  14759261 ns/op

## TODO

`Touch`, `Bury`, `Release` and TTL related implementations. Possibly review stats. Review UDF lifecycle management.