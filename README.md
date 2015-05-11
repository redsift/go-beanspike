# Beanspike

Beanstalk inspired job queue backed by Aerospike KVS.

## Basic usage

### Put

	conn, _ := DialDefault()
	tube, _ := conn.Use("testtube")
	id, err := tube.Put([]byte("hello"), 0, 0)

`Put(body []byte, delay time.Duration, ttr time.Duration)` where `delay` will hold the job in a delayed mode before releasing it for processing. Note, in practice all delayed jobs will be behind any immediately available jobs in the tube. `ttr` is the time the consumer of the job must `Touch` the task within or have it return to the ready state.

### Reserve & Delete
	
	conn, _ := DialDefault()
	tube, _ := conn.Use("testtube")
	id, body, ttl, err := tube.Reserve()
	....
	exists, err := tube.Delete(id)

### Touch
	
	err = tube.Touch(id)

If a job is `Put` in the tube with a `ttr`, the job must be touched at some period smaller than this value to keep it reserved. An error on `Touch` should be treated as an instruction to abandon the job because it has been deleted or has timed out.

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

## Unit Tests

	$ gpm
	$ source gvp
	$ go test

## Benchmarks

	$ go test --bench=.
	...
	BenchmarkPut	    	2000	    589034 ns/op
	BenchmarkReserve	     200	  14759261 ns/op
	BenchmarkRelease	    3000	    607290 ns/op
	
## TODO

- Fix TTL related implementations. 
- Messaging implementation for `Reserve`. 
- Possibly review stats. Review UDF lifecycle management.
- `Kick` and `Bury` unit tests