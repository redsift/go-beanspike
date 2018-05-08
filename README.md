# Beanspike

Beanstalk inspired job queue backed by Aerospike KVS.

## Basic usage

### Put

	conn, _ := DialDefault()
	tube, _ := conn.Use("testtube")
	id, err := tube.Put([]byte("hello"), 0, 0, true)

`Put(body []byte, delay time.Duration, ttr time.Duration, lz bool)` where `delay` will hold the job in a delayed mode before releasing it for processing. Note, in practice all delayed jobs will be behind any immediately available jobs in the tube. `ttr` is the time the consumer of the job must `Touch` the task within or have it return to the ready state. If `lz` is true, the action will try and compress the payload trading some performance for memory usage on Aerospike. Note, if the payload is too small or the LZ4 compression is unable to provide a space saving, the entry may be inserted uncompressed anyway. Passing `true` is largely safe from a performance prespective unless you know that `body` is large and high entorpy. Cases where compression was attempted and abandoned in currently present jobs are reflected in `Stats.SkippedSize`.

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
	
	
### Benchmarks of version 728fb18f taken on MacBook Pro (Retina, 15-inch, Mid 2015) C02S218TG8WL

```sh
% go test -bench=. -v
=== RUN   TestConnection
--- PASS: TestConnection (0.12s)
=== RUN   TestPut
--- PASS: TestPut (6.47s)
=== RUN   TestReserve
--- PASS: TestReserve (6.47s)
=== RUN   TestReserveBatched
--- PASS: TestReserveBatched (6.47s)
=== RUN   TestReserveBatchedDelayed
--- SKIP: TestReserveBatchedDelayed (0.00s)
=== RUN   TestReserveBatched1
--- PASS: TestReserveBatched1 (6.46s)
=== RUN   TestReserveBatchedNoMetadata
--- PASS: TestReserveBatchedNoMetadata (6.48s)
=== RUN   TestReserveCompressedRnd
--- PASS: TestReserveCompressedRnd (6.46s)
=== RUN   TestReserveCompressedStr
--- PASS: TestReserveCompressedStr (6.48s)
=== RUN   TestPutTtr
--- PASS: TestPutTtr (10.49s)
=== RUN   TestPutTouch
--- PASS: TestPutTouch (8.47s)
=== RUN   TestRelease
--- PASS: TestRelease (7.47s)
=== RUN   TestDelete
--- PASS: TestDelete (3.29s)
=== RUN   TestStats
--- PASS: TestStats (8.68s)
        beanspike_test.go:547: Stats returned, &{Jobs:40 Ready:39 Buried:0 Delayed:0 Reserved:1 Deleted:0 JobSize:60160 UsedSize:600 SkippedSize:0}
=== RUN   TestShouldOperate
--- PASS: TestShouldOperate (11.81s)
=== RUN   TestBumpDelayed
--- PASS: TestBumpDelayed (18.47s)
=== RUN   TestRetries
--- PASS: TestRetries (6.46s)
=== RUN   TestRetriesWithoutIncrement
--- PASS: TestRetriesWithoutIncrement (22.53s)
goos: darwin
goarch: amd64
pkg: github.com/redsift/go-beanspike
BenchmarkPut-8              2000            849611 ns/op
BenchmarkReserve-8           300           3628816 ns/op
BenchmarkRelease-8          2000            767221 ns/op
PASS
ok      github.com/redsift/go-beanspike 208.153s
```

## TODO

- Fix TTL related implementations. 
- Messaging implementation for `Reserve`. 
- Possibly review stats. Review UDF lifecycle management.
- `Kick` and `Bury` unit tests