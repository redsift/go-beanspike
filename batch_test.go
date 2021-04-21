package beanspike_test

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/redsift/go-beanspike"
)

func ExampleTube_ReserveBatch() {
	log.SetFlags(0)

	conn, err := beanspike.Dial("", "localhost", 3000, func(string, string, float64) {})
	if err != nil {
		log.Fatal("dial ", err)
	}

	tube, err := conn.Use("jmappush_google_stream_in")
	if err != nil {
		log.Fatal("use ", err)
	}

	beanspike.DefaultErrorLogger = beanspike.ErrorLoggerFunc(func(err error) {
		log.Printf("ERROR %s\n", err)
	})

	decoder := beanspike.JobDecoderFunc(func(_ []byte) (interface{}, error) { return nil, nil })

	handler := beanspike.JobHandlerFunc(func(_ context.Context, _ *beanspike.ManagedJob, _ interface{}) {})

	sinceStart := func(round time.Duration) func() time.Duration {
		start := time.Now()
		return func() time.Duration { return time.Since(start).Round(round) }
	}(time.Second)

	var reserved, cycles, emptyCycles int

	ctx := context.TODO()

	for {
		cycles++
		n := tube.ReserveBatch(ctx, decoder, handler, 500)

		log.Printf("Reserved %d\n", n)
		if n == 0 {
			emptyCycles++
			if emptyCycles == 3 {
				break
			}

			log.Printf("Zzzz... elapsed %s\n", sinceStart())
			time.Sleep(2 * time.Second)
			continue
		} else {
			emptyCycles = 0
		}

		reserved += n
	}

	log.Printf("Total reserved %d in %s and %d cycles\n", reserved, sinceStart(), cycles)

	// NOTE I do not use golang's Output as timing and actual number of jobs may and will vary,
	// however one can expect output like this
	//
	// Reserved 500
	// Reserved 500
	// Reserved 500
	// Reserved 500
	// Reserved 500
	// Reserved 500
	// Reserved 500
	// Reserved 500
	// Reserved 123
	// Reserved 0
	// Zzzz... elapsed 14s
	// Reserved 0
	// Zzzz... elapsed 16s
	// Reserved 0
	// Total reserved 4123 in 19s and 12 cycles

	// Output:
	//
}

func ExampleClient_StartReserveAndKeepLoop() {
	log.SetFlags(0)

	beanspike.DefaultErrorLogger = beanspike.ErrorLoggerFunc(func(err error) {
		log.Printf("ERROR %s", err)
	})

	client := beanspike.NewClient(context.TODO())

	if err := client.Connect("localhost", 3000, func(string, string, float64) {}); err != nil {
		log.Fatal("dial: ", err)
	}

	decoder := beanspike.JobDecoderFunc(func(_ []byte) (interface{}, error) { return nil, nil })

	reserved := make(chan struct{}, 1)

	var numReserved, numReleased int32

	handler := beanspike.JobHandlerFunc(func(ctx context.Context, job *beanspike.ManagedJob, _ interface{}) {
		go func() {
			<-ctx.Done()
			//log.Printf("job %d has been released", job.ID)
			atomic.AddInt32(&numReleased,1)
		}()
		//log.Printf("job %d has been reserved", job.ID)
		reserved <- struct{}{}
		atomic.AddInt32(&numReserved,1)
	})

	if err := client.StartReserveAndKeepLoop("jmappush_google_stream_in", decoder, handler, 500); err != nil {
		log.Fatal("failed to start reserve-n-keep loop: ", err)
	}

	sinceStart := func(round time.Duration) func() time.Duration {
		start := time.Now()
		return func() time.Duration { return time.Since(start).Round(round) }
	}(time.Second)

	done := make(chan struct{})

	go func() {
		n := 0
		for {
			select {
			case <-reserved:
				n++
				if n%200 == 0 {
					log.Printf("reserved %d in %s", n, sinceStart())
				}
				if n > 4000 {
					log.Printf("all jobs reserved in %s", sinceStart())
					close(done)
					return
				}
			}
		}
	}()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-sigs:
		log.Printf("interrupted")
	case <-done:
		log.Printf("done; reserved %d, released %d",
			atomic.LoadInt32(&numReserved), atomic.LoadInt32(&numReleased))
	}

	client.Close()

	log.Printf("client.Close(); reserved %d, released %d",
		atomic.LoadInt32(&numReserved), atomic.LoadInt32(&numReleased))

	log.Printf("finished in %s", sinceStart())

	// NOTE I do not use golang's Output as timing and actual number of jobs may and will vary,
	// however one can expect output like this
	//
	// reserved 200 in 2s
	// reserved 400 in 2s
	// reserved 600 in 3s
	// reserved 800 in 3s
	// reserved 1000 in 3s
	// reserved 1200 in 3s
	// reserved 1400 in 3s
	// reserved 1600 in 4s
	// reserved 1800 in 4s
	// reserved 2000 in 4s
	// reserved 2200 in 4s
	// reserved 2400 in 4s
	// reserved 2600 in 4s
	// reserved 2800 in 5s
	// reserved 3000 in 5s
	// reserved 3200 in 5s
	// reserved 3400 in 5s
	// reserved 3600 in 5s
	// reserved 3800 in 5s
	// reserved 4000 in 5s
	// all jobs reserved in 6s
	// done; reserved 4001, released 0
	// client.Close(); reserved 4001, released 4001
	// finished in 7s

	// Output:
	//
}
