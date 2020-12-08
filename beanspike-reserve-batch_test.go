package beanspike_test

import (
	"fmt"
	"log"
	"time"

	"github.com/redsift/go-beanspike"
)

func ExampleTube_ReserveBatch() {
	conn, err := beanspike.Dial("", "localhost", 3000, func(string, string, float64) {})
	if err != nil {
		log.Fatal("dial ", err)
	}

	tube, err := conn.Use("jmappush_google_stream_in")
	if err != nil {
		log.Fatal("use ", err)
	}

	beanspike.DefaultErrorLogger = beanspike.ErrorLoggerFunc(func(err error) {
		fmt.Printf("ERROR %s\n", err)
	})
	consumer := beanspike.ConsumerFunc(func(job *beanspike.Job) {
		// spin off job keeping goroutine
	})

	sinceStart := func(round time.Duration) func() time.Duration {
		start := time.Now()
		return func() time.Duration { return time.Since(start).Round(round) }
	}(time.Second)

	var reserved, cycles, emptyCycles int

	for {
		cycles++
		n := tube.ReserveBatch(500, consumer)

		fmt.Printf("Reserved %d\n", n)
		if n == 0 {
			emptyCycles++
			if emptyCycles == 3 {
				break
			}

			fmt.Printf("Zzzz... elapsed %s\n", sinceStart())
			time.Sleep(2 * time.Second)
			continue
		} else {
			emptyCycles = 0
		}

		reserved += n
	}

	fmt.Printf("Total reserved %d in %s and %d cycles\n", reserved, sinceStart(), cycles)

	// Note: actual mileage may differ

	// Output:
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
	//
}
