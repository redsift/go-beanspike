//
// Beanspike job wrapper
//
// Copyright (c) 2016 Redsift Limited. All rights reserved.
//

// Package bsjob is a wrapper around beanspike jobs
package beanspike

import (
	"errors"
	"fmt"
	"time"
)

var ErrParsing = errors.New("error parsing body")

const aerospikeRetries = 20

// Job represents a Beanspike Job
type Job struct {
	ID        int64
	Body      []byte
	TTR       time.Duration
	Retries   int
	RetryFlag bool
	Tube      *Tube
}

func (job *Job) RetryCount() int {
	if job.RetryFlag && job.Retries == 0 {
		return 1
	}

	return job.Retries
}

func (job *Job) String() string {
	return fmt.Sprintf("[Job: id=%d, tube= %s]", job.ID, job.Tube.Name)
}

func (job *Job) Touch() error {
	return job.Tube.Touch(job.ID)
}

func (job *Job) Delete() (bool, error) {
	return job.Tube.Delete(job.ID)
}

func (job *Job) Release(delay time.Duration) error {
	return job.ReleaseWithRetry(delay, false, false)
}

func (job *Job) ReleaseWithRetry(delay time.Duration, incr, retryFlag bool) error {
	return job.Tube.ReleaseWithRetry(job.ID, delay, incr, retryFlag)
}

func (job *Job) Bury(reason string) error {
	return job.Tube.Bury(job.ID, []byte(reason))
}

func (job *Job) Monitor() chan struct{} {
	cancelCh := make(chan struct{}, 1)
	if job.TTR > 0 {
		go func() {
			ticker := time.NewTicker(job.TTR / 2)
			defer ticker.Stop()

			for {
				select {
				case <-ticker.C:
					if err := job.Touch(); err != nil {
						_ = job.Release(0)
						return
					}

				case <-cancelCh:
					return
				}
			}
		}()
	}

	return cancelCh
}

func Reserve(tube *Tube) (*Job, error) {
	id, body, ttr, retries, retryFlag, err := tube.Reserve()

	if err != nil || id == 0 || body == nil {
		return nil, err
	}

	return &Job{ID: id, Body: body, TTR: ttr, Retries: retries, RetryFlag: retryFlag, Tube: tube}, nil
}

func DialDefaultWithRetry(statsHandler func(string, string, float64)) *Conn {
	conn, err := DialDefault(statsHandler)

	if err != nil {
		fmt.Println("Aerospike connect error!", err)

		loopC := 0
		ticker := time.NewTicker(100 * time.Millisecond)
		for {
			<-ticker.C // ratelimit
			conn, err = DialDefault(statsHandler)
			if err == nil {
				break
			}
			loopC++
			if loopC > aerospikeRetries {
				// TODO: Log to statsd
				fmt.Println("Struggling to connect to aerospike:", err)
				loopC = 0
			}
		}
		ticker.Stop()
	}

	fmt.Println("Connected to beanspike")
	return conn
}
