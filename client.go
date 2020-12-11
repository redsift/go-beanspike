package beanspike

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redsift/clockwork/workerpool"
	"github.com/redsift/clockwork/workerpool/bdeq"

	srand "crypto/rand"

	"github.com/redsift/clockwork/cron"

	"github.com/bluele/gcache"
)

const (
	IdleInTubeTime = 2 * time.Second
	OutTubeTTR     = 30 * time.Second
)

var (
	ErrAlreadyConnected = errors.New("already connected")
	ErrNotConnected     = errors.New("not connected")
)

type TubeID string

// JobDecoder defines interface app should implement for decoding incoming jobs into the app specific objects.
type JobDecoder interface {
	// Decode unmarshalls job's payload.
	Decode([]byte) (interface{}, error)
}

type JobDecoderFunc func([]byte) (interface{}, error)

func (f JobDecoderFunc) Decode(b []byte) (interface{}, error) { return f(b) }

type JobHandler interface {
	// Handle processes an incoming job.
	// Application should watch given context; it is being canceled on job release.
	Handle(context.Context, *ManagedJob, interface{})
}

type JobHandlerFunc func(context.Context, *ManagedJob, interface{})

func (f JobHandlerFunc) Handle(ctx context.Context, job *ManagedJob, v interface{}) { f(ctx, job, v) }

type ManagedJob struct {
	*Job
	cancel func()
}

// StopAndRelease provides application way to stop job keeping activity and release the job
func (job *ManagedJob) StopAndRelease() { job.cancel() }

func NewManagedJob(job *Job, cancel func()) *ManagedJob {
	return &ManagedJob{
		Job:    job,
		cancel: cancel,
	}
}

var retryDuration = func() func(uint) time.Duration {
	rng := &sync.Pool{New: func() interface{} {
		var seed [8]byte
		if _, err := srand.Read(seed[:]); err != nil {
			panic("cannot seed math/rand package with cryptographically secure random number generator")
		}

		return rand.New(rand.NewSource(time.Now().UnixNano() * int64(binary.LittleEndian.Uint64(seed[:]))))
	}}

	return func(c uint) time.Duration {
		if c > 10 {
			c = 10
		}
		rnd := rng.Get().(*rand.Rand)
		d := time.Duration(rnd.Intn(1<<c)) * time.Second
		rng.Put(rnd)
		return d
	}
}()

type Client struct {
	ctx          context.Context
	cancel       context.CancelFunc
	conn         *Conn
	outTubes     gcache.Cache
	cron         *cron.Cron
	reservedJobs map[int64]*ManagedJob
	lock         *sync.RWMutex
	shutdown     int32
}

func NewClient(ctx context.Context) *Client {
	ctx, cancel := context.WithCancel(ctx)

	client := &Client{
		ctx:          ctx,
		cancel:       cancel,
		reservedJobs: make(map[int64]*ManagedJob),
		lock:         new(sync.RWMutex),
		shutdown:     0,
	}

	client.cron = cron.New(cron.HandlerFunc(func(_ time.Time, keys []interface{}) {
		for _, k := range keys {
			client.lock.RLock()
			job, found := client.reservedJobs[k.(int64)]
			client.lock.RUnlock()
			if !found {
				continue
			}
			if err := job.Touch(); err != nil {
				job.StopAndRelease()
			}
		}
	}), cron.WithPrecision(time.Second), cron.WithCalendarMinCap(1<<13))

	// Hope it was a correct answer
	client.outTubes = gcache.New(42).LRU().LoaderFunc(func(k interface{}) (interface{}, error) {
		if client.conn == nil {
			return nil, ErrNotConnected
		}
		return client.conn.Use(k.(string))
	}).Build()

	return client
}

func (c *Client) Connect(host string, port int, statsHandler func(string, string, float64)) error {
	if c.conn != nil {
		return ErrAlreadyConnected
	}
	conn, err := Dial("", host, port, statsHandler)
	if err != nil {
		return err
	}
	c.conn = conn
	return nil
}

func (c *Client) Close() {
	atomic.StoreInt32(&c.shutdown, 1) // disable normal job release
	c.cancel()                        // notify application (all jobs)
	c.cron.Stop()                     // stop keeping

	wp := workerpool.New(128, bdeq.New(1<<12))
	wp.Start()

	var wg sync.WaitGroup

	c.lock.Lock()
	for _, v := range c.reservedJobs {
		job := v
		wg.Add(1)
		wp.Submit(func() {
			// release immediately
			if err := job.Release(0); err != nil {
				DefaultErrorLogger.LogError(fmt.Errorf("stopping: %d release error: %w", job.ID, err))
			}
			wg.Done()
		}, 0)
	}
	c.lock.Unlock()

	wg.Wait()

	// TODO close connection
	// TODO stop worker pool when queue is empty
}

func (c *Client) StartReserveAndKeepLoop(id TubeID, dec JobDecoder, handler JobHandler, batchSize int) error {
	tube, err := c.conn.Use(string(id))
	if err != nil {
		return err
	}

	managingHandler := JobHandlerFunc(func(ctx context.Context, job *ManagedJob, v interface{}) {
		ctx, cancel := context.WithCancel(ctx)

		// wrap job into ManagedJob with a new cancel func
		managedJob := NewManagedJob(job.Job, func() {
			cancel() // notify application

			// remove from reserved jobs
			c.lock.Lock()
			delete(c.reservedJobs, job.ID)
			c.lock.Unlock()

			// stop cron task
			c.cron.Remove(job.ID)

			// release us usual if not shutdown; otherwise job will be released immediately
			if atomic.LoadInt32(&c.shutdown) == 0 {
				_ = job.ReleaseWithRetry(retryDuration(uint(job.Retries)), true, true)
			}

			// call the original cancel func
			job.StopAndRelease()
		})

		// add to reserved jobs
		c.lock.Lock()
		c.reservedJobs[job.ID] = managedJob
		c.lock.Unlock()

		// schedule regular touch; skip if job has been scheduled already
		_ = c.cron.Add(cron.ScheduleFunc(func(now time.Time) time.Time {
			return now.Add(job.TTR / 2)
		}), job.ID)

		// call the original handler
		handler.Handle(ctx, managedJob, v)
	})

	go func() {
		ticker := time.Tick(IdleInTubeTime)
	LOOP:
		for {
			select {
			case <-c.ctx.Done():
				return
			case <-ticker:
				for c.ctx.Err() == nil {
					n := tube.ReserveBatch(c.ctx, dec, managingHandler, batchSize)
					if n == 0 {
						continue LOOP
					}
				}
			}
		}
	}()

	return nil
}

func (c *Client) Put(tubeID string, v interface{}, metadata string, tob int64) (int64, error) {
	var (
		err  error
		tube interface{}
	)
	tube, err = c.outTubes.Get(tubeID)
	if err != nil {
		return 0, err
	}

	var b []byte
	b, err = json.Marshal(v)
	if err != nil {
		return 0, err
	}

	var id int64
	id, err = tube.(*Tube).Put(b, 0, OutTubeTTR, true, metadata, tob)
	return id, err
}
