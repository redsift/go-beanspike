package beanspike

import (
	"context"
	"encoding/json"
	"errors"
	"math/rand"
	"sync"
	"time"

	"github.com/bluele/gcache"
	"github.com/redsift/go-shared/bsjob"
	"github.com/redsift/go-shared/constants"
	"github.com/redsift/go-utils/errs"
	"github.com/redsift/go-utils/stats"
	"github.com/redsift/go-utils/statsd"
)

var (
	ErrAlreadyConnected = errors.New("already connected")
	ErrNotConnected     = errors.New("not connected")
)

type TubeID string

type TaskHandler interface {
	// ReserveAndDecode reserve a task and decode it to appropriate value type
	ReserveAndDecode(*Tube) (*bsjob.Job, interface{}, error)
	// Handle processes incoming message.
	// Calling context.CancelFunc stops all underlying goroutines and release a beanspike job
	Handle(context.CancelFunc, interface{})
	// OnRelease will be called when handler context canceled
	OnRelease(interface{})
}

type Client struct {
	ctx       context.Context
	cancel    context.CancelFunc
	conn      *Conn
	collector stats.Collector
	jobs      sync.WaitGroup
	outTubes  gcache.Cache
}

func NewClient(ctx context.Context, commit string) *Client {
	ctx, cancel := context.WithCancel(ctx)

	client := &Client{
		ctx:       ctx,
		cancel:    cancel,
		collector: statsd.New(commit),
	}

	createTube := func(k interface{}) (interface{}, error) {
		if client.conn == nil {
			return nil, ErrNotConnected
		}
		return client.conn.Use(k.(string))
	}

	// Hope it was correct answer
	client.outTubes = gcache.New(42).ARC().LoaderFunc(createTube).Build()

	return client
}

func (c *Client) Connect() error {
	if c.conn != nil {
		return ErrAlreadyConnected
	}
	conn, err := DialDefault(statsd.NewBeanspikeStats(c.collector).Handler)
	if err != nil {
		return err
	}
	c.conn = conn
	return nil
}

func (c *Client) Close() {
	c.cancel()
	c.jobs.Wait()
	// TODO close connection
}

func (c *Client) Handle(id TubeID, h TaskHandler) error {
	tube, err := c.conn.Use(string(id))
	if err != nil {
		c.collectCoffeeCode(errs.Turkish, err)
		return err
	}
	go func() {
		ticker := time.Tick(constants.IdleInTubeTime)
	LOOP:
		for {
			select {
			case <-c.ctx.Done():
				return
			case <-ticker:
				for {
					job, payload, err := h.ReserveAndDecode(tube)
					if err != nil {
						continue LOOP
					}
					c.jobs.Add(2)
					ctx, cancel := context.WithCancel(c.ctx)
					go func(cancel context.CancelFunc, v interface{}) {
						h.Handle(cancel, v)
						c.jobs.Done()
					}(cancel, payload)
					release := func(v interface{}) func() {
						return func() { h.OnRelease(v) }
					}
					go func(ctx context.Context, job *bsjob.Job, f func()) {
						c.handle(ctx, tube, job, f)
						c.jobs.Done()
					}(ctx, job, release(payload))
				}
			}
		}
	}()
	return nil
}

var backoffDuration = func() func(uint) time.Duration {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return func(c uint) time.Duration {
		if c > 10 {
			c = 10
		}
		return time.Duration(r.Intn(1<<c)) * time.Second
	}
}()

func (c *Client) handle(ctx context.Context, tube *Tube, job *bsjob.Job, release func()) {
	ticker := time.NewTicker(job.TTR / 2) // TODO (dmitry) reuse timers to save allocations
	defer func() {
		release()
		ticker.Stop()
		_ = tube.ReleaseWithRetry(job.ID, backoffDuration(uint(job.Retries)), true, true)
	}()
	for {
		select {
		case <-ctx.Done():
			// context canceled by handler or by system
			return
		case <-ticker.C:
			if err := tube.Touch(job.ID); err != nil {
				// User deleted the sift or other reason
				return
			}
		}
	}
}

func (c *Client) Put(tubeID string, v interface{}) (int64, error) {
	var (
		err  error
		tube interface{}
	)
	tube, err = c.outTubes.Get(tubeID)
	if err != nil {
		c.collectCoffeeCode(errs.Turkish, err)
		return 0, err
	}

	var b []byte
	b, err = json.Marshal(v)
	if err != nil {
		return 0, err
	}

	var id int64
	id, err = tube.(*Tube).Put(b, 0, constants.OutTubeTTR, true)
	c.collectCoffeeCode(errs.Turkish, err)
	return id, err
}

func (c *Client) collectCoffeeCode(f errs.InternalState, e error) {
	if e == nil {
		return
	}
	c.collector.Error(errs.WrapWithCode(f, e), nil)
}
