package beanspike

// As Aerospike does not currently have any trigger or wait state functionality
// efficient job notification requires a stateless messaging implementation

import (
	"time"
)

type MessageBus interface {
    Notify(symbol string, items int)
    WaitFor(symbol string, timeout time.Duration) (chan int)
}