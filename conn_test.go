package beanspike

import (
	"slices"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	server = "localhost"
)

func TestConn_newJobID(t *testing.T) {
	t.Run("no concurrent Hot Key errors", func(t *testing.T) {
		conn, err := Dial("", server, 3000, func(string, string, float64) {})
		require.NoError(t, err)

		const concurrency = 100
		var start, wg sync.WaitGroup
		var once sync.Once
		ids := make(chan int64, concurrency)
		done := make(chan struct{})

		for i := 0; i < concurrency; i++ {
			start.Add(1)
			wg.Add(1)
			go func() {
				defer wg.Done()

				start.Done()
				<-done
				once.Do(func() {
					t.Logf("start %d connections", concurrency)
				})

				id, err := conn.newJobID()
				assert.NoError(t, err)
				ids <- id
				t.Log(id)
			}()
		}
		start.Wait()
		close(done)
		wg.Wait()
		close(ids)

		var got []int64
		for i := range ids {
			got = append(got, i)
		}
		assert.Equal(t, concurrency, len(got))
		slices.Sort(got)
		for i := 0; i < len(got)-1; i++ {
			assert.Equal(t, int64(1), got[i+1]-got[i])
		}
	})
}
