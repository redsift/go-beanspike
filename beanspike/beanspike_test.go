package beanspike

import (
	"bytes"
	"crypto/rand"
	"strings"
	"testing"
	"time"
)

const unitTube = "unittesttube"
const benchTube = "benchtesttube"

// Hack to let AS stabilise after sleeps
func deleteSleep() {
	time.Sleep(2 * time.Second)
}

func max(a time.Duration, b time.Duration) time.Duration {
	if b > a {
		return b
	}
	return a
}

func cleanup(t *testing.T, conn *Conn, tube *Tube, id int64) {
	err := tube.ReleaseWithRetry(id, 0, true, true)
	if err != nil {
		t.Fatalf("Error releasing job: %d. %s", id, err)
	}

	_, err = tube.Delete(id)
	if err != nil {
		t.Fatalf("Error deleting job: %d. %s", id, err)
	}

	conn.Delete(unitTube)
	deleteSleep()
}

func tube(t *testing.T) (*Conn, *Tube) {
	conn, err := DialDefault(nil)
	if err != nil {
		// fatal as looks like nothing else will work
		t.Fatalf("Unable to connect. %v", err)
	}

	conn.Delete(unitTube)
	deleteSleep()

	tube, err := conn.Use(unitTube)
	if err != nil {
		t.Fatalf("Error getting tube: %v", err)
	}

	return conn, tube
}

func TestConnection(t *testing.T) {
	_, err := DialDefault(nil)
	if err != nil {
		// fatal as looks like nothing else will work
		t.Fatalf("Unable to connect. %v", err)
	}
}

func TestPut(t *testing.T) {
	conn, tube := tube(t)

	id, err := tube.Put([]byte("hello"), 0, 0, false, "metadata")
	if err != nil {
		t.Fatal(err)
	}

	err = tube.Touch(id)
	if err == nil {
		t.Fatal("Was able to touch a job that was not timeout-able")
	}

	conn.Delete(unitTube)
	deleteSleep()
}

func TestReserve(t *testing.T) {
	conn, tube := tube(t)

	payload := []byte("hello")
	id, err := tube.Put(payload, 0, 0, false, "metadata")
	if err != nil {
		t.Fatal(err)
	}

	id, body, _, _, _, metadata, err := tube.Reserve()
	if err != nil {
		t.Fatal(err)
	}
	if id == 0 {
		t.Log(err)
		t.Fatal("No job reserved")
	}
	if len(payload) != len(body) {
		t.Fatalf("Job payload does not match expected size got:%v vs sent:%v", len(body), len(payload))
	}
	if !bytes.Equal(payload, body) {
		t.Fatal("Body does not match submitted payload")
	}
	if metadata != "metadata" {
		t.Fatal("metadata does not match")
	}

	cleanup(t, conn, tube, id)
}

func TestReserveBatched(t *testing.T) {
	conn, tube := tube(t)

	id, err := tube.Put([]byte("hello"), 0, 0, false, "metadata")
	if err != nil {
		t.Fatal(err)
	}

	_, err = tube.Put([]byte("hi"), 0, 0, false, "metadata")
	if err != nil {
		t.Fatal(err)
	}

	jobs, err := tube.ReserveBatch(2)
	if err != nil {
		t.Fatal(err)
	}

	if len(jobs) == 0 {
		t.Log(err)
		t.Fatal("No job reserved")
	}
	if len(jobs) != 2 {
		t.Fatal("expecting 2 jobs but got", len(jobs))
	}

	cleanup(t, conn, tube, id)
}

func TestReserveBatched1(t *testing.T) {
	conn, tube := tube(t)

	id, err := tube.Put([]byte("hello"), 0, 0, false, "metadata")
	if err != nil {
		t.Fatal(err)
	}

	id1, err := tube.Put([]byte("hi"), 0, 0, false, "metadata1")
	if err != nil {
		t.Fatal(err)
	}

	jobs, err := tube.ReserveBatch(2)
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range jobs {
		t.Logf("id=%d,%s", v.ID, v.Body)
	}

	if len(jobs) == 0 {
		t.Log(err)
		t.Fatal("No job reserved")
	}
	if len(jobs) != 1 {
		t.Fatal("expecting 1 job but got", len(jobs))
	}
	if jobs[0].ID != id {
		t.Fatalf("expecting job id %d but got %d", id, jobs[0].ID)
	}

	jobs, err = tube.ReserveBatch(2)
	if err != nil {
		t.Fatal(err)
	}

	if len(jobs) == 0 {
		t.Log(err)
		t.Fatal("No job reserved")
	}
	if len(jobs) != 1 {
		t.Fatal("expecting 1 job but got", len(jobs))
	}
	if jobs[0].ID != id1 {
		t.Fatalf("expecting job id %d but got %d", id1, jobs[0].ID)
	}

	cleanup(t, conn, tube, id)
}

func TestReserveCompressedRnd(t *testing.T) {
	conn, tube := tube(t)

	sz := 100 * 1024
	payload := make([]byte, sz)
	_, err := rand.Read(payload)
	if err != nil {
		t.Fatal(err)
	}

	id, err := tube.Put(payload, 0, 0, true, "metadata")
	if err != nil {
		t.Fatal(err)
	}

	id, body, _, _, _, _, err := tube.Reserve()
	if err != nil {
		t.Fatal(err)
	}
	if id == 0 {
		t.Fatal("No job reserved")
	}
	//println("Rnd Job payload got:%v vs sent:%v", len(body), len(payload))
	if len(payload) != len(body) {
		t.Fatalf("Job payload does not match expected size got:%v vs sent:%v", len(body), len(payload))
	}
	if !bytes.Equal(payload, body) {
		t.Fatal("Body do not match submitted payload")
	}

	cleanup(t, conn, tube, id)
}

func TestReserveCompressedStr(t *testing.T) {
	conn, tube := tube(t)

	sz := 100 * 1024
	payload := []byte(strings.Repeat("R", sz))

	id, err := tube.Put(payload, 0, 0, true, "metadata")
	if err != nil {
		t.Fatal(err)
	}

	id, body, _, _, _, _, err := tube.Reserve()
	if err != nil {
		t.Fatal(err)
	}
	if id == 0 {
		t.Fatal("No job reserved")
	}
	//println("Str Job payload got:%v vs sent:%v", len(body), len(payload))
	if len(payload) != len(body) {
		t.Fatalf("Job payload does not match expected size got:%v vs sent:%v", len(body), len(payload))
	}
	if !bytes.Equal(payload, body) {
		t.Fatal("Body do not match submitted payload")
	}

	cleanup(t, conn, tube, id)
}

func TestPutTtr(t *testing.T) {
	conn, tube := tube(t)

	ttrVal := 1 * time.Second

	// Make sure the Job it Put
	id, err := tube.Put([]byte("hello"), 0, ttrVal, false, "metadata")
	if err != nil {
		t.Fatal(err)
	}
	if id == 0 {
		t.Fatal("No job put")
	}

	// Make sure the Job was reserved
	idR, _, ttr, _, _, _, err := tube.Reserve()
	if err != nil {
		t.Fatal(err)
	}
	if id != idR {
		t.Fatalf("Wrong job reserved %v", idR)
	}
	if ttr != ttrVal {
		t.Fatal("Wrong ttr value returned")
	}

	// Make sure no job is available to be reserved
	idR2, _, _, _, _, _, err := tube.Reserve()
	if err != nil && err != ErrJobNotFound {
		t.Fatal(err)
	}
	if idR2 != 0 {
		t.Fatalf("Unexpected job reserved %v", idR2)
	}

	// Wait till the ttrVal has expired or a scan is ready to go
	time.Sleep(max(AerospikeAdminDelay*time.Second*2, ttrVal*2))

	// Make sure Job has timed out and can be reserved
	idR3, _, _, _, _, _, err := tube.Reserve()
	if err != nil {
		t.Fatal(err)
	}
	if idR3 != id {
		t.Fatalf("Job not reserved after ttr, got %v", idR3)
	}

	cleanup(t, conn, tube, id)
}

func TestPutTouch(t *testing.T) {
	conn, tube := tube(t)

	ttrVal := 2 * time.Second

	id, err := tube.Put([]byte("hello"), 0, ttrVal, false, "metadata")
	if err != nil {
		t.Fatal(err)
	}

	if id == 0 {
		t.Fatal("No job put")
	}

	idR, _, ttr, _, _, _, err := tube.Reserve()
	if err != nil {
		t.Fatal(err)
	}

	if id != idR {
		t.Fatalf("Wrong job reserved, %v instead of %v", idR, id)
	}

	if ttr != ttrVal {
		t.Fatal("Wrong ttr value returned")
	}

	time.Sleep(time.Second * 1)

	err = tube.Touch(idR)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Second * 1)

	idR3, _, _, _, _, _, err := tube.Reserve()
	if err != nil && err != ErrJobNotFound {
		t.Fatal(err)
	}

	if idR3 != 0 {
		t.Fatal("Touch did not keep the job reserved")
	}

	cleanup(t, conn, tube, id)
}

func TestRelease(t *testing.T) {
	conn, tube := tube(t)

	magicTtr := 42 * time.Second

	id, err := tube.Put([]byte("hello"), 0, magicTtr, false, "metadata")
	if err != nil {
		t.Fatal(err)
	}

	err = tube.ReleaseWithRetry(id, 0, true, true)
	if err == nil {
		t.Fatal("Released job that was not reserved")
	}

	time.Sleep(1 * time.Second)

	idR, _, ttr, _, _, _, err := tube.Reserve()
	if err != nil {
		t.Fatal(err)
	}
	if idR == 0 {
		t.Fatal("No job reserved")
	}
	if id != idR {
		t.Fatalf("Wrong job %v reserved vs expected %v", idR, id)
	}
	if ttr != magicTtr {
		t.Fatalf("Returned TTR %v not the expected TTR %v", ttr, magicTtr)
	}

	idN, _, _, _, _, _, err := tube.Reserve()
	if idN != 0 {
		t.Fatal("Unexpected job reserved")
	}

	err = tube.ReleaseWithRetry(id, 0, true, true)
	if err != nil {
		t.Fatal(err)
	}

	idN, _, _, _, _, _, err = tube.Reserve()
	if idN == 0 {
		t.Log(err)
		t.Fatal("No job reserved after release")
	}
	if idN != id {
		t.Fatal("Unexpected job reserved after release")
	}

	cleanup(t, conn, tube, id)
}

func TestDelete(t *testing.T) {
	_, tube := tube(t)

	id, err := tube.Put([]byte("to delete"), 0*time.Second, 120*time.Second, false, "metadata")
	if err != nil {
		t.Fatal(err)
	}

	// id should exist, delete should work
	exists, err := tube.Delete(id)
	if err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Fatal("Job Id should exist")
	}

	// id should not exist, delete should work
	exists, err = tube.Delete(id)
	if exists {
		t.Fatal("Job Id should not exist")
	}
}

func TestStats(t *testing.T) {
	const basicjobs = 20
	const compressjobs = 20

	basic := []byte("to count")
	compress := []byte(strings.Repeat("R", 3000))

	conn, tube := tube(t)

	// Submit `basicjobs+compressjobs` jobs, reserve 1
	for i := 0; i < basicjobs; i++ {
		_, err := tube.Put(basic, 0, 0, false, "metadata")
		if err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < compressjobs; i++ {
		_, err := tube.Put(compress, 0, 0, true, "metadata")
		if err != nil {
			t.Fatal(err)
		}
	}

	_, _, _, _, _, _, err := tube.Reserve()
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(1 * time.Second)

	stats, err := tube.Stats()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Stats returned, %+v\n", stats)

	jobs := basicjobs + compressjobs

	if stats.Jobs != jobs {
		t.Fatalf("Wrong number of jobs reported, %v vs %v", stats.Jobs, jobs)
	}

	if stats.Ready != jobs-1 {
		t.Fatalf("Wrong number of ready jobs reported, %v vs an expected %v", stats.Ready, jobs-1)
	}

	conn.Delete(unitTube)
	deleteSleep()
}

func TestShouldOperate(t *testing.T) {
	const unitkey = "unittest"
	conn, _ := DialDefault(nil)
	err := conn.Delete(unitTube)
	if err != nil {
		t.Fatal(err)
	}
	err = conn.Delete(unitTube + "alt")
	if err != nil {
		t.Fatal(err)
	}
	deleteSleep()

	tube1, err := conn.Use(unitTube)
	if err != nil {
		t.Fatal(err)
	}
	tube2, err := conn.Use(unitTube + "alt")
	if err != nil {
		t.Fatal(err)
	}

	should := tube1.shouldOperate(unitkey)
	if !should {
		t.Fatal("Was not allowed to operate")
	}

	should = tube1.shouldOperate(unitkey)
	if should {
		t.Fatal("Was allowed to operate")
	}

	should = tube2.shouldOperate(unitkey)
	if !should {
		t.Fatal("Secondary tube was not allowed to operate")
	}

	// sleep for AerospikeAdminDelay and tube1 should be ok again
	time.Sleep(time.Duration(AerospikeAdminDelay+1) * time.Second)
	should = tube1.shouldOperate(unitkey)
	if !should {
		t.Fatalf("Was not allowed to operate after waiting %v seconds", AerospikeAdminDelay)
	}

	conn.Delete(unitTube)
	conn.Delete(unitTube + "alt")
	deleteSleep()
}

func TestBumpDelayed(t *testing.T) {
	conn, tube := tube(t)

	delay := AerospikeAdminDelay + 1
	idJ, err := tube.Put([]byte("hello"), time.Duration(delay)*time.Second, 0, false, "metadata")
	if err != nil {
		t.Fatal(err)
	}
	id, _, _, _, _, _, err := tube.Reserve()
	if err != nil && err != ErrJobNotFound {
		t.Fatal(err)
	}
	if id != 0 {
		t.Fatalf("Job %v got reserved when none should have been", id)
	}

	// sleep for timeout
	time.Sleep(time.Duration(delay*2) * time.Second)

	id, _, _, _, _, _, err = tube.Reserve()
	if err != nil {
		t.Fatal(err)
	}
	if id == 0 {
		t.Fatal("Job should have been released after wait")
	}
	if id != idJ {
		t.Fatal("Delayed job was not presented")
	}

	err = tube.ReleaseWithRetry(id, time.Duration(delay)*time.Second, true, true)
	if err != nil {
		t.Fatal(err)
	}
	id, _, _, _, _, _, err = tube.Reserve()
	if err != nil && err != ErrJobNotFound {
		t.Fatal(err)
	}
	if id != 0 {
		t.Fatal("Job should have been delayed")
	}

	time.Sleep(time.Duration(delay*2) * time.Second)

	id, _, _, _, _, _, err = tube.Reserve()
	if err != nil {
		t.Fatal(err)
	}
	if id != idJ {
		t.Fatal("Job should have been presented after delay")
	}

	cleanup(t, conn, tube, id)
}

func TestRetries(t *testing.T) {
	conn, tube := tube(t)

	_, err := tube.Put([]byte("hello"), 0, 0, false, "metadata")
	if err != nil {
		t.Fatal(err)
	}

	i := 0
	for i < 5 {
		id, _, _, retries, retryFlag, _, err := tube.Reserve()
		if err != nil {
			t.Fatal(err)
		}

		if retries != i {
			t.Fatalf("Retries should be %d but was %d instead", i, retries)
		}

		if i > 0 && !retryFlag {
			t.Fatalf("Retry flag should be %t but was %t instead", true, !retryFlag)
		}

		err = tube.ReleaseWithRetry(id, 0, true, true)
		if err != nil {
			t.Fatal(err)
		}

		i += 1
	}

	conn.Delete(unitTube)
	deleteSleep()
}

func TestRetriesWithoutIncrement(t *testing.T) {
	conn, tube := tube(t)

	_, err := tube.Put([]byte("hello"), 0, 0, false, "metadata")
	if err != nil {
		t.Fatal(err)
	}

	i := 0
	for i < 5 {
		time.Sleep(2 * time.Second)
		id, _, _, retries, retryFlag, _, err := tube.Reserve()
		if id == 0 {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}

		if retries != 0 {
			t.Fatalf("Retries should be %d but was %d instead", 1, retries)
		}

		if retryFlag {
			t.Fatalf("Retry flag should be false")
		}

		err = tube.Release(id, time.Second)
		if err != nil {
			t.Fatal(err)
		}

		i += 1
	}

	conn.Delete(unitTube)
	deleteSleep()
}

// Sitting at 600us / put
func BenchmarkPut(b *testing.B) {
	conn, _ := DialDefault(nil)
	conn.Delete(benchTube)
	deleteSleep()

	tube, _ := conn.Use(benchTube)

	val := []byte("hello")

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		_, err := tube.Put(val, 0, 0, false, "metadata")
		if err != nil {
			b.Fatalf("Error putting Job. %v", err)
		}

	}

	conn.Delete(unitTube)
	deleteSleep()
}

// Sitting at 16ms / reserve
func BenchmarkReserve(b *testing.B) {
	conn, _ := DialDefault(nil)
	conn.Delete(benchTube)
	deleteSleep()

	tube, _ := conn.Use(benchTube)

	val := []byte("hello")
	for n := 0; n < b.N; n++ {
		tube.Put(val, 0, 0, false, "metadata")
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		id, _, _, _, _, _, err := tube.Reserve()
		if err != nil {
			b.Fatalf("Error reserving Job. %v", err)
		}
		if id == 0 {
			b.Fatal("No reserved Job.")
		}
	}

	conn.Delete(unitTube)
	deleteSleep()
}

func BenchmarkRelease(b *testing.B) {
	conn, _ := DialDefault(nil)
	conn.Delete(benchTube)
	deleteSleep()

	tube, _ := conn.Use(benchTube)

	val := []byte("hello")
	for n := 0; n < b.N; n++ {
		tube.Put(val, 0, 0, false, "metadata")
	}
	ids := make([]int64, b.N)
	for n := 0; n < b.N; n++ {
		id, _, _, _, _, _, err := tube.Reserve()
		if err != nil {
			b.Fatalf("Error reserving Job. %v", err)
		}

		ids[n] = id
	}

	b.ResetTimer()
	for _, id := range ids {
		err := tube.ReleaseWithRetry(id, 0, true, true)
		if err != nil {
			b.Fatalf("Error releasing Job. %v", err)
		}
	}

	conn.Delete(unitTube)
	deleteSleep()
}
