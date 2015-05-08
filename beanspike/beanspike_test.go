package beanspike

import (
	"time"
	"testing"
)

const unitTube = "unittesttube"
const benchTube = "benchtesttube"

func TestConnection(t *testing.T) {
	_, err := DialDefault()
	
	if err != nil {
		// fatal as looks like nothing else will work
		t.Fatalf("Unable to connect. %v", err) 
	}
}


func TestPut(t *testing.T) {
	conn, _ := DialDefault()
	tube, err := conn.Use(unitTube)
	
	if err != nil {
		t.Fatal(err) 
	}

	id, err := tube.Put([]byte("hello"), 0, 0)
	if err != nil {
		t.Fatal(err) 
	}
	
	err = tube.Touch(id)
	if err == nil {
		t.Fatal("Was able to touch a job that was not timeoutable") 
	}	
}

func TestReserve(t *testing.T) {
	conn, _ := DialDefault()
	tube, err := conn.Use(unitTube)
	
	if err != nil {
		t.Fatal(err) 
	}

	id, _, _, err := tube.Reserve()
	if err != nil {
		t.Fatal(err) 
	}
	
	if id == 0 {
		t.Fatal("No job reserved")
	}
}

func TestPutTtr(t *testing.T) {
	conn, _ := DialDefault()
	err := conn.Delete(unitTube)
	if err != nil {
		t.Fatal(err) 
	}
		
	tube, err := conn.Use(unitTube)
	
	if err != nil {
		t.Fatal(err) 
	}
	
	ttrVal := 1*time.Second
	
	id, err := tube.Put([]byte("hello"), 0, ttrVal)
	if err != nil {
		t.Fatal(err) 
	}
	
	if id == 0 {
		t.Fatal("No job put")
	}
	
	idR, _, ttr, err := tube.Reserve()
	if err != nil {
		t.Fatal(err) 
	}
	
	if id != idR {
		t.Fatalf("Wrong job reserved %v", idR)
	}	
	
	if ttr != ttrVal {
		t.Fatal("Wrong ttr value returned")	
	}	
	
	idR2, _, _, err := tube.Reserve()
	if err != nil {
		t.Fatal(err) 
	}
	
	if idR2 != 0 {
		t.Fatal("Unexpected job reserved")
	}	
	
	time.Sleep(ttrVal*2)	
	
	idR3, _, _, err := tube.Reserve()
	if err != nil {
		t.Fatal(err) 
	}
	
	if idR3 != id {
		t.Fatalf("Job not reserved after ttr, got %v", idR3)
	}	
}

func TestPutTouch(t *testing.T) {
	conn, _ := DialDefault()
	err := conn.Delete(unitTube)
	if err != nil {
		t.Fatal(err) 
	}
		
	tube, err := conn.Use(unitTube)
	
	if err != nil {
		t.Fatal(err) 
	}
	
	ttrVal := 2*time.Second
	
	id, err := tube.Put([]byte("hello"), 0, ttrVal)
	if err != nil {
		t.Fatal(err) 
	}
	
	if id == 0 {
		t.Fatal("No job put")
	}
	
	idR, _, ttr, err := tube.Reserve()
	if err != nil {
		t.Fatal(err) 
	}
	
	if id != idR {
		t.Fatal("Wrong job reserved, %v instead of %v", idR, id)
	}	
	
	if ttr != ttrVal {
		t.Fatal("Wrong ttr value returned")	
	}	

	time.Sleep(time.Second*1)	
	
	err = tube.Touch(idR)
	if err != nil {
		t.Fatal(err) 
	}
		
	time.Sleep(time.Second*1)	
	
	idR3, _, _, err := tube.Reserve()
	if err != nil {
		t.Fatal(err) 
	}
	
	if idR3 != 0 {
		t.Fatal("Touch did not keep the job reserved")
	}	
}


func TestRelease(t *testing.T) {
	conn, _ := DialDefault()
	
	err := conn.Delete(unitTube)
	if err != nil {
		t.Fatal(err) 
	}	
	
	tube, err := conn.Use(unitTube)
	if err != nil {
		t.Fatal(err) 
	}
	
	magicTtr := 42*time.Second
	
	id, err := tube.Put([]byte("hello"), 0, magicTtr)
	if err != nil {
		t.Fatal(err) 
	}
	
	err = tube.Release(id, 0)
	if err == nil {
		t.Fatal("Released job that was not reserved") 
	}
	
	time.Sleep(1*time.Second)
	
	idR, _, ttr, err := tube.Reserve()
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
	
	idN, _, _, err := tube.Reserve()
	if idN != 0 {
		t.Fatal("Unexpected job reserved")
	}
	
	err = tube.Release(id, 0)
	if err != nil {
		t.Fatal(err) 
	}
		
	idN, _, _, err = tube.Reserve()
	if idN == 0 {
		t.Fatal("No job reserved after release")
	}
	if idN != id {
		t.Fatal("Unexpected job reserved after release")
	}
}

func TestDelete(t *testing.T) {
	conn, _ := DialDefault()
	tube, err := conn.Use(unitTube)
	
	if err != nil {
		t.Fatal(err) 
	}

	id, err := tube.Put([]byte("to delete"), 0*time.Second, 120*time.Second)
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
	if err != nil {
		t.Fatal(err) 
	}	
	if exists {
		t.Fatal("Job Id should not exist") 
	}
}

func TestStats(t *testing.T) {
	const jobs = 20
	conn, _ := DialDefault()

	err := conn.Delete(unitTube)
	if err != nil {
		t.Fatal(err) 
	}		
	tube, _ := conn.Use(unitTube)
	
	// Submit `jobs` jobs, reserve 1
	for i := 0; i < jobs; i++ {
		_, err = tube.Put([]byte("to count"), 0, 0)
		if err != nil {
			t.Fatal(err) 
		}
	}
	_, _, _, err = tube.Reserve()
	if err != nil {
		t.Fatal(err) 
	}
		
	time.Sleep(1*time.Second)
		
	stats, err := tube.Stats()
	if err != nil {
		t.Fatal(err) 
	}	
	t.Logf("Stats returned, %+v\n", stats)
	
	if stats.Jobs != jobs {
		t.Fatalf("Wrong number of jobs reported, %v vs %v", stats.Jobs, jobs) 	
	}

	if stats.Ready != jobs - 1 {
		t.Fatalf("Wrong number of ready jobs reported, %v vs an expected %v", stats.Ready, jobs - 1) 	
	}
}	

func TestShouldOperate(t *testing.T) {
	const unitkey = "unittest"
	conn, _ := DialDefault()
	
	err := conn.Delete(unitTube)
	if err != nil {
		t.Fatal(err) 
	}	
	err = conn.Delete(unitTube+"alt")
	if err != nil {
		t.Fatal(err) 
	}	
			
	tube1, err := conn.Use(unitTube)
	if err != nil {
		t.Fatal(err) 
	}
	tube2, err := conn.Use(unitTube+"alt")
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
	time.Sleep(time.Duration(AerospikeAdminDelay + 1)*time.Second)
	should = tube1.shouldOperate(unitkey)
	if !should {
		t.Fatalf("Was not allowed to operate after waiting %v seconds", AerospikeAdminDelay) 
	}	
}

func TestBumpDelayed(t *testing.T) {
	conn, _ := DialDefault()
	
	conn.Delete(unitTube)
	tube, err := conn.Use(unitTube)
	if err != nil {
		t.Fatal(err) 
	}
	delay := AerospikeAdminDelay + 1
	idJ, err := tube.Put([]byte("hello"), time.Duration(delay)*time.Second, 0)
	if err != nil {
		t.Fatal(err) 
	}
	id, _, _, err := tube.Reserve()
	if err != nil {
		t.Fatal(err) 
	}
	if id != 0 {
		t.Fatalf("Job %v got reserved when none should have been", id) 
	}
		
	// sleep for timeout
	time.Sleep(time.Duration(delay*2)*time.Second)
	
	id, _, _, err = tube.Reserve()
	if err != nil {
		t.Fatal(err) 
	}
	if id == 0 {
		t.Fatal("Job should have been released after wait") 
	}
	if id != idJ {
		t.Fatal("Delayed job was not presented") 
	}
	
	err = tube.Release(id, time.Duration(delay)*time.Second)
	if err != nil {
		t.Fatal(err) 
	}	
	id, _, _, err = tube.Reserve()
	if err != nil {
		t.Fatal(err) 
	}	
	if id != 0 {
		t.Fatal("Job should have been delayed") 
	}	
	
	time.Sleep(time.Duration(delay*2)*time.Second)
	
	id, _, _, err = tube.Reserve()
	if err != nil {
		t.Fatal(err) 
	}	
	if id != idJ {
		t.Fatal("Job should have been presented after delay") 
	}		
}

var result int64

// Sitting at 600us / put
func BenchmarkPut(b *testing.B) {
	conn, _ := DialDefault()
	
	conn.Delete(benchTube)
	tube, _ := conn.Use(benchTube)
	
	val := []byte("hello")

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		_, err := tube.Put(val, 0, 0)
		if err != nil {
			b.Fatalf("Error putting Job. %v", err)
		}	
		
	}
}

// Sitting at 16ms / reserve
func BenchmarkReserve(b *testing.B) {
	conn, _ := DialDefault()
	
	conn.Delete(benchTube)
	tube, _ := conn.Use(benchTube)
	
	val := []byte("hello")
	for n := 0; n < b.N; n++ {
		tube.Put(val, 0, 0)
	}
	
	var id int64
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		id, _, _, err := tube.Reserve()
		if err != nil {
			b.Fatalf("Error reserving Job. %v", err)
		}		
		if id == 0 {
			b.Fatal("No reserved Job.")
		}	
	}
	result = id
}

func BenchmarkRelease(b *testing.B) {
	conn, _ := DialDefault()
	
	conn.Delete(benchTube)
	tube, _ := conn.Use(benchTube)
	
	val := []byte("hello")
	for n := 0; n < b.N; n++ {
		tube.Put(val, 0, 0)
	}
	ids := make([]int64, b.N)
	for n := 0; n < b.N; n++ {
		id, _, _, err := tube.Reserve()
		if err != nil {
			b.Fatalf("Error reserving Job. %v", err)
		}
		
		ids[n] = id
	}
		
	b.ResetTimer()
	for _, id := range ids {
		err := tube.Release(id, 0)
		if err != nil {
			b.Fatalf("Error releasing Job. %v", err)
		}		
	}
}


