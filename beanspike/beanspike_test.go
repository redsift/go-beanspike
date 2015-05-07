package beanspike

import (
	"time"
	"testing"
	"fmt"
)

func TestConnection(t *testing.T) {
	_, err := DialDefault()
	
	if err != nil {
		// fatal as looks like nothing else will work
		t.Fatal(err) 
	}
}


func TestPut(t *testing.T) {
	conn, err := DialDefault()
	
	if err != nil {
		// fatal as looks like nothing else will work
		t.Fatal(err) 
	}
	
	tube, err := conn.Use("testtube4")
	
	if err != nil {
		t.Fatal(err) 
	}

	id, err := tube.Put([]byte("hello"), 0, 0)
	if err != nil {
		t.Fatal(err) 
	}
	
	println("put job=", id)
}

func TestReserve(t *testing.T) {
	conn, err := DialDefault()
	
	if err != nil {
		// fatal as looks like nothing else will work
		t.Fatal(err) 
	}

	tube, err := conn.Use("testtube4")
	
	if err != nil {
		t.Fatal(err) 
	}

	id, _, err := tube.Reserve()
	if err != nil {
		t.Fatal(err) 
	}
	
	if id == 0 {
		t.Fatal("No job reserved")
	}
	
	println("got job=", id)
}

func TestDelete(t *testing.T) {
	conn, _ := DialDefault()
	tube, err := conn.Use("testtube-delete")
	
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
		
	println("deleted job=", id)
}

func TestStats(t *testing.T) {
	const jobs = 20
	conn, _ := DialDefault()

	err := conn.Delete("testtube-stats")
	if err != nil {
		t.Fatal(err) 
	}		
	tube, _ := conn.Use("testtube-stats")
	
	// Submit `jobs` jobs, reserve 1
	for i := 0; i < jobs; i++ {
		_, err = tube.Put([]byte("to count"), 0*time.Second, 120*time.Second)
		if err != nil {
			t.Fatal(err) 
		}
	}
	_, _, err = tube.Reserve()
	if err != nil {
		t.Fatal(err) 
	}
		
	stats, err := tube.Stats()
	if err != nil {
		t.Fatal(err) 
	}	
	fmt.Printf("stats, %+v\n", stats)
	
	if stats.Jobs != jobs {
		t.Fatal("Wrong number of jobs reported") 	
	}

	if stats.Ready != jobs - 1 {
		t.Fatal("Wrong number of ready jobs reported") 	
	}
}	

var result int64

// Sitting at 600us / put
func BenchmarkPut(b *testing.B) {
	conn, _ := DialDefault()
	
	conn.Delete("puttest")
	tube, _ := conn.Use("puttest")
	
	val := []byte("hello")
	var id int64
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		id, _ = tube.Put(val, 0, 0)
	}
	result = id
}

// Sitting at 16ms / reserve
func BenchmarkReserve(b *testing.B) {
	conn, _ := DialDefault()
	
	conn.Delete("putreservetest")
	tube, _ := conn.Use("putreservetest")
	
	val := []byte("hello")
	for n := 0; n < b.N; n++ {
		tube.Put(val, 0, 0)
	}
	
	var id int64
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		id, _, _ = tube.Reserve()
	}
	result = id
}



