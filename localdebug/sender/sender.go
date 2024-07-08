package main

import (
	"fmt"
	"log"
	"net"
	"time"

	"pgregory.net/rand"
)

// primitive tool for testing conveyor jitter
// alternative agent port, because default agent receives tons of other metrics from API
func main() {
	conn, err := net.Dial("udp", "127.0.0.1:13334")
	if err != nil {
		log.Fatalf("Dial error: %v", err)
	}

	ts := time.Now().Unix()
	for {
		time.Sleep(time.Millisecond * 100)
		ts2 := time.Now().Unix()
		if ts == ts2 {
			continue
		}
		time.Sleep(time.Millisecond * time.Duration(rand.Intn(2000))) // simulate slow packet delivery
		for ; ts < ts2; ts++ {                                        // skip no events
			str := fmt.Sprintf(`{"metrics":[{"ts":%d, "name":"gbuteyko_investigation","tags":{"1":"I_test_statshouse","2":"2"},"counter":1}]}`, ts)
			fmt.Println(str)
			if _, err := conn.Write([]byte(str)); err != nil {
				log.Printf("error writing UDP: %v", err)
			}
		}
	}
}
