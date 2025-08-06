package main

import (
	"flag"
	"log"
	"time"

	"github.com/VKCOM/statshouse/internal/aggregator"
)

func main() {
	var (
		khAddr     = flag.String("kh-addr", "localhost:8123", "ClickHouse address")
		khUser     = flag.String("kh-user", "", "ClickHouse user")
		khPassword = flag.String("kh-password", "", "ClickHouse password")
		timestamp  = flag.Uint("timestamp", 0, "Timestamp to migrate (rounded to hour). If 0, uses current hour")
		shardKey   = flag.Int("shard-key", 1, "Shard key (1-16)")
		createTest = flag.Bool("create-test", false, "Create test data before migration")
	)
	flag.Parse()

	if *timestamp == 0 {
		// Use current hour rounded down
		now := time.Now()
		*timestamp = uint(now.Truncate(time.Hour).Unix())
	}

	log.Printf("Testing migration with ClickHouse at %s", *khAddr)
	log.Printf("Timestamp: %d (%s)", *timestamp, time.Unix(int64(*timestamp), 0).Format("2006-01-02 15:04:05"))
	log.Printf("Shard key: %d", *shardKey)

	if *createTest {
		log.Println("Creating test data...")
		err := aggregator.CreateTestDataV2(*khAddr, *khUser, *khPassword, uint32(*timestamp))
		if err != nil {
			log.Fatalf("Failed to create test data: %v", err)
		}
		log.Println("Test data created successfully")
	}

	log.Println("Starting migration...")
	err := aggregator.TestMigrateSingleHour(*khAddr, *khUser, *khPassword, uint32(*timestamp), int32(*shardKey))
	if err != nil {
		log.Fatalf("Migration failed: %v", err)
	}
	log.Println("Migration completed successfully!")
}
