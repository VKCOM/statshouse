// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package aggregator

import (
	"context"
	"log"
	"time"
)

const (
	noErrorsWindow = 5 * 60 // 5 minutes
	maxInsertTime  = 2.0    // 2 seconds
	sleepInterval  = 30 * time.Second
)

// goMigrate runs the migration loop in a goroutine.
// It checks Aggregator's timeSinceLastError and insertTimeEWMA to decide whether to proceed.
func (a *Aggregator) goMigrate(cancelCtx context.Context) {
	// TODO: implement, rough plan below
	// border ts is timestamp after which all data is migrated
	// single ts can be too big, so we need to migrate in chunks in order to do it we store additional offset
	// 1. [x] check remote config flag for migration
	// 2. [x] check current load and decide if we need to migrate or just wait (look at insert timings and errors)
	// 3. [ ] look for migration state if there is no state, create it (some table in ClickHouse)
	// 4. [ ] if we need to migrate more data, select data from V2 - data is capped by 2GB per shard per hour, so no need to break hour into chunks
	// 5. [ ] insert into V3
	// 6. [ ] if success, save new border of migration and offset on current border in ClickHouse migration state table

	if a.replicaKey != 1 {
		return // Only one replica should run migration
	}
	log.Println("[migration] Starting migration routine")
	for {
		nowUnix := uint32(time.Now().Unix())
		if a.lastErrorTs != 0 && nowUnix >= a.lastErrorTs && nowUnix-a.lastErrorTs < noErrorsWindow {
			log.Printf("[migration] Skipping: last error was %d seconds ago", nowUnix-a.lastErrorTs)
			time.Sleep(time.Duration(noErrorsWindow-(nowUnix-a.lastErrorTs)) * time.Second)
			continue
		}
		if a.insertTimeEWMA > maxInsertTime {
			log.Printf("[migration] Skipping: EWMA insert time is too high (%.2fs)", a.insertTimeEWMA)
			time.Sleep(sleepInterval)
			continue
		}

		// TODO: Add actual migration logic here
		log.Println("[migration] Would run migration step here")
		time.Sleep(10 * time.Second)

		select {
		case <-cancelCtx.Done():
			log.Println("[migration] Exiting migration routine (context cancelled)")
			return
		default:
		}
	}
}
