//  Copyright (c) 2020 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 		http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package index

import (
	"sync/atomic"
	"testing"
)

func TestStats_ToMap(t *testing.T) {
	stats := &Stats{
		TotUpdates: 1,
		TotDeletes: 2,
		// Initialize other fields as needed
	}

	atomic.StoreUint64(&stats.TotBatches, 3)
	atomic.StoreUint64(&stats.CurOnDiskBytes, 1000)

	m := stats.ToMap()

	if m["TotUpdates"] != uint64(1) {
		t.Errorf("Expected TotUpdates to be 1, got %v", m["TotUpdates"])
	}
	if m["TotDeletes"] != uint64(2) {
		t.Errorf("Expected TotDeletes to be 2, got %v", m["TotDeletes"])
	}
	if m["TotBatches"] != uint64(3) {
		t.Errorf("Expected TotBatches to be 3, got %v", m["TotBatches"])
	}
	if m["CurOnDiskBytes"] != uint64(1000) {
		t.Errorf("Expected CurOnDiskBytes to be 1000, got %v", m["CurOnDiskBytes"])
	}
}

func TestStats_FromMap(t *testing.T) {
	stats := &Stats{}

	m := map[string]interface{}{
		"TotUpdates":        uint64(10),
		"TotDeletes":        uint64(20),
		"CurOnDiskBytes":    uint64(3000),
		"CurOnDiskFiles":    uint64(5),
		"TotBatchesEmpty":   uint64(7),
		"MaxBatchIntroTime": uint64(15),
	}

	stats.FromMap(m)

	if atomic.LoadUint64(&stats.TotUpdates) != 10 {
		t.Errorf("Expected TotUpdates to be 10, got %v", stats.TotUpdates)
	}
	if atomic.LoadUint64(&stats.TotDeletes) != 20 {
		t.Errorf("Expected TotDeletes to be 20, got %v", stats.TotDeletes)
	}
	if atomic.LoadUint64(&stats.CurOnDiskBytes) != 3000 {
		t.Errorf("Expected CurOnDiskBytes to be 3000, got %v", stats.CurOnDiskBytes)
	}
	if atomic.LoadUint64(&stats.CurOnDiskFiles) != 5 {
		t.Errorf("Expected CurOnDiskFiles to be 5, got %v", stats.CurOnDiskFiles)
	}
	if atomic.LoadUint64(&stats.TotBatchesEmpty) != 7 {
		t.Errorf("Expected TotBatchesEmpty to be 7, got %v", stats.TotBatchesEmpty)
	}
	if atomic.LoadUint64(&stats.MaxBatchIntroTime) != 15 {
		t.Errorf("Expected MaxBatchIntroTime to be 15, got %v", stats.MaxBatchIntroTime)
	}
}

func TestStats_Clone(t *testing.T) {
	stats := &Stats{
		TotUpdates: 5,
		TotDeletes: 10,
	}

	atomic.StoreUint64(&stats.TotBatches, 15)
	atomic.StoreUint64(&stats.CurOnDiskBytes, 5000)

	clone := stats.Clone()

	if clone.TotUpdates != 5 {
		t.Errorf("Expected cloned TotUpdates to be 5, got %v", clone.TotUpdates)
	}
	if clone.TotDeletes != 10 {
		t.Errorf("Expected cloned TotDeletes to be 10, got %v", clone.TotDeletes)
	}
	if clone.TotBatches != 15 {
		t.Errorf("Expected cloned TotBatches to be 15, got %v", clone.TotBatches)
	}
	if clone.CurOnDiskBytes != 5000 {
		t.Errorf("Expected cloned CurOnDiskBytes to be 5000, got %v", clone.CurOnDiskBytes)
	}

	// Modify original and ensure clone is unaffected
	atomic.StoreUint64(&stats.TotUpdates, 50)
	atomic.StoreUint64(&stats.CurOnDiskBytes, 10000)

	if clone.TotUpdates != 5 {
		t.Errorf("Expected cloned TotUpdates to remain 5, got %v", clone.TotUpdates)
	}
	if clone.CurOnDiskBytes != 5000 {
		t.Errorf("Expected cloned CurOnDiskBytes to remain 5000, got %v", clone.CurOnDiskBytes)
	}
}
