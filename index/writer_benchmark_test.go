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
	"fmt"
	"os"
	"runtime/pprof"
	"testing"
	"time"
)

// goos: darwin
// goarch: arm64
// pkg: github.com/blugelabs/bluge/index
// cpu: Apple M1 Pro
// BenchmarkWriter_removeExistingDocuments-NoCache    	   24632	     41085 ns/op	   18711 B/op	     204 allocs/op
// BenchmarkWriter_removeExistingDocuments-Cache    	  161628	      6865 ns/op	    2456 B/op	     102 allocs/op
func BenchmarkWriter_removeExistingDocuments(b *testing.B) {
	cfg, cleanup := CreateConfig("BenchmarkWriter_removeExistingDocuments")
	cfg.CacheMaxBytes = 100 << 20
	defer func() {
		err := cleanup()
		if err != nil {
			b.Log(err)
		}
	}()

	idx, err := OpenWriter(cfg)
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		err = idx.Close()
		if err != nil {
			b.Fatal(err)
		}
	}()
	for i := 0; i < 3000; i += 100 {
		batch := NewBatch()
		for j := 0; j < 100; j++ {
			serviceName := fmt.Sprintf("service-%d", (i+j)%10)              // 10 different service names
			ipAddress := fmt.Sprintf("192.168.%d.%d", (i+j)/256, (i+j)%256) // IP addresses
			docID := fmt.Sprintf("%s-%s", serviceName, ipAddress)
			doc := &FakeDocument{
				NewFakeField("_id", docID, true, false, true),
				NewFakeField("title", fmt.Sprintf("mister-%d", i), true, false, true),
			}
			batch.Insert(doc)
		}
		if err := idx.Batch(batch); err != nil {
			b.Fatalf("failed to apply batch: %v", err)
		}
	}
	time.Sleep(1 * time.Second)
	batchRemove := NewBatch()
	for j := 0; j < 100; j++ {
		serviceName := fmt.Sprintf("service-%d", j%10)          // 10 different service names
		ipAddress := fmt.Sprintf("192.168.%d.%d", j/256, j%256) // IP addresses
		docID := fmt.Sprintf("%s-%s", serviceName, ipAddress)   // Document ID composed of service name and IP address
		doc := &FakeDocument{
			NewFakeField("_id", docID, true, false, true),
			NewFakeField("title", fmt.Sprintf("mister-%d", j), true, false, true),
		}
		batchRemove.InsertIfAbsent(testIdentifier(docID), []string{"title"}, doc)
	}

	// Start profiling
	f, err := os.Create("cpu.prof")
	if err != nil {
		b.Fatal(err)
	}
	defer f.Close()
	if err := pprof.StartCPUProfile(f); err != nil {
		b.Fatal(err)
	}
	defer pprof.StopCPUProfile()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		batchRemove.ResetDoc()
		err := idx.removeExistingDocuments(batchRemove)
		if err != nil {
			b.Fatal(err)
		}
	}
}
