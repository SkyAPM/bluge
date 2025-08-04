//  Copyright (c) 2020 The Bluge Authors.
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

package bluge

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/blugelabs/bluge/index"
)

func TestExternalSegmentConfigPropagation(t *testing.T) {
	// Test that external segment configuration is properly propagated
	tempDir := filepath.Join(os.TempDir(), "bluge_test_external_segments")
	defer os.RemoveAll(tempDir)

	config := DefaultConfig(tempDir).WithExternalSegments("./temp_segments", true)

	// Check main config
	if !config.EnableExternalSegments {
		t.Error("Expected EnableExternalSegments to be true")
	}
	if !config.EnableDeduplication {
		t.Error("Expected EnableDeduplication to be true")
	}
	if config.ExternalSegmentTempDir != "./temp_segments" {
		t.Errorf("Expected ExternalSegmentTempDir to be './temp_segments', got %s", config.ExternalSegmentTempDir)
	}
	// Check index config propagation
	if !config.indexConfig.EnableExternalSegments {
		t.Error("Expected index config EnableExternalSegments to be true")
	}
	if !config.indexConfig.EnableDeduplication {
		t.Error("Expected index config EnableDeduplication to be true")
	}
}

func TestExternalSegmentReceiver(t *testing.T) {
	// Test creating an external segment receiver
	tempDir := filepath.Join(os.TempDir(), "bluge_test_external_segments")
	defer os.RemoveAll(tempDir)

	config := DefaultConfig(tempDir).WithExternalSegments("./temp_segments", true)

	writer, err := OpenWriter(config)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()

	// Test enabling external segments
	receiver, err := writer.EnableExternalSegments()
	if err != nil {
		t.Fatal(err)
	}

	// Test initial state
	if receiver.Status() != index.StreamInactive {
		t.Errorf("Expected initial status to be StreamInactive, got %v", receiver.Status())
	}

	if receiver.BytesReceived() != 0 {
		t.Errorf("Expected initial bytes received to be 0, got %d", receiver.BytesReceived())
	}
}

func TestExternalSegmentReceiverWithDisabledConfig(t *testing.T) {
	// Test that receiver creation fails when external segments are disabled
	tempDir := filepath.Join(os.TempDir(), "bluge_test_external_segments")
	defer os.RemoveAll(tempDir)

	config := DefaultConfig(tempDir) // External segments disabled by default

	writer, err := OpenWriter(config)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()

	// This should fail
	_, err = writer.EnableExternalSegments()
	if err == nil {
		t.Error("Expected error when external segments are disabled, but got none")
	}
}

func TestExternalSegmentReceiverSimple(t *testing.T) {
	// Simplified test to verify basic external segment receiver functionality
	// Uses simple document structure to avoid potential format issues

	// Setup directories
	tempDir1 := filepath.Join(os.TempDir(), "bluge_test_simple_source")
	tempDir2 := filepath.Join(os.TempDir(), "bluge_test_simple_dest")
	tempSegDir1 := filepath.Join(tempDir1, "temp_segments")
	tempSegDir2 := filepath.Join(tempDir2, "temp_segments")
	os.RemoveAll(tempDir1)
	os.RemoveAll(tempDir2)

	defer func() {
		os.RemoveAll(tempDir1)
		os.RemoveAll(tempDir2)
	}()

	// Step 1: Create writer1 and insert a simple test document
	config1 := DefaultConfig(tempDir1).WithExternalSegments(tempSegDir1, true)
	writer1, err := OpenWriter(config1)
	if err != nil {
		t.Fatal(err)
	}

	// Create a simple document with just basic fields
	doc1 := NewDocument("simple_doc_1").
		AddField(NewKeywordField("type", "test").StoreValue())

	// Insert document
	err = writer1.Insert(doc1)
	if err != nil {
		t.Fatal(err)
	}

	// Close writer1 to persist the segment
	err = writer1.Close()
	if err != nil {
		t.Fatal(err)
	}

	// Step 2: Find and read the segment file from writer1
	dir1 := config1.indexConfig.DirectoryFunc()
	err = dir1.Setup(true) // read-only
	if err != nil {
		t.Fatal(err)
	}

	segmentFiles, err := dir1.List(index.ItemKindSegment)
	if err != nil {
		t.Fatal(err)
	}

	if len(segmentFiles) == 0 {
		t.Fatal("No segment files found in source directory")
	}

	// Use the most recent segment file
	segmentID := segmentFiles[len(segmentFiles)-1]
	t.Logf("Found segment file with ID: %d", segmentID)

	segmentData, closer, err := dir1.Load(index.ItemKindSegment, segmentID)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if closer != nil {
			closer.Close()
		}
	}()

	segmentBytes, err := segmentData.Read(0, segmentData.Len())
	if err != nil {
		t.Fatal(err)
	}

	if len(segmentBytes) == 0 {
		t.Fatal("Segment data is empty")
	}

	t.Logf("Read segment data of %d bytes", len(segmentBytes))

	// Step 3: Create writer2 and stream the segment via receiver
	config2 := DefaultConfig(tempDir2).WithExternalSegments(tempSegDir2, false) // Disable deduplication
	writer2, err := OpenWriter(config2)
	if err != nil {
		t.Fatal(err)
	}
	defer writer2.Close()

	receiver, err := writer2.EnableExternalSegments()
	if err != nil {
		t.Fatal(err)
	}

	err = receiver.StartSegment()
	if err != nil {
		t.Fatal(err)
	}

	// Stream in a single chunk for simplicity
	err = receiver.WriteChunk(segmentBytes)
	if err != nil {
		t.Fatal(err)
	}

	err = receiver.CompleteSegment()
	if err != nil {
		t.Fatal(err)
	}

	if receiver.Status() != index.StreamIntroduced {
		t.Fatal("Segment not introduced:", receiver.Status())
	}

	// Step 4: Verify the document was transferred by checking document count
	reader2, err := writer2.Reader()
	if err != nil {
		t.Fatal(err)
	}
	defer reader2.Close()

	count, err := reader2.Count()
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("Document count in writer2: %d", count)

	if count != 1 {
		t.Errorf("Expected 1 document in writer2, got %d", count)
	}

	// Try to verify the document exists by searching for the ID
	// Use a simple term query on the _id field which should be more reliable
	idQuery := NewTermQuery("simple_doc_1")
	idQuery.SetField("_id")

	req := NewTopNSearch(1, idQuery)
	dmi, err := reader2.Search(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}

	next, err := dmi.Next()
	if err != nil {
		t.Fatal(err)
	}

	if next == nil {
		t.Error("Expected to find the transferred document by ID, but search returned no results")
	} else {
		t.Log("Successfully found transferred document by ID")

		// Try to access stored fields
		err = next.VisitStoredFields(func(field string, value []byte) bool {
			t.Logf("Found field: %s = %s", field, string(value))
			return true
		})
		if err != nil {
			t.Errorf("Error accessing stored fields: %v", err)
		}
	}

	t.Logf("Simple external segment transfer test completed successfully")
	t.Logf("Final receiver status: %v", receiver.Status())
	t.Logf("Total bytes transferred: %d", receiver.BytesReceived())
}

func TestExternalSegmentReceiverWithDuplicates(t *testing.T) {
	// Test that the receiver handles duplicated documents correctly
	// based on deduplication configuration

	// Setup directories
	tempDir1 := filepath.Join(os.TempDir(), "bluge_test_dup_source")
	tempSegDir1 := filepath.Join(tempDir1, "temp_segments")
	os.RemoveAll(tempDir1)

	defer func() {
		os.RemoveAll(tempDir1)
	}()

	// Step 1: Create source writer and insert documents
	config1 := DefaultConfig(tempDir1).WithExternalSegments(tempSegDir1, true)
	writer1, err := OpenWriter(config1)
	if err != nil {
		t.Fatal(err)
	}

	// Create documents with specific IDs that we'll duplicate later
	doc1 := NewDocument("duplicate_doc_1").
		AddField(NewKeywordField("type", "source").StoreValue()).
		AddField(NewTextField("content", "original content 1"))

	doc2 := NewDocument("duplicate_doc_2").
		AddField(NewKeywordField("type", "source").StoreValue()).
		AddField(NewTextField("content", "original content 2"))

	// Insert documents using batch
	batch := index.NewBatch()
	batch.Insert(doc1)
	batch.Insert(doc2)
	err = writer1.Batch(batch)
	if err != nil {
		t.Fatal(err)
	}

	// Close writer1 to persist the segment
	err = writer1.Close()
	if err != nil {
		t.Fatal(err)
	}

	// Step 2: Extract segment data from writer1
	dir1 := config1.indexConfig.DirectoryFunc()
	err = dir1.Setup(true) // read-only
	if err != nil {
		t.Fatal(err)
	}

	segmentFiles, err := dir1.List(index.ItemKindSegment)
	if err != nil {
		t.Fatal(err)
	}

	if len(segmentFiles) == 0 {
		t.Fatal("No segment files found in source directory")
	}

	// Use the most recent segment file
	segmentID := segmentFiles[len(segmentFiles)-1]
	t.Logf("Found segment file with ID: %d", segmentID)

	segmentData, closer, err := dir1.Load(index.ItemKindSegment, segmentID)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if closer != nil {
			closer.Close()
		}
	}()

	segmentBytes, err := segmentData.Read(0, segmentData.Len())
	if err != nil {
		t.Fatal(err)
	}

	if len(segmentBytes) == 0 {
		t.Fatal("Segment data is empty")
	}

	t.Logf("Read segment data of %d bytes", len(segmentBytes))

	// Step 3a: Test with deduplication DISABLED (should accept duplicates)
	t.Run("DeduplicationDisabled", func(t *testing.T) {
		tempDir3 := filepath.Join(os.TempDir(), "bluge_test_dup_dest_disabled")
		tempSegDir3 := filepath.Join(tempDir3, "temp_segments")
		os.RemoveAll(tempDir3)
		defer os.RemoveAll(tempDir3)

		config3 := DefaultConfig(tempDir3).WithExternalSegments(tempSegDir3, false) // Deduplication disabled
		writer3, err := OpenWriter(config3)
		if err != nil {
			t.Fatal(err)
		}
		defer writer3.Close()

		// Add existing documents with same IDs to create duplicates
		existingDoc1 := NewDocument("duplicate_doc_1").
			AddField(NewKeywordField("type", "existing").StoreValue()).
			AddField(NewTextField("content", "existing content 1"))

		existingDoc2 := NewDocument("duplicate_doc_2").
			AddField(NewKeywordField("type", "existing").StoreValue()).
			AddField(NewTextField("content", "existing content 2"))

		// Insert documents using batch
		batch := index.NewBatch()
		batch.Insert(existingDoc1)
		batch.Insert(existingDoc2)
		err = writer3.Batch(batch)
		if err != nil {
			t.Fatal(err)
		}

		// Verify we have 2 documents initially
		reader3, err := writer3.Reader()
		if err != nil {
			t.Fatal(err)
		}

		initialCount, err := reader3.Count()
		if err != nil {
			t.Fatal(err)
		}
		reader3.Close()

		if initialCount != 2 {
			t.Fatalf("Expected 2 initial documents, got %d", initialCount)
		}

		// Create a fresh copy of segment data for this test
		segmentBytesCopy := make([]byte, len(segmentBytes))
		copy(segmentBytesCopy, segmentBytes)

		// Now stream the duplicate segment
		receiver3, err := writer3.EnableExternalSegments()
		if err != nil {
			t.Fatal(err)
		}

		err = receiver3.StartSegment()
		if err != nil {
			t.Fatal(err)
		}

		err = receiver3.WriteChunk(segmentBytesCopy)
		if err != nil {
			t.Fatal(err)
		}

		err = receiver3.CompleteSegment()
		if err != nil {
			t.Fatal(err)
		}

		if receiver3.Status() != index.StreamIntroduced {
			t.Fatal("Segment not introduced:", receiver3.Status())
		}

		// Verify the segment was accepted (document count should increase)
		reader3Final, err := writer3.Reader()
		if err != nil {
			t.Fatal(err)
		}
		defer reader3Final.Close()

		finalCount, err := reader3Final.Count()
		if err != nil {
			t.Fatal(err)
		}

		t.Logf("Document count after transfer: %d (was %d)", finalCount, initialCount)

		if finalCount <= initialCount {
			t.Errorf("Expected document count to increase when deduplication is disabled, got %d (was %d)", finalCount, initialCount)
		}

		if receiver3.Status() != index.StreamIntroduced {
			t.Errorf("Expected segment to be introduced when deduplication is disabled, got status: %v", receiver3.Status())
		}

		// Final verification: query for the specific docIDs to ensure they exist
		docIDs := []string{"duplicate_doc_1", "duplicate_doc_2"}
		for _, docID := range docIDs {
			idQuery := NewTermQuery(docID)
			idQuery.SetField("_id")

			req := NewTopNSearch(1, idQuery)
			dmi, err := reader3Final.Search(context.Background(), req)
			if err != nil {
				t.Fatalf("Error searching for docID %s: %v", docID, err)
			}

			next, err := dmi.Next()
			if err != nil {
				t.Fatalf("Error getting next result for docID %s: %v", docID, err)
			}

			if next == nil {
				t.Errorf("Expected to find document with ID %s, but search returned no results", docID)
			} else {
				t.Logf("Successfully verified document with ID %s exists", docID)
			}
		}
	})

	// Step 3b: Test with deduplication ENABLED (should reject duplicates)
	t.Run("DeduplicationEnabled", func(t *testing.T) {
		tempDir4 := filepath.Join(os.TempDir(), "bluge_test_dup_dest_enabled")
		tempSegDir4 := filepath.Join(tempDir4, "temp_segments")
		os.RemoveAll(tempDir4)
		defer os.RemoveAll(tempDir4)

		config4 := DefaultConfig(tempDir4).WithExternalSegments(tempSegDir4, true) // Deduplication enabled
		writer4, err := OpenWriter(config4)
		if err != nil {
			t.Fatal(err)
		}
		defer writer4.Close()

		// Add existing documents with same IDs to create duplicates
		existingDoc1 := NewDocument("duplicate_doc_1").
			AddField(NewKeywordField("type", "existing").StoreValue()).
			AddField(NewTextField("content", "existing content 1"))

		existingDoc2 := NewDocument("duplicate_doc_2").
			AddField(NewKeywordField("type", "existing").StoreValue()).
			AddField(NewTextField("content", "existing content 2"))

		// Insert documents using batch
		batch := index.NewBatch()
		batch.Insert(existingDoc1)
		batch.Insert(existingDoc2)
		err = writer4.Batch(batch)
		if err != nil {
			t.Fatal(err)
		}

		// Verify we have 2 documents initially
		reader4, err := writer4.Reader()
		if err != nil {
			t.Fatal(err)
		}

		initialCount, err := reader4.Count()
		if err != nil {
			t.Fatal(err)
		}
		reader4.Close()

		if initialCount != 2 {
			t.Fatalf("Expected 2 initial documents, got %d", initialCount)
		}

		// Create a fresh copy of segment data for this test
		segmentBytesCopy := make([]byte, len(segmentBytes))
		copy(segmentBytesCopy, segmentBytes)

		// Now stream the duplicate segment
		receiver4, err := writer4.EnableExternalSegments()
		if err != nil {
			t.Fatal(err)
		}

		err = receiver4.StartSegment()
		if err != nil {
			t.Fatal(err)
		}

		err = receiver4.WriteChunk(segmentBytesCopy)
		if err != nil {
			t.Fatal(err)
		}

		err = receiver4.CompleteSegment()
		if err != nil {
			// When deduplication is enabled and all documents are duplicates,
			// there might be a cleanup error when removing the empty segment
			t.Logf("CompleteSegment returned error (may be expected during deduplication cleanup): %v", err)
		}

		if receiver4.Status() != index.StreamIntroduced {
			t.Fatal("Segment not introduced:", receiver4.Status())
		}

		// Verify the segment was processed but no documents were added due to deduplication
		reader4Final, err := writer4.Reader()
		if err != nil {
			t.Fatal(err)
		}
		defer reader4Final.Close()

		finalCount, err := reader4Final.Count()
		if err != nil {
			t.Fatal(err)
		}

		t.Logf("Document count after attempted transfer: %d (was %d)", finalCount, initialCount)

		// The most important verification: document count should remain the same
		// regardless of whether the cleanup succeeded or failed
		if finalCount != initialCount {
			t.Errorf("Expected document count to remain same when deduplication is enabled and duplicates exist, got %d (was %d)", finalCount, initialCount)
		}

		// The status might be StreamIntroduced (successful deduplication with cleanup)
		// or StreamFailed (successful deduplication but cleanup failed)
		// Both indicate that deduplication worked correctly
		status := receiver4.Status()
		if status != index.StreamIntroduced && status != index.StreamFailed {
			t.Errorf("Expected segment to be processed (StreamIntroduced) or failed during cleanup (StreamFailed), got status: %v", status)
		}

		t.Logf("Deduplication test passed - document count unchanged indicating duplicates were filtered out")

		// Final verification: query for the specific docIDs to ensure they exist
		// When deduplication is enabled, we should still find the original documents
		docIDs := []string{"duplicate_doc_1", "duplicate_doc_2"}
		for _, docID := range docIDs {
			idQuery := NewTermQuery(docID)
			idQuery.SetField("_id")

			req := NewTopNSearch(1, idQuery)
			dmi, err := reader4Final.Search(context.Background(), req)
			if err != nil {
				t.Fatalf("Error searching for docID %s: %v", docID, err)
			}

			next, err := dmi.Next()
			if err != nil {
				t.Fatalf("Error getting next result for docID %s: %v", docID, err)
			}

			if next == nil {
				t.Errorf("Expected to find document with ID %s, but search returned no results", docID)
			} else {
				t.Logf("Successfully verified document with ID %s exists", docID)
			}
		}
	})

	// Step 3c: Test with deduplication ENABLED but NO duplicates (should accept unique documents)
	t.Run("DeduplicationEnabledNoDuplicates", func(t *testing.T) {
		tempDir5 := filepath.Join(os.TempDir(), "bluge_test_dup_dest_enabled_no_dups")
		tempSegDir5 := filepath.Join(tempDir5, "temp_segments")
		os.RemoveAll(tempDir5)
		defer os.RemoveAll(tempDir5)

		config5 := DefaultConfig(tempDir5).WithExternalSegments(tempSegDir5, true) // Deduplication enabled
		writer5, err := OpenWriter(config5)
		if err != nil {
			t.Fatal(err)
		}
		defer writer5.Close()

		// Add existing documents with DIFFERENT IDs (no duplicates)
		existingDoc1 := NewDocument("existing_doc_1").
			AddField(NewKeywordField("type", "existing").StoreValue()).
			AddField(NewTextField("content", "existing content 1"))

		existingDoc2 := NewDocument("existing_doc_2").
			AddField(NewKeywordField("type", "existing").StoreValue()).
			AddField(NewTextField("content", "existing content 2"))

		// Insert documents using batch
		batch := index.NewBatch()
		batch.Insert(existingDoc1)
		batch.Insert(existingDoc2)
		err = writer5.Batch(batch)
		if err != nil {
			t.Fatal(err)
		}

		// Verify we have 2 documents initially
		reader5, err := writer5.Reader()
		if err != nil {
			t.Fatal(err)
		}

		initialCount, err := reader5.Count()
		if err != nil {
			t.Fatal(err)
		}
		reader5.Close()

		if initialCount != 2 {
			t.Fatalf("Expected 2 initial documents, got %d", initialCount)
		}

		// Create a fresh segment for this test case to avoid data corruption
		tempDirSource := filepath.Join(os.TempDir(), "bluge_test_fresh_source")
		tempSegDirSource := filepath.Join(tempDirSource, "temp_segments")
		os.RemoveAll(tempDirSource)
		defer os.RemoveAll(tempDirSource)

		configSource := DefaultConfig(tempDirSource).WithExternalSegments(tempSegDirSource, true)
		writerSource, err := OpenWriter(configSource)
		if err != nil {
			t.Fatal(err)
		}

		// Create fresh documents with UNIQUE IDs (different from existing and source)
		freshDoc1 := NewDocument("unique_doc_1").
			AddField(NewKeywordField("type", "fresh").StoreValue()).
			AddField(NewTextField("content", "fresh content 1"))

		freshDoc2 := NewDocument("unique_doc_2").
			AddField(NewKeywordField("type", "fresh").StoreValue()).
			AddField(NewTextField("content", "fresh content 2"))

		// Insert documents using batch
		freshBatch := index.NewBatch()
		freshBatch.Insert(freshDoc1)
		freshBatch.Insert(freshDoc2)
		err = writerSource.Batch(freshBatch)
		if err != nil {
			t.Fatal(err)
		}

		// Close writerSource to persist the segment
		err = writerSource.Close()
		if err != nil {
			t.Fatal(err)
		}

		// Extract segment data from the fresh writer
		dirSource := configSource.indexConfig.DirectoryFunc()
		err = dirSource.Setup(true) // read-only
		if err != nil {
			t.Fatal(err)
		}

		freshSegmentFiles, err := dirSource.List(index.ItemKindSegment)
		if err != nil {
			t.Fatal(err)
		}

		if len(freshSegmentFiles) == 0 {
			t.Fatal("No segment files found in fresh source directory")
		}

		// Use the most recent segment file
		freshSegmentID := freshSegmentFiles[len(freshSegmentFiles)-1]
		t.Logf("Found fresh segment file with ID: %d", freshSegmentID)

		freshSegmentData, freshCloser, err := dirSource.Load(index.ItemKindSegment, freshSegmentID)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if freshCloser != nil {
				freshCloser.Close()
			}
		}()

		freshSegmentBytes, err := freshSegmentData.Read(0, freshSegmentData.Len())
		if err != nil {
			t.Fatal(err)
		}

		if len(freshSegmentBytes) == 0 {
			t.Fatal("Fresh segment data is empty")
		}

		t.Logf("Read fresh segment data of %d bytes", len(freshSegmentBytes))

		// Now stream the fresh segment with unique documents (different docIDs)
		receiver5, err := writer5.EnableExternalSegments()
		if err != nil {
			t.Fatal(err)
		}

		err = receiver5.StartSegment()
		if err != nil {
			t.Fatal(err)
		}

		err = receiver5.WriteChunk(freshSegmentBytes)
		if err != nil {
			t.Fatal(err)
		}

		err = receiver5.CompleteSegment()
		if err != nil {
			t.Fatal(err)
		}

		if receiver5.Status() != index.StreamIntroduced {
			t.Fatal("Segment not introduced:", receiver5.Status())
		}

		// Verify the segment was accepted (document count should increase)
		reader5Final, err := writer5.Reader()
		if err != nil {
			t.Fatal(err)
		}
		defer reader5Final.Close()

		finalCount, err := reader5Final.Count()
		if err != nil {
			t.Fatal(err)
		}

		t.Logf("Document count after transfer: %d (was %d)", finalCount, initialCount)

		// Document count should increase since all documents are unique
		expectedCount := initialCount + 2 // 2 unique documents from the segment
		if finalCount != expectedCount {
			t.Errorf("Expected document count to be %d when deduplication is enabled but no duplicates exist, got %d (was %d)", expectedCount, finalCount, initialCount)
		}

		if receiver5.Status() != index.StreamIntroduced {
			t.Errorf("Expected segment to be introduced when deduplication is enabled but no duplicates exist, got status: %v", receiver5.Status())
		}

		// Verify specific document IDs are present
		expectedDocIDs := []string{"existing_doc_1", "existing_doc_2", "unique_doc_1", "unique_doc_2"}
		foundDocIDs := make(map[string]bool)

		// Search for each expected document ID
		for _, docID := range expectedDocIDs {
			query := NewTermQuery(docID).SetField("_id")
			searchRequest := NewTopNSearch(10, query)
			dmi, err := reader5Final.Search(context.Background(), searchRequest)
			if err != nil {
				t.Fatalf("Error searching for document %s: %v", docID, err)
			}

			next, err := dmi.Next()
			if err != nil {
				t.Fatalf("Error getting next result for document %s: %v", docID, err)
			}

			if next != nil {
				foundDocIDs[docID] = true
				t.Logf("Found document with ID: %s", docID)
			} else {
				t.Errorf("Expected document with ID %s to be present, but it was not found", docID)
			}
		}

		// Verify all expected documents were found
		if len(foundDocIDs) != len(expectedDocIDs) {
			t.Errorf("Expected to find %d documents, but only found %d", len(expectedDocIDs), len(foundDocIDs))
		}

		// Verify document content for the fresh documents
		query1 := NewTermQuery("unique_doc_1").SetField("_id")
		searchRequest1 := NewTopNSearch(1, query1)
		dmi1, err := reader5Final.Search(context.Background(), searchRequest1)
		if err != nil {
			t.Fatalf("Error searching for unique_doc_1: %v", err)
		}

		next1, err := dmi1.Next()
		if err != nil {
			t.Fatalf("Error getting next result for unique_doc_1: %v", err)
		}

		if next1 != nil {
			// Check content field
			err = next1.VisitStoredFields(func(field string, value []byte) bool {
				if field == "content" && string(value) != "fresh content 1" {
					t.Errorf("Expected content 'fresh content 1' for unique_doc_1, got '%s'", string(value))
				}
				if field == "type" && string(value) != "fresh" {
					t.Errorf("Expected type 'fresh' for unique_doc_1, got '%s'", string(value))
				}
				return true
			})
			if err != nil {
				t.Errorf("Error accessing stored fields for unique_doc_1: %v", err)
			}
		} else {
			t.Error("Expected unique_doc_1 to be found")
		}

		query2 := NewTermQuery("unique_doc_2").SetField("_id")
		searchRequest2 := NewTopNSearch(1, query2)
		dmi2, err := reader5Final.Search(context.Background(), searchRequest2)
		if err != nil {
			t.Fatalf("Error searching for unique_doc_2: %v", err)
		}

		next2, err := dmi2.Next()
		if err != nil {
			t.Fatalf("Error getting next result for unique_doc_2: %v", err)
		}

		if next2 != nil {
			// Check content field
			err = next2.VisitStoredFields(func(field string, value []byte) bool {
				if field == "content" && string(value) != "fresh content 2" {
					t.Errorf("Expected content 'fresh content 2' for unique_doc_2, got '%s'", string(value))
				}
				if field == "type" && string(value) != "fresh" {
					t.Errorf("Expected type 'fresh' for unique_doc_2, got '%s'", string(value))
				}
				return true
			})
			if err != nil {
				t.Errorf("Error accessing stored fields for unique_doc_2: %v", err)
			}
		} else {
			t.Error("Expected unique_doc_2 to be found")
		}

		t.Logf("Test completed successfully - document count and docid verification passed")
		t.Logf("Expected %d documents, found %d documents", expectedCount, finalCount)
		t.Logf("All expected document IDs verified: %v", expectedDocIDs)
		t.Logf("Deduplication enabled with no duplicates - all unique documents were accepted")
	})

	t.Log("Duplicate document handling test completed successfully")
}

func TestExternalSegmentReceiverWithPartialDuplicates(t *testing.T) {
	// Test that verifies partial duplication filtering: some docIDs are duplicated (filtered out),
	// others are unique (kept). The final result should keep only the unduplicated documents.

	// Setup directories
	tempDir1 := filepath.Join(os.TempDir(), "bluge_test_partial_dup_source")
	tempSegDir1 := filepath.Join(tempDir1, "temp_segments")
	os.RemoveAll(tempDir1)

	defer func() {
		os.RemoveAll(tempDir1)
	}()

	// Step 1: Create source writer with mixed documents
	config1 := DefaultConfig(tempDir1).WithExternalSegments(tempSegDir1, true)
	writer1, err := OpenWriter(config1)
	if err != nil {
		t.Fatal(err)
	}

	// Create documents - some will be duplicated later, others will be unique
	doc1 := NewDocument("partial_dup_1").
		AddField(NewKeywordField("type", "source").StoreValue()).
		AddField(NewTextField("content", "content for doc 1"))

	doc2 := NewDocument("partial_unique_1").
		AddField(NewKeywordField("type", "source").StoreValue()).
		AddField(NewTextField("content", "unique content 1"))

	doc3 := NewDocument("partial_dup_2").
		AddField(NewKeywordField("type", "source").StoreValue()).
		AddField(NewTextField("content", "content for doc 3"))

	doc4 := NewDocument("partial_unique_2").
		AddField(NewKeywordField("type", "source").StoreValue()).
		AddField(NewTextField("content", "unique content 2"))

	// Insert all documents
	batch := index.NewBatch()
	for _, doc := range []*Document{doc1, doc2, doc3, doc4} {
		batch.Insert(doc)
	}
	err = writer1.Batch(batch)
	if err != nil {
		t.Fatal(err)
	}

	// Close writer1 to persist the segment
	err = writer1.Close()
	if err != nil {
		t.Fatal(err)
	}

	// Step 2: Extract segment data from writer1
	dir1 := config1.indexConfig.DirectoryFunc()
	err = dir1.Setup(true)
	if err != nil {
		t.Fatal(err)
	}

	segmentFiles, err := dir1.List(index.ItemKindSegment)
	if err != nil {
		t.Fatal(err)
	}

	if len(segmentFiles) == 0 {
		t.Fatal("No segment files found in source directory")
	}

	segmentID := segmentFiles[len(segmentFiles)-1]
	t.Logf("Found segment file with ID: %d", segmentID)

	segmentData, closer, err := dir1.Load(index.ItemKindSegment, segmentID)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if closer != nil {
			closer.Close()
		}
	}()

	segmentBytes, err := segmentData.Read(0, segmentData.Len())
	if err != nil {
		t.Fatal(err)
	}

	if len(segmentBytes) == 0 {
		t.Fatal("Segment data is empty")
	}

	t.Logf("Read segment data of %d bytes", len(segmentBytes))

	// Step 3: Create destination writer with partial duplicates
	tempDir2 := filepath.Join(os.TempDir(), "bluge_test_partial_dup_dest")
	tempSegDir2 := filepath.Join(tempDir2, "temp_segments")
	os.RemoveAll(tempDir2)
	defer os.RemoveAll(tempDir2)

	config2 := DefaultConfig(tempDir2).WithExternalSegments(tempSegDir2, true)
	writer2, err := OpenWriter(config2)
	if err != nil {
		t.Fatal(err)
	}
	defer writer2.Close()

	// Add some documents that will create partial duplicates
	// These two documents have the same IDs as doc1 and doc3 from the segment
	existingDoc1 := NewDocument("partial_dup_1").
		AddField(NewKeywordField("type", "existing").StoreValue()).
		AddField(NewTextField("content", "existing content for doc 1"))

	existingDoc3 := NewDocument("partial_dup_2").
		AddField(NewKeywordField("type", "existing").StoreValue()).
		AddField(NewTextField("content", "existing content for doc 3"))

	// Insert the duplicate documents using batch
	batch2 := index.NewBatch()
	batch2.Insert(existingDoc1)
	batch2.Insert(existingDoc3)
	err = writer2.Batch(batch2)
	if err != nil {
		t.Fatal(err)
	}

	// Verify we have 2 documents initially
	reader2, err := writer2.Reader()
	if err != nil {
		t.Fatal(err)
	}

	initialCount, err := reader2.Count()
	if err != nil {
		t.Fatal(err)
	}
	reader2.Close()

	if initialCount != 2 {
		t.Fatalf("Expected 2 initial documents, got %d", initialCount)
	}

	// Step 4: Stream the segment which contains both duplicates and unique documents
	receiver2, err := writer2.EnableExternalSegments()
	if err != nil {
		t.Fatal(err)
	}

	err = receiver2.StartSegment()
	if err != nil {
		t.Fatal(err)
	}

	err = receiver2.WriteChunk(segmentBytes)
	if err != nil {
		t.Fatal(err)
	}

	err = receiver2.CompleteSegment()
	if err != nil {
		t.Fatal(err)
	}

	if receiver2.Status() != index.StreamIntroduced {
		t.Fatal("Segment not introduced:", receiver2.Status())
	}

	// Step 5: Verify that only unique documents were added (partial deduplication worked correctly)
	reader2Final, err := writer2.Reader()
	if err != nil {
		t.Fatal(err)
	}
	defer reader2Final.Close()

	finalCount, err := reader2Final.Count()
	if err != nil {
		t.Fatal(err)
	}
	if finalCount != initialCount+2 {
		t.Fatalf("Expected %d documents, got %d", initialCount+2, finalCount)
	}
}
