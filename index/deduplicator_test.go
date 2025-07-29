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

// deduplicator_test.go provides comprehensive test coverage for ExternalSegmentDeduplicator.
//
// Test Coverage:
// - Constructor validation (NewExternalSegmentDeduplicator)
// - Deduplication behavior when disabled
// - Processing empty index scenarios
// - No duplicates found (segment accepted)
// - Duplicates found (segment rejected)
// - Documents without _id fields
// - Mixed documents (with and without _id)
// - Multiple calls and caching behavior
// - Large index performance testing
//
// The tests verify the "keep existing" deduplication strategy where
// incoming segments are rejected entirely if any documents already exist in the index.

package index

import (
	"fmt"
	"strconv"
	"testing"

	segment "github.com/blugelabs/bluge_segment_api"
)

// Test helper functions for common operations
func setupTestWriter(testName string) (*Writer, func() error, error) {
	cfg, cleanup := CreateConfig(testName)
	writer, err := OpenWriter(cfg)
	if err != nil {
		if cleanupErr := cleanup(); cleanupErr != nil {
			return nil, nil, fmt.Errorf("failed to open writer: %w, and failed to cleanup: %w", err, cleanupErr)
		}
		return nil, nil, err
	}

	cleanupFn := func() error {
		err := writer.Close()
		if err != nil {
			if cleanupErr := cleanup(); cleanupErr != nil {
				return fmt.Errorf("failed to close writer: %w, and failed to cleanup: %w", err, cleanupErr)
			}
			return err
		}
		return cleanup()
	}

	return writer, cleanupFn, nil
}

func createTestDocument(id, content string) segment.Document {
	if id != "" {
		return &FakeDocument{
			NewFakeField("_id", id, true, false, false),
			NewFakeField("content", content, true, false, true),
		}
	}
	return &FakeDocument{
		NewFakeField("content", content, true, false, true),
	}
}

func addDocumentsToIndex(writer *Writer, docs map[string]string) error {
	batch := NewBatch()
	for id, content := range docs {
		doc := &FakeDocument{
			NewFakeField("_id", id, true, false, false),
			NewFakeField("content", content, true, false, true),
		}
		batch.Update(testIdentifier(id), doc)
	}
	return writer.Batch(batch)
}

func createTestSegment(writer *Writer, docs []segment.Document) (*segmentWrapper, error) {
	segWrapper, _, err := writer.newSegment(docs)
	return segWrapper, err
}

// Helper function to extract all documents from a segment
func extractDocumentsFromSegment(segWrapper *segmentWrapper) (map[string]map[string][]byte, error) {
	documents := make(map[string]map[string][]byte)
	docCount := segWrapper.Count()

	for docNum := uint64(0); docNum < docCount; docNum++ {
		docFields := make(map[string][]byte)
		var docID string

		err := segWrapper.VisitStoredFields(docNum, func(field string, value []byte) bool {
			// Make a copy of the value to avoid issues with reused buffers
			valueCopy := make([]byte, len(value))
			copy(valueCopy, value)
			docFields[field] = valueCopy

			if field == "_id" {
				docID = string(value)
			}
			return true
		})

		if err != nil {
			return nil, fmt.Errorf("failed to visit document %d: %w", docNum, err)
		}

		// Use docID if available, otherwise use docNum as key
		if docID != "" {
			documents[docID] = docFields
		} else {
			documents[fmt.Sprintf("doc_%d", docNum)] = docFields
		}
	}

	return documents, nil
}

// Helper function to verify segment contains expected documents
func verifySegmentDocuments(t *testing.T, segWrapper *segmentWrapper, expectedDocs map[string]map[string]string) {
	if segWrapper == nil {
		t.Fatal("Segment wrapper is nil")
	}

	actualDocs, err := extractDocumentsFromSegment(segWrapper)
	if err != nil {
		t.Fatalf("Failed to extract documents from segment: %v", err)
	}

	// Verify document count
	if len(actualDocs) != len(expectedDocs) {
		t.Errorf("Expected %d documents, got %d", len(expectedDocs), len(actualDocs))
	}

	// Verify each expected document
	for expectedID, expectedFields := range expectedDocs {
		actualFields, exists := actualDocs[expectedID]
		if !exists {
			t.Errorf("Expected document with ID '%s' not found", expectedID)
			continue
		}

		// Verify each field in the document
		for fieldName, expectedValue := range expectedFields {
			actualValue, fieldExists := actualFields[fieldName]
			if !fieldExists {
				t.Errorf("Field '%s' not found in document '%s'", fieldName, expectedID)
				continue
			}

			if string(actualValue) != expectedValue {
				t.Errorf("Field '%s' in document '%s': expected '%s', got '%s'",
					fieldName, expectedID, expectedValue, string(actualValue))
			}
		}
	}
}

// Helper function to verify segment is empty or contains specific documents
func verifySegmentResult(t *testing.T, result *segmentWrapper, originalDocs []segment.Document, expectNil bool, testName string) {
	if expectNil {
		if result != nil {
			t.Errorf("Expected nil result but got segment (%s)", testName)
		}
		return
	}

	if result == nil {
		t.Errorf("Expected segment but got nil (%s)", testName)
		return
	}

	// Build expected documents map from original docs
	expectedDocs := make(map[string]map[string]string)
	for _, doc := range originalDocs {
		var docID string
		docFields := make(map[string]string)

		doc.EachField(func(field segment.Field) {
			fieldName := field.Name()
			fieldValue := string(field.Value())
			docFields[fieldName] = fieldValue

			if fieldName == "_id" {
				docID = fieldValue
			}
		})

		if docID != "" {
			expectedDocs[docID] = docFields
		} else {
			// For documents without _id, use a generated key
			expectedDocs[fmt.Sprintf("doc_%d", len(expectedDocs))] = docFields
		}
	}

	verifySegmentDocuments(t, result, expectedDocs)
}

func TestNewExternalSegmentDeduplicator(t *testing.T) {
	cfg, cleanup := CreateConfig("TestNewExternalSegmentDeduplicator")
	defer func() {
		err := cleanup()
		if err != nil {
			t.Log(err)
		}
	}()

	writer, err := OpenWriter(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = writer.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	tests := []struct {
		name    string
		enabled bool
	}{
		{"deduplication enabled", true},
		{"deduplication disabled", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dedup := NewExternalSegmentDeduplicator(writer, tt.enabled)
			if dedup == nil {
				t.Fatal("NewExternalSegmentDeduplicator returned nil")
			}
			if dedup.writer != writer {
				t.Error("writer not set correctly")
			}
			if dedup.enabled != tt.enabled {
				t.Errorf("enabled = %v, want %v", dedup.enabled, tt.enabled)
			}
			if dedup.existingDocIDs != nil {
				t.Error("existingDocIDs should be nil initially")
			}
		})
	}
}

func TestProcessExternalSegment_DeduplicationDisabled(t *testing.T) {
	writer, cleanup, err := setupTestWriter("TestProcessExternalSegment_DeduplicationDisabled")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := cleanup(); err != nil {
			t.Log(err)
		}
	}()

	dedup := NewExternalSegmentDeduplicator(writer, false)

	// Create a test segment using helper
	docs := []segment.Document{
		createTestDocument("1", "test content"),
	}

	segWrapper, err := createTestSegment(writer, docs)
	if err != nil {
		t.Fatal(err)
	}

	result, err := dedup.ProcessExternalSegment(segWrapper)
	if err != nil {
		t.Errorf("ProcessExternalSegment failed: %v", err)
	}

	// Verify the result contains the expected documents
	verifySegmentResult(t, result, docs, false, "deduplication disabled")
}

func TestProcessExternalSegment_EmptyIndex(t *testing.T) {
	cfg, cleanup := CreateConfig("TestProcessExternalSegment_EmptyIndex")
	defer func() {
		err := cleanup()
		if err != nil {
			t.Log(err)
		}
	}()

	writer, err := OpenWriter(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = writer.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	dedup := NewExternalSegmentDeduplicator(writer, true)

	// Create a test segment
	docs := []segment.Document{
		&FakeDocument{
			NewFakeField("_id", "1", true, false, false),
			NewFakeField("content", "test content", true, false, true),
		},
	}

	segWrapper, _, err := writer.newSegment(docs)
	if err != nil {
		t.Fatal(err)
	}

	result, err := dedup.ProcessExternalSegment(segWrapper)
	if err != nil {
		t.Errorf("ProcessExternalSegment failed: %v", err)
	}

	// Verify the result contains the expected documents
	verifySegmentResult(t, result, docs, false, "empty index")
}

func TestProcessExternalSegment_NoDuplicates(t *testing.T) {
	cfg, cleanup := CreateConfig("TestProcessExternalSegment_NoDuplicates")
	defer func() {
		err := cleanup()
		if err != nil {
			t.Log(err)
		}
	}()

	writer, err := OpenWriter(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = writer.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	// Add some existing documents to the index
	batch := NewBatch()
	batch.Update(testIdentifier("existing1"), &FakeDocument{
		NewFakeField("_id", "existing1", true, false, false),
		NewFakeField("content", "existing content 1", true, false, true),
	})
	batch.Update(testIdentifier("existing2"), &FakeDocument{
		NewFakeField("_id", "existing2", true, false, false),
		NewFakeField("content", "existing content 2", true, false, true),
	})
	err = writer.Batch(batch)
	if err != nil {
		t.Fatal(err)
	}

	dedup := NewExternalSegmentDeduplicator(writer, true)

	// Create a test segment with new documents
	docs := []segment.Document{
		&FakeDocument{
			NewFakeField("_id", "new1", true, false, false),
			NewFakeField("content", "new content 1", true, false, true),
		},
		&FakeDocument{
			NewFakeField("_id", "new2", true, false, false),
			NewFakeField("content", "new content 2", true, false, true),
		},
	}

	segWrapper, _, err := writer.newSegment(docs)
	if err != nil {
		t.Fatal(err)
	}

	result, err := dedup.ProcessExternalSegment(segWrapper)
	if err != nil {
		t.Errorf("ProcessExternalSegment failed: %v", err)
	}

	// Verify the result contains the expected documents
	verifySegmentResult(t, result, docs, false, "no duplicates")
}

func TestProcessExternalSegment_WithDuplicates(t *testing.T) {
	writer, cleanup, err := setupTestWriter("TestProcessExternalSegment_WithDuplicates")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := cleanup(); err != nil {
			t.Log(err)
		}
	}()

	// Add existing documents using helper
	existingDocs := map[string]string{
		"doc1": "existing content 1",
		"doc2": "existing content 2",
	}
	if err := addDocumentsToIndex(writer, existingDocs); err != nil {
		t.Fatal(err)
	}

	dedup := NewExternalSegmentDeduplicator(writer, true)

	// Create a test segment with duplicate documents
	docs := []segment.Document{
		createTestDocument("doc1", "new content for doc1"), // Duplicate!
		createTestDocument("new_doc", "truly new content"),
	}

	segWrapper, err := createTestSegment(writer, docs)
	if err != nil {
		t.Fatal(err)
	}

	result, err := dedup.ProcessExternalSegment(segWrapper)
	if err != nil {
		t.Errorf("ProcessExternalSegment failed: %v", err)
	}

	// Verify the result contains only non-duplicate documents
	if result == nil {
		t.Error("Expected filtered segment but got nil")
		return
	}

	// Extract documents from the filtered segment
	actualDocs, err := extractDocumentsFromSegment(result)
	if err != nil {
		t.Fatalf("Failed to extract documents from filtered segment: %v", err)
	}

	// Should only have the non-duplicate document "new_doc"
	expectedCount := 1
	if len(actualDocs) != expectedCount {
		t.Errorf("Expected %d documents after filtering, got %d", expectedCount, len(actualDocs))
	}

	// Verify "new_doc" is present and "doc1" is filtered out
	if _, exists := actualDocs["new_doc"]; !exists {
		t.Error("Expected non-duplicate document 'new_doc' not found in filtered segment")
	}
	if _, exists := actualDocs["doc1"]; exists {
		t.Error("Duplicate document 'doc1' should have been filtered out")
	}
}

func TestProcessExternalSegment_DocumentsWithoutID(t *testing.T) {
	cfg, cleanup := CreateConfig("TestProcessExternalSegment_DocumentsWithoutID")
	defer func() {
		err := cleanup()
		if err != nil {
			t.Log(err)
		}
	}()

	writer, err := OpenWriter(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = writer.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	dedup := NewExternalSegmentDeduplicator(writer, true)

	// Create a test segment with documents without _id field
	docs := []segment.Document{
		&FakeDocument{
			NewFakeField("content", "content without id", true, false, true),
			NewFakeField("title", "title field", true, false, true),
		},
	}

	segWrapper, _, err := writer.newSegment(docs)
	if err != nil {
		t.Fatal(err)
	}

	result, err := dedup.ProcessExternalSegment(segWrapper)
	if err != nil {
		t.Errorf("ProcessExternalSegment failed: %v", err)
	}

	// Verify the result contains the expected documents
	verifySegmentResult(t, result, docs, false, "documents without ID")
}

func TestProcessExternalSegment_MixedDocumentsWithAndWithoutID(t *testing.T) {
	cfg, cleanup := CreateConfig("TestProcessExternalSegment_MixedDocumentsWithAndWithoutID")
	defer func() {
		err := cleanup()
		if err != nil {
			t.Log(err)
		}
	}()

	writer, err := OpenWriter(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = writer.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	// Add existing document
	batch := NewBatch()
	batch.Update(testIdentifier("existing"), &FakeDocument{
		NewFakeField("_id", "existing", true, false, false),
		NewFakeField("content", "existing content", true, false, true),
	})
	err = writer.Batch(batch)
	if err != nil {
		t.Fatal(err)
	}

	dedup := NewExternalSegmentDeduplicator(writer, true)

	// Create a test segment with mixed documents
	docs := []segment.Document{
		&FakeDocument{
			NewFakeField("content", "content without id", true, false, true),
		},
		&FakeDocument{
			NewFakeField("_id", "new_doc", true, false, false),
			NewFakeField("content", "new content", true, false, true),
		},
		&FakeDocument{
			NewFakeField("_id", "existing", true, false, false), // Duplicate!
			NewFakeField("content", "duplicate content", true, false, true),
		},
	}

	segWrapper, _, err := writer.newSegment(docs)
	if err != nil {
		t.Fatal(err)
	}

	result, err := dedup.ProcessExternalSegment(segWrapper)
	if err != nil {
		t.Errorf("ProcessExternalSegment failed: %v", err)
	}

	// Verify the result contains only non-duplicate documents
	if result == nil {
		t.Error("Expected filtered segment but got nil")
		return
	}

	// Extract documents from the filtered segment
	actualDocs, err := extractDocumentsFromSegment(result)
	if err != nil {
		t.Fatalf("Failed to extract documents from filtered segment: %v", err)
	}

	// Should have 2 documents: one without ID and "new_doc"
	// The "existing" document should be filtered out as it's a duplicate
	expectedCount := 2
	if len(actualDocs) != expectedCount {
		t.Errorf("Expected %d documents after filtering, got %d", expectedCount, len(actualDocs))
	}

	// Verify "new_doc" is present and "existing" is filtered out
	if _, exists := actualDocs["new_doc"]; !exists {
		t.Error("Expected non-duplicate document 'new_doc' not found in filtered segment")
	}
	if _, exists := actualDocs["existing"]; exists {
		t.Error("Duplicate document 'existing' should have been filtered out")
	}
}

func TestProcessExternalSegment_MultipleCalls(t *testing.T) {
	cfg, cleanup := CreateConfig("TestProcessExternalSegment_MultipleCalls")
	defer func() {
		err := cleanup()
		if err != nil {
			t.Log(err)
		}
	}()

	writer, err := OpenWriter(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = writer.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	// Add existing documents
	batch := NewBatch()
	batch.Update(testIdentifier("doc1"), &FakeDocument{
		NewFakeField("_id", "doc1", true, false, false),
		NewFakeField("content", "content 1", true, false, true),
	})
	err = writer.Batch(batch)
	if err != nil {
		t.Fatal(err)
	}

	dedup := NewExternalSegmentDeduplicator(writer, true)

	// First call - no duplicates
	docs1 := []segment.Document{
		&FakeDocument{
			NewFakeField("_id", "new1", true, false, false),
			NewFakeField("content", "new content 1", true, false, true),
		},
	}
	segWrapper1, _, err := writer.newSegment(docs1)
	if err != nil {
		t.Fatal(err)
	}

	result1, err := dedup.ProcessExternalSegment(segWrapper1)
	if err != nil {
		t.Errorf("First ProcessExternalSegment failed: %v", err)
	}

	// Verify the first result contains the expected documents
	verifySegmentResult(t, result1, docs1, false, "first call - no duplicates")

	// Second call - with duplicate
	docs2 := []segment.Document{
		&FakeDocument{
			NewFakeField("_id", "doc1", true, false, false), // Duplicate!
			NewFakeField("content", "duplicate content", false, false, true),
		},
	}
	segWrapper2, _, err := writer.newSegment(docs2)
	if err != nil {
		t.Fatal(err)
	}

	result2, err := dedup.ProcessExternalSegment(segWrapper2)
	if err != nil {
		t.Errorf("Second ProcessExternalSegment failed: %v", err)
	}

	// Verify the second result is nil due to duplicates
	verifySegmentResult(t, result2, docs2, true, "second call - with duplicate")

	// Verify that the index was built only once
	if dedup.existingDocIDs == nil {
		t.Error("existingDocIDs should have been built")
	}
}

func TestExternalSegmentDeduplicator_LargeIndex(t *testing.T) {
	cfg, cleanup := CreateConfig("TestExternalSegmentDeduplicator_LargeIndex")
	defer func() {
		err := cleanup()
		if err != nil {
			t.Log(err)
		}
	}()

	writer, err := OpenWriter(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = writer.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	// Add many existing documents using improved ID generation
	batch := NewBatch()
	for i := range 1000 {
		docID := testIdentifier("doc" + strconv.Itoa(i))
		batch.Update(docID, &FakeDocument{
			NewFakeField("_id", string(docID), true, false, false),
			NewFakeField("content", "content "+string(docID), false, false, true),
		})
	}
	err = writer.Batch(batch)
	if err != nil {
		t.Fatal(err)
	}

	dedup := NewExternalSegmentDeduplicator(writer, true)

	tests := []struct {
		name        string
		docID       string
		content     string
		expectNil   bool
		description string
	}{
		{
			name:        "new document",
			docID:       "new_large_test",
			content:     "new content",
			expectNil:   false,
			description: "should accept new document",
		},
		{
			name:        "duplicate document",
			docID:       "doc0",
			content:     "duplicate content",
			expectNil:   true,
			description: "should reject duplicate from large index",
		},
		{
			name:        "mid-range duplicate",
			docID:       "doc500",
			content:     "another duplicate",
			expectNil:   true,
			description: "should reject mid-range duplicate",
		},
		{
			name:        "last document duplicate",
			docID:       "doc999",
			content:     "last duplicate",
			expectNil:   true,
			description: "should reject last document duplicate",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			docs := []segment.Document{
				&FakeDocument{
					NewFakeField("_id", tt.docID, true, false, false),
					NewFakeField("content", tt.content, true, false, true),
				},
			}

			segWrapper, _, err := writer.newSegment(docs)
			if err != nil {
				t.Fatal(err)
			}

			result, err := dedup.ProcessExternalSegment(segWrapper)
			if err != nil {
				t.Errorf("ProcessExternalSegment failed: %v", err)
			}

			// Verify the result using the improved checking
			verifySegmentResult(t, result, docs, tt.expectNil, tt.description)
		})
	}
}

func TestProcessExternalSegment_EdgeCases(t *testing.T) {
	writer, cleanup, err := setupTestWriter("TestProcessExternalSegment_EdgeCases")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := cleanup(); err != nil {
			t.Log(err)
		}
	}()

	dedup := NewExternalSegmentDeduplicator(writer, true)

	tests := []struct {
		name        string
		docs        []segment.Document
		expectError bool
		expectNil   bool
		description string
	}{
		{
			name:        "empty segment",
			docs:        []segment.Document{},
			expectError: false,
			expectNil:   false,
			description: "should handle empty segments gracefully",
		},
		{
			name: "multiple documents without IDs",
			docs: []segment.Document{
				createTestDocument("", "content 1"),
				createTestDocument("", "content 2"),
			},
			expectError: false,
			expectNil:   false,
			description: "should handle multiple documents without IDs",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			segWrapper, err := createTestSegment(writer, tt.docs)
			if err != nil {
				t.Fatal(err)
			}

			result, err := dedup.ProcessExternalSegment(segWrapper)

			if tt.expectError && err == nil {
				t.Errorf("Expected error but got none (%s)", tt.description)
			}
			if !tt.expectError && err != nil {
				t.Errorf("Unexpected error: %v (%s)", err, tt.description)
			}

			// Verify the result using the improved checking
			verifySegmentResult(t, result, tt.docs, tt.expectNil, tt.description)
		})
	}
}

func TestProcessExternalSegment_CachingBehavior(t *testing.T) {
	cfg, cleanup := CreateConfig("TestProcessExternalSegment_CachingBehavior")
	defer func() {
		err := cleanup()
		if err != nil {
			t.Log(err)
		}
	}()

	writer, err := OpenWriter(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = writer.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	// Add existing document
	batch := NewBatch()
	batch.Update(testIdentifier("existing"), &FakeDocument{
		NewFakeField("_id", "existing", true, false, false),
		NewFakeField("content", "existing content", true, false, true),
	})
	err = writer.Batch(batch)
	if err != nil {
		t.Fatal(err)
	}

	dedup := NewExternalSegmentDeduplicator(writer, true)

	// Verify initial state
	if dedup.existingDocIDs != nil {
		t.Error("existingDocIDs should be nil initially")
	}

	// First call should build the cache
	docs := []segment.Document{
		&FakeDocument{
			NewFakeField("_id", "new", true, false, false),
			NewFakeField("content", "new content", true, false, true),
		},
	}
	segWrapper, _, err := writer.newSegment(docs)
	if err != nil {
		t.Fatal(err)
	}

	result1, err := dedup.ProcessExternalSegment(segWrapper)
	if err != nil {
		t.Errorf("First ProcessExternalSegment failed: %v", err)
	}

	// Verify the first result contains the expected documents
	verifySegmentResult(t, result1, docs, false, "first call - cache building")

	// Verify cache was built
	if dedup.existingDocIDs == nil {
		t.Error("existingDocIDs should have been built after first call")
	}

	// Verify cache contains expected document
	if _, exists := dedup.existingDocIDs["existing"]; !exists {
		t.Error("Cache should contain 'existing' document")
	}

	// Second call should use the cache (not rebuild it)
	docs2 := []segment.Document{
		&FakeDocument{
			NewFakeField("_id", "existing", true, false, false),
			NewFakeField("content", "duplicate content", true, false, true),
		},
	}
	segWrapper2, _, err := writer.newSegment(docs2)
	if err != nil {
		t.Fatal(err)
	}

	result2, err := dedup.ProcessExternalSegment(segWrapper2)
	if err != nil {
		t.Errorf("Second ProcessExternalSegment failed: %v", err)
	}

	// Verify the second result is nil due to duplicate in cache
	verifySegmentResult(t, result2, docs2, true, "second call - cache usage")
}

// Benchmark tests for performance evaluation
func BenchmarkProcessExternalSegment_SmallIndex(b *testing.B) {
	writer, cleanup, err := setupTestWriter("BenchmarkProcessExternalSegment_SmallIndex")
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		if err := cleanup(); err != nil {
			b.Log(err)
		}
	}()

	// Add 100 existing documents
	existingDocs := make(map[string]string)
	for i := range 100 {
		existingDocs["existing"+strconv.Itoa(i)] = "content " + strconv.Itoa(i)
	}
	if err := addDocumentsToIndex(writer, existingDocs); err != nil {
		b.Fatal(err)
	}

	dedup := NewExternalSegmentDeduplicator(writer, true)

	// Create test segment
	docs := []segment.Document{
		createTestDocument("new_doc", "new content"),
	}
	segWrapper, err := createTestSegment(writer, docs)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for range b.N {
		_, err := dedup.ProcessExternalSegment(segWrapper)
		if err != nil {
			b.Errorf("ProcessExternalSegment failed: %v", err)
		}
	}
}

func BenchmarkProcessExternalSegment_LargeIndex(b *testing.B) {
	writer, cleanup, err := setupTestWriter("BenchmarkProcessExternalSegment_LargeIndex")
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		if err := cleanup(); err != nil {
			b.Log(err)
		}
	}()

	// Add 10,000 existing documents
	batch := NewBatch()
	for i := range 10000 {
		docID := testIdentifier("doc" + strconv.Itoa(i))
		batch.Update(docID, &FakeDocument{
			NewFakeField("_id", string(docID), true, false, false),
			NewFakeField("content", "content "+string(docID), true, false, true),
		})
	}
	if err := writer.Batch(batch); err != nil {
		b.Fatal(err)
	}

	dedup := NewExternalSegmentDeduplicator(writer, true)

	// Create test segment
	docs := []segment.Document{
		createTestDocument("new_doc", "new content"),
	}
	segWrapper, err := createTestSegment(writer, docs)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for range b.N {
		_, err := dedup.ProcessExternalSegment(segWrapper)
		if err != nil {
			b.Errorf("ProcessExternalSegment failed: %v", err)
		}
	}
}
