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

package index

import (
	"fmt"

	segment "github.com/blugelabs/bluge_segment_api"
)

const (
	// DocumentIDField is the field name used for document IDs
	DocumentIDField = "_id"
)

// ExternalSegmentDeduplicator handles deduplication of a single external segment
// using a "keep existing" strategy with exact map approach
type ExternalSegmentDeduplicator struct {
	writer         *Writer
	enabled        bool
	existingDocIDs map[string]struct{} // Direct map for memory efficiency
}

// NewExternalSegmentDeduplicator creates a new deduplicator
func NewExternalSegmentDeduplicator(writer *Writer, enabled bool) *ExternalSegmentDeduplicator {
	return &ExternalSegmentDeduplicator{
		writer:  writer,
		enabled: enabled,
	}
}

// ProcessExternalSegment checks for duplicate documents in the segment
// Returns the original segment if there are any non-duplicate documents, or nil if all documents are duplicates
// TODO: Implement proper filtering to remove only duplicate documents while keeping non-duplicates
func (esd *ExternalSegmentDeduplicator) ProcessExternalSegment(segWrapper *segmentWrapper) (*segmentWrapper, error) {
	if !esd.enabled {
		return segWrapper, nil
	}

	// Build index of existing documents on first use
	if esd.existingDocIDs == nil {
		err := esd.buildExistingDocIndex()
		if err != nil {
			return nil, fmt.Errorf("failed to build doc index: %w", err)
		}
	}

	// Create filtered segment containing only non-duplicate documents
	filteredSegment, err := esd.createFilteredSegment(segWrapper)
	if err != nil {
		return nil, fmt.Errorf("failed to create filtered segment: %w", err)
	}

	return filteredSegment, nil
}

// buildExistingDocIndex builds an index of all existing document IDs
func (esd *ExternalSegmentDeduplicator) buildExistingDocIndex() error {
	// Get current snapshot
	snapshot := esd.writer.currentSnapshot()
	defer snapshot.Close()

	// Estimate document count for optimal bloom filter sizing
	totalDocs := uint64(0)
	for _, segSnapshot := range snapshot.segment {
		totalDocs += segSnapshot.Count()
	}

	// Initialize exact map data structure
	esd.existingDocIDs = make(map[string]struct{}, totalDocs)

	// Scan all segments for document IDs
	for _, segSnapshot := range snapshot.segment {
		err := esd.extractDocIDsFromSegment(segSnapshot)
		if err != nil {
			return fmt.Errorf("failed to extract doc IDs from segment %d: %w", segSnapshot.id, err)
		}
	}

	return nil
}

// extractDocIDsFromSegment extracts all document IDs from a segment
func (esd *ExternalSegmentDeduplicator) extractDocIDsFromSegment(segSnapshot *segmentSnapshot) error {
	// Iterate through all documents in the segment
	docCount := segSnapshot.segment.Count()
	for docNum := uint64(0); docNum < docCount; docNum++ {
		// Skip deleted documents
		if segSnapshot.deleted != nil && segSnapshot.deleted.Contains(uint32(docNum)) {
			continue
		}

		var docID string
		var foundID bool

		// Extract document ID using efficient field visitor pattern
		err := segSnapshot.segment.VisitStoredFields(docNum, func(field string, value []byte) bool {
			if field == DocumentIDField {
				docID = string(value)
				foundID = true
				return false // Early termination once ID is found
			}
			return true
		})

		if err != nil {
			return fmt.Errorf("failed to visit document %d: %w", docNum, err)
		}

		if foundID {
			esd.existingDocIDs[docID] = struct{}{}
		}
	}

	return nil
}

// FilteredDocument represents a document from a segment for deduplication
type FilteredDocument struct {
	fields    map[string][]byte
	timestamp int64
}

// Identity returns the document's identity field and term
func (fd *FilteredDocument) Identity() (field string, term []byte) {
	if idValue, exists := fd.fields[DocumentIDField]; exists {
		return DocumentIDField, idValue
	}
	return "", nil
}

// Analyze is called to analyze the document fields
func (fd *FilteredDocument) Analyze() {
	// No analysis needed for filtered documents
}

// Len returns the number of fields in the document
func (fd *FilteredDocument) Len() int {
	return len(fd.fields)
}

// Timestamp returns the document timestamp
func (fd *FilteredDocument) Timestamp() int64 {
	return fd.timestamp
}

// EachField calls the visitor function for each field
func (fd *FilteredDocument) EachField(vf segment.VisitField) {
	for fieldName, fieldValue := range fd.fields {
		field := &FilteredField{
			name:  fieldName,
			value: fieldValue,
		}
		vf(field)
	}
}

// FilteredField represents a field in a filtered document
type FilteredField struct {
	name  string
	value []byte
}

// Name returns the field name
func (ff *FilteredField) Name() string {
	return ff.name
}

// Value returns the field value
func (ff *FilteredField) Value() []byte {
	return ff.value
}

// Store returns whether the field should be stored
func (ff *FilteredField) Store() bool {
	return true // Default to stored
}

// Index returns whether the field should be indexed
func (ff *FilteredField) Index() bool {
	return true // Default to indexable
}

// IndexDocValues returns whether the field should have doc values
func (ff *FilteredField) IndexDocValues() bool {
	return false // Default to no doc values for simplicity
}

// Length returns the number of terms in the field
func (ff *FilteredField) Length() int {
	return 1 // Simple fields have one term
}

// EachTerm calls the visitor function for each term in the field
func (ff *FilteredField) EachTerm(vt segment.VisitTerm) {
	// Create a simple term from the field value
	term := &FilteredTerm{
		term: ff.value,
	}
	vt(term)
}

// FilteredTerm represents a term in a filtered field
type FilteredTerm struct {
	term []byte
}

// Term returns the term bytes
func (ft *FilteredTerm) Term() []byte {
	return ft.term
}

// Frequency returns the term frequency (always 1 for stored fields)
func (ft *FilteredTerm) Frequency() int {
	return 1
}

// EachLocation calls the visitor function for each location
func (ft *FilteredTerm) EachLocation(vl segment.VisitLocation) {
	// No location information for stored fields
}

// createFilteredSegment creates a new segment containing only non-duplicate documents
func (esd *ExternalSegmentDeduplicator) createFilteredSegment(segWrapper *segmentWrapper) (*segmentWrapper, error) {
	var filteredDocs []segment.Document
	docCount := segWrapper.Count()

	// Handle empty segments
	if docCount == 0 {
		return segWrapper, nil
	}

	// Process each document in the segment
	for docNum := uint64(0); docNum < docCount; docNum++ {
		var docID string
		var foundID bool
		docFields := make(map[string][]byte)

		// Extract all stored fields from the document
		err := segWrapper.VisitStoredFields(docNum, func(field string, value []byte) bool {
			// Make a copy of the value to avoid issues with reused buffers
			valueCopy := make([]byte, len(value))
			copy(valueCopy, value)
			docFields[field] = valueCopy

			if field == DocumentIDField {
				docID = string(value)
				foundID = true
			}
			return true
		})

		if err != nil {
			return nil, fmt.Errorf("failed to extract document %d: %w", docNum, err)
		}

		// Check if this document is a duplicate
		isDuplicate := false
		if foundID && esd.existingDocIDs != nil {
			if _, exists := esd.existingDocIDs[docID]; exists {
				isDuplicate = true
			}
		}

		// Only include non-duplicate documents
		if !isDuplicate {
			filteredDoc := &FilteredDocument{
				fields:    docFields,
				timestamp: 0, // Default timestamp
			}
			filteredDocs = append(filteredDocs, filteredDoc)
		}
	}

	// If all documents were duplicates, return nil
	if len(filteredDocs) == 0 {
		return nil, nil
	}

	// If no duplicates were found, return original segment
	if len(filteredDocs) == int(docCount) {
		return segWrapper, nil
	}

	// Create new segment from filtered documents
	newSegWrapper, _, err := esd.writer.newSegment(filteredDocs)
	if err != nil {
		return nil, fmt.Errorf("failed to create new segment: %w", err)
	}

	return newSegWrapper, nil
}
