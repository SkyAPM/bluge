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
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/RoaringBitmap/roaring"
)

// StreamStatus represents the current status of an external segment stream
type StreamStatus int

const (
	StreamInactive StreamStatus = iota
	StreamActive
	StreamComplete
	StreamFailed
	StreamIntroduced
)

func (s StreamStatus) String() string {
	switch s {
	case StreamInactive:
		return "inactive"
	case StreamActive:
		return "active"
	case StreamComplete:
		return "complete"
	case StreamFailed:
		return "failed"
	case StreamIntroduced:
		return "introduced"
	default:
		return "unknown"
	}
}

// ExternalSegmentReceiver handles streaming of external segments into a Bluge index
type ExternalSegmentReceiver struct {
	writer        *Writer
	config        Config
	segmentID     uint64
	tempFilePath  string
	file          *os.File
	bytesReceived uint64
	mutex         sync.Mutex
	status        StreamStatus
	createdAt     time.Time
	deduplicator  *ExternalSegmentDeduplicator
}

// NewExternalSegmentReceiver creates a new external segment receiver
func NewExternalSegmentReceiver(writer *Writer, config Config) (*ExternalSegmentReceiver, error) {
	if !config.EnableExternalSegments {
		return nil, fmt.Errorf("external segments not enabled in config")
	}

	// Set default temp directory if not specified
	tempDir := config.ExternalSegmentTempDir
	if tempDir == "" {
		tempDir = filepath.Join(os.TempDir(), "bluge_external_segments")
	}

	// Ensure temp directory exists
	err := os.MkdirAll(tempDir, 0755)
	if err != nil {
		return nil, fmt.Errorf("failed to create temp directory: %w", err)
	}

	receiver := &ExternalSegmentReceiver{
		writer: writer,
		config: config,
		status: StreamInactive,
	}

	// Initialize deduplicator if enabled
	if config.EnableDeduplication {
		receiver.deduplicator = NewExternalSegmentDeduplicator(writer, true)
	}

	return receiver, nil
}

// StartSegment initializes streaming for a new segment
func (esr *ExternalSegmentReceiver) StartSegment() error {
	esr.mutex.Lock()
	defer esr.mutex.Unlock()

	// Check if receiver is already active
	if esr.status == StreamActive {
		return fmt.Errorf("receiver already has an active segment")
	}

	// Generate new segment ID for this index
	esr.segmentID = atomic.AddUint64(&esr.writer.nextSegmentID, 1)

	// Create temporary file
	tempFileName := fmt.Sprintf("segment_%d.tmp", esr.segmentID)
	tempDir := esr.config.ExternalSegmentTempDir
	if tempDir == "" {
		tempDir = filepath.Join(os.TempDir(), "bluge_external_segments")
	}
	esr.tempFilePath = filepath.Join(tempDir, tempFileName)

	file, err := os.Create(esr.tempFilePath)
	if err != nil {
		esr.status = StreamFailed
		return fmt.Errorf("failed to create temp file: %w", err)
	}

	// Initialize receiver state
	esr.file = file
	esr.status = StreamActive
	esr.createdAt = time.Now()
	esr.bytesReceived = 0

	return nil
}

// WriteChunk writes a chunk of segment data to the temporary file
func (esr *ExternalSegmentReceiver) WriteChunk(data []byte) error {
	esr.mutex.Lock()
	defer esr.mutex.Unlock()

	if esr.status != StreamActive {
		return fmt.Errorf("receiver is not active (status: %s)", esr.status.String())
	}

	if len(data) == 0 {
		return nil // Skip empty chunks
	}

	// Write chunk data to file
	n, err := esr.file.Write(data)
	if err != nil {
		esr.status = StreamFailed
		return fmt.Errorf("failed to write chunk: %w", err)
	}

	if n != len(data) {
		esr.status = StreamFailed
		return fmt.Errorf("incomplete write: expected %d bytes, wrote %d", len(data), n)
	}

	esr.bytesReceived += uint64(n)

	return nil
}

// CompleteSegment signals that streaming is complete and triggers introduction
func (esr *ExternalSegmentReceiver) CompleteSegment() error {
	esr.mutex.Lock()
	defer esr.mutex.Unlock()

	if esr.status != StreamActive {
		return fmt.Errorf("receiver is not active (status: %s)", esr.status.String())
	}

	return esr.finalizeSegment()
}

// finalizeSegment handles the completion of segment streaming
func (esr *ExternalSegmentReceiver) finalizeSegment() error {
	// Set up cleanup on any failure
	defer func() {
		if esr.status == StreamFailed {
			if esr.file != nil {
				esr.file.Close()
			}
			if esr.tempFilePath != "" {
				os.Remove(esr.tempFilePath)
			}
		}
	}()

	// Close and sync file
	err := esr.file.Sync()
	if err != nil {
		esr.status = StreamFailed
		return fmt.Errorf("failed to sync file: %w", err)
	}

	err = esr.file.Close()
	if err != nil {
		esr.status = StreamFailed
		return fmt.Errorf("failed to close file: %w", err)
	}

	esr.status = StreamComplete

	err = esr.introduceSegment()
	if err != nil {
		esr.status = StreamFailed
		return fmt.Errorf("segment introduction failed: %w", err)
	}

	return nil
}

// introduceSegment handles integration with Bluge's segment introduction pipeline
func (esr *ExternalSegmentReceiver) introduceSegment() error {
	// First, persist the segment file using the directory's persist method
	err := esr.persistSegmentFile()
	if err != nil {
		esr.status = StreamFailed
		return fmt.Errorf("failed to persist segment: %w", err)
	}

	// Load the segment
	originalSegWrapper, err := esr.loadSegmentWithID()
	if err != nil {
		esr.status = StreamFailed
		return fmt.Errorf("failed to load segment: %w", err)
	}

	// Apply deduplication if enabled
	var segWrapper *segmentWrapper
	if esr.config.EnableDeduplication && esr.deduplicator != nil {
		segWrapper, err = esr.deduplicator.ProcessExternalSegment(originalSegWrapper)
		if err != nil {
			_ = originalSegWrapper.Close()
			esr.status = StreamFailed
			return fmt.Errorf("deduplication failed: %w", err)
		}
		if segWrapper != originalSegWrapper {
			// Close the original segment wrapper to release file handles
			// before attempting to remove the file
			_ = originalSegWrapper.Close()

			// Try to remove the empty segment file, but don't fail if it's temporarily unavailable
			err = esr.writer.directory.Remove(ItemKindSegment, esr.segmentID)
			if err != nil {
				// Log the error but don't fail the operation
				// The file will be cleaned up later by the deletion policy
				if esr.writer.config.AsyncError != nil {
					esr.writer.config.AsyncError(fmt.Errorf("failed to remove empty segment (will retry later): %w", err))
				}
			}
		}
		if segWrapper == nil {
			esr.status = StreamIntroduced
			return nil
		}
	} else {
		// No deduplication, use the original segment wrapper
		segWrapper = originalSegWrapper
	}

	// Use the existing segment introduction mechanism
	err = esr.introduceSegmentToIndex(segWrapper)
	if err != nil {
		esr.status = StreamFailed
		return fmt.Errorf("segment introduction failed: %w", err)
	}

	esr.status = StreamIntroduced
	return nil
}

// persistSegmentFile persists the temporary file using the directory's Persist method
func (esr *ExternalSegmentReceiver) persistSegmentFile() error {
	// Open the temporary file for reading
	file, err := os.Open(esr.tempFilePath)
	if err != nil {
		return fmt.Errorf("failed to open temp file: %w", err)
	}
	defer file.Close()

	// Create a WriterTo that copies from our temp file
	writerTo := &fileWriterTo{file: file}

	// Use the directory's Persist method to store the segment
	err = esr.writer.directory.Persist(ItemKindSegment, esr.segmentID, writerTo, nil)
	if err != nil {
		return fmt.Errorf("failed to persist segment: %w", err)
	}

	// Clean up temp file
	err = os.Remove(esr.tempFilePath)
	if err != nil {
		if esr.writer.config.AsyncError != nil {
			esr.writer.config.AsyncError(fmt.Errorf("failed to remove temp file: %w", err))
		}
	}

	return nil
}

// loadSegmentWithID loads the segment from its persisted location
func (esr *ExternalSegmentReceiver) loadSegmentWithID() (*segmentWrapper, error) {
	// Load the segment data
	data, closer, err := esr.writer.directory.Load(ItemKindSegment, esr.segmentID)
	if err != nil {
		return nil, fmt.Errorf("failed to load segment data: %w", err)
	}

	// Load segment using the configured segment plugin
	seg, err := esr.writer.segPlugin.Load(data)
	if err != nil {
		if closer != nil {
			_ = closer.Close()
		}
		return nil, fmt.Errorf("error loading segment: %v", err)
	}

	return &segmentWrapper{
		Segment: seg,
		refCounter: &closeOnLastRefCounter{
			closer: closer,
			refs:   1,
		},
		persisted: true,
	}, nil
}

// introduceSegmentToIndex integrates the segment into the index using existing introduction pipeline
func (esr *ExternalSegmentReceiver) introduceSegmentToIndex(segWrapper *segmentWrapper) error {
	// Create a segment introduction using the existing mechanism
	introduction := &segmentIntroduction{
		id:                esr.segmentID,
		data:              segWrapper,
		idTerms:           nil, // No deletions for external segments
		obsoletes:         make(map[uint64]*roaring.Bitmap),
		internal:          nil,
		applied:           make(chan error),
		persistedCallback: func(error) {}, // No callback needed
	}

	// Submit to writer's introduction channel
	esr.writer.introductions <- introduction
	// Wait for introduction to complete
	err := <-introduction.applied
	if err != nil {
		return fmt.Errorf("segment introduction failed: %w", err)
	}
	return nil
}

// RecoverFromFailure allows recovery from a failed state
func (esr *ExternalSegmentReceiver) RecoverFromFailure() error {
	esr.mutex.Lock()
	defer esr.mutex.Unlock()

	if esr.status == StreamFailed {
		// Use defer-style cleanup function
		defer func() {
			if esr.file != nil {
				esr.file.Close()
				esr.file = nil
			}
			if esr.tempFilePath != "" {
				os.Remove(esr.tempFilePath)
				esr.tempFilePath = ""
			}
			esr.bytesReceived = 0
		}()

		// Reset receiver state to allow new segment
		esr.status = StreamInactive
	}

	return nil
}

// Status returns the current status of the receiver
func (esr *ExternalSegmentReceiver) Status() StreamStatus {
	esr.mutex.Lock()
	defer esr.mutex.Unlock()
	return esr.status
}

// BytesReceived returns the number of bytes received so far
func (esr *ExternalSegmentReceiver) BytesReceived() uint64 {
	esr.mutex.Lock()
	defer esr.mutex.Unlock()
	return esr.bytesReceived
}

// CreatedAt returns when the current segment streaming started
func (esr *ExternalSegmentReceiver) CreatedAt() time.Time {
	esr.mutex.Lock()
	defer esr.mutex.Unlock()
	return esr.createdAt
}

// fileWriterTo implements WriterTo for copying from a file
type fileWriterTo struct {
	file *os.File
}

func (fwt *fileWriterTo) WriteTo(w io.Writer, closeCh chan struct{}) (n int64, err error) {
	// Use io.CopyBuffer with cancellation support
	buffer := make([]byte, 32*1024) // 32KB buffer

	for {
		// Check for cancellation if closeCh is provided
		if closeCh != nil {
			select {
			case <-closeCh:
				return n, fmt.Errorf("write operation canceled")
			default:
				// Continue with copy operation
			}
		}

		nr, er := fwt.file.Read(buffer)
		if nr > 0 {
			nw, ew := w.Write(buffer[:nr])
			if nw < 0 || nr < nw {
				nw = 0
				if ew == nil {
					ew = fmt.Errorf("invalid write result")
				}
			}
			n += int64(nw)
			if ew != nil {
				err = ew
				break
			}
			if nr != nw {
				err = io.ErrShortWrite
				break
			}
		}
		if er != nil {
			if er != io.EOF {
				err = er
			}
			break
		}
	}
	return n, err
}
