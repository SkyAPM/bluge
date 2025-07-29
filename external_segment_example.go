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
	"fmt"
	"io"
	"log"
)

// ExampleExternalSegmentStreaming demonstrates how to use the external segment streaming feature
func ExampleExternalSegmentStreaming() {
	// Configure writer with external segment support
	config := DefaultConfig("./index").WithExternalSegments("./temp_segments", true)

	writer, err := OpenWriter(config)
	if err != nil {
		log.Printf("failed to open writer: %v", err)
		return
	}
	defer writer.Close()

	// Enable external segment receiver
	receiver, err := writer.EnableExternalSegments()
	if err != nil {
		log.Printf("failed to enable external segments: %v", err)
		return
	}

	// Start receiving a segment
	err = receiver.StartSegment()
	if err != nil {
		log.Printf("failed to start segment: %v", err)
		return
	}

	// Simulate streaming chunks from an external source
	// In a real scenario, this would come from a network stream, file, etc.
	var externalSegmentData []byte // This would be your actual segment data
	chunkSize := 64 * 1024         // 64KB chunks

	for len(externalSegmentData) > 0 {
		chunkEnd := chunkSize
		if chunkEnd > len(externalSegmentData) {
			chunkEnd = len(externalSegmentData)
		}

		chunk := externalSegmentData[:chunkEnd]
		externalSegmentData = externalSegmentData[chunkEnd:]

		err = receiver.WriteChunk(chunk)
		if err != nil {
			log.Printf("failed to write chunk: %v", err)
			return
		}
	}

	// Signal completion of segment streaming
	err = receiver.CompleteSegment()
	if err != nil {
		log.Printf("failed to complete segment: %v", err)
		return
	}

	fmt.Printf("External segment streamed successfully\n")
	fmt.Printf("Status: %v\n", receiver.Status())
	fmt.Printf("Bytes received: %d\n", receiver.BytesReceived())
}

// ExampleExternalSegmentStreamingFromReader demonstrates streaming from an io.Reader
func ExampleExternalSegmentStreamingFromReader(segmentReader io.Reader) error {
	// Configure writer with external segment support
	config := DefaultConfig("./index").WithExternalSegments("./temp_segments", true)

	writer, err := OpenWriter(config)
	if err != nil {
		return err
	}
	defer writer.Close()

	// Enable external segment receiver
	receiver, err := writer.EnableExternalSegments()
	if err != nil {
		return err
	}

	// Start receiving a segment
	err = receiver.StartSegment()
	if err != nil {
		return err
	}

	// Stream data from reader
	chunkSize := 64 * 1024 // 64KB chunks
	buffer := make([]byte, chunkSize)

	for {
		n, err := segmentReader.Read(buffer)
		if err != nil && err != io.EOF {
			if recoverErr := receiver.RecoverFromFailure(); recoverErr != nil {
				return fmt.Errorf("failed to recover from failure: %w, original error: %w", recoverErr, err)
			}
			return err
		}

		if n == 0 {
			break // End of data
		}

		// Write chunk
		err = receiver.WriteChunk(buffer[:n])
		if err != nil {
			return err
		}

		if err == io.EOF {
			break
		}
	}

	// Signal completion
	err = receiver.CompleteSegment()
	if err != nil {
		return err
	}

	fmt.Printf("External segment streamed successfully from reader\n")
	return nil
}
