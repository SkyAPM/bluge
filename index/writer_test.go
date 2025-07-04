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
	"math"
	"os"
	"reflect"
	"sort"
	"strconv"
	"sync"
	"testing"

	segment "github.com/blugelabs/bluge_segment_api"
)

func CreateConfig(name string) (config Config, cleanup func() error) {
	path, err := os.MkdirTemp("", "bluge-index-test"+name)
	if err != nil {
		panic(err)
	}
	rv := DefaultConfig(path).
		WithPersisterNapTimeMSec(1).
		WithNormCalc(func(_ string, numTerms int) float32 {
			return math.Float32frombits(uint32(numTerms))
		}).
		WithVirtualField(NewFakeField("", "", false, false, false))
	return rv, func() error { return os.RemoveAll(path) }
}

type testIdentifier string

func (i testIdentifier) Field() string {
	return "_id"
}

func (i testIdentifier) Term() []byte {
	return []byte(i)
}

func TestIndexOpenReopen(t *testing.T) {
	cfg, cleanup := CreateConfig("TestIndexOpenReopen")
	defer func() {
		err := cleanup()
		if err != nil {
			t.Log(err)
		}
	}()

	idx, err := OpenWriter(cfg)
	if err != nil {
		t.Fatal(err)
	}

	var expectedCount uint64
	reader, err := idx.Reader()
	if err != nil {
		t.Fatal(err)
	}
	docCount, err := reader.Count()
	if err != nil {
		t.Error(err)
	}
	if docCount != expectedCount {
		t.Errorf("Expected document count to be %d got %d", expectedCount, docCount)
	}
	err = reader.Close()
	if err != nil {
		t.Fatal(err)
	}

	// insert a doc
	doc := &FakeDocument{
		NewFakeField("_id", "1", true, false, false),
		NewFakeField("name", "test", false, false, true),
	}
	b := NewBatch()
	b.Update(testIdentifier("1"), doc)
	err = idx.Batch(b)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}
	expectedCount++

	reader, err = idx.Reader()
	if err != nil {
		t.Fatal(err)
	}
	docCount, err = reader.Count()
	if err != nil {
		t.Error(err)
	}
	if docCount != expectedCount {
		t.Errorf("Expected document count to be %d got %d", expectedCount, docCount)
	}
	err = reader.Close()
	if err != nil {
		t.Fatal(err)
	}

	// now close it
	err = idx.Close()
	if err != nil {
		t.Fatal(err)
	}

	idx, err = OpenWriter(cfg)
	if err != nil {
		t.Fatal(err)
	}

	// check the doc count again after reopening it
	reader, err = idx.Reader()
	if err != nil {
		t.Fatal(err)
	}
	docCount, err = reader.Count()
	if err != nil {
		t.Error(err)
	}
	if docCount != expectedCount {
		t.Errorf("Expected document count to be %d got %d", expectedCount, docCount)
	}
	err = reader.Close()
	if err != nil {
		t.Fatal(err)
	}

	// now close it
	err = idx.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestIndexOpenReopenWithInsert(t *testing.T) {
	cfg, cleanup := CreateConfig("TestIndexOpenReopen")
	defer func() {
		err := cleanup()
		if err != nil {
			t.Log(err)
		}
	}()

	idx, err := OpenWriter(cfg)
	if err != nil {
		t.Fatal(err)
	}

	var expectedCount uint64
	reader, err := idx.Reader()
	if err != nil {
		t.Fatal(err)
	}
	docCount, err := reader.Count()
	if err != nil {
		t.Error(err)
	}
	if docCount != expectedCount {
		t.Errorf("Expected document count to be %d got %d", expectedCount, docCount)
	}
	err = reader.Close()
	if err != nil {
		t.Fatal(err)
	}

	// insert a doc
	doc := &FakeDocument{
		NewFakeField("_id", "1", true, false, false),
		NewFakeField("name", "test", false, false, true),
	}
	b := NewBatch()
	b.Update(testIdentifier("1"), doc)
	err = idx.Batch(b)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}
	expectedCount++

	reader, err = idx.Reader()
	if err != nil {
		t.Fatal(err)
	}
	docCount, err = reader.Count()
	if err != nil {
		t.Error(err)
	}
	if docCount != expectedCount {
		t.Errorf("Expected document count to be %d got %d", expectedCount, docCount)
	}
	err = reader.Close()
	if err != nil {
		t.Fatal(err)
	}

	// now close it
	err = idx.Close()
	if err != nil {
		t.Fatal(err)
	}

	// try to open the index and insert data
	idx, err = OpenWriter(cfg)
	if err != nil {
		t.Fatalf("error opening index: %v", err)
	}

	// insert a doc
	doc = &FakeDocument{
		NewFakeField("_id", "2", true, false, false),
		NewFakeField("name", "test2", false, false, true),
	}
	b2 := NewBatch()
	b2.Update(testIdentifier("2"), doc)
	err = idx.Batch(b2)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}
	expectedCount++

	// check the doc count again after reopening it
	reader, err = idx.Reader()
	if err != nil {
		t.Fatal(err)
	}
	docCount, err = reader.Count()
	if err != nil {
		t.Error(err)
	}
	if docCount != expectedCount {
		t.Errorf("Expected document count to be %d got %d", expectedCount, docCount)
	}
	err = reader.Close()
	if err != nil {
		t.Fatal(err)
	}

	// now close it
	err = idx.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestIndexInsert(t *testing.T) {
	cfg, cleanup := CreateConfig("TestIndexInsert")
	defer func() {
		err := cleanup()
		if err != nil {
			t.Log(err)
		}
	}()

	idx, err := OpenWriter(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = idx.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	var expectedCount uint64
	reader, err := idx.Reader()
	if err != nil {
		t.Fatal(err)
	}
	docCount, err := reader.Count()
	if err != nil {
		t.Error(err)
	}
	if docCount != expectedCount {
		t.Errorf("Expected document count to be %d got %d", expectedCount, docCount)
	}
	err = reader.Close()
	if err != nil {
		t.Fatal(err)
	}

	doc := &FakeDocument{
		NewFakeField("_id", "1", true, false, false),
		NewFakeField("name", "test", false, false, true),
	}
	b := NewBatch()
	b.Update(testIdentifier("1"), doc)
	err = idx.Batch(b)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}
	expectedCount++

	reader, err = idx.Reader()
	if err != nil {
		t.Fatal(err)
	}
	docCount, err = reader.Count()
	if err != nil {
		t.Error(err)
	}
	if docCount != expectedCount {
		t.Errorf("Expected document count to be %d got %d", expectedCount, docCount)
	}
	err = reader.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestIndexInsertThenDelete(t *testing.T) {
	cfg, cleanup := CreateConfig("TestIndexInsertThenDelete")
	defer func() {
		err := cleanup()
		if err != nil {
			t.Log(err)
		}
	}()

	idx, err := OpenWriter(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = idx.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	var expectedCount uint64
	reader, err := idx.Reader()
	if err != nil {
		t.Fatal(err)
	}
	docCount, err := reader.Count()
	if err != nil {
		t.Error(err)
	}
	if docCount != expectedCount {
		t.Errorf("Expected document count to be %d got %d", expectedCount, docCount)
	}
	err = reader.Close()
	if err != nil {
		t.Fatal(err)
	}

	doc := &FakeDocument{
		NewFakeField("_id", "1", true, false, false),
		NewFakeField("name", "test", false, false, true),
	}
	b := NewBatch()
	b.Update(testIdentifier("1"), doc)
	err = idx.Batch(b)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}
	expectedCount++

	doc2 := &FakeDocument{
		NewFakeField("_id", "2", true, false, false),
		NewFakeField("name", "test", false, false, true),
	}
	b2 := NewBatch()
	b2.Update(testIdentifier("2"), doc2)
	err = idx.Batch(b2)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}
	expectedCount++

	reader, err = idx.Reader()
	if err != nil {
		t.Fatal(err)
	}
	docCount, err = reader.Count()
	if err != nil {
		t.Error(err)
	}
	if docCount != expectedCount {
		t.Errorf("Expected document count to be %d got %d", expectedCount, docCount)
	}
	err = reader.Close()
	if err != nil {
		t.Fatal(err)
	}

	b3 := NewBatch()
	b3.Delete(testIdentifier("1"))
	err = idx.Batch(b3)
	if err != nil {
		t.Errorf("Error deleting entry from index: %v", err)
	}
	expectedCount--

	reader, err = idx.Reader()
	if err != nil {
		t.Fatal(err)
	}
	docCount, err = reader.Count()
	if err != nil {
		t.Error(err)
	}
	if docCount != expectedCount {
		t.Errorf("Expected document count to be %d got %d", expectedCount, docCount)
	}
	err = reader.Close()
	if err != nil {
		t.Fatal(err)
	}

	// now close it
	err = idx.Close()
	if err != nil {
		t.Fatal(err)
	}

	idx, err = OpenWriter(cfg) // reopen
	if err != nil {
		t.Fatal(err)
	}

	reader, err = idx.Reader()
	if err != nil {
		t.Fatal(err)
	}
	docCount, err = reader.Count()
	if err != nil {
		t.Error(err)
	}
	if docCount != expectedCount {
		t.Errorf("Expected document count to be %d got %d", expectedCount, docCount)
	}
	err = reader.Close()
	if err != nil {
		t.Fatal(err)
	}

	b4 := NewBatch()
	b4.Delete(testIdentifier("2"))
	err = idx.Batch(b4)
	if err != nil {
		t.Errorf("Error deleting entry from index: %v", err)
	}
	expectedCount--

	reader, err = idx.Reader()
	if err != nil {
		t.Fatal(err)
	}
	docCount, err = reader.Count()
	if err != nil {
		t.Error(err)
	}
	if docCount != expectedCount {
		t.Errorf("Expected document count to be %d got %d", expectedCount, docCount)
	}
	err = reader.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestIndexInsertThenUpdate(t *testing.T) {
	cfg, cleanup := CreateConfig("TestIndexInsertThenUpdate")
	defer func() {
		err := cleanup()
		if err != nil {
			t.Log(err)
		}
	}()

	idx, err := OpenWriter(cfg)
	if err != nil {
		t.Fatal(err)
	}
	var expectedCount uint64
	defer func() {
		err = idx.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	doc := &FakeDocument{
		NewFakeField("_id", "1", true, false, false),
		NewFakeField("name", "test", false, false, true),
	}
	b := NewBatch()
	b.Update(testIdentifier("1"), doc)
	err = idx.Batch(b)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}
	expectedCount++

	// this update should overwrite one term, and introduce one new one
	doc = &FakeDocument{
		NewFakeField("_id", "1", true, false, false),
		NewFakeField("name", "test fail", false, false, true),
	}
	b2 := NewBatch()
	b2.Update(testIdentifier("1"), doc)
	err = idx.Batch(b2)
	if err != nil {
		t.Errorf("Error deleting entry from index: %v", err)
	}

	// now do another update that should remove one of the terms
	doc = &FakeDocument{
		NewFakeField("_id", "1", true, false, false),
		NewFakeField("name", "fail", false, false, true),
	}
	b3 := NewBatch()
	b3.Update(testIdentifier("1"), doc)
	err = idx.Batch(b3)
	if err != nil {
		t.Errorf("Error deleting entry from index: %v", err)
	}

	reader, err := idx.Reader()
	if err != nil {
		t.Fatal(err)
	}
	docCount, err := reader.Count()
	if err != nil {
		t.Error(err)
	}
	if docCount != expectedCount {
		t.Errorf("Expected document count to be %d got %d", expectedCount, docCount)
	}
	err = reader.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestIndexInsertMultiple(t *testing.T) {
	cfg, cleanup := CreateConfig("TestIndexInsertMultiple")
	defer func() {
		err := cleanup()
		if err != nil {
			t.Log(err)
		}
	}()

	idx, err := OpenWriter(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = idx.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	var expectedCount uint64

	doc := &FakeDocument{
		NewFakeField("_id", "1", true, false, false),
		NewFakeField("name", "test", false, false, true),
	}
	b := NewBatch()
	b.Update(testIdentifier("1"), doc)
	err = idx.Batch(b)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}
	expectedCount++

	doc = &FakeDocument{
		NewFakeField("_id", "2", true, false, false),
		NewFakeField("name", "test", false, false, true),
	}
	b2 := NewBatch()
	b2.Update(testIdentifier("2"), doc)
	err = idx.Batch(b2)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}
	expectedCount++

	doc = &FakeDocument{
		NewFakeField("_id", "3", true, false, false),
		NewFakeField("name", "test", false, false, true),
	}
	b3 := NewBatch()
	b3.Update(testIdentifier("3"), doc)
	err = idx.Batch(b3)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}
	expectedCount++

	reader, err := idx.Reader()
	if err != nil {
		t.Fatal(err)
	}
	docCount, err := reader.Count()
	if err != nil {
		t.Error(err)
	}
	if docCount != expectedCount {
		t.Errorf("Expected document count to be %d got %d", expectedCount, docCount)
	}
	err = reader.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestIndexInsertWithStore(t *testing.T) {
	cfg, cleanup := CreateConfig("TestIndexInsertWithStore")
	defer func() {
		err := cleanup()
		if err != nil {
			t.Log(err)
		}
	}()

	idx, err := OpenWriter(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = idx.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	var expectedCount uint64
	reader, err := idx.Reader()
	if err != nil {
		t.Fatal(err)
	}
	docCount, err := reader.Count()
	if err != nil {
		t.Error(err)
	}
	if docCount != expectedCount {
		t.Errorf("Expected document count to be %d got %d", expectedCount, docCount)
	}
	err = reader.Close()
	if err != nil {
		t.Fatal(err)
	}

	doc := &FakeDocument{
		NewFakeField("_id", "1", true, false, false),
		NewFakeField("name", "test", true, false, true),
	}
	b := NewBatch()
	b.Update(testIdentifier("1"), doc)
	err = idx.Batch(b)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}
	expectedCount++

	reader, err = idx.Reader()
	if err != nil {
		t.Fatal(err)
	}
	docCount, err = reader.Count()
	if err != nil {
		t.Error(err)
	}
	if docCount != expectedCount {
		t.Errorf("Expected document count to be %d got %d", expectedCount, docCount)
	}
	err = reader.Close()
	if err != nil {
		t.Fatal(err)
	}

	indexReader, err := idx.Reader()
	if err != nil {
		t.Error(err)
	}
	defer func() {
		err = indexReader.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	docNum1, err := findNumberByID(indexReader, "1")
	if err != nil {
		t.Fatal(err)
	}

	var storedFieldCount int
	err = indexReader.VisitStoredFields(docNum1, func(field string, value []byte) bool {
		storedFieldCount++
		if field == "name" {
			if string(value) != "test" {
				t.Errorf("expected name to be 'test', got '%s'", string(value))
			}
		} else if field == "_id" {
			if string(value) != "1" {
				t.Errorf("expected _id to be 1, got '%s'", string(value))
			}
		}
		return true
	})
	if err != nil {
		t.Fatal(err)
	}
	if storedFieldCount != 2 {
		t.Errorf("expected 2 stored fields, got %d", storedFieldCount)
	}
}

func TestIndexBatch(t *testing.T) {
	cfg, cleanup := CreateConfig("TestIndexBatch")
	defer func() {
		err := cleanup()
		if err != nil {
			t.Log(err)
		}
	}()

	idx, err := OpenWriter(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = idx.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	var expectedCount uint64

	// first create 2 docs the old fashioned way
	doc := &FakeDocument{
		NewFakeField("_id", "1", true, false, false),
		NewFakeField("name", "test", false, false, true),
	}
	b := NewBatch()
	b.Update(testIdentifier("1"), doc)
	err = idx.Batch(b)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}
	expectedCount++

	doc = &FakeDocument{
		NewFakeField("_id", "2", true, false, false),
		NewFakeField("name", "test2", false, false, true),
	}
	b2 := NewBatch()
	b2.Update(testIdentifier("2"), doc)
	err = idx.Batch(b2)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}
	expectedCount++

	// now create a batch which does 3 things
	// insert new doc
	// update existing doc
	// delete existing doc
	// net document count change 0

	batch := NewBatch()
	doc = &FakeDocument{
		NewFakeField("_id", "3", true, false, false),
		NewFakeField("name", "test3", false, false, true),
	}
	batch.Update(testIdentifier("3"), doc)
	doc = &FakeDocument{
		NewFakeField("_id", "2", true, false, false),
		NewFakeField("name", "test2updated", false, false, true),
	}
	batch.Update(testIdentifier("2"), doc)
	batch.Delete(testIdentifier("1"))

	err = idx.Batch(batch)
	if err != nil {
		t.Error(err)
	}

	indexReader, err := idx.Reader()
	if err != nil {
		t.Error(err)
	}
	defer func() {
		err = indexReader.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	numSegments := len(indexReader.segment)
	if numSegments <= 0 {
		t.Errorf("expected some segments, got: %d", numSegments)
	}

	docCount, err := indexReader.Count()
	if err != nil {
		t.Fatal(err)
	}
	if docCount != expectedCount {
		t.Errorf("Expected document count to be %d got %d", expectedCount, docCount)
	}

	pi, err := indexReader.postingsIteratorAll("")
	if err != nil {
		t.Fatal(err)
	}

	var docIDs []string
	var posting segment.Posting
	posting, err = pi.Next()
	for err == nil && posting != nil {
		err = indexReader.VisitStoredFields(posting.Number(), func(field string, value []byte) bool {
			if field == "_id" {
				docIDs = append(docIDs, string(value))
			}
			return true
		})
		if err != nil {
			t.Fatal(err)
		}
		posting, err = pi.Next()
	}
	if err != nil {
		t.Fatalf("error getting postings")
	}

	sort.Strings(docIDs)
	expectedIDs := []string{"2", "3"}
	if !reflect.DeepEqual(expectedIDs, docIDs) {
		t.Errorf("expected ids: %v, got ids: %v", expectedIDs, docIDs)
	}
}

func TestIndexBatchWithCallbacks(t *testing.T) {
	cfg, cleanup := CreateConfig("TestIndexBatchWithCallbacks")
	defer func() {
		err := cleanup()
		if err != nil {
			t.Fatal(err)
		}
	}()

	idx, err := OpenWriter(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = idx.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	// Check that callback function works
	var wg sync.WaitGroup
	wg.Add(1)

	batch := NewBatch()
	doc := &FakeDocument{
		NewFakeField("_id", "3", true, false, false),
		NewFakeField("name", "test3", false, false, true),
	}
	batch.Update(testIdentifier("3"), doc)
	batch.SetPersistedCallback(func(e error) {
		wg.Done()
	})

	err = idx.Batch(batch)
	if err != nil {
		t.Error(err)
	}

	wg.Wait()
	// test has no assertion but will timeout if callback doesn't fire
}

func TestIndexUpdateComposites(t *testing.T) {
	cfg, cleanup := CreateConfig("TestIndexUpdateComposites")
	defer func() {
		err := cleanup()
		if err != nil {
			t.Log(err)
		}
	}()

	idx, err := OpenWriter(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = idx.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	doc := &FakeDocument{
		NewFakeField("_id", "1", true, false, false),
		NewFakeField("name", "test", true, false, true),
		NewFakeField("title", "mister", true, false, true),
	}
	doc.FakeComposite("_all", nil)
	b := NewBatch()
	b.Update(testIdentifier("1"), doc)
	err = idx.Batch(b)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}

	// now lets update it
	doc = &FakeDocument{
		NewFakeField("_id", "1", true, false, false),
		NewFakeField("name", "testupdated", true, false, true),
		NewFakeField("title", "misterupdated", true, false, true),
	}
	doc.FakeComposite("_all", nil)
	b2 := NewBatch()
	b2.Update(testIdentifier("1"), doc)
	err = idx.Batch(b2)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}

	indexReader, err := idx.Reader()
	if err != nil {
		t.Error(err)
	}
	defer func() {
		err = indexReader.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	docNum1, err := findNumberByID(indexReader, "1")
	if err != nil {
		t.Fatal(err)
	}

	var fieldCount int
	err = indexReader.VisitStoredFields(docNum1, func(field string, value []byte) bool {
		fieldCount++
		if field == "name" {
			if string(value) != "testupdated" {
				t.Errorf("expected field content 'testupdated', got '%s'", string(value))
			}
		}
		return true
	})
	if err != nil {
		t.Fatal(err)
	}

	if fieldCount != 3 {
		t.Errorf("expected 3 stored fields, got %d", fieldCount)
	}
}

func TestIndexTermReaderCompositeFields(t *testing.T) {
	cfg, cleanup := CreateConfig("TestIndexTermReaderCompositeFields")
	defer func() {
		err := cleanup()
		if err != nil {
			t.Log(err)
		}
	}()

	idx, err := OpenWriter(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = idx.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	doc := &FakeDocument{
		NewFakeField("_id", "1", true, false, false),
		NewFakeField("name", "test", true, true, true),
		NewFakeField("title", "mister", true, true, true),
	}
	doc.FakeComposite("_all", nil)
	b := NewBatch()
	b.Update(testIdentifier("1"), doc)
	err = idx.Batch(b)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}

	indexReader, err := idx.Reader()
	if err != nil {
		t.Error(err)
	}
	defer func() {
		err = indexReader.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	docNum, err := findNumberByUniqueFieldTerm(indexReader, "_all", "mister")
	if err != nil {
		t.Fatal(err)
	}

	err = checkDocIDForNumber(indexReader, docNum, "1")
	if err != nil {
		t.Fatal(err)
	}

	pi, err := indexReader.PostingsIterator([]byte("mister"), "_all", true, true, true)
	if err != nil {
		t.Fatal(err)
	}

	var docIDs []string
	var posting segment.Posting
	posting, err = pi.Next()
	for err == nil && posting != nil {
		err = indexReader.VisitStoredFields(posting.Number(), func(field string, value []byte) bool {
			if field == "_id" {
				docIDs = append(docIDs, string(value))
			}
			return true
		})
		if err != nil {
			t.Fatal(err)
		}
		posting, err = pi.Next()
	}
	if err != nil {
		t.Fatalf("error getting postings")
	}

	sort.Strings(docIDs)
	expectedIDs := []string{"1"}
	if !reflect.DeepEqual(expectedIDs, docIDs) {
		t.Errorf("expected ids: %v, got ids: %v", expectedIDs, docIDs)
	}
}

func TestIndexDocumentVisitFieldTerms(t *testing.T) {
	cfg, cleanup := CreateConfig("TestIndexDocumentVisitFieldTerms")
	defer func() {
		err := cleanup()
		if err != nil {
			t.Log(err)
		}
	}()

	idx, err := OpenWriter(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = idx.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	doc := &FakeDocument{
		NewFakeField("_id", "1", true, false, false),
		NewFakeField("name", "test", true, true, true),
		NewFakeField("title", "mister", true, true, true),
	}
	b := NewBatch()
	b.Update(testIdentifier("1"), doc)
	err = idx.Batch(b)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}

	indexReader, err := idx.Reader()
	if err != nil {
		t.Error(err)
	}
	defer func() {
		err = indexReader.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	fieldTerms := make(map[string][]string)

	docNum1, err := findNumberByID(indexReader, "1")
	if err != nil {
		t.Fatal(err)
	}

	dvReader, err := indexReader.DocumentValueReader([]string{"name", "title"})
	if err != nil {
		t.Fatal(err)
	}
	err = dvReader.VisitDocumentValues(docNum1, func(field string, term []byte) {
		fieldTerms[field] = append(fieldTerms[field], string(term))
	})
	if err != nil {
		t.Error(err)
	}
	expectedFieldTerms := map[string][]string{
		"name":  {"test"},
		"title": {"mister"},
	}
	if !reflect.DeepEqual(fieldTerms, expectedFieldTerms) {
		t.Errorf("expected field terms: %#v, got: %#v", expectedFieldTerms, fieldTerms)
	}
}

func TestConcurrentUpdate(t *testing.T) {
	cfg, cleanup := CreateConfig("TestConcurrentUpdate")
	defer func() {
		err := cleanup()
		if err != nil {
			t.Log(err)
		}
	}()

	idx, err := OpenWriter(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = idx.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	// do some concurrent updates
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			doc := &FakeDocument{
				NewFakeField("_id", "1", true, false, false),
				NewFakeField(strconv.Itoa(i), strconv.Itoa(i), true, false, true),
			}
			b := NewBatch()
			b.Update(testIdentifier("1"), doc)
			err2 := idx.Batch(b)
			if err2 != nil {
				t.Errorf("Error updating index: %v", err2)
			}
			wg.Done()
		}(i)
	}
	wg.Wait()

	// now load the name field and see what we get
	r, err := idx.Reader()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = r.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	docNum1, err := findNumberByID(r, "1")
	if err != nil {
		t.Fatal(err)
	}

	var fieldCount int
	err = r.VisitStoredFields(docNum1, func(field string, value []byte) bool {
		fieldCount++
		return true
	})
	if err != nil {
		t.Fatal(err)
	}
	if fieldCount != 2 {
		t.Errorf("expected 2 fields, got %d", fieldCount)
	}
}

func TestLargeField(t *testing.T) {
	cfg, cleanup := CreateConfig("TestLargeField")
	defer func() {
		err := cleanup()
		if err != nil {
			t.Log(err)
		}
	}()

	idx, err := OpenWriter(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = idx.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	var largeFieldValue []byte
	for len(largeFieldValue) < 4096 {
		largeFieldValue = append(largeFieldValue, bleveWikiArticle1K...)
	}

	d := &FakeDocument{
		NewFakeField("_id", "large", true, false, false),
		NewFakeField("desc", string(largeFieldValue), true, false, true),
	}

	b := NewBatch()
	b.Update(testIdentifier("1"), d)
	err = idx.Batch(b)
	if err != nil {
		t.Fatal(err)
	}
}

var bleveWikiArticle1K = []byte(`Boiling liquid expanding vapor explosion
From Wikipedia, the free encyclopedia
See also: Boiler explosion and Steam explosion

Flames subsequent to a flammable liquid BLEVE from a tanker. BLEVEs do not necessarily involve fire.

This article's tone or style may not reflect the encyclopedic tone used on Wikipedia. See Wikipedia's guide to writing better articles for suggestions. (July 2013)
A boiling liquid expanding vapor explosion (BLEVE, /ˈblɛviː/ blev-ee) is an explosion caused by the rupture of a vessel containing a pressurized liquid above its boiling point.[1]
Contents  [hide]
1 Mechanism
1.1 Water example
1.2 BLEVEs without chemical reactions
2 Fires
3 Incidents
4 Safety measures
5 See also
6 References
7 External links
Mechanism[edit]

This section needs additional citations for verification. Please help improve this article by adding citations to reliable sources. Unsourced material may be challenged and removed. (July 2013)
There are three characteristics of liquids which are relevant to the discussion of a BLEVE:`)

func TestIndexDocumentVisitFieldTermsWithMultipleDocs(t *testing.T) {
	cfg, cleanup := CreateConfig("TestIndexDocumentVisitFieldTermsWithMultipleDocs")
	defer func() {
		err := cleanup()
		if err != nil {
			t.Log(err)
		}
	}()

	idx, err := OpenWriter(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = idx.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	doc := &FakeDocument{
		NewFakeField("_id", "1", true, false, false),
		NewFakeField("name", "test", true, true, true),
		NewFakeField("title", "mister", true, true, true),
	}

	b := NewBatch()
	b.Update(testIdentifier("1"), doc)
	err = idx.Batch(b)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}

	indexReader, err := idx.Reader()
	if err != nil {
		t.Error(err)
	}

	fieldTerms := make(map[string][]string)
	docNumber1, err := findNumberByID(indexReader, "1")
	if err != nil {
		t.Fatal(err)
	}
	dvReader, err := indexReader.DocumentValueReader([]string{"name", "title"})
	if err != nil {
		t.Fatal(err)
	}
	err = dvReader.VisitDocumentValues(docNumber1, func(field string, term []byte) {
		fieldTerms[field] = append(fieldTerms[field], string(term))
	})
	if err != nil {
		t.Error(err)
	}
	expectedFieldTerms := map[string][]string{
		"name":  {"test"},
		"title": {"mister"},
	}
	if !reflect.DeepEqual(fieldTerms, expectedFieldTerms) {
		t.Errorf("expected field terms: %#v, got: %#v", expectedFieldTerms, fieldTerms)
	}
	err = indexReader.Close()
	if err != nil {
		t.Fatal(err)
	}

	doc2 := &FakeDocument{
		NewFakeField("_id", "2", true, false, false),
		NewFakeField("name", "test2", true, true, true),
		NewFakeField("title", "mister2", true, true, true),
	}
	b2 := NewBatch()
	b2.Update(testIdentifier("2"), doc2)
	err = idx.Batch(b2)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}
	indexReader, err = idx.Reader()
	if err != nil {
		t.Error(err)
	}

	fieldTerms = make(map[string][]string)
	docNumber2, err := findNumberByID(indexReader, "2")
	if err != nil {
		t.Fatal(err)
	}
	dvReader, err = indexReader.DocumentValueReader([]string{"name", "title"})
	if err != nil {
		t.Fatal(err)
	}
	err = dvReader.VisitDocumentValues(docNumber2, func(field string, term []byte) {
		fieldTerms[field] = append(fieldTerms[field], string(term))
	})
	if err != nil {
		t.Error(err)
	}
	expectedFieldTerms = map[string][]string{
		"name":  {"test2"},
		"title": {"mister2"},
	}
	if !reflect.DeepEqual(fieldTerms, expectedFieldTerms) {
		t.Errorf("expected field terms: %#v, got: %#v", expectedFieldTerms, fieldTerms)
	}
	err = indexReader.Close()
	if err != nil {
		t.Fatal(err)
	}

	doc3 := &FakeDocument{
		NewFakeField("_id", "3", true, false, false),
		NewFakeField("name3", "test3", true, true, true),
		NewFakeField("title3", "mister3", true, true, true),
	}
	b3 := NewBatch()
	b3.Update(testIdentifier("3"), doc3)
	err = idx.Batch(b3)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}
	indexReader, err = idx.Reader()
	if err != nil {
		t.Error(err)
	}

	fieldTerms = make(map[string][]string)
	docNumber3, err := findNumberByID(indexReader, "3")
	if err != nil {
		t.Fatal(err)
	}
	dvReader, err = indexReader.DocumentValueReader([]string{"name3", "title3"})
	if err != nil {
		t.Fatal(err)
	}
	err = dvReader.VisitDocumentValues(docNumber3, func(field string, term []byte) {
		fieldTerms[field] = append(fieldTerms[field], string(term))
	})
	if err != nil {
		t.Error(err)
	}
	expectedFieldTerms = map[string][]string{
		"name3":  {"test3"},
		"title3": {"mister3"},
	}
	if !reflect.DeepEqual(fieldTerms, expectedFieldTerms) {
		t.Errorf("expected field terms: %#v, got: %#v", expectedFieldTerms, fieldTerms)
	}

	fieldTerms = make(map[string][]string)
	docNumber1, err = findNumberByID(indexReader, "1")
	if err != nil {
		t.Fatal(err)
	}
	dvReader, err = indexReader.DocumentValueReader([]string{"name", "title"})
	if err != nil {
		t.Fatal(err)
	}
	err = dvReader.VisitDocumentValues(docNumber1, func(field string, term []byte) {
		fieldTerms[field] = append(fieldTerms[field], string(term))
	})
	if err != nil {
		t.Error(err)
	}
	expectedFieldTerms = map[string][]string{
		"name":  {"test"},
		"title": {"mister"},
	}
	if !reflect.DeepEqual(fieldTerms, expectedFieldTerms) {
		t.Errorf("expected field terms: %#v, got: %#v", expectedFieldTerms, fieldTerms)
	}
	err = indexReader.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestIndexDocumentVisitFieldTermsWithMultipleFieldOptions(t *testing.T) {
	cfg, cleanup := CreateConfig("TestIndexDocumentVisitFieldTermsWithMultipleFieldOptions")
	defer func() {
		err := cleanup()
		if err != nil {
			t.Log(err)
		}
	}()

	idx, err := OpenWriter(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = idx.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	// mix of field options, this exercises the run time/ on the fly un inverting of
	// doc values for custom options enabled field like designation, dept.
	doc := &FakeDocument{
		NewFakeField("_id", "1", true, false, false),
		NewFakeField("name", "test", false, false, true),
		NewFakeField("title", "mister", false, false, true),
		NewFakeField("designation", "engineer", true, true, true),
		NewFakeField("dept", "bleve", true, true, true),
	}
	b := NewBatch()
	b.Update(testIdentifier("1"), doc)
	err = idx.Batch(b)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}

	indexReader, err := idx.Reader()
	if err != nil {
		t.Error(err)
	}

	fieldTerms := make(map[string][]string)
	docNumber1, err := findNumberByID(indexReader, "1")
	if err != nil {
		t.Fatal(err)
	}
	dvReader, err := indexReader.DocumentValueReader([]string{"name", "designation", "dept"})
	if err != nil {
		t.Fatal(err)
	}
	err = dvReader.VisitDocumentValues(docNumber1, func(field string, term []byte) {
		fieldTerms[field] = append(fieldTerms[field], string(term))
	})
	if err != nil {
		t.Error(err)
	}
	expectedFieldTerms := map[string][]string{
		"name":        {"test"},
		"designation": {"engineer"},
		"dept":        {"bleve"},
	}
	if !reflect.DeepEqual(fieldTerms, expectedFieldTerms) {
		t.Errorf("expected field terms: %#v, got: %#v", expectedFieldTerms, fieldTerms)
	}
	err = indexReader.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestAllFieldWithDifferentTermVectorsEnabled(t *testing.T) {
	// Based on https://github.com/blevesearch/bleve/issues/895 from xeizmendi
	cfg, cleanup := CreateConfig("TestAllFieldWithDifferentTermVectorsEnabled")
	defer func() {
		err := cleanup()
		if err != nil {
			t.Log(err)
		}
	}()

	idx, err := OpenWriter(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = idx.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	doc := &FakeDocument{
		NewFakeField("_id", "1", true, false, false),
		NewFakeField("keyword", "something", false, false, true),
		NewFakeField("text", "A sentence that includes something within.", false, true, true),
	}
	doc.FakeComposite("_all", nil)

	b := NewBatch()
	b.Update(testIdentifier("1"), doc)
	err = idx.Batch(b)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}
}

func TestIndexSeekBackwardsStats(t *testing.T) {
	cfg, cleanup := CreateConfig("TestIndexOpenReopen")
	defer func() {
		err := cleanup()
		if err != nil {
			t.Log(err)
		}
	}()

	idx, err := OpenWriter(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = idx.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	// insert a doc
	batch := NewBatch()
	doc := &FakeDocument{
		NewFakeField("_id", "1", true, false, false),
		NewFakeField("name", "cat", true, false, true),
	}
	doc.FakeComposite("_all", nil)
	batch.Update(testIdentifier("1"), doc)
	err = idx.Batch(batch)
	if err != nil {
		t.Error(err)
	}

	// insert another doc
	batch.Reset()
	doc = &FakeDocument{
		NewFakeField("_id", "2", true, false, false),
		NewFakeField("name", "cat", true, false, true),
	}
	doc.FakeComposite("_all", nil)
	batch.Update(testIdentifier("2"), doc)
	err = idx.Batch(batch)
	if err != nil {
		t.Error(err)
	}

	reader, err := idx.Reader()
	if err != nil {
		t.Fatalf("error getting index reader: %v", err)
	}
	defer func() {
		err = reader.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	tfr, err := reader.PostingsIterator([]byte("cat"), "name", false, false, false)
	if err != nil {
		t.Fatalf("error getting term field readyer for name/cat: %v", err)
	}

	tfdFirst, err := tfr.Next()
	if err != nil {
		t.Fatalf("error getting first tfd: %v", err)
	}

	_, err = tfr.Next()
	if err != nil {
		t.Fatalf("error getting second tfd: %v", err)
	}

	// seek backwards to the first
	_, err = tfr.Advance(tfdFirst.Number())
	if err != nil {
		t.Fatalf("error adancing backwards: %v", err)
	}

	err = tfr.Close()
	if err != nil {
		t.Fatalf("error closing term field reader: %v", err)
	}

	if idx.stats.TotTermSearchersStarted != idx.stats.TotTermSearchersFinished {
		t.Errorf("expected term searchers started %d to equal term searchers finished %d",
			idx.stats.TotTermSearchersStarted,
			idx.stats.TotTermSearchersFinished)
	}
}

func TestBatch_InsertIfAbsent(t *testing.T) {
	tests := []struct {
		name          string
		cacheMaxBytes int
	}{
		{"WithoutCache", 0},
		{"WithCache", 1024 * 1024}, // 1MB cache
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, cleanup := CreateConfig("TestBatch_InsertIfAbsent")
			defer func() {
				err := cleanup()
				if err != nil {
					t.Log(err)
				}
			}()

			cfg.CacheMaxBytes = tt.cacheMaxBytes

			idx, err := OpenWriter(cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				err := idx.Close()
				if err != nil {
					t.Fatal(err)
				}
			}()

			var expectedCount uint64

			// Verify initial document count is zero
			reader, err := idx.Reader()
			if err != nil {
				t.Fatal(err)
			}
			docCount, err := reader.Count()
			if err != nil {
				t.Error(err)
			}
			if docCount != expectedCount {
				t.Errorf("Expected document count to be %d got %d", expectedCount, docCount)
			}
			err = reader.Close()
			if err != nil {
				t.Fatal(err)
			}

			// Insert a document using InsertIfAbsent
			docID := "doc-1"
			doc := &FakeDocument{
				NewFakeField("_id", docID, true, false, false),
				NewFakeField("title", "mister", false, false, true),
			}
			batch := NewBatch()
			batch.InsertIfAbsent(testIdentifier(docID), []string{"title"}, doc)

			// Apply the batch
			if err := idx.Batch(batch); err != nil {
				t.Fatalf("failed to apply batch: %v", err)
			}
			expectedCount++

			// Verify document count after insertion
			reader, err = idx.Reader()
			if err != nil {
				t.Fatal(err)
			}
			docCount, err = reader.Count()
			if err != nil {
				t.Error(err)
			}
			if docCount != expectedCount {
				t.Errorf("Expected document count to be %d got %d", expectedCount, docCount)
			}
			err = reader.Close()
			if err != nil {
				t.Fatal(err)
			}

			// Attempt to InsertIfAbsent with the same ID
			docDuplicate := &FakeDocument{
				NewFakeField("_id", docID, true, false, false),
				NewFakeField("title", "mister2", true, false, true),
			}
			batchDuplicate := NewBatch()
			batchDuplicate.InsertIfAbsent(testIdentifier(docID), []string{"title"}, docDuplicate)

			// Apply the duplicate batch
			if err := idx.Batch(batchDuplicate); err != nil {
				t.Fatalf("failed to apply duplicate batch: %v", err)
			}

			// Since it's InsertIfAbsent, the document should not be duplicated
			// Verify document count remains the same
			reader, err = idx.Reader()
			if err != nil {
				t.Fatal(err)
			}
			docCount, err = reader.Count()
			if err != nil {
				t.Error(err)
			}
			if docCount != expectedCount {
				t.Errorf("Expected document count to be %d after duplicate insert, got %d", expectedCount, docCount)
			}

			docNum1, err := findNumberByID(reader, docID)
			if err != nil {
				t.Fatal(err)
			}

			dvr, err := reader.DocumentValueReader([]string{"title"})
			if err != nil {
				t.Fatal(err)
			}
			err = dvr.VisitDocumentValues(docNum1, func(field string, term []byte) {
				if field == "title" {
					if string(term) != "mister" {
						t.Errorf("expected title to be 'mister', got '%s'", string(term))
					}
				}
			})
			if err != nil {
				t.Fatal(err)
			}

			err = reader.VisitStoredFields(docNum1, func(field string, value []byte) bool {
				if field == "title" {
					if string(value) != "mister" {
						t.Errorf("expected title to be 'mister', got '%s'", string(value))
					}
				}
				return true
			})
			if err != nil {
				t.Fatal(err)
			}

			err = reader.Close()
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestBatch_InsertAndUpdateContent(t *testing.T) {
	tests := []struct {
		name          string
		cacheMaxBytes int
	}{
		{"WithoutCache", 0},
		{"WithCache", 1024 * 1024}, // 1MB cache
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, cleanup := CreateConfig("TestBatch_InsertAndUpdateContent")
			defer func() {
				err := cleanup()
				if err != nil {
					t.Log(err)
				}
			}()

			cfg.CacheMaxBytes = tt.cacheMaxBytes

			idx, err := OpenWriter(cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				err := idx.Close()
				if err != nil {
					t.Fatal(err)
				}
			}()

			var expectedCount uint64

			// Insert a document
			docID := "doc-1"
			doc := &FakeDocument{
				NewFakeField("_id", docID, true, false, false),
				NewFakeField("title", "mister", false, false, true),
			}
			batch := NewBatch()
			batch.InsertIfAbsent(testIdentifier(docID), []string{"title"}, doc)

			// Apply the batch
			if err := idx.Batch(batch); err != nil {
				t.Fatalf("failed to apply batch: %v", err)
			}
			expectedCount++

			// Verify document count after insertion
			reader, err := idx.Reader()
			if err != nil {
				t.Fatal(err)
			}
			docCount, err := reader.Count()
			if err != nil {
				t.Error(err)
			}
			if docCount != expectedCount {
				t.Errorf("Expected document count to be %d got %d", expectedCount, docCount)
			}
			err = reader.Close()
			if err != nil {
				t.Fatal(err)
			}

			// Update the document with new content
			docUpdated := &FakeDocument{
				NewFakeField("_id", docID, true, false, false),
				NewFakeField("title", "mister", false, false, true),
				NewFakeField("content", "updated content", false, false, true),
			}
			batchUpdate := NewBatch()
			batchUpdate.InsertIfAbsent(testIdentifier(docID), []string{"title", "content"}, docUpdated)

			// Apply the update batch
			if err := idx.Batch(batchUpdate); err != nil {
				t.Fatalf("failed to apply update batch: %v", err)
			}

			// Verify document count remains the same
			reader, err = idx.Reader()
			if err != nil {
				t.Fatal(err)
			}
			docCount, err = reader.Count()
			if err != nil {
				t.Error(err)
			}
			if docCount != expectedCount {
				t.Errorf("Expected document count to be %d after update, got %d", expectedCount, docCount)
			}

			docNum1, err := findNumberByID(reader, docID)
			if err != nil {
				t.Fatal(err)
			}

			// Verify the updated content
			err = reader.VisitStoredFields(docNum1, func(field string, value []byte) bool {
				if field == "content" {
					if string(value) != "updated content" {
						t.Errorf("expected content to be 'updated content', got '%s'", string(value))
					}
				}
				return true
			})
			if err != nil {
				t.Fatal(err)
			}

			err = reader.Close()
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}
