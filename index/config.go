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
	"math"

	segment "github.com/blugelabs/bluge_segment_api"

	"github.com/blugelabs/bluge/index/mergeplan"
	"github.com/blugelabs/ice"

	"github.com/RoaringBitmap/roaring"
)

type Config struct {
	SegmentType    string
	SegmentVersion uint32

	supportedSegmentPlugins map[string]map[uint32]*SegmentPlugin

	UnsafeBatch        bool
	EventCallback      func(Event)
	AsyncError         func(error)
	MergePlanOptions   mergeplan.Options
	NumAnalysisWorkers int
	AnalysisChan       chan func()
	GoFunc             func(func())
	DeletionPolicyFunc func() DeletionPolicy
	DirectoryFunc      func() Directory
	NormCalc           func(string, int) float32

	MergeBufferSize int

	// Time filter
	FilterTimeMin int64
	FilterTimeMax int64

	// Optimizations
	OptimizeConjunction          bool
	OptimizeConjunctionUnadorned bool
	OptimizeDisjunctionUnadorned bool

	// MinSegmentsForInMemoryMerge represents the number of
	// in-memory zap segments that persistSnapshotMaybeMerge() needs to
	// see in an Snapshot before it decides to merge and persist
	// those segments
	MinSegmentsForInMemoryMerge int

	// PersisterNapTimeMSec controls the wait/delay injected into
	// persistence workloop to improve the chances for
	// a healthier and heavier in-memory merging
	PersisterNapTimeMSec int

	// PersisterNapTimeMSec > 0, and the number of files is less than
	// PersisterNapUnderNumFiles, then the persister will sleep
	// PersisterNapTimeMSec amount of time to improve the chances for
	// a healthier and heavier in-memory merging
	PersisterNapUnderNumFiles int

	// MemoryPressurePauseThreshold let persister to have a better leeway
	// for prudently performing the memory merge of segments on a memory
	// pressure situation. Here the config value is an upper threshold
	// for the number of paused application threads. The default value would
	// be a very high number to always favor the merging of memory segments.
	MemoryPressurePauseThreshold int

	ValidateSnapshotCRC bool

	virtualFields map[string][]segment.Field

	CacheMaxBytes int

	// PerFieldSimilarity allows specifying a custom callback before merge operation
	PrepareMergeFunc func(src []*roaring.Bitmap, segments []segment.Segment, id uint64) (dest []*roaring.Bitmap, err error)
}

func (config Config) WithSegmentType(typ string) Config {
	config.SegmentType = typ
	return config
}

func (config Config) WithSegmentVersion(ver uint32) Config {
	config.SegmentVersion = ver
	return config
}

func (config Config) WithPersisterNapTimeMSec(napTime int) Config {
	config.PersisterNapTimeMSec = napTime
	return config
}

func (config Config) WithVirtualField(field segment.Field) Config {
	config.virtualFields[field.Name()] = append(config.virtualFields[field.Name()], field)
	return config
}

func (config Config) WithNormCalc(calc func(field string, numTerms int) float32) Config {
	config.NormCalc = calc
	return config
}

func (config Config) WithTimeRange(timeMin, timeMax int64) Config {
	config.FilterTimeMin = timeMin
	config.FilterTimeMax = timeMax
	return config
}

func (config Config) WithSegmentPlugin(plugin *SegmentPlugin) Config {
	if _, ok := config.supportedSegmentPlugins[plugin.Type]; !ok {
		config.supportedSegmentPlugins[plugin.Type] = map[uint32]*SegmentPlugin{}
	}
	config.supportedSegmentPlugins[plugin.Type][plugin.Version] = plugin
	return config
}

func (config Config) DisableOptimizeConjunction() Config {
	config.OptimizeConjunction = false
	return config
}

func (config Config) DisableOptimizeConjunctionUnadorned() Config {
	config.OptimizeConjunctionUnadorned = false
	return config
}

func (config Config) DisableOptimizeDisjunctionUnadorned() Config {
	config.OptimizeDisjunctionUnadorned = false
	return config
}

func (config Config) WithUnsafeBatches() Config {
	config.UnsafeBatch = true
	return config
}

func DefaultConfig(path string) Config {
	rv := defaultConfig()
	rv.DirectoryFunc = func() Directory {
		return NewFileSystemDirectory(path)
	}
	return rv
}

func InMemoryOnlyConfig() Config {
	rv := defaultConfig()
	rv.DirectoryFunc = func() Directory {
		return NewInMemoryDirectory()
	}
	return rv
}

func DefaultConfigWithDirectory(df func() Directory) Config {
	rv := defaultConfig()
	rv.DirectoryFunc = df
	return rv
}

func defaultConfig() Config {
	rv := Config{
		SegmentType:      ice.Type,
		SegmentVersion:   ice.Version,
		MergePlanOptions: mergeplan.DefaultMergePlanOptions,
		DeletionPolicyFunc: func() DeletionPolicy {
			return NewKeepNLatestDeletionPolicy(1)
		},

		MergeBufferSize: 1024 * 1024,

		// Optimizations enabled
		OptimizeConjunction:          true,
		OptimizeConjunctionUnadorned: true,
		OptimizeDisjunctionUnadorned: true,

		MinSegmentsForInMemoryMerge: 2,

		// DefaultPersisterNapTimeMSec is kept to zero as this helps in direct
		// persistence of segments with the default safe batch option.
		// If the default safe batch option results in high number of
		// files on disk, then users may initialize this configuration parameter
		// with higher values so that the persister will nap a bit within it's
		// work loop to favor better in-memory merging of segments to result
		// in fewer segment files on disk. But that may come with an indexing
		// performance overhead.
		// Unsafe batch users are advised to override this to higher value
		// for better performance especially with high data density.
		PersisterNapTimeMSec: 0,

		// DefaultPersisterNapUnderNumFiles helps in controlling the pace of
		// persister. At times of a slow merger progress with heavy file merging
		// operations, its better to pace down the persister for letting the merger
		// to catch up within a range defined by this parameter.
		// Fewer files on disk (as per the merge plan) would result in keeping the
		// file handle usage under limit, faster disk merger and a healthier index.
		// Its been observed that such a loosely sync'ed introducer-persister-merger
		// trio results in better overall performance.
		PersisterNapUnderNumFiles: 1000,

		MemoryPressurePauseThreshold: math.MaxInt32,

		// VirtualFields allow you to describe a set of fields
		// The index will behave as if all documents in this index were
		// indexed with these fields, even though nothing is
		// physically persisted about them in the index.
		virtualFields: map[string][]segment.Field{},

		NumAnalysisWorkers: 4,
		AnalysisChan:       make(chan func()),
		GoFunc: func(f func()) {
			go f()
		},

		ValidateSnapshotCRC: true,

		supportedSegmentPlugins: map[string]map[uint32]*SegmentPlugin{},
	}

	rv.WithSegmentPlugin(&SegmentPlugin{
		Type:    ice.Type,
		Version: ice.Version,
		New:     ice.New,
		Load:    ice.Load,
		Merge:   ice.Merge,
	})

	return rv
}
