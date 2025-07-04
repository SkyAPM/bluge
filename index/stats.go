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
	"reflect"
	"sync/atomic"

	"github.com/VictoriaMetrics/fastcache"
)

func (s *Writer) DirectoryStats() (numFilesOnDisk, numBytesUsedDisk uint64) {
	return s.directory.Stats()
}

func (s *Writer) Stats() Stats {
	// add some computed values
	numFilesOnDisk, numBytesUsedDisk := s.directory.Stats()

	// Update the stats atomically
	atomic.StoreUint64(&s.stats.CurOnDiskBytes, numBytesUsedDisk)
	atomic.StoreUint64(&s.stats.CurOnDiskFiles, numFilesOnDisk)
	stats := s.stats.Clone()
	c := s.cache.Load()
	if c != nil {
		var cs fastcache.Stats
		c.UpdateStats(&cs)
		stats.CacheGetCalls = cs.GetCalls
		stats.CacheSetCalls = cs.SetCalls
		stats.CacheMisses = cs.Misses
		stats.CacheEntriesCount = cs.EntriesCount
		stats.CacheBytesSize = cs.BytesSize
		stats.CacheMaxBytesSize = cs.MaxBytesSize
	}
	return stats
}

// Stats tracks statistics about the index, fields that are
// prefixed like CurXxxx are gauges (can go up and down),
// and fields that are prefixed like TotXxxx are monotonically
// increasing counters.
type Stats struct {
	TotUpdates uint64
	TotDeletes uint64

	TotBatches        uint64
	TotBatchesEmpty   uint64
	TotBatchIntroTime uint64
	MaxBatchIntroTime uint64

	CurRootEpoch       uint64
	LastPersistedEpoch uint64
	LastMergedEpoch    uint64

	TotOnErrors uint64

	TotAnalysisTime uint64
	TotIndexTime    uint64

	TotIndexedPlainTextBytes uint64

	TotTermSearchersStarted  uint64
	TotTermSearchersFinished uint64

	TotIntroduceLoop       uint64
	TotIntroduceSegmentBeg uint64
	TotIntroduceSegmentEnd uint64
	TotIntroducePersistBeg uint64
	TotIntroducePersistEnd uint64
	TotIntroduceMergeBeg   uint64
	TotIntroduceMergeEnd   uint64
	TotIntroduceRevertBeg  uint64
	TotIntroduceRevertEnd  uint64

	TotIntroducedItems         uint64
	TotIntroducedSegmentsBatch uint64
	TotIntroducedSegmentsMerge uint64

	TotPersistLoopBeg          uint64
	TotPersistLoopErr          uint64
	TotPersistLoopProgress     uint64
	TotPersistLoopWait         uint64
	TotPersistLoopWaitNotified uint64
	TotPersistLoopEnd          uint64

	TotPersistedItems    uint64
	TotItemsToPersist    uint64
	TotPersistedSegments uint64

	TotPersisterSlowMergerPause  uint64
	TotPersisterSlowMergerResume uint64

	TotPersisterNapPauseCompleted uint64
	TotPersisterMergerNapBreak    uint64

	TotFileMergeLoopBeg uint64
	TotFileMergeLoopErr uint64
	TotFileMergeLoopEnd uint64

	TotFileMergePlan     uint64
	TotFileMergePlanErr  uint64
	TotFileMergePlanNone uint64
	TotFileMergePlanOk   uint64

	TotFileMergePlanTasks              uint64
	TotFileMergePlanTasksDone          uint64
	TotFileMergePlanTasksErr           uint64
	TotFileMergePlanTasksSegments      uint64
	TotFileMergePlanTasksSegmentsEmpty uint64

	TotFileMergeSegmentsEmpty uint64
	TotFileMergeSegments      uint64
	TotFileSegmentsAtRoot     uint64
	TotFileMergeWrittenBytes  uint64

	TotFileMergeZapBeg              uint64
	TotFileMergeZapEnd              uint64
	TotFileMergeZapTime             uint64
	MaxFileMergeZapTime             uint64
	TotFileMergeZapIntroductionTime uint64
	MaxFileMergeZapIntroductionTime uint64

	TotFileMergeIntroductions          uint64
	TotFileMergeIntroductionsDone      uint64
	TotFileMergeIntroductionsSkipped   uint64
	TotFileMergeIntroductionsObsoleted uint64

	CurFilesIneligibleForRemoval     uint64
	TotSnapshotsRemovedFromMetaStore uint64

	TotMemMergeBeg          uint64
	TotMemMergeErr          uint64
	TotMemMergeDone         uint64
	TotMemMergeZapBeg       uint64
	TotMemMergeZapEnd       uint64
	TotMemMergeZapTime      uint64
	MaxMemMergeZapTime      uint64
	TotMemMergeSegments     uint64
	TotMemorySegmentsAtRoot uint64

	TotEventFired    uint64
	TotEventReturned uint64

	CurOnDiskBytes           uint64
	CurOnDiskBytesUsedByRoot uint64 // FIXME not currently supported
	CurOnDiskFiles           uint64

	// the following stats are only used internally
	persistEpoch          uint64
	persistSnapshotSize   uint64
	mergeEpoch            uint64
	mergeSnapshotSize     uint64
	newSegBufBytesAdded   uint64
	newSegBufBytesRemoved uint64
	analysisBytesAdded    uint64
	analysisBytesRemoved  uint64

	CacheGetCalls     uint64
	CacheSetCalls     uint64
	CacheMisses       uint64
	CacheEntriesCount uint64
	CacheBytesSize    uint64
	CacheMaxBytesSize uint64
}

func (s *Stats) ToMap() map[string]interface{} {
	m := map[string]interface{}{}
	sve := reflect.ValueOf(s).Elem()
	svet := sve.Type()
	for i := 0; i < svet.NumField(); i++ {
		svef := sve.Field(i)
		structField := svet.Field(i)
		if svef.CanAddr() && structField.IsExported() {
			svefp := svef.Addr().Interface()
			m[svet.Field(i).Name] = atomic.LoadUint64(svefp.(*uint64))
		}
	}
	return m
}

func (s *Stats) FromMap(m map[string]interface{}) {
	sve := reflect.ValueOf(s).Elem()
	svet := sve.Type()
	for i := 0; i < svet.NumField(); i++ {
		svef := sve.Field(i)
		structField := svet.Field(i)
		if svef.CanAddr() && structField.IsExported() {
			svefp := svef.Addr().Interface()
			if v, ok := m[svet.Field(i).Name]; ok {
				atomic.StoreUint64(svefp.(*uint64), v.(uint64))
			}
		}
	}
}

func (s *Stats) Clone() (n Stats) {
	n.FromMap(s.ToMap())
	return n
}

func (s *Writer) numEventsBlocking() int {
	eventsReturned := atomic.LoadUint64(&s.stats.TotEventReturned)
	eventsFired := atomic.LoadUint64(&s.stats.TotEventFired)
	return int(eventsFired - eventsReturned)
}
