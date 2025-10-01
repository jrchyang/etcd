// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import pb "go.etcd.io/etcd/raft/v3/raftpb"

// unstable.entries[i] has raft log position i+unstable.offset.
// Note that unstable.offset may be less than the highest log
// position in storage; this means that the next write to storage
// might need to truncate the log before persisting unstable.entries.
//
// unstable 使用内存数组维护其中所有的 Entry 记录
// 对于 Leader 节点而言，它维护了客户端请求对应的 Entry 记录
// 对于 Follower 节点而言，它维护的是从 Leader 节点复制来的 Entry 记录
// 无论是 Leader 节点还是 Follower 节点，对于刚刚收到的 Entry 记录，首先都会被存储在
// unstable 中，然后按照 Raft 协议将 unstable 中缓存的这些 Entry 记录交给上层模块进行处理，
// 上层模块会将这些 Entry 记录发送到集群中其他节点或进行保存（写入 Storage）。之后，上层模块会
// 调用 Advance() 方法通知底层的 raft 模块将 unstable 中对应的 Entry 记录删除（因为已经
// 保存到了 Storage 中）。正因为 unstable 中保存的 Entry 记录并未持久化，可能会因节点故障
// 而意外丢失，所以被称为 unstable
type unstable struct {
	// the incoming unstable snapshot, if any.
	// 快照数据，该快照数据也是未写入 Storage 中的
	snapshot *pb.Snapshot
	// all entries that have not yet been written to storage.
	// 保存未写入 Storage 中的 Entry 记录
	entries []pb.Entry
	// entries 中的第一条 Entry 记录的索引值
	offset uint64

	logger Logger
}

// maybeFirstIndex returns the index of the first possible entry in entries
// if it has a snapshot.
func (u *unstable) maybeFirstIndex() (uint64, bool) {
	if u.snapshot != nil {
		return u.snapshot.Metadata.Index + 1, true
	}
	return 0, false
}

// maybeLastIndex returns the last index if it has at least one
// unstable entry or snapshot.
func (u *unstable) maybeLastIndex() (uint64, bool) {
	if l := len(u.entries); l != 0 {
		return u.offset + uint64(l) - 1, true
	}
	if u.snapshot != nil {
		return u.snapshot.Metadata.Index, true
	}
	return 0, false
}

// maybeTerm returns the term of the entry at index i, if there
// is any.
func (u *unstable) maybeTerm(i uint64) (uint64, bool) {
	// offset 是 unstable 中的第一条 entry 的索引，如果 i < u.offset
	// 则说索引 i 对应的 entry 要么在快照中要么在 Storage 中
	if i < u.offset {
		// 如果是快照最后一条 entry 的索引，则返回
		// 不相等的话，由于快照中已经没有 index 和 term 了，直接返回没找到
		if u.snapshot != nil && u.snapshot.Metadata.Index == i {
			return u.snapshot.Metadata.Term, true
		}
		return 0, false
	}

	// 获取 unstable 中最后一条 entry 的索引
	// ok == false 表明 unstable 中没有 entry，直接返回
	// i > last 表明超过了 unstable 中的所有 entry，直接返回
	last, ok := u.maybeLastIndex()
	if !ok {
		return 0, false
	}
	if i > last {
		return 0, false
	}

	// 此时说明 i 就在 unstable 中，由于 index 必定是连续的，可以直接取值
	return u.entries[i-u.offset].Term, true
}

func (u *unstable) stableTo(i, t uint64) {
	gt, ok := u.maybeTerm(i)
	if !ok {
		return
	}
	// if i < offset, term is matched with the snapshot
	// only update the unstable entries if term is matched with
	// an unstable entry.
	if gt == t && i >= u.offset {
		u.entries = u.entries[i+1-u.offset:]
		u.offset = i + 1
		u.shrinkEntriesArray()
	}
}

// shrinkEntriesArray discards the underlying array used by the entries slice
// if most of it isn't being used. This avoids holding references to a bunch of
// potentially large entries that aren't needed anymore. Simply clearing the
// entries wouldn't be safe because clients might still be using them.
func (u *unstable) shrinkEntriesArray() {
	// We replace the array if we're using less than half of the space in
	// it. This number is fairly arbitrary, chosen as an attempt to balance
	// memory usage vs number of allocations. It could probably be improved
	// with some focused tuning.
	const lenMultiple = 2
	if len(u.entries) == 0 {
		u.entries = nil
	} else if len(u.entries)*lenMultiple < cap(u.entries) {
		newEntries := make([]pb.Entry, len(u.entries))
		copy(newEntries, u.entries)
		u.entries = newEntries
	}
}

func (u *unstable) stableSnapTo(i uint64) {
	if u.snapshot != nil && u.snapshot.Metadata.Index == i {
		u.snapshot = nil
	}
}

func (u *unstable) restore(s pb.Snapshot) {
	u.offset = s.Metadata.Index + 1
	u.entries = nil
	u.snapshot = &s
}

func (u *unstable) truncateAndAppend(ents []pb.Entry) {
	after := ents[0].Index
	switch {
	// 索引连续直接追加
	case after == u.offset+uint64(len(u.entries)):
		// after is the next index in the u.entries
		// directly append
		u.entries = append(u.entries, ents...)
	// 索引更小，直接替换
	case after <= u.offset:
		u.logger.Infof("replace the unstable entries from index %d", after)
		// The log is being truncated to before our current offset
		// portion, so set the offset and replace the entries
		u.offset = after
		u.entries = ents
	// 如果 after > u.offset+uint64(len(u.entries)) 会直接 panic
	// 因此这里处理的是部分覆盖的场景，直接使用新的替换旧的
	default:
		// truncate to after and copy to u.entries
		// then append
		u.logger.Infof("truncate the unstable entries before index %d", after)
		u.entries = append([]pb.Entry{}, u.slice(u.offset, after)...)
		u.entries = append(u.entries, ents...)
	}
}

func (u *unstable) slice(lo uint64, hi uint64) []pb.Entry {
	u.mustCheckOutOfBounds(lo, hi)
	return u.entries[lo-u.offset : hi-u.offset]
}

// u.offset <= lo <= hi <= u.offset+len(u.entries)
func (u *unstable) mustCheckOutOfBounds(lo, hi uint64) {
	if lo > hi {
		u.logger.Panicf("invalid unstable.slice %d > %d", lo, hi)
	}
	upper := u.offset + uint64(len(u.entries))
	if lo < u.offset || hi > upper {
		u.logger.Panicf("unstable.slice[%d,%d) out of bound [%d,%d]", lo, hi, u.offset, upper)
	}
}
