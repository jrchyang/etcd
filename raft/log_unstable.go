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
// unstable 使用内存数组维护其中所有的 entry 记录
// 对于 leader 节点而言，它维护了客户端请求对应的 entry 记录
// 对于 follower 节点而言，它维护的是从 leader 节点复制来的 entry 记录
// 无论是 leader 节点还是 follower 节点，对于刚刚接收到的 entry 记录首先都会被存储到
// unstable 中，然后按照 raft 协议将 unstable 中缓存的这些 entry 记录交给上层模块处理
// 上层模块会调用 Advance() 方法通知底层的 etcd-raft 模块将 unstable 中对应的 Entry
// 记录删除（因为已经保存到了 Storage 中）。正因为 unstable 中保存的 Entry
// 记录并未进行持久化，可能会因节点故障而意外丢失，故称为 unstable
type unstable struct {
	// the incoming unstable snapshot, if any.
	snapshot *pb.Snapshot
	// all entries that have not yet been written to storage.
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
	if i < u.offset {
		if u.snapshot != nil && u.snapshot.Metadata.Index == i {
			return u.snapshot.Metadata.Term, true
		}
		return 0, false
	}

	last, ok := u.maybeLastIndex()
	if !ok {
		return 0, false
	}
	if i > last {
		return 0, false
	}

	return u.entries[i-u.offset].Term, true
}

// 当 unstable.entries 中的 entry 记录已经被写入 storage 之后，
// 会调用 unstable.stableTo() 方法清除 entries 中对应的 entry 记录
func (u *unstable) stableTo(i, t uint64) {
	// 查找指定 entry 记录的 term
	// 若超找失败则表示对应的 entry 不在 unstaable 中直接返回
	gt, ok := u.maybeTerm(i)
	if !ok {
		return
	}
	// if i < offset, term is matched with the snapshot
	// only update the unstable entries if term is matched with
	// an unstable entry.
	// 指定的 entry 记录在 unstable.entries 中保存
	if gt == t && i >= u.offset {
		// 指定索引值之前的 entry 记录都已经完成持久化，则将其之前的全部 entry 记录删除
		u.entries = u.entries[i+1-u.offset:]
		u.offset = i + 1 // 更新 offset 字段
		// shrinkEntriesArray() 方法会在底层数组长度超过实际占用的两倍时
		// 对底层数组进行缩减
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

// 快照被写入 storage 之后调用该方法将 snapshot 字段清空
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

// 向 unstable.entries 中追加 entry 记录，其实现与 Storage.Append() 方法类似，
// 也会涉及截断的场景
func (u *unstable) truncateAndAppend(ents []pb.Entry) {
	// 获取第一条待追加的 entry 记录的索引值
	after := ents[0].Index
	switch {
	case after == u.offset+uint64(len(u.entries)):
		// after is the next index in the u.entries
		// directly append
		// 如果待追加的记录与 entries 中记录的正好连续，则直接向 entries 中追加
		u.entries = append(u.entries, ents...)
	case after <= u.offset:
		// 直接用待追加的 entry 记录替换当前的 entries 字段并更新 offset
		u.logger.Infof("replace the unstable entries from index %d", after)
		// The log is being truncated to before our current offset
		// portion, so set the offset and replace the entries
		u.offset = after
		u.entries = ents
	default:
		// truncate to after and copy to u.entries
		// then append
		// after 在 offset ~ last 之间，则 after ~ last 之间的 entry 记录冲突
		// 这里会将 offset ~ after 之间的记录保留，抛弃 after 之后的记录
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
