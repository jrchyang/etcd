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

import (
	"fmt"
	"log"

	pb "go.etcd.io/etcd/raft/v3/raftpb"
)

type raftLog struct {
	// storage contains all stable entries since the last snapshot.
	// 存储了快照数据及该快照之后的 Entry 记录
	storage Storage

	// unstable contains all unstable entries and snapshot.
	// they will be saved into storage.
	// 用于存储未写入 Storage 的快照数据及 Entry 记录
	unstable unstable

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	// 已提交的位置，即已提交的 Entry 记录中最大的索引值
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	// 已应用的位置，即已应用的 Entry 记录中最大的索引值，始终满足 applied <= committed
	applied uint64

	logger Logger

	// maxNextEntsSize is the maximum number aggregate byte size of the messages
	// returned from calls to nextEnts.
	maxNextEntsSize uint64
}

// newLog returns log using the given storage and default options. It
// recovers the log to the state that it just commits and applies the
// latest snapshot.
func newLog(storage Storage, logger Logger) *raftLog {
	return newLogWithSize(storage, logger, noLimit)
}

// newLogWithSize returns a log using the given storage and max
// message size.
/*
 *                  | <--- MemoryStorage.ents ---> |
 * |--------------| | FirstIndex         LastIndex |
 * |   snapshot   | |                              |
 * |--------------| |******************************| <---  unstable.ents  ---> |
 *                  |******************************| offset                    |
 *                  |******************************|                           |
 *                  |                              |###########################|
 *                  | committed                    |###########################|
 *                  | applied                      |###########################|
 *
 *                                          此时 unstable.snapshot 和 ents 为空
 */
func newLogWithSize(storage Storage, logger Logger, maxNextEntsSize uint64) *raftLog {
	if storage == nil {
		log.Panic("storage must not be nil")
	}
	// 创建 raftLog 实例并初始化 storage
	log := &raftLog{
		storage:         storage,
		logger:          logger,
		maxNextEntsSize: maxNextEntsSize,
	}
	// 获取 Storage 中的第一条和最后一条 Entry 的索引
	firstIndex, err := storage.FirstIndex()
	if err != nil {
		panic(err) // TODO(bdarnell)
	}
	lastIndex, err := storage.LastIndex()
	if err != nil {
		panic(err) // TODO(bdarnell)
	}
	// 初始化 unstable.offset
	log.unstable.offset = lastIndex + 1
	log.unstable.logger = logger
	// Initialize our committed and applied pointers to the time of the last compaction.
	// 初始化 committed 和 applied 字段
	log.committed = firstIndex - 1
	log.applied = firstIndex - 1

	return log
}

func (l *raftLog) String() string {
	return fmt.Sprintf("committed=%d, applied=%d, unstable.offset=%d, len(unstable.Entries)=%d", l.committed, l.applied, l.unstable.offset, len(l.unstable.entries))
}

// maybeAppend returns (0, false) if the entries cannot be appended. Otherwise,
// it returns (last index of new entries, true).
func (l *raftLog) maybeAppend(index, logTerm, committed uint64, ents ...pb.Entry) (lastnewi uint64, ok bool) {
	// 检查 Leader 节点认为的本节点的最后一条 Entry 是否存在
	// matchTerm 会根据 index 进行查找，而不是仅检查最后一条 entry，
	// 可以起到相同的作用，同时在 if 内，findConflict 会进行进一步检查
	if l.matchTerm(index, logTerm) {
		// 进入到这里表明在 raftLog 中，索引 index 对应的 entry 的 term == logTerm
		// 可以追加新的 entries

		// 最后一条 entry 对应的索引
		lastnewi = index + uint64(len(ents))
		ci := l.findConflict(ents)
		switch {
		// 已全部处理
		case ci == 0:
		// ci 不等于 0 的话分为三种情况：
		// 1. ents 与本地日志不存在冲突，可以直接复制
		// 2. ents 与本地日志存在冲突，但冲突部分尚未提交，可以直接覆盖
		// 3. ents 与本地日志存在冲突，且冲突部分已经提交（ci <= l.committed），这种错误理论上不应该出现，无法处理直接 panic
		case ci <= l.committed:
			l.logger.Panicf("entry %d conflict with committed entry [committed(%d)]", ci, l.committed)
		// 处理可以复制的情况
		default:
			// index 是 ents 中第一条记录的前一条的索引
			// +1 表示 ents 中第一条记录的索引
			offset := index + 1
			// ci-offset 表示无效的个数
			// 将 ents 记录到 unstable 中
			// 接口入口处的 matchTerm 保证 ents 与本地日志索引连续
			// ents 自身也保证 index 连续，所以可以直接追加
			l.append(ents[ci-offset:]...)
		}
		// lastnewi 表示已经收到的日志的索引
		// committed 表示 Leader 节点已经应用的日志
		// 取最小值即可
		// commitTo 接口内部会检查 raftLog 的 commit 是否出现倒退的情况
		l.commitTo(min(committed, lastnewi))
		return lastnewi, true
	}
	// 没有匹配到 index 和 term 时，直接返回错误
	return 0, false
}

func (l *raftLog) append(ents ...pb.Entry) uint64 {
	if len(ents) == 0 {
		return l.lastIndex()
	}
	// after 是本批次 ents 的第一条 entry 的前一条 entry 的索引
	// after < l.committed 表示 ents 记录已经处理了，走到这里表示出现了无法恢复的异常
	// 所以 panic
	if after := ents[0].Index - 1; after < l.committed {
		l.logger.Panicf("after(%d) is out of range [committed(%d)]", after, l.committed)
	}
	// 先加入到 unstable 中
	l.unstable.truncateAndAppend(ents)
	return l.lastIndex()
}

// findConflict finds the index of the conflict.
// It returns the first pair of conflicting entries between the existing
// entries and the given entries, if there are any.
// If there is no conflicting entries, and the existing entries contains
// all the given entries, zero will be returned.
// If there is no conflicting entries, but the given entries contains new
// entries, the index of the first new entry will be returned.
// An entry is considered to be conflicting if it has the same index but
// a different term.
// The index of the given entries MUST be continuously increasing.
//
// 0 - 已全部存在
// index - 存在冲突 或 raftLog 中不存在
func (l *raftLog) findConflict(ents []pb.Entry) uint64 {
	// 遍历要追加的日志
	for _, ne := range ents {
		// 查找要追加的 entry 的 index 和 term 在 raftLog 中是否存在
		if !l.matchTerm(ne.Index, ne.Term) { // 有 Index 不存在或 Term 不匹配的了
			// 如果满足条件，说明本地日志与 Leader 日志之间存在 Index 相同
			// 但 Term 不同的 entry，即存在冲突
			if ne.Index <= l.lastIndex() {
				l.logger.Infof("found conflict at index %d [existing term: %d, conflicting term: %d]",
					ne.Index, l.zeroTermOnErrCompacted(l.term(ne.Index)), ne.Term)
			}
			return ne.Index
		}
	}
	// 全部存在，则返回 0，表明全部日志均存在
	return 0
}

// findConflictByTerm takes an (index, term) pair (indicating a conflicting log
// entry on a leader/follower during an append) and finds the largest index in
// log l with a term <= `term` and an index <= `index`. If no such index exists
// in the log, the log's first index is returned.
//
// The index provided MUST be equal to or less than l.lastIndex(). Invalid
// inputs log a warning and the input index is returned.
func (l *raftLog) findConflictByTerm(index uint64, term uint64) uint64 {
	if li := l.lastIndex(); index > li {
		// NB: such calls should not exist, but since there is a straightfoward
		// way to recover, do it.
		//
		// It is tempting to also check something about the first index, but
		// there is odd behavior with peers that have no log, in which case
		// lastIndex will return zero and firstIndex will return one, which
		// leads to calls with an index of zero into this method.
		l.logger.Warningf("index(%d) is out of range [0, lastIndex(%d)] in findConflictByTerm",
			index, li)
		return index
	}
	// 从后往前查找 Term 小于等于指定任期的 entry，即可复制日志的位置
	for {
		logTerm, err := l.term(index)
		if logTerm <= term || err != nil {
			break
		}
		index--
	}
	return index
}

func (l *raftLog) unstableEntries() []pb.Entry {
	if len(l.unstable.entries) == 0 {
		return nil
	}
	return l.unstable.entries
}

// nextEnts returns all the available entries for execution.
// If applied is smaller than the index of snapshot, it returns all committed
// entries after the index of snapshot.
func (l *raftLog) nextEnts() (ents []pb.Entry) {
	// 获取当前已经应用记录的位置
	off := max(l.applied+1, l.firstIndex())
	// 是否存在已提交但尚未应用的 Entry 记录
	if l.committed+1 > off {
		// 获取全部已提交但尚未应用的 Entry 记录
		ents, err := l.slice(off, l.committed+1, l.maxNextEntsSize)
		if err != nil {
			l.logger.Panicf("unexpected error when getting unapplied entries (%v)", err)
		}
		return ents
	}
	// 已全部应用
	return nil
}

// hasNextEnts returns if there is any available entries for execution. This
// is a fast check without heavy raftLog.slice() in raftLog.nextEnts().
func (l *raftLog) hasNextEnts() bool {
	// 获取当前已经应用记录的位置
	off := max(l.applied+1, l.firstIndex())
	// 是否存在已提交但尚未应用的 Entry 记录
	return l.committed+1 > off
}

// hasPendingSnapshot returns if there is pending snapshot waiting for applying.
func (l *raftLog) hasPendingSnapshot() bool {
	return l.unstable.snapshot != nil && !IsEmptySnap(*l.unstable.snapshot)
}

func (l *raftLog) snapshot() (pb.Snapshot, error) {
	if l.unstable.snapshot != nil {
		return *l.unstable.snapshot, nil
	}
	return l.storage.Snapshot()
}

// 该方法返回已经在快照中的最后一条 Entry 的下一条 Entry 的索引
func (l *raftLog) firstIndex() uint64 {
	if i, ok := l.unstable.maybeFirstIndex(); ok {
		return i
	}
	index, err := l.storage.FirstIndex()
	if err != nil {
		panic(err) // TODO(bdarnell)
	}
	return index
}

func (l *raftLog) lastIndex() uint64 {
	if i, ok := l.unstable.maybeLastIndex(); ok {
		return i
	}
	i, err := l.storage.LastIndex()
	if err != nil {
		panic(err) // TODO(bdarnell)
	}
	return i
}

func (l *raftLog) commitTo(tocommit uint64) {
	// never decrease commit
	if l.committed < tocommit {
		// 不能出现空洞
		if l.lastIndex() < tocommit {
			l.logger.Panicf("tocommit(%d) is out of range [lastIndex(%d)]. Was the raft log corrupted, truncated, or lost?", tocommit, l.lastIndex())
		}
		l.committed = tocommit
	}
}

func (l *raftLog) appliedTo(i uint64) {
	if i == 0 {
		return
	}
	if l.committed < i || i < l.applied {
		l.logger.Panicf("applied(%d) is out of range [prevApplied(%d), committed(%d)]", i, l.applied, l.committed)
	}
	l.applied = i
}

func (l *raftLog) stableTo(i, t uint64) { l.unstable.stableTo(i, t) }

func (l *raftLog) stableSnapTo(i uint64) { l.unstable.stableSnapTo(i) }

func (l *raftLog) lastTerm() uint64 {
	t, err := l.term(l.lastIndex())
	if err != nil {
		l.logger.Panicf("unexpected error when getting the last term (%v)", err)
	}
	return t
}

// 查找索引 i 对应 entry 的 term 值
func (l *raftLog) term(i uint64) (uint64, error) {
	// the valid term range is [index of dummy entry, last index]
	dummyIndex := l.firstIndex() - 1
	// i 超出上下限了，返回 0
	if i < dummyIndex || i > l.lastIndex() {
		// TODO: return an error instead?
		return 0, nil
	}

	// 在 unsable 中找到则返回，找着则返回
	if t, ok := l.unstable.maybeTerm(i); ok {
		return t, nil
	}

	// 在 unstable 中没有找到，在 Storage 中尝试寻找
	// err == nil 说明已经找到，直接返回
	t, err := l.storage.Term(i)
	if err == nil {
		return t, nil
	}
	if err == ErrCompacted || err == ErrUnavailable {
		return 0, err
	}
	panic(err) // TODO(bdarnell)
}

func (l *raftLog) entries(i, maxsize uint64) ([]pb.Entry, error) {
	// 超出范围时直接返回空
	if i > l.lastIndex() {
		return nil, nil
	}
	// 返回指定范围的 Entry
	return l.slice(i, l.lastIndex()+1, maxsize)
}

// allEntries returns all entries in the log.
func (l *raftLog) allEntries() []pb.Entry {
	ents, err := l.entries(l.firstIndex(), noLimit)
	if err == nil {
		return ents
	}
	if err == ErrCompacted { // try again if there was a racing compaction
		return l.allEntries()
	}
	// TODO (xiangli): handle error?
	panic(err)
}

// isUpToDate determines if the given (lastIndex,term) log is more up-to-date
// by comparing the index and term of the last entries in the existing logs.
// If the logs have last entries with different terms, then the log with the
// later term is more up-to-date. If the logs end with the same term, then
// whichever log has the larger lastIndex is more up-to-date. If the logs are
// the same, the given log is up-to-date.
//
// 判断给定 lastindex 和 logTerm 的 raftLog 是否比我们更新
// 1. 如果对方的任期值更大则认为对方更新
// 2. 如果任期相同，但对象的索引大于我们最新的 Entry 的索引，则认为对象更新
func (l *raftLog) isUpToDate(lasti, term uint64) bool {
	return term > l.lastTerm() || (term == l.lastTerm() && lasti >= l.lastIndex())
}

func (l *raftLog) matchTerm(i, term uint64) bool {
	// 获取索引 i 对应的 term
	t, err := l.term(i)
	// 没找到，返回错误
	if err != nil {
		return false
	}
	// 检查是否相等
	return t == term
}

func (l *raftLog) maybeCommit(maxIndex, term uint64) bool {
	if maxIndex > l.committed && l.zeroTermOnErrCompacted(l.term(maxIndex)) == term {
		l.commitTo(maxIndex)
		return true
	}
	return false
}

func (l *raftLog) restore(s pb.Snapshot) {
	l.logger.Infof("log [%s] starts to restore snapshot [index: %d, term: %d]", l, s.Metadata.Index, s.Metadata.Term)
	l.committed = s.Metadata.Index
	l.unstable.restore(s)
}

// slice returns a slice of log entries from lo through hi-1, inclusive.
func (l *raftLog) slice(lo, hi, maxSize uint64) ([]pb.Entry, error) {
	// 边界检测
	err := l.mustCheckOutOfBounds(lo, hi)
	if err != nil {
		return nil, err
	}
	// 长度为零
	if lo == hi {
		return nil, nil
	}

	var ents []pb.Entry
	// 如果 lo < l.unstable.offset，则需要从 storage 中获取记录
	if lo < l.unstable.offset {
		storedEnts, err := l.storage.Entries(lo, min(hi, l.unstable.offset), maxSize)
		if err == ErrCompacted { // 如果 Entry 已经在快照中了，返回错误
			return nil, err
		} else if err == ErrUnavailable { // 如果 storage 中没有 Entry 则直接 panic
			l.logger.Panicf("entries[%d:%d) is unavailable from storage", lo, min(hi, l.unstable.offset))
		} else if err != nil {
			panic(err) // TODO(bdarnell)
		}

		// check if ents has reached the size limitation
		if uint64(len(storedEnts)) < min(hi, l.unstable.offset)-lo {
			return storedEnts, nil
		}

		ents = storedEnts
	}
	// 如果 hi > l.unstable.offset，则还需要从 unstable 中获取记录
	if hi > l.unstable.offset {
		unstable := l.unstable.slice(max(lo, l.unstable.offset), hi)
		if len(ents) > 0 {
			combined := make([]pb.Entry, len(ents)+len(unstable))
			n := copy(combined, ents)
			copy(combined[n:], unstable)
			ents = combined
		} else {
			ents = unstable
		}
	}
	return limitSize(ents, maxSize), nil
}

// l.firstIndex <= lo <= hi <= l.firstIndex + len(l.entries)
func (l *raftLog) mustCheckOutOfBounds(lo, hi uint64) error {
	if lo > hi {
		l.logger.Panicf("invalid slice %d > %d", lo, hi)
	}
	fi := l.firstIndex()
	if lo < fi {
		return ErrCompacted
	}

	length := l.lastIndex() + 1 - fi
	if hi > fi+length {
		l.logger.Panicf("slice[%d,%d) out of bound [%d,%d]", lo, hi, fi, l.lastIndex())
	}
	return nil
}

func (l *raftLog) zeroTermOnErrCompacted(t uint64, err error) uint64 {
	if err == nil {
		return t
	}
	if err == ErrCompacted {
		return 0
	}
	l.logger.Panicf("unexpected error (%v)", err)
	return 0
}
