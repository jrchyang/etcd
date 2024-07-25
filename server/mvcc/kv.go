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

package mvcc

import (
	"context"

	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/pkg/v3/traceutil"
	"go.etcd.io/etcd/server/v3/lease"
	"go.etcd.io/etcd/server/v3/mvcc/backend"
)

type RangeOptions struct {
	Limit int64 // 此次查询返回的键值对个数的上限
	Rev   int64 // 扫描内存索引时使用到的 main revision 部分的值
	Count bool  // 如果该值设置为 true，则只返回键值对个数，并不返回具体的键值对数据
}

type RangeResult struct {
	KVs   []mvccpb.KeyValue // 此次查询得到的键值对数据的集合
	Rev   int64             //
	Count int               // 此次查询返回的键值对个数
}

type ReadView interface {
	// FirstRev returns the first KV revision at the time of opening the txn.
	// After a compaction, the first revision increases to the compaction
	// revision.
	// 返回开启当前只读事务时的 revision 信息，与 Rev() 方法相同，但是当进行一次压缩
	// 操作之后，该方法的返回值会被更新成压缩时的 revision 信息，也就是压缩后的最小
	// revision
	FirstRev() int64

	// Rev returns the revision of the KV at the time of opening the txn.
	// 返回开启当前只读事务时的 revision 信息
	Rev() int64

	// Range gets the keys in the range at rangeRev.
	// The returned rev is the current revision of the KV when the operation is executed.
	// If rangeRev <=0, range gets the keys at currentRev.
	// If `end` is nil, the request returns the key.
	// If `end` is not nil and not empty, it gets the keys in range [key, range_end).
	// If `end` is not nil and empty, it gets the keys greater than or equal to key.
	// Limit limits the number of keys returned.
	// If the required rev is compacted, ErrCompacted will be returned.
	// 范围查询
	Range(ctx context.Context, key, end []byte, ro RangeOptions) (r *RangeResult, err error)
}

// TxnRead represents a read-only transaction with operations that will not
// block other read transactions.
type TxnRead interface {
	ReadView
	// End marks the transaction is complete and ready to commit.
	// 用来表示当前事务已完成并准备提交
	End()
}

type WriteView interface {
	// DeleteRange deletes the given range from the store.
	// A deleteRange increases the rev of the store if any key in the range exists.
	// The number of key deleted will be returned.
	// The returned rev is the current revision of the KV when the operation is executed.
	// It also generates one event for each key delete in the event history.
	// if the `end` is nil, deleteRange deletes the key.
	// if the `end` is not nil, deleteRange deletes the keys in range [key, range_end).
	// 范围删除
	DeleteRange(key, end []byte) (n, rev int64)

	// Put puts the given key, value into the store. Put also takes additional argument lease to
	// attach a lease to a key-value pair as meta-data. KV implementation does not validate the lease
	// id.
	// A put also increases the rev of the store, and generates one event in the event history.
	// The returned rev is the current revision of the KV when the operation is executed.
	// 添加指定的键值对
	Put(key, value []byte, lease lease.LeaseID) (rev int64)
}

// TxnWrite represents a transaction that can modify the store.
type TxnWrite interface {
	TxnRead
	WriteView
	// Changes gets the changes made since opening the write txn.
	// 返回自事务开启之后修改的键值对信息
	Changes() []mvccpb.KeyValue
}

// txnReadWrite coerces a read txn to a write, panicking on any write operation.
type txnReadWrite struct{ TxnRead }

func (trw *txnReadWrite) DeleteRange(key, end []byte) (n, rev int64) { panic("unexpected DeleteRange") }
func (trw *txnReadWrite) Put(key, value []byte, lease lease.LeaseID) (rev int64) {
	panic("unexpected Put")
}
func (trw *txnReadWrite) Changes() []mvccpb.KeyValue { return nil }

func NewReadOnlyTxnWrite(txn TxnRead) TxnWrite { return &txnReadWrite{txn} }

type ReadTxMode uint32

const (
	// Use ConcurrentReadTx and the txReadBuffer is copied
	ConcurrentReadTxMode = ReadTxMode(1)
	// Use backend ReadTx and txReadBuffer is not copied
	SharedBufReadTxMode = ReadTxMode(2)
)

type KV interface {
	ReadView
	WriteView

	// Read creates a read transaction.
	// 创建只读事务
	Read(mode ReadTxMode, trace *traceutil.Trace) TxnRead

	// Write creates a write transaction.
	// 创建读写事务
	Write(trace *traceutil.Trace) TxnWrite

	// HashStorage returns HashStorage interface for KV storage.
	HashStorage() HashStorage

	// Compact frees all superseded keys with revisions less than rev.
	// 对整个 KV 存储进行压缩
	Compact(trace *traceutil.Trace, rev int64) (<-chan struct{}, error)

	// Commit commits outstanding txns into the underlying backend.
	// 提交事务
	Commit()

	// Restore restores the KV store from a backend.
	// 从 BoltDB 中恢复内存索引
	Restore(b backend.Backend) error
	Close() error
}

// WatchableKV is a KV that can be watched.
type WatchableKV interface {
	KV
	Watchable
}

// Watchable is the interface that wraps the NewWatchStream function.
type Watchable interface {
	// NewWatchStream returns a WatchStream that can be used to
	// watch events happened or happening on the KV.
	NewWatchStream() WatchStream
}
