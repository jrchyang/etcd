// Copyright 2017 The etcd Authors
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

package backend

import (
	"bytes"
	"sort"
)

const bucketBufferInitialSize = 512

// txBuffer handles functionality shared between txWriteBuffer and txReadBuffer.
type txBuffer struct {
	buckets map[BucketID]*bucketBuffer
}

func (txb *txBuffer) reset() {
	// 遍历 Bucket
	for k, v := range txb.buckets {
		// 删除未使用的 bucketBuffer
		if v.used == 0 {
			// demote
			delete(txb.buckets, k)
		}
		// 清空使用过的 bucketBuffer
		v.used = 0
	}
}

// txWriteBuffer buffers writes of pending updates that have not yet committed.
type txWriteBuffer struct {
	txBuffer
	// Map from bucket ID into information whether this bucket is edited
	// sequentially (i.e. keys are growing monotonically).
	bucket2seq map[BucketID]bool
}

func (txw *txWriteBuffer) put(bucket Bucket, k, v []byte) {
	txw.bucket2seq[bucket.ID()] = false
	txw.putInternal(bucket, k, v)
}

func (txw *txWriteBuffer) putSeq(bucket Bucket, k, v []byte) {
	// TODO: Add (in tests?) verification whether k>b[len(b)]
	txw.putInternal(bucket, k, v)
}

func (txw *txWriteBuffer) putInternal(bucket Bucket, k, v []byte) {
	b, ok := txw.buckets[bucket.ID()] // 获取指定的 bucketBuffer
	if !ok {                          // 如果未查找到则创建对应的 bucketBuffer 实例并保存到 buckets 中
		b = newBucketBuffer()
		txw.buckets[bucket.ID()] = b
	}
	b.add(k, v)
}

func (txw *txWriteBuffer) reset() {
	txw.txBuffer.reset()
	for k := range txw.bucket2seq {
		v, ok := txw.buckets[k]
		if !ok {
			delete(txw.bucket2seq, k)
		} else if v.used == 0 {
			txw.bucket2seq[k] = true
		}
	}
}

func (txw *txWriteBuffer) writeback(txr *txReadBuffer) {
	// 遍历所有的 bucketBuffer
	for k, wb := range txw.buckets {
		// 从传入的 bucketBuffer 中查找指定的 bucketBuffer
		rb, ok := txr.buckets[k]
		// 如果 txReadBuffer 中不存在对应的 bucketBuffer，则直接使用
		// txWriteBuffer 中缓存的 bucketBuffer 实例
		if !ok {
			delete(txw.buckets, k)
			txr.buckets[k] = wb
			continue
		}
		if seq, ok := txw.bucket2seq[k]; ok && !seq && wb.used > 1 {
			// assume no duplicate keys
			// 如果当前 txWriteBuffer 中的键值对是非顺序写入的，则需要先进行排序
			sort.Sort(wb)
		}
		// 通过 bucketBuffer.merge() 方法，合并两个 bucketBuffer 实例并去重
		rb.merge(wb)
	}
	txw.reset() // 清空 txWriteBuffer
	// increase the buffer version
	txr.bufVersion++
}

// txReadBuffer accesses buffered updates.
type txReadBuffer struct {
	txBuffer
	// bufVersion is used to check if the buffer is modified recently
	bufVersion uint64
}

func (txr *txReadBuffer) Range(bucket Bucket, key, endKey []byte, limit int64) ([][]byte, [][]byte) {
	// 查询指定的 txBuffer 实例
	if b := txr.buckets[bucket.ID()]; b != nil {
		return b.Range(key, endKey, limit)
	}
	return nil, nil
}

func (txr *txReadBuffer) ForEach(bucket Bucket, visitor func(k, v []byte) error) error {
	if b := txr.buckets[bucket.ID()]; b != nil {
		return b.ForEach(visitor)
	}
	return nil
}

// unsafeCopy returns a copy of txReadBuffer, caller should acquire backend.readTx.RLock()
func (txr *txReadBuffer) unsafeCopy() txReadBuffer {
	txrCopy := txReadBuffer{
		txBuffer: txBuffer{
			buckets: make(map[BucketID]*bucketBuffer, len(txr.txBuffer.buckets)),
		},
		bufVersion: 0,
	}
	for bucketName, bucket := range txr.txBuffer.buckets {
		txrCopy.txBuffer.buckets[bucketName] = bucket.Copy()
	}
	return txrCopy
}

type kv struct {
	key []byte
	val []byte
}

// bucketBuffer buffers key-value pairs that are pending commit.
type bucketBuffer struct {
	// 每个元素都表示一个键值对，kv.key 和 kv.value 都是 []byte 类型
	// 在初始化时，该切片的默认大小是 512
	buf []kv
	// used tracks number of elements in use so buf can be reused without reallocation.
	// 该字段记录 buf 中目前使用的下标位置
	used int
}

func newBucketBuffer() *bucketBuffer {
	return &bucketBuffer{buf: make([]kv, bucketBufferInitialSize), used: 0}
}

func (bb *bucketBuffer) Range(key, endKey []byte, limit int64) (keys [][]byte, vals [][]byte) {
	// 定义 key 的比较方式
	f := func(i int) bool { return bytes.Compare(bb.buf[i].key, key) >= 0 }
	// 查询 0~used 之间是否有指定的 key
	idx := sort.Search(bb.used, f)
	if idx < 0 {
		return nil, nil
	}
	// 没有指定的 endKey，则只返回 key 对应的键值对
	if len(endKey) == 0 {
		if bytes.Equal(key, bb.buf[idx].key) {
			keys = append(keys, bb.buf[idx].key)
			vals = append(vals, bb.buf[idx].val)
		}
		return keys, vals
	}
	// 如果指定了 endKey 则检测 endKey 的合法性
	if bytes.Compare(endKey, bb.buf[idx].key) <= 0 {
		return nil, nil
	}
	// 从前面查找到的 idx 位置开始遍历，直到遍历到 endKey 或是遍历的键值对个数达到 limit 上限为止
	for i := idx; i < bb.used && int64(len(keys)) < limit; i++ {
		if bytes.Compare(endKey, bb.buf[i].key) <= 0 {
			break
		}
		keys = append(keys, bb.buf[i].key)
		vals = append(vals, bb.buf[i].val)
	}
	// 返回全部符合条件的键值对
	return keys, vals
}

func (bb *bucketBuffer) ForEach(visitor func(k, v []byte) error) error {
	// 遍历 used 之前的所有全速
	for i := 0; i < bb.used; i++ {
		// 调用 visitor() 函数处理键值对
		if err := visitor(bb.buf[i].key, bb.buf[i].val); err != nil {
			return err
		}
	}
	return nil
}

func (bb *bucketBuffer) add(k, v []byte) {
	bb.buf[bb.used].key, bb.buf[bb.used].val = k, v // 添加键值对
	bb.used++                                       // 递增 used
	if bb.used == len(bb.buf) {                     // 当 buf 空间被用尽时对其进行扩容
		buf := make([]kv, (3*len(bb.buf))/2)
		copy(buf, bb.buf)
		bb.buf = buf
	}
}

// merge merges data from bbsrc into bb.
func (bb *bucketBuffer) merge(bbsrc *bucketBuffer) {
	// 将 bbsrc 中的键值对添加到当前 bucketBuffer 中
	for i := 0; i < bbsrc.used; i++ {
		bb.add(bbsrc.buf[i].key, bbsrc.buf[i].val)
	}
	// 复制之前，如果当前 bucketBuffer 是空的，则复制完键值对之后直接返回
	if bb.used == bbsrc.used {
		return
	}
	// 复制之前，如果当前 bucketBuffer 不是空的，则需要判断复制之后，是否需要进行排序
	if bytes.Compare(bb.buf[(bb.used-bbsrc.used)-1].key, bbsrc.buf[0].key) < 0 {
		return
	}

	// 如果需要排序，则调用该方法对 bucketBuffer 进行排序
	// 注意，该函数是稳定排序，即相等键值对的相对位置在排序之后不会改变
	sort.Stable(bb)

	// remove duplicates, using only newest update
	// 删除重复的 key，使用 key 的最新的值
	widx := 0
	for ridx := 1; ridx < bb.used; ridx++ {
		if !bytes.Equal(bb.buf[ridx].key, bb.buf[widx].key) {
			widx++
		}
		bb.buf[widx] = bb.buf[ridx] // 新添加的键值对覆盖原有的键值对
	}
	bb.used = widx + 1
}

func (bb *bucketBuffer) Len() int { return bb.used }
func (bb *bucketBuffer) Less(i, j int) bool {
	return bytes.Compare(bb.buf[i].key, bb.buf[j].key) < 0
}
func (bb *bucketBuffer) Swap(i, j int) { bb.buf[i], bb.buf[j] = bb.buf[j], bb.buf[i] }

func (bb *bucketBuffer) Copy() *bucketBuffer {
	bbCopy := bucketBuffer{
		buf:  make([]kv, len(bb.buf)),
		used: bb.used,
	}
	copy(bbCopy.buf, bb.buf)
	return &bbCopy
}
