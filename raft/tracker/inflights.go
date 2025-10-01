// Copyright 2019 The etcd Authors
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

package tracker

// Inflights limits the number of MsgApp (represented by the largest index
// contained within) sent to followers but not yet acknowledged by them. Callers
// use Full() to check whether more messages can be sent, call Add() whenever
// they are sending a new append, and release "quota" via FreeLE() whenever an
// ack is received.
type Inflights struct {
	// the starting index in the buffer
	// buffer 数组被当作一个环形数组使用，该字段记录 buffer 中第一条 MsgApp 消息的下标
	start int
	// number of inflights in the buffer
	// 当前 inflights 实例中记录的 MsgApp 消息个数
	count int
	// the size of the buffer
	// 当前 inflights 实例中能够记录 MsgApp 消息个数的上限
	size int
	// buffer contains the index of the last entry
	// inside one message.
	// 用来记录 MsgApp 消息相关信息的数组，其中记录的是 MsgApp 消息中最后一条 Entry 记录的索引值
	buffer []uint64
}

// NewInflights sets up an Inflights that allows up to 'size' inflight messages.
func NewInflights(size int) *Inflights {
	return &Inflights{
		size: size,
	}
}

// Clone returns an *Inflights that is identical to but shares no memory with
// the receiver.
func (in *Inflights) Clone() *Inflights {
	ins := *in
	ins.buffer = append([]uint64(nil), in.buffer...)
	return &ins
}

// Add notifies the Inflights that a new message with the given index is being
// dispatched. Full() must be called prior to Add() to verify that there is room
// for one more message, and consecutive calls to add Add() must provide a
// monotonic sequence of indexes.
func (in *Inflights) Add(inflight uint64) {
	// buffer 已满无法添加
	if in.Full() {
		panic("cannot add into a Full inflights")
	}
	// 计算新增消息的下标
	next := in.start + in.count
	size := in.size
	// 唤醒队列处理
	if next >= size {
		next -= size
	}
	// 初始化时的 buffer 数组比较短，随着使用会不断进行扩容，
	// 但其扩容的上限为 size
	if next >= len(in.buffer) {
		in.grow()
	}
	// 记录最后一条 entry 记录的索引值
	in.buffer[next] = inflight
	// 递增 count 字段
	in.count++
}

// grow the inflight buffer by doubling up to inflights.size. We grow on demand
// instead of preallocating to inflights.size to handle systems which have
// thousands of Raft groups per process.
func (in *Inflights) grow() {
	newSize := len(in.buffer) * 2
	if newSize == 0 {
		newSize = 1
	} else if newSize > in.size {
		newSize = in.size
	}
	newBuffer := make([]uint64, newSize)
	copy(newBuffer, in.buffer)
	in.buffer = newBuffer
}

// FreeLE frees the inflights smaller or equal to the given `to` flight.
func (in *Inflights) FreeLE(to uint64) {
	// 边界检测
	if in.count == 0 || to < in.buffer[in.start] {
		// out of the left side of the window
		return
	}

	idx := in.start
	var i int
	// 从 start 开始遍历
	for i = 0; i < in.count; i++ {
		// 查找第一个大于指定索引值的位置
		if to < in.buffer[idx] { // found the first large inflight
			break
		}

		// increase index and maybe rotate
		size := in.size
		// 因为是环形队列，如果 idx 越界，则从 0 开始继续遍历
		if idx++; idx >= size {
			idx -= size
		}
	}
	// free i inflights and set new start index
	// i 记录了此次释放消息的个数
	// 调整 count 和 start
	in.count -= i
	in.start = idx
	// 如果都释放完了，可以从 0 从头开始
	if in.count == 0 {
		// inflights is empty, reset the start index so that we don't grow the
		// buffer unnecessarily.
		in.start = 0
	}
}

// FreeFirstOne releases the first inflight. This is a no-op if nothing is
// inflight.
func (in *Inflights) FreeFirstOne() { in.FreeLE(in.buffer[in.start]) }

// Full returns true if no more messages can be sent at the moment.
func (in *Inflights) Full() bool {
	return in.count == in.size
}

// Count returns the number of inflight messages.
func (in *Inflights) Count() int { return in.count }

// reset frees all inflights.
func (in *Inflights) reset() {
	in.count = 0
	in.start = 0
}
