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

package wal

import (
	"encoding/binary"
	"hash"
	"io"
	"os"
	"sync"

	"go.etcd.io/etcd/pkg/v3/crc"
	"go.etcd.io/etcd/pkg/v3/ioutil"
	"go.etcd.io/etcd/server/v3/wal/walpb"
)

// walPageBytes is the alignment for flushing records to the backing Writer.
// It should be a multiple of the minimum sector size so that WAL can safely
// distinguish between torn writes and ordinary data corruption.
const walPageBytes = 8 * minSectorSize

type encoder struct {
	// 在进行读写文件的时候需要加锁同步
	mu sync.Mutex
	// PageWriter 是带有缓冲区的 Writer，在写入时，每写满一个 Page 大小的缓冲区，
	// 就会自动触发一次 Flush 操作，将数据同步刷新到磁盘上。每个 Page 的大小是
	// walPageBytes 常量指定的
	bw *ioutil.PageWriter

	crc hash.Hash32
	// 日志序列化之后，会暂存在该缓冲区，该缓冲区会被复用，这就防止了每次序列化创建
	// 缓冲区到来的开销
	buf []byte
	// 在写入一条日志记录时，该缓冲区用来暂存一个 Frame 的长度的数据
	// （Frame 由日志数据和填充数据沟通）
	uint64buf []byte
}

func newEncoder(w io.Writer, prevCrc uint32, pageOffset int) *encoder {
	return &encoder{
		bw:  ioutil.NewPageWriter(w, walPageBytes, pageOffset),
		crc: crc.New(prevCrc, crcTable),
		// 1MB buffer
		buf:       make([]byte, 1024*1024),
		uint64buf: make([]byte, 8),
	}
}

// newFileEncoder creates a new encoder with current file offset for the page writer.
func newFileEncoder(f *os.File, prevCrc uint32) (*encoder, error) {
	offset, err := f.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, err
	}
	return newEncoder(f, prevCrc, int(offset)), nil
}

func (e *encoder) encode(rec *walpb.Record) error {
	e.mu.Lock()         // 加锁同步
	defer e.mu.Unlock() // 函数结束后自动释放锁

	// 计算 crc 校验码
	e.crc.Write(rec.Data)
	rec.Crc = e.crc.Sum32()
	var (
		data []byte
		err  error
		n    int
	)

	if rec.Size() > len(e.buf) { // 如果日志记录太大无法复用 encoder.buf 这个缓冲区，则直接序列化
		data, err = rec.Marshal()
		if err != nil {
			return err
		}
	} else { // 复用 encoder.buf 这个缓冲区
		n, err = rec.MarshalTo(e.buf)
		if err != nil {
			return err
		}
		data = e.buf[:n]
	}

	// 计算序列化之后的数据长度，在 encodeFrameSize() 方法中会完成 8 字节对齐，
	// 这里将真正的数据和填充数据看做一个 Frame，返回值分别是整个 Frame 的长度
	// 以及其中填充数据的长度
	lenField, padBytes := encodeFrameSize(len(data))
	// 将 Frame 的长度序列化到 encoder.uint64buf 数组中，然后写入文件
	if err = writeUint64(e.bw, lenField, e.uint64buf); err != nil {
		return err
	}

	// 向 data 中写入填充字节
	if padBytes != 0 {
		data = append(data, make([]byte, padBytes)...)
	}
	// 将 data 中的序列化数据写入文件
	n, err = e.bw.Write(data)
	walWriteBytes.Add(float64(n))
	return err
}

func encodeFrameSize(dataBytes int) (lenField uint64, padBytes int) {
	lenField = uint64(dataBytes)
	// force 8 byte alignment so length never gets a torn write
	padBytes = (8 - (dataBytes % 8)) % 8
	if padBytes != 0 {
		lenField |= uint64(0x80|padBytes) << 56
	}
	return lenField, padBytes
}

func (e *encoder) flush() error {
	e.mu.Lock()
	n, err := e.bw.FlushN()
	e.mu.Unlock()
	walWriteBytes.Add(float64(n))
	return err
}

func writeUint64(w io.Writer, n uint64, buf []byte) error {
	// http://golang.org/src/encoding/binary/binary.go
	binary.LittleEndian.PutUint64(buf, n)
	nv, err := w.Write(buf)
	walWriteBytes.Add(float64(nv))
	return err
}
