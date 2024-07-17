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

package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"

	"go.etcd.io/etcd/client/pkg/v3/fileutil"
	"go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/etcdserver/api/rafthttp"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	stats "go.etcd.io/etcd/server/v3/etcdserver/api/v2stats"
	"go.etcd.io/etcd/server/v3/wal"
	"go.etcd.io/etcd/server/v3/wal/walpb"

	"go.uber.org/zap"
)

type commit struct {
	data       []string
	applyDoneC chan<- struct{}
}

// A key-value stream backed by raft
// 主要功能：
//   - 将客户端发来的请求传递给底层 etcd-raft 组件中进行处理
//   - 从 node.readyc 通道中读取 Ready 实例，并处理其中封装的数据
//   - 管理 WAL 日志文件
//   - 管理快照数据
//   - 管理逻辑时钟
//   - 将 etcd-raft 模块返回的待发送消息通过网络组件发送到指定的节点
type raftNode struct {
	// 在 raftexample 示例中，HTTP PUT 请求表示添加键值对数据，当收到 HTTP PUT
	// 请求时，httpKVAPI 会将请求中的键值信息通过该通道传递给 raftNode 实例进行处理
	proposeC <-chan string // proposed messages (k,v)
	// 在 raftexample 实例中，HTTP POST 请求表示集群节点修改的请求，当收到 POST
	// 请求时，httpKVAPI 会通过 confChangeC 通道将修改的节点 ID 传递给
	// raftNode 实例进行处理
	confChangeC <-chan raftpb.ConfChange // proposed cluster config changes
	// 在创建 raftNode 实例之后（raftNode 实例的创建过程是在 newRaftNode() 函数
	// 中完成的）会返回 commitC、errorC、snapshotterReady 三个通道。raftNode
	// 会将 etcd-raft 模块返回的待应用 Entry 记录（封装在 Ready 实例中）写入
	// commitC 通道，另一方面，kvstore 会从 commitC 通道中读取这些待应用的 Entry
	// 记录并保存其中的键值对信息
	commitC chan<- *commit // entries committed to log (k,v)
	// 当 etcd-raft 模块关闭或是出现异常的时候，会通过 errorC 通道将信息通知上层模块
	errorC chan<- error // errors from raft session

	// 当前节点 ID
	id int // client ID for raft session
	// 当前集群中所有节点的地址，当前节点会通过该字段中保存的地址向集群中其他节点发送消息
	peers []string // raft peer URLs
	// 当前节点是否为后续加入到一个集群的节点
	join bool // node is joining an existing cluster
	// 存放 WAL 日志文件的目录
	waldir string // path to WAL directory
	// 存放快照文件的目录
	snapdir string // path to snapshot directory
	// 用于获取快照数据的函数，在 raftexample 示例中，该函数会调用 kvstore.getSnapshot()
	// 方法获取 kvstore.kvStore 字段的数据
	getSnapshot func() ([]byte, error)

	// 用于记录当前的集群状态，该状态就是从 node.confstatec 通道中获取的
	confState raftpb.ConfState
	// 保存当前快照的相关元数据，即快照所包含的最后一条 Entry 记录的索引值
	snapshotIndex uint64
	// 保存上层模块已应用的位置，即已应用的最后一条 Entry 记录的索引值
	appliedIndex uint64

	// raft backing for the commit/error channel
	// 即前面介绍的 etcd-raft 模块中的 node 实例，它实现了 Node 接口，并将 etcd-raft
	// 模块的 API 接口暴露给了上层接口
	node raft.Node
	// 在 raftexample 示例中，该 MemoryStorage 实例与底层 raftLog.storage 字段
	// 指向了同一个实例
	raftStorage *raft.MemoryStorage
	// 负责 WAL 日志的管理。当节点收到一条 Entry 记录时，首先会将其保存到 raftLog.unstable
	// 中，之后会将其封装到 Ready 实例中并交给上层模块发送给集群中的其他节点，并完成持久化。
	// 在 raftexample 实例中，Entry 记录的持久化是将其写入 raftLog.storage 中。
	// 在持久化之前，Entry 记录还会被写入 WAL 日志文件中，这样就可以保证这些 Entry
	// 记录不会丢失。WAL 日志文件是顺序写入的，所以其写入性能不会影响节点的整体性能
	wal *wal.WAL

	// 负责管理 etcd-raft 模块产生的快照数据，etcd-raft 模块并没有完成快照数据的管理，
	// 而是将其独立成一个单独的模块
	snapshotter *snap.Snapshotter
	// 主要用于初始化的过程中监听 snapshotter 实例是否创建完成
	snapshotterReady chan *snap.Snapshotter // signals when snapshotter is ready

	// 两次生成快照之间间隔的 Entry 记录数，即当前节点每处理一定数量的 Entry 记录，
	// 就要触发一次快照的创建。每次生成快照时，即可释放掉一定量的 WAL 日志及 raftLog
	// 中保存的 Entry 记录，从而避免大量 Entry 记录带来的内存压力及大量的 WAL 日志
	// 文件带来的磁盘压力；另外，定期创建快照也能减少节点重启时回放 WAL 日志的数量，
	// 加速了启动时间
	snapCount uint64
	transport *rafthttp.Transport
	stopc     chan struct{} // signals proposal channel closed

	// 以下两个通道相互协作，完成当前节点的关闭工作，两者的工作方式与前面介绍的
	// node.done 和 node.stop 的工作方式类似
	httpstopc chan struct{} // signals http server to shutdown
	httpdonec chan struct{} // signals http server shutdown complete

	logger *zap.Logger
}

var defaultSnapshotCount uint64 = 10000

// newRaftNode initiates a raft instance and returns a committed log entry
// channel and error channel. Proposals for log updates are sent over the
// provided the proposal channel. All log entries are replayed over the
// commit channel, followed by a nil message (to indicate the channel is
// current), then new log entries. To shutdown, close proposeC and read errorC.
func newRaftNode(id int, peers []string, join bool, getSnapshot func() ([]byte, error), proposeC <-chan string,
	confChangeC <-chan raftpb.ConfChange) (<-chan *commit, <-chan error, <-chan *snap.Snapshotter) {

	// 创建 commitC 和 errorC 通道
	commitC := make(chan *commit)
	errorC := make(chan error)

	rc := &raftNode{
		proposeC:    proposeC,
		confChangeC: confChangeC,
		commitC:     commitC,
		errorC:      errorC,
		id:          id,
		peers:       peers,
		join:        join,
		waldir:      fmt.Sprintf("raftexample-%d", id),
		snapdir:     fmt.Sprintf("raftexample-%d-snap", id),
		getSnapshot: getSnapshot,
		snapCount:   defaultSnapshotCount,
		stopc:       make(chan struct{}),
		httpstopc:   make(chan struct{}),
		httpdonec:   make(chan struct{}),

		logger: zap.NewExample(),

		snapshotterReady: make(chan *snap.Snapshotter, 1),
		// rest of structure populated after WAL replay
		// 其余字段在 WAL 日志回放完成之后才会初始化
	}
	// 单独启动一个 goroutine 执行 startRaft() 方法，在该方法中完成剩余初始化操作
	go rc.startRaft()
	return commitC, errorC, rc.snapshotterReady
}

func (rc *raftNode) saveSnap(snap raftpb.Snapshot) error {
	// 根据快照元数据创建 walpb.Snapshot 实例
	walSnap := walpb.Snapshot{
		Index:     snap.Metadata.Index,
		Term:      snap.Metadata.Term,
		ConfState: &snap.Metadata.ConfState,
	}
	// save the snapshot file before writing the snapshot to the wal.
	// This makes it possible for the snapshot file to become orphaned, but prevents
	// a WAL snapshot entry from having no corresponding snapshot file.
	// 将新快照数据写入快照文件中
	if err := rc.snapshotter.SaveSnap(snap); err != nil {
		return err
	}
	// WAL 会将上述快照的元数据信息封装成一条日志记录
	if err := rc.wal.SaveSnapshot(walSnap); err != nil {
		return err
	}
	// 根据快照的元数据信息，释放一些无用的 WAL 日志文件的句柄
	return rc.wal.ReleaseLockTo(snap.Metadata.Index)
}

func (rc *raftNode) entriesToApply(ents []raftpb.Entry) (nents []raftpb.Entry) {
	if len(ents) == 0 {
		return ents
	}
	firstIdx := ents[0].Index
	// 检查 firstIdx 是否合法
	if firstIdx > rc.appliedIndex+1 {
		log.Fatalf("first index of committed entry[%d] should <= progress.appliedIndex[%d]+1", firstIdx, rc.appliedIndex)
	}
	// 过滤掉已应用的 Entry 记录
	if rc.appliedIndex-firstIdx+1 < uint64(len(ents)) {
		nents = ents[rc.appliedIndex-firstIdx+1:]
	}
	return nents
}

// publishEntries writes committed log entries to commit channel and returns
// whether all entries could be published.
func (rc *raftNode) publishEntries(ents []raftpb.Entry) (<-chan struct{}, bool) {
	if len(ents) == 0 {
		return nil, true
	}

	data := make([]string, 0, len(ents))
	for i := range ents {
		switch ents[i].Type {
		case raftpb.EntryNormal:
			if len(ents[i].Data) == 0 {
				// ignore empty messages
				break
			}
			s := string(ents[i].Data)
			data = append(data, s)
		case raftpb.EntryConfChange:
			// 将 EntryConfChange 类型的记录封装成 ConfChange
			var cc raftpb.ConfChange
			cc.Unmarshal(ents[i].Data)
			// 将 ConfChange 实例传入底层的 etcd-raft 组件
			rc.confState = *rc.node.ApplyConfChange(cc)

			// 除了 etcd-raft 组件中需要创建或删除对应的 Progress 实例，
			// 网络层也需要做出相应的调整，即添加或删除相应的 Peer 实例
			switch cc.Type {
			case raftpb.ConfChangeAddNode:
				if len(cc.Context) > 0 {
					rc.transport.AddPeer(types.ID(cc.NodeID), []string{string(cc.Context)})
				}
			case raftpb.ConfChangeRemoveNode:
				if cc.NodeID == uint64(rc.id) {
					log.Println("I've been removed from the cluster! Shutting down.")
					return nil, false
				}
				rc.transport.RemovePeer(types.ID(cc.NodeID))
			}
		}
	}

	var applyDoneC chan struct{}

	if len(data) > 0 {
		applyDoneC = make(chan struct{}, 1)
		select {
		// 将数据写入 commitC 通道，kvstore 会从其中读取并记录相应的 kv 值
		case rc.commitC <- &commit{data, applyDoneC}:
		case <-rc.stopc:
			return nil, false
		}
	}

	// after commit, update appliedIndex
	// 处理完成之后，更新 raftNode 记录的已应用位置，该值在过滤已应用的 entriesToApply()
	// 方法及后面即将介绍的 maybeTriggerSnapshot() 方法中都有使用
	rc.appliedIndex = ents[len(ents)-1].Index

	return applyDoneC, true
}

func (rc *raftNode) loadSnapshot() *raftpb.Snapshot {
	if wal.Exist(rc.waldir) {
		walSnaps, err := wal.ValidSnapshotEntries(rc.logger, rc.waldir)
		if err != nil {
			log.Fatalf("raftexample: error listing snapshots (%v)", err)
		}
		snapshot, err := rc.snapshotter.LoadNewestAvailable(walSnaps)
		if err != nil && err != snap.ErrNoSnapshot {
			log.Fatalf("raftexample: error loading snapshot (%v)", err)
		}
		return snapshot
	}
	return &raftpb.Snapshot{}
}

// openWAL returns a WAL ready for reading.
func (rc *raftNode) openWAL(snapshot *raftpb.Snapshot) *wal.WAL {
	if !wal.Exist(rc.waldir) {
		if err := os.Mkdir(rc.waldir, 0750); err != nil {
			log.Fatalf("raftexample: cannot create dir for wal (%v)", err)
		}

		w, err := wal.Create(zap.NewExample(), rc.waldir, nil)
		if err != nil {
			log.Fatalf("raftexample: create wal error (%v)", err)
		}
		w.Close()
	}

	// walsnap.Snapshot 只包含了快照元数据中的 Term 值和索引值，并不包含真正的快照数据
	walsnap := walpb.Snapshot{}
	if snapshot != nil {
		walsnap.Index, walsnap.Term = snapshot.Metadata.Index, snapshot.Metadata.Term
	}
	log.Printf("loading WAL at term %d and index %d", walsnap.Term, walsnap.Index)
	w, err := wal.Open(zap.NewExample(), rc.waldir, walsnap)
	if err != nil {
		log.Fatalf("raftexample: error loading wal (%v)", err)
	}

	return w
}

// replayWAL replays WAL entries into the raft instance.
func (rc *raftNode) replayWAL() *wal.WAL {
	log.Printf("replaying WAL of member %d", rc.id)
	// 读取快照文件
	snapshot := rc.loadSnapshot()
	// 根据读取到的 SnapShot 实例的元数据创建 WAL 实例
	w := rc.openWAL(snapshot)
	// 读取快照数据之后的全部 WAL 日志数据，并获取状态信息
	_, st, ents, err := w.ReadAll()
	if err != nil {
		log.Fatalf("raftexample: failed to read WAL (%v)", err)
	}
	// 创建 MemoryStorage 实例
	rc.raftStorage = raft.NewMemoryStorage()
	if snapshot != nil {
		// 将快照数据加载到 MemoryStorage 中
		rc.raftStorage.ApplySnapshot(*snapshot)
	}
	// 将读取 WAL 日志之后得到的 HardState 加载到 MemoryStorage 中
	rc.raftStorage.SetHardState(st)

	// append to storage so raft starts at the right place in log
	// 将读取 WAL 日志得到的 Entry 记录加载到 MemoryStorage 中
	rc.raftStorage.Append(ents)

	return w
}

func (rc *raftNode) writeError(err error) {
	rc.stopHTTP()
	close(rc.commitC)
	rc.errorC <- err
	close(rc.errorC)
	rc.node.Stop()
}

//  1. 创建 Snapshotter，并将该实例返回给上层模块
//  2. 创建 WAL 实例，然后加载快照并回放 WAL 日志
//  3. 创建 raft.Config 实例，其中包含了启动 etcd-raft 模块的所有配置
//  4. 初始化底层 etcd-raft 模块，得到 node 实例
//  5. 创建 Transport 实例，该实例负责集群中各个节点之间的网络通信
//  6. 建立与集群中其他节点的网络连接
//  7. 启动网络组件，其中会监听当前节点与集群中其他节点之间的网络连接，并进行节点之间的消息读写
//  8. 启动两个后台 goroutine，它们的主要工作都是处理上层模块与底层 etcd-raft 模块的交互，
//     但处理的具体内容不同
func (rc *raftNode) startRaft() {
	// 检测 用于存放定期生成的快照数据 的目录是否存在，如果不存在则创建，创建失败则终止程序
	if !fileutil.Exist(rc.snapdir) {
		if err := os.Mkdir(rc.snapdir, 0750); err != nil {
			log.Fatalf("raftexample: cannot create dir for snapshot (%v)", err)
		}
	}
	// 步骤一：创建 Snapshotter 实例，并该 Snapshotter 实例通过 snapshotterReady
	// 通道返回给上层应用，Snapshotter 实例提供了读写快照文件的功能
	rc.snapshotter = snap.New(zap.NewExample(), rc.snapdir)

	// 步骤二：创建 WAL 实例，然后加载快照并回放 WAL 日志
	oldwal := wal.Exist(rc.waldir) // 检测 waldir 目录是否存在旧的 WAL 日志文件
	rc.wal = rc.replayWAL()        // 在 replayWAL() 方法中会先加载快照数据，然后重放 WAL

	// signal replay has finished
	rc.snapshotterReady <- rc.snapshotter

	// 步骤三：创建 raft.Config 实例
	rpeers := make([]raft.Peer, len(rc.peers))
	for i := range rpeers {
		rpeers[i] = raft.Peer{ID: uint64(i + 1)}
	}
	c := &raft.Config{
		ID:                        uint64(rc.id),
		ElectionTick:              10,             // 选举超时时间
		HeartbeatTick:             1,              // 心跳超时时间
		Storage:                   rc.raftStorage, // 持久化存储
		MaxSizePerMsg:             1024 * 1024,    // 每条消息的最大长度
		MaxInflightMsgs:           256,            // 已发送但尚未收到响应的消息个数上限
		MaxUncommittedEntriesSize: 1 << 30,
	}

	// 步骤四：初始化底层的 etcd-raft 模块
	// 这里会根据 WAL 日志的回放情况，判断当前节点是首次启动还是重新启动
	if oldwal || rc.join {
		rc.node = raft.RestartNode(c) // 重启
	} else {
		rc.node = raft.StartNode(c, rpeers) // 初次启动
	}

	// 步骤五：创建 Transport 实例并启动，它负责 raft 节点之间通信的网络服务
	rc.transport = &rafthttp.Transport{
		Logger:      rc.logger,
		ID:          types.ID(rc.id),
		ClusterID:   0x1000,
		Raft:        rc,
		ServerStats: stats.NewServerStats("", ""),
		LeaderStats: stats.NewLeaderStats(zap.NewExample(), strconv.Itoa(rc.id)),
		ErrorC:      make(chan error),
	}

	rc.transport.Start() // 启动网络服务相关的组件

	// 步骤六：建立与集群中其他各个节点的连接
	for i := range rc.peers {
		if i+1 != rc.id {
			rc.transport.AddPeer(types.ID(i+1), []string{rc.peers[i]})
		}
	}

	// 步骤七：启动一个 goroutine，其中会监听当前节点与集群中其他节点之间的网络连接
	go rc.serveRaft()
	// 步骤八：启动后台 goroutine 处理上层应用与底层 etcd-raft 模块的交互
	go rc.serveChannels()
}

// stop closes http, closes all channels, and stops raft.
func (rc *raftNode) stop() {
	rc.stopHTTP()
	close(rc.commitC)
	close(rc.errorC)
	rc.node.Stop()
}

func (rc *raftNode) stopHTTP() {
	rc.transport.Stop()
	close(rc.httpstopc)
	<-rc.httpdonec
}

func (rc *raftNode) publishSnapshot(snapshotToSave raftpb.Snapshot) {
	if raft.IsEmptySnap(snapshotToSave) {
		return
	}

	log.Printf("publishing snapshot at index %d", rc.snapshotIndex)
	defer log.Printf("finished publishing snapshot at index %d", rc.snapshotIndex)

	if snapshotToSave.Metadata.Index <= rc.appliedIndex {
		log.Fatalf("snapshot index [%d] should > progress.appliedIndex [%d]", snapshotToSave.Metadata.Index, rc.appliedIndex)
	}
	// 使用 commitC 通道通知上层应用加载新生成的快照数据
	rc.commitC <- nil // trigger kvstore to load snapshot
	// 记录新快照的元数据
	rc.confState = snapshotToSave.Metadata.ConfState
	rc.snapshotIndex = snapshotToSave.Metadata.Index
	rc.appliedIndex = snapshotToSave.Metadata.Index
}

var snapshotCatchUpEntriesN uint64 = 10000

func (rc *raftNode) maybeTriggerSnapshot(applyDoneC <-chan struct{}) {
	if rc.appliedIndex-rc.snapshotIndex <= rc.snapCount {
		return
	}

	// wait until all committed entries are applied (or server is closed)
	if applyDoneC != nil {
		select {
		case <-applyDoneC:
		case <-rc.stopc:
			return
		}
	}

	log.Printf("start snapshot [applied index: %d | last snapshot index: %d]", rc.appliedIndex, rc.snapshotIndex)
	// 获取快照数据，在 raftexample 实例中是获取 kvstore 中记录的全部键值对数据
	data, err := rc.getSnapshot()
	if err != nil {
		log.Panic(err)
	}
	// 创建 Snapshot 实例，同时也会将快照和元数据更新到 raftLog.MemoryStorage 中
	snap, err := rc.raftStorage.CreateSnapshot(rc.appliedIndex, &rc.confState, data)
	if err != nil {
		panic(err)
	}
	// 保存快照数据，raftNode.saveSnap() 方法在前面已经介绍
	if err := rc.saveSnap(snap); err != nil {
		panic(err)
	}

	// 计算压实的位置，压实之后，该位置之前的全部记录都会被抛弃
	compactIndex := uint64(1)
	if rc.appliedIndex > snapshotCatchUpEntriesN {
		compactIndex = rc.appliedIndex - snapshotCatchUpEntriesN
	}
	// 压实 raftLog 中保存的 Entry 记录
	if err := rc.raftStorage.Compact(compactIndex); err != nil {
		panic(err)
	}

	log.Printf("compacted log at index %d", compactIndex)
	rc.snapshotIndex = rc.appliedIndex
}

func (rc *raftNode) serveChannels() {
	// 在 replayWAL() 方法中读取了快照数据、WAL 日志等信息，并记录到了
	// raftNode.raftStorage 中，这里会用到这些数据

	// 获取快照数据和元数据
	snap, err := rc.raftStorage.Snapshot()
	if err != nil {
		panic(err)
	}
	rc.confState = snap.Metadata.ConfState
	rc.snapshotIndex = snap.Metadata.Index
	rc.appliedIndex = snap.Metadata.Index

	defer rc.wal.Close()

	// 创建一个每隔 100ms 触发一次的定时器，那么在逻辑上，即是 etcd-raft 组件的最小
	// 时间单位，该定时器每触发一次，则逻辑时钟推进一次
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	// send proposals over raft
	// 单独启动一个 goroutine 负责将 proposeC、confChangeC 通道上接收到的数据传递
	// 给 etcd-raft 组件进行处理
	go func() {
		confChangeCount := uint64(0)

		for rc.proposeC != nil && rc.confChangeC != nil {
			select {
			case prop, ok := <-rc.proposeC: // 收到上层应用通过 proposeC 通道传递过来的数据
				if !ok {
					// 如果发生异常，则将 raftNode.proposeC 字段置空，当前循环及整个 goroutine 都会结束
					rc.proposeC = nil
				} else {
					// blocks until accepted by raft state machine
					// 通过该方法将数据传入底层 etcd-raft 组件进行处理
					rc.node.Propose(context.TODO(), []byte(prop))
				}

			case cc, ok := <-rc.confChangeC: // 收到上层应用通过 confChangeC 通道传递过来的数据
				if !ok {
					// 如果发生异常，则将 raftNode.proposeC 字段置空，当前循环及整个 goroutine 都会结束
					rc.confChangeC = nil
				} else {
					// 统计集群变更请求的个数，并将其作为 ID
					confChangeCount++
					cc.ID = confChangeCount
					// 通过该方法将数据传入底层 etcd-raft 组件进行处理
					rc.node.ProposeConfChange(context.TODO(), cc)
				}
			}
		}
		// client closed channel; shutdown raft if not already
		close(rc.stopc)
	}()

	// event loop on raft state machine updates
	// 该循环主要负责处理底层 etcd-raft 组件返回的 Ready 数据
	for {
		select {
		case <-ticker.C:
			rc.node.Tick() // 上述 ticker 定时器触发一次，即会推进 etcd-raft 组件的逻辑时钟

		// store raft entries to wal, then publish over commit channel
		// 读取 node.readyc 通道，前面介绍 etcd-raft 组件时也提到，该通道是
		// etcd-raft 组件与上层应用交互的主要通道之一，其中传递的 Ready 实例
		// 也封装了很多信息
		case rd := <-rc.node.Ready():
			// 将当前 etcd-raft 组件的状态信息，以及待持久化的 Entry 记录
			// 先记录到 WAL 日志文件中，即使之后宕机，这些信息也可以在节点
			// 下次启动时，通过前面回放 WAL 日志的方式进行恢复
			rc.wal.Save(rd.HardState, rd.Entries)

			if !raft.IsEmptySnap(rd.Snapshot) { // 检测 etcd-raft 组件生成了新的快照数据
				rc.saveSnap(rd.Snapshot)                  // 将新的快照数据写入快照文件中
				rc.raftStorage.ApplySnapshot(rd.Snapshot) // 将新快照持久化到 raftStorage
				rc.publishSnapshot(rd.Snapshot)           // 通知上层应用加载新快照
			}

			// 将待持久化的 Entry 记录追加到 raftStorage 中完成持久化
			rc.raftStorage.Append(rd.Entries)
			// 将待发送的消息发送到指定节点
			rc.transport.Send(rd.Messages)
			// 将已提交、待应用的 Entry 记录应用到上层应用的状态机中
			applyDoneC, ok := rc.publishEntries(rc.entriesToApply(rd.CommittedEntries))
			if !ok {
				rc.stop()
				return
			}
			// 随着节点的运行，WAL 日志量和 raftLog.storage 中的 Entry 记录会不断增加，
			// 所以节点每处理 10000 条（默认值）Entry 记录，就会触发一次创建快照的过程，
			// 同时 WAL 会释放一些日志文件的句柄，raftLog.storage 也会压缩其保存的 Entry 记录
			rc.maybeTriggerSnapshot(applyDoneC)
			// 上层应用处理完该 Ready 实例，通知 etcd-raft 组件准备返回下一个 Ready 实例
			rc.node.Advance()

		case err := <-rc.transport.ErrorC: // 处理网络异常
			rc.writeError(err) // 关闭与集群中其他节点的网络连接
			return

		case <-rc.stopc: // 处理关闭命令
			rc.stop()
			return
		}
	}
}

func (rc *raftNode) serveRaft() {
	// 获取当前节点的 url 地址
	url, err := url.Parse(rc.peers[rc.id-1])
	if err != nil {
		log.Fatalf("raftexample: Failed parsing URL (%v)", err)
	}

	// 创建 stoppableListener 实例，stoppableListener 集成了 net.TCPListener
	// （当前也实现了 net.Listener 接口）接口，它会与 http.Server 配合实现对当前节点
	// 的 URL 地址进行监听
	ln, err := newStoppableListener(url.Host, rc.httpstopc)
	if err != nil {
		log.Fatalf("raftexample: Failed to listen rafthttp (%v)", err)
	}

	// 创建 http.Server 实例，它会通上面的 stoppableListener 实例监听当前节点的 URL 地址
	// stoppableListener.Accept() 方法监听到新连接到来时，会创建对应的 net.Conn 实例，http.Server
	// 会为每个连接创建单独的 goroutine 处理，每个请求都会由 http.Server.Handler 处理。这里的
	// Handler 是由 rafthttp.Transporter 创建的，另外需要了解的是 http.Server.Serve()
	// 方法会一直阻塞，直到 http.Server 关闭
	err = (&http.Server{Handler: rc.transport.Handler()}).Serve(ln)
	select {
	case <-rc.httpstopc:
	default:
		log.Fatalf("raftexample: Failed to serve rafthttp (%v)", err)
	}
	close(rc.httpdonec)
}

func (rc *raftNode) Process(ctx context.Context, m raftpb.Message) error {
	return rc.node.Step(ctx, m)
}
func (rc *raftNode) IsIDRemoved(id uint64) bool  { return false }
func (rc *raftNode) ReportUnreachable(id uint64) { rc.node.ReportUnreachable(id) }
func (rc *raftNode) ReportSnapshot(id uint64, status raft.SnapshotStatus) {
	rc.node.ReportSnapshot(id, status)
}
