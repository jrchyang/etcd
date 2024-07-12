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
	"context"
	"errors"

	pb "go.etcd.io/etcd/raft/v3/raftpb"
)

type SnapshotStatus int

const (
	SnapshotFinish  SnapshotStatus = 1
	SnapshotFailure SnapshotStatus = 2
)

var (
	emptyState = pb.HardState{}

	// ErrStopped is returned by methods on Nodes that have been stopped.
	ErrStopped = errors.New("raft: stopped")
)

// SoftState provides state that is useful for logging and debugging.
// The state is volatile and does not need to be persisted to the WAL.
type SoftState struct {
	Lead      uint64    // Leader 节点 ID must use atomic operations to access; keep 64-bit aligned.
	RaftState StateType // 当前节点的角色
}

func (a *SoftState) equal(b *SoftState) bool {
	return a.Lead == b.Lead && a.RaftState == b.RaftState
}

// Ready encapsulates the entries and messages that are ready to read,
// be saved to stable storage, committed or sent to other peers.
// All fields in Ready are read-only.
//
// Ready 实例是用来传递数据的，其全部字段都是只读的
type Ready struct {
	// The current volatile state of a Node.
	// SoftState will be nil if there is no update.
	// It is not required to consume or store SoftState.
	*SoftState

	// The current state of a Node to be saved to stable storage BEFORE
	// Messages are sent.
	// HardState will be equal to empty state if there is no update.
	pb.HardState

	// ReadStates can be used for node to serve linearizable read requests locally
	// when its applied index is greater than the index in ReadState.
	// Note that the readState will be returned when raft receives msgReadIndex.
	// The returned is only valid for the request that requested to read.
	//
	// 该字段记录了当前节点中等待处理的只读请求
	ReadStates []ReadState

	// Entries specifies entries to be saved to stable storage BEFORE
	// Messages are sent.
	//
	// 该字段中的 Entry 记录是从 unstable 中读取出来的，上层模块会将其保存到 Storage 中
	Entries []pb.Entry

	// Snapshot specifies the snapshot to be saved to stable storage.
	// 待持久化的快照数据，pb.Snapshot 中封装了快照数据及相关元数据
	Snapshot pb.Snapshot

	// CommittedEntries specifies entries to be committed to a
	// store/state-machine. These have previously been committed to stable
	// store.
	//
	// 已提交、待应用的 Entry 记录，这些 Entry 记录之前已经保存到了 Storage 中
	CommittedEntries []pb.Entry

	// Messages specifies outbound messages to be sent AFTER Entries are
	// committed to stable storage.
	// If it contains a MsgSnap message, the application MUST report back to raft
	// when the snapshot has been received or has failed by calling ReportSnapshot.
	Messages []pb.Message

	// MustSync indicates whether the HardState and Entries must be synchronously
	// written to disk or if an asynchronous write is permissible.
	MustSync bool
}

func isHardStateEqual(a, b pb.HardState) bool {
	return a.Term == b.Term && a.Vote == b.Vote && a.Commit == b.Commit
}

// IsEmptyHardState returns true if the given HardState is empty.
func IsEmptyHardState(st pb.HardState) bool {
	return isHardStateEqual(st, emptyState)
}

// IsEmptySnap returns true if the given Snapshot is empty.
func IsEmptySnap(sp pb.Snapshot) bool {
	return sp.Metadata.Index == 0
}

func (rd Ready) containsUpdates() bool {
	return rd.SoftState != nil ||
		!IsEmptyHardState(rd.HardState) || // 检测 raft 状态是否发生变化
		!IsEmptySnap(rd.Snapshot) || // 检测是否有新的快照数据
		len(rd.Entries) > 0 || // 检测是否有待持久化的 Entry 记录
		len(rd.CommittedEntries) > 0 || // 检测是否有待应用的 Entry 记录
		len(rd.Messages) > 0 || // 检测是否有待发送的消息
		len(rd.ReadStates) != 0 // 检测是否有处理的只读请求
}

// appliedCursor extracts from the Ready the highest index the client has
// applied (once the Ready is confirmed via Advance). If no information is
// contained in the Ready, returns zero.
func (rd Ready) appliedCursor() uint64 {
	if n := len(rd.CommittedEntries); n > 0 {
		return rd.CommittedEntries[n-1].Index
	}
	if index := rd.Snapshot.Metadata.Index; index > 0 {
		return index
	}
	return 0
}

// Node represents a node in a raft cluster.
type Node interface {
	// Tick increments the internal logical clock for the Node by a single tick. Election
	// timeouts and heartbeat timeouts are in units of ticks.
	//
	// 用来推进逻辑时钟的指针，从而推进 选举计时器 和 心跳计时器 两个计时器
	Tick()
	// Campaign causes the Node to transition to candidate state and start campaigning to become leader.
	//
	// 当选举计时器超时后，会调用该方法将当前节点切换成 Candidate 状态（或是 PreCandidate
	// 状态），底层就是通过发送 MsgHup 消息实现的
	Campaign(ctx context.Context) error
	// Propose proposes that data be appended to the log. Note that proposals can be lost without
	// notice, therefore it is user's job to ensure proposal retries.
	//
	// 接收到 Client 发来的写请求，Node 实例会调用 Propose() 方法进行处理，
	// 底层就是通过发送 MsgProp 消息实现的
	Propose(ctx context.Context, data []byte) error
	// ProposeConfChange proposes a configuration change. Like any proposal, the
	// configuration change may be dropped with or without an error being
	// returned. In particular, configuration changes are dropped unless the
	// leader has certainty that there is no prior unapplied configuration
	// change in its log.
	//
	// The method accepts either a pb.ConfChange (deprecated) or pb.ConfChangeV2
	// message. The latter allows arbitrary configuration changes via joint
	// consensus, notably including replacing a voter. Passing a ConfChangeV2
	// message is only allowed if all Nodes participating in the cluster run a
	// version of this library aware of the V2 API. See pb.ConfChangeV2 for
	// usage details and semantics.
	//
	// Client 除了会发送读写请求，还会发送修改集群配置的请求（例如：新增集群中的节点），
	// 这种请求 Node 实例会调用 ProposeConfChange() 方法进行处理，底层就是通过
	// MsgProp 消息实现的，只不过其中记录的 Entry 记录是 EntryConfChange 类型
	ProposeConfChange(ctx context.Context, cc pb.ConfChangeI) error

	// Step advances the state machine using the given message. ctx.Err() will be returned, if any.
	//
	// 当前节点收到其他节点的消息时，会通过该方法将消息交给底层封装的 raft 实例进行处理
	Step(ctx context.Context, msg pb.Message) error

	// Ready returns a channel that returns the current point-in-time state.
	// Users of the Node must call Advance after retrieving the state returned by Ready.
	//
	// NOTE: No committed entries from the next Ready may be applied until all committed entries
	// and snapshots from the previous one have finished.
	//
	// 该方法返回的是一个 Channel，通过该 Channel 返回的 Ready 实例中封装了底层 raft
	// 实例的相关状态数据，例如，需要发送到其他节点的消息、交给上层模块的 Entry 记录，等等。
	// 这是 etcd-raft 模块与上层模块交互的主要方式
	Ready() <-chan Ready

	// Advance notifies the Node that the application has saved progress up to the last Ready.
	// It prepares the node to return the next available Ready.
	//
	// The application should generally call Advance after it applies the entries in last Ready.
	//
	// However, as an optimization, the application may call Advance while it is applying the
	// commands. For example. when the last Ready contains a snapshot, the application might take
	// a long time to apply the snapshot data. To continue receiving Ready without blocking raft
	// progress, it can call Advance before finishing applying the last ready.
	//
	// 当上层模块处理完从上述 Channel 中返回的 Ready 实例之后，需要调用该方法通知底层
	// 的 etcd-raft 模块返回新的 Ready 实例
	Advance()
	// ApplyConfChange applies a config change (previously passed to
	// ProposeConfChange) to the node. This must be called whenever a config
	// change is observed in Ready.CommittedEntries, except when the app decides
	// to reject the configuration change (i.e. treats it as a noop instead), in
	// which case it must not be called.
	//
	// Returns an opaque non-nil ConfState protobuf which must be recorded in
	// snapshots.
	//
	// 在收到集群配置请求时，会通过调用该方法进行处理
	ApplyConfChange(cc pb.ConfChangeI) *pb.ConfState

	// TransferLeadership attempts to transfer leadership to the given transferee.
	// 该方法用于 Leader 节点的转移
	TransferLeadership(ctx context.Context, lead, transferee uint64)

	// ReadIndex request a read state. The read state will be set in the ready.
	// Read state has a read index. Once the application advances further than the read
	// index, any linearizable read requests issued before the read request can be
	// processed safely. The read state will have the same rctx attached.
	// Note that request can be lost without notice, therefore it is user's job
	// to ensure read index retries.
	//
	// 用于处理只读请求
	ReadIndex(ctx context.Context, rctx []byte) error

	// Status returns the current status of the raft state machine.
	// 返回当前节点的状态运行态
	Status() Status
	// ReportUnreachable reports the given node is not reachable for the last send.
	ReportUnreachable(id uint64)
	// ReportSnapshot reports the status of the sent snapshot. The id is the raft ID of the follower
	// who is meant to receive the snapshot, and the status is SnapshotFinish or SnapshotFailure.
	// Calling ReportSnapshot with SnapshotFinish is a no-op. But, any failure in applying a
	// snapshot (for e.g., while streaming it from leader to follower), should be reported to the
	// leader with SnapshotFailure. When leader sends a snapshot to a follower, it pauses any raft
	// log probes until the follower can apply the snapshot and advance its state. If the follower
	// can't do that, for e.g., due to a crash, it could end up in a limbo, never getting any
	// updates from the leader. Therefore, it is crucial that the application ensures that any
	// failure in snapshot sending is caught and reported back to the leader; so it can resume raft
	// log probing in the follower.
	// 用于通知底层的 raft 实例上次发送快照的结果
	ReportSnapshot(id uint64, status SnapshotStatus)
	// Stop performs any necessary termination of the Node.
	// 关闭当前节点
	Stop()
}

type Peer struct {
	ID      uint64
	Context []byte
}

// StartNode returns a new Node given configuration and a list of raft peers.
// It appends a ConfChangeAddNode entry for each given peer to the initial log.
//
// Peers must not be zero length; call RestartNode in that case.
//
// Peer 封装了节点 ID，peers 记录当前集群中全部节点的 ID
func StartNode(c *Config, peers []Peer) Node {
	if len(peers) == 0 {
		panic("no peers given; use RestartNode instead")
	}
	rn, err := NewRawNode(c) // 创建 raft 实例
	if err != nil {
		panic(err)
	}
	rn.Bootstrap(peers) // 切换为 Follower 状态

	n := newNode(rn) // 初始化 node 实例

	// 启动一个 goroutine，其中会根据底层 raft 的状态及上层模块传递的数据
	// 协调处理 node 中各种通道的数据
	go n.run()
	return &n
}

// RestartNode is similar to StartNode but does not take a list of peers.
// The current membership of the cluster will be restored from the Storage.
// If the caller has an existing state machine, pass in the last log index that
// has been applied to it; otherwise use zero.
func RestartNode(c *Config) Node {
	rn, err := NewRawNode(c)
	if err != nil {
		panic(err)
	}
	n := newNode(rn) // 创建 node 实例
	go n.run()
	return &n
}

type msgWithResult struct {
	m      pb.Message
	result chan error
}

// node is the canonical implementation of the Node interface
type node struct {
	propc      chan msgWithResult   // 该通道用于接收 MsgProp 类型的消息
	recvc      chan pb.Message      // 除 MsgProp 外的其他类型的消息都是由该通道接收的
	confc      chan pb.ConfChangeV2 // 当节点收到 EntryConfChange 类型的 Entry 记录时，会转换成 ConfChange，并写入该通道中等待处理
	confstatec chan pb.ConfState    // 在 ConfState 中封装了当前集群中所有节点的 ID，该通道用于向上层模块返回 ConfState 实例
	readyc     chan Ready           // 该通道用于向上层模块返回 Ready 实例，即 node.Ready() 方法的返回值
	advancec   chan struct{}        // 当上层模块处理完通过上述 readyc 通道获取到的 Ready 实例之后，会通过 node.Advance() 方法向该通道写入信号，从而通知底层 raft 实例
	tickc      chan struct{}        // 用来接收逻辑时钟发出的信号，之后会根据当前节点的角色推进选举计时器和心跳计时器
	done       chan struct{}        // 当检测到 done 通道关闭后，在其阻塞的 goroutine 会继续执行，并进行相应的关闭操作
	stop       chan struct{}        // 当 node.Stop() 方法被调用时，会向该通道发送信号
	status     chan chan Status
	rn         *RawNode
}

func newNode(rn *RawNode) node {
	return node{
		propc:      make(chan msgWithResult),
		recvc:      make(chan pb.Message),
		confc:      make(chan pb.ConfChangeV2),
		confstatec: make(chan pb.ConfState),
		readyc:     make(chan Ready),
		advancec:   make(chan struct{}),
		// make tickc a buffered chan, so raft node can buffer some ticks when the node
		// is busy processing raft messages. Raft node will resume process buffered
		// ticks when it becomes idle.
		tickc:  make(chan struct{}, 128), // 带缓冲的 Channel
		done:   make(chan struct{}),
		stop:   make(chan struct{}),
		status: make(chan chan Status),
		rn:     rn,
	}
}

func (n *node) Stop() {
	select {
	case n.stop <- struct{}{}:
		// Not already stopped, so trigger it
	case <-n.done:
		// Node has already been stopped - no need to do anything
		return
	}
	// Block until the stop has been acknowledged by run()
	<-n.done
}

func (n *node) run() {
	var propc chan msgWithResult // 指向 node.propc 通道
	var readyc chan Ready        // 指向 node.readyc 通道
	var advancec chan struct{}   // 指向 node.advancec 通道
	var rd Ready

	r := n.rn.raft

	lead := None // 用于记录当前 Leader 节点

	// 在该循环中完成了对所有相关 Channel 的处理
	for {
		if advancec != nil {
			// 上层模块还在处理上次从 readyc 通道返回的 Ready 实例，
			// 所以不能继续向 readyc 中写入数据
			readyc = nil
		} else if n.rn.HasReady() {
			// Populate a Ready. Note that this Ready is not guaranteed to
			// actually be handled. We will arm readyc, but there's no guarantee
			// that we will actually send on it. It's possible that we will
			// service another channel instead, loop around, and then populate
			// the Ready again. We could instead force the previous Ready to be
			// handled first, but it's generally good to emit larger Readys plus
			// it simplifies testing (by emitting less frequently and more
			// predictably).
			rd = n.rn.readyWithoutAccept()
			readyc = n.readyc
		}

		// 检测当前的 Leader 节点是否发生变化
		if lead != r.lead {
			// 如果当前节点无法确定集群中的 Leader 节点，则清空 propc，
			// 此次循环不再处理 MsgPropc 消息
			if r.hasLeader() {
				if lead == None {
					r.logger.Infof("raft.node: %x elected leader %x at term %d", r.id, r.lead, r.Term)
				} else {
					r.logger.Infof("raft.node: %x changed leader from %x to %x at term %d", r.id, lead, r.lead, r.Term)
				}
				propc = n.propc
			} else {
				r.logger.Infof("raft.node: %x lost leader %x at term %d", r.id, lead, r.Term)
				propc = nil
			}
			lead = r.lead // 更新 Leader
		}

		select {
		// TODO: maybe buffer the config propose if there exists one (the way
		// described in raft dissertation)
		// Currently it is dropped in Step silently.
		// 读取 propc 通道，获取 MsgPropc 消息，并交给 raft.Step() 方法处理
		case pm := <-propc:
			m := pm.m
			m.From = r.id
			err := r.Step(m)
			if pm.result != nil {
				pm.result <- err
				close(pm.result)
			}
		// 读取 node.recvc 通道，获取消息（非 MsgPropc 类型），
		// 并交给 raft.Step() 方法处理
		case m := <-n.recvc:
			// filter out response message from unknown From.
			// 如果是来自未知节点的响应消息则会被过滤
			if pr := r.prs.Progress[m.From]; pr != nil || !IsResponseMsg(m.Type) {
				r.Step(m)
			}
		// 读取 node.confc 通道，获取 ConfChange 实例，下面根据 ConfChange 中的类型进行分类处理
		case cc := <-n.confc:
			_, okBefore := r.prs.Progress[r.id]
			cs := r.applyConfChange(cc)
			// If the node was removed, block incoming proposals. Note that we
			// only do this if the node was in the config before. Nodes may be
			// a member of the group without knowing this (when they're catching
			// up on the log and don't have the latest config) and we don't want
			// to block the proposal channel in that case.
			//
			// NB: propc is reset when the leader changes, which, if we learn
			// about it, sort of implies that we got readded, maybe? This isn't
			// very sound and likely has bugs.
			if _, okAfter := r.prs.Progress[r.id]; okBefore && !okAfter {
				var found bool
			outer:
				for _, sl := range [][]uint64{cs.Voters, cs.VotersOutgoing} {
					for _, id := range sl {
						if id == r.id {
							found = true
							break outer
						}
					}
				}
				if !found {
					propc = nil
				}
			}
			select {
			case n.confstatec <- cs:
			case <-n.done:
			}
		// 逻辑时钟每推进一次，就会向 tickc 通道写入一个信号
		case <-n.tickc:
			// Leader 节点推进选举计时器，Follower 节点推进心跳计时器
			n.rn.Tick()
		// 将前面创建的 Ready 实例写入 node.readyc 通道中，等待上层模块读取
		case readyc <- rd:
			n.rn.acceptReady(rd)
			advancec = n.advancec
		// 上层模块处理完 Ready 实例之后，会向 advance 通道写入信号
		case <-advancec:
			n.rn.Advance(rd)
			rd = Ready{}
			advancec = nil
		case c := <-n.status:
			c <- getStatus(r)
		case <-n.stop:
			// 当从 stop 通道读取到信号时，会将 done 通道关闭，阻塞在其上的
			// goroutine 可以继续执行
			close(n.done)
			return
		}
	}
}

// Tick increments the internal logical clock for this Node. Election timeouts
// and heartbeat timeouts are in units of ticks.
func (n *node) Tick() {
	select {
	case n.tickc <- struct{}{}:
	case <-n.done:
	default:
		n.rn.raft.logger.Warningf("%x A tick missed to fire. Node blocks too long!", n.rn.raft.id)
	}
}

func (n *node) Campaign(ctx context.Context) error { return n.step(ctx, pb.Message{Type: pb.MsgHup}) }

func (n *node) Propose(ctx context.Context, data []byte) error {
	return n.stepWait(ctx, pb.Message{Type: pb.MsgProp, Entries: []pb.Entry{{Data: data}}})
}

func (n *node) Step(ctx context.Context, m pb.Message) error {
	// ignore unexpected local messages receiving over network
	if IsLocalMsg(m.Type) {
		// TODO: return an error?
		return nil
	}
	return n.step(ctx, m)
}

func confChangeToMsg(c pb.ConfChangeI) (pb.Message, error) {
	typ, data, err := pb.MarshalConfChange(c)
	if err != nil {
		return pb.Message{}, err
	}
	return pb.Message{Type: pb.MsgProp, Entries: []pb.Entry{{Type: typ, Data: data}}}, nil
}

func (n *node) ProposeConfChange(ctx context.Context, cc pb.ConfChangeI) error {
	msg, err := confChangeToMsg(cc)
	if err != nil {
		return err
	}
	return n.Step(ctx, msg)
}

func (n *node) step(ctx context.Context, m pb.Message) error {
	return n.stepWithWaitOption(ctx, m, false)
}

func (n *node) stepWait(ctx context.Context, m pb.Message) error {
	return n.stepWithWaitOption(ctx, m, true)
}

// Step advances the state machine using msgs. The ctx.Err() will be returned,
// if any.
func (n *node) stepWithWaitOption(ctx context.Context, m pb.Message, wait bool) error {
	if m.Type != pb.MsgProp {
		select {
		case n.recvc <- m:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-n.done:
			return ErrStopped
		}
	}
	ch := n.propc
	pm := msgWithResult{m: m}
	if wait {
		pm.result = make(chan error, 1)
	}
	select {
	case ch <- pm:
		if !wait {
			return nil
		}
	case <-ctx.Done():
		return ctx.Err()
	case <-n.done:
		return ErrStopped
	}
	select {
	case err := <-pm.result:
		if err != nil {
			return err
		}
	case <-ctx.Done():
		return ctx.Err()
	case <-n.done:
		return ErrStopped
	}
	return nil
}

func (n *node) Ready() <-chan Ready { return n.readyc }

func (n *node) Advance() {
	select {
	case n.advancec <- struct{}{}:
	case <-n.done:
	}
}

func (n *node) ApplyConfChange(cc pb.ConfChangeI) *pb.ConfState {
	var cs pb.ConfState
	select {
	case n.confc <- cc.AsV2():
	case <-n.done:
	}
	select {
	case cs = <-n.confstatec:
	case <-n.done:
	}
	return &cs
}

func (n *node) Status() Status {
	c := make(chan Status)
	select {
	case n.status <- c:
		return <-c
	case <-n.done:
		return Status{}
	}
}

func (n *node) ReportUnreachable(id uint64) {
	select {
	case n.recvc <- pb.Message{Type: pb.MsgUnreachable, From: id}:
	case <-n.done:
	}
}

func (n *node) ReportSnapshot(id uint64, status SnapshotStatus) {
	rej := status == SnapshotFailure

	select {
	case n.recvc <- pb.Message{Type: pb.MsgSnapStatus, From: id, Reject: rej}:
	case <-n.done:
	}
}

func (n *node) TransferLeadership(ctx context.Context, lead, transferee uint64) {
	select {
	// manually set 'from' and 'to', so that leader can voluntarily transfers its leadership
	case n.recvc <- pb.Message{Type: pb.MsgTransferLeader, From: transferee, To: lead}:
	case <-n.done:
	case <-ctx.Done():
	}
}

func (n *node) ReadIndex(ctx context.Context, rctx []byte) error {
	return n.step(ctx, pb.Message{Type: pb.MsgReadIndex, Entries: []pb.Entry{{Data: rctx}}})
}

// prevSoftSt、prevHardSt 是上次创建 Ready 实例时记录的 raft 实例的相关状态
func newReady(r *raft, prevSoftSt *SoftState, prevHardSt pb.HardState) Ready {
	rd := Ready{
		// 获取 raftLog 中 unstable 部分存储的 Entry 记录
		// 这些 Entry 记录会交给上层模块进行持久化
		Entries: r.raftLog.unstableEntries(),
		// 获取已提交但未应用的 Entry 记录，
		// 即 raftLog 中 applied ~ committed 之间的所有记录
		CommittedEntries: r.raftLog.nextEnts(),
		// 获取待发送的消息，在上一节介绍 raft 实例处理消息的相关代码可以看到，
		// 最终所有待发送的消息都会记录到 raft 实例的 msgs 字段中
		Messages: r.msgs,
	}
	// 检测两次创建 Ready 实例之间，raft 实例状态是否发生变化，如果没有变化，则将
	// Ready 实例相关字段设置为 nil，表示无须上层模块处理，SoftState 和
	// HardState 的比较方法比较简单，这里不再赘述
	if softSt := r.softState(); !softSt.equal(prevSoftSt) {
		rd.SoftState = softSt
	}
	if hardSt := r.hardState(); !isHardStateEqual(hardSt, prevHardSt) {
		rd.HardState = hardSt
	}

	// 检测 unstable 中是否记录了新的快照数据，如果有，则将其封装到 Ready 实例中，
	// 交给上层模块进行处理
	if r.raftLog.unstable.snapshot != nil {
		rd.Snapshot = *r.raftLog.unstable.snapshot
	}
	// 在前面介绍只读请求的处理时，raft 最终会将能响应的请求信息封装成 ReadState
	// 并记录到 readStates 中，这里会检测 raft.readStates 字段，并将其封装到
	// Ready 实例返回给上层模块
	if len(r.readStates) != 0 {
		rd.ReadStates = r.readStates
	}
	rd.MustSync = MustSync(r.hardState(), prevHardSt, len(rd.Entries))
	return rd
}

// MustSync returns true if the hard state and count of Raft entries indicate
// that a synchronous write to persistent storage is required.
func MustSync(st, prevst pb.HardState, entsnum int) bool {
	// Persistent state on all servers:
	// (Updated on stable storage before responding to RPCs)
	// currentTerm
	// votedFor
	// log entries[]
	return entsnum != 0 || st.Vote != prevst.Vote || st.Term != prevst.Term
}
