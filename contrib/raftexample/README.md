# raftexample

raftexample is an example usage of etcd's [raft library](../../raft). It provides a simple REST API for a key-value store cluster backed by the [Raft][raft] consensus algorithm.

[raft]: http://raftconsensus.github.io/

## Getting Started

### Building raftexample

Clone `etcd` to `<directory>/src/go.etcd.io/etcd`

```sh
export GOPATH=<directory>
cd <directory>/src/go.etcd.io/etcd/contrib/raftexample
go build -o raftexample
```

### Running single node raftexample

First start a single-member cluster of raftexample:

```sh
raftexample --id 1 --cluster http://127.0.0.1:12379 --port 12380
```

Each raftexample process maintains a single raft instance and a key-value server.
The process's list of comma separated peers (--cluster), its raft ID index into the peer list (--id), and http key-value server port (--port) are passed through the command line.

Next, store a value ("hello") to a key ("my-key"):

```
curl -L http://127.0.0.1:12380/my-key -XPUT -d hello
```

Finally, retrieve the stored key:

```
curl -L http://127.0.0.1:12380/my-key
```

### Running a local cluster

First install [goreman](https://github.com/mattn/goreman), which manages Procfile-based applications.

The [Procfile script](./Procfile) will set up a local example cluster. Start it with:

```sh
goreman start
```

This will bring up three raftexample instances.

Now it's possible to write a key-value pair to any member of the cluster and likewise retrieve it from any member.

### Fault Tolerance

To test cluster recovery, first start a cluster and write a value "foo":
```sh
goreman start
curl -L http://127.0.0.1:12380/my-key -XPUT -d foo
```

Next, remove a node and replace the value with "bar" to check cluster availability:

```sh
goreman run stop raftexample2
curl -L http://127.0.0.1:12380/my-key -XPUT -d bar
curl -L http://127.0.0.1:32380/my-key
```

Finally, bring the node back up and verify it recovers with the updated value "bar":
```sh
goreman run start raftexample2
curl -L http://127.0.0.1:22380/my-key
```

### Dynamic cluster reconfiguration

Nodes can be added to or removed from a running cluster using requests to the REST API.

For example, suppose we have a 3-node cluster that was started with the commands:
```sh
raftexample --id 1 --cluster http://127.0.0.1:12379,http://127.0.0.1:22379,http://127.0.0.1:32379 --port 12380
raftexample --id 2 --cluster http://127.0.0.1:12379,http://127.0.0.1:22379,http://127.0.0.1:32379 --port 22380
raftexample --id 3 --cluster http://127.0.0.1:12379,http://127.0.0.1:22379,http://127.0.0.1:32379 --port 32380
```

A fourth node with ID 4 can be added by issuing a POST:
```sh
curl -L http://127.0.0.1:12380/4 -XPOST -d http://127.0.0.1:42379
```

Then the new node can be started as the others were, using the --join option:
```sh
raftexample --id 4 --cluster http://127.0.0.1:12379,http://127.0.0.1:22379,http://127.0.0.1:32379,http://127.0.0.1:42379 --port 42380 --join
```

The new node should join the cluster and be able to service key/value requests.

We can remove a node using a DELETE request:
```sh
curl -L http://127.0.0.1:12380/3 -XDELETE
```

Node 3 should shut itself down once the cluster has processed this request.

## Design

The raftexample consists of three components: a raft-backed key-value store, a REST API server, and a raft consensus server based on etcd's raft implementation.

The raft-backed key-value store is a key-value map that holds all committed key-values.
The store bridges communication between the raft server and the REST server.
Key-value updates are issued through the store to the raft server.
The store updates its map once raft reports the updates are committed.

The REST server exposes the current raft consensus by accessing the raft-backed key-value store.
A GET command looks up a key in the store and returns the value, if any.
A key-value PUT command issues an update proposal to the store.

The raft server participates in consensus with its cluster peers.
When the REST server submits a proposal, the raft server transmits the proposal to its peers.
When raft reaches a consensus, the server publishes all committed updates over a commit channel.
For raftexample, this commit channel is consumed by the key-value store.

## 设计

核心组件如下：

- raftNode : raftNode 是该实例的核心组件（它定义在 raftexample/raft.go 文件中），raftNode 是上层模块和底层 etcd-raft 组件之间衔接的桥梁。raftNode 是对 etcd-raft 模块的一层封装，对上层模块提供了更简洁、更方便使用的调用方式，可以让实例中其他部分无须过多关注 etcd-raft 模块的实现细节，降低了系统耦合程度。除此之外，raftNode 还封装了很多其他的功能，例如，前面其他的 WAL 日志管理、快照管理及网络层相关功能。在该实例中，raftNode 相对于 etcd-raft 模块来说，扮演了上层模块的角色
- kvStore : 用于存储键值对信息，kvStore 扮演了持久化存储的角色
- httpKVAPI : 该实例向外提供的是 HTTP 接口，用户可以通过调用 HTTP 接口来模拟客户端的行为
