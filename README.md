# Distributed Key-Value Store 🗝️

**Redis-compatible distributed key-value store** with clustering, replication, and persistence.

## 🌟 Features

- ✅ **Distributed Architecture** - Multi-node cluster with consistent hashing
- ✅ **Replication** - Master-slave replication for high availability
- ✅ **Persistence** - AOF and RDB snapshot support
- ✅ **Data Structures** - Strings, Lists, Sets, Hashes, Sorted Sets
- ✅ **Pub/Sub** - Real-time messaging between clients
- ✅ **Transactions** - ACID-compliant multi-key operations
- ✅ **Lua Scripting** - Server-side scripting support

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    CLIENT LAYER                         │
│              (Redis Protocol Compatible)                │
└────────────────────────┬────────────────────────────────┘
                         │
┌────────────────────────▼────────────────────────────────┐
│                   CLUSTER LAYER                         │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐  │
│  │ Node 1  │  │ Node 2  │  │ Node 3  │  │ Node N  │  │
│  │ Master  │  │ Master  │  │ Master  │  │ Master  │  │
│  │  [A-M]  │  │  [N-Z]  │  │ [0-9]   │  │ [...]   │  │
│  └────┬────┘  └────┬────┘  └────┬────┘  └────┬────┘  │
│       │            │            │            │        │
│  ┌────▼────┐  ┌────▼────┐  ┌────▼────┐  ┌────▼────┐  │
│  │ Replica │  │ Replica │  │ Replica │  │ Replica │  │
│  └─────────┘  └─────────┘  └─────────┘  └─────────┘  │
└─────────────────────────────────────────────────────────┘
```

## 🚀 Quick Start

```bash
# Start single node
python -m kvstore.server --port 6379

# Start cluster
python -m kvstore.cluster --nodes 3

# Connect with redis-cli
redis-cli -p 6379
> SET key value
OK
> GET key
"value"
```

## 📚 Supported Commands

```bash
# Strings
SET key value
GET key
INCR counter
MSET key1 val1 key2 val2

# Lists
LPUSH mylist item
RPUSH mylist item
LRANGE mylist 0 -1

# Sets
SADD myset member
SMEMBERS myset

# Hashes
HSET user:1 name "John"
HGETALL user:1

# Pub/Sub
PUBLISH channel message
SUBSCRIBE channel
```

## 👥 Author

Efe Altıparmakoğlu
