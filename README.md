# Distributed Search System in Go

A high-performance, decentralized distributed search system capable of handling 10,000+ Queries Per Second (QPS). This project implements core distributed systems concepts including data partitioning via consistent hashing, fault tolerance through replication, and the scatter-gather search pattern.

## 🚀 Key Features

- **Decentralized Architecture**: Shared-nothing architecture with Coordinator and Data Shard nodes.
- **Inverted Index**: Efficient in-memory index structure for fast text lookups.
- **Consistent Hashing**: CRC32-based ring with virtual nodes for even data distribution and term-based sharding.
- **Scatter-Gather Pattern**: Coordinator fans out queries to relevant shards and merges results with AND logic.
- **Worker Pools**: Shards use worker pools to manage concurrency and prevent OOM under high load.
- **Fault Tolerance**: Primary-replica replication and background health checks for automatic node failover.
- **Containerized**: Multi-stage Docker builds resulting in minimal binaries (~15MB) running on `scratch`.

## 🛠️ Technology Stack

- **Go 1.24+**: Core language for its lightweight goroutines and fast networking.
- **Docker & Docker Compose**: Environment isolation and cluster orchestration.
- **HTTP/JSON**: Communication protocol between nodes (extensible to gRPC).

## 📁 Project Structure

```text
distributed-search-go/
├── cmd/node/
│   └── main.go         # Unified entry point for Coordinator and Shard
├── internal/
│   ├── cluster/
│   │   └── hashring.go # Consistent hashing implementation
│   └── index/
│       └── index.go    # Inverted index and search logic
├── Dockerfile          # Multi-stage production build
└── docker-compose.yml  # Cluster orchestration (1 Coord, 4 Shards)
```

## 🚦 Getting Started

### Prerequisites

- [Go 1.24+](https://go.dev/doc/install)
- [Docker](https://docs.docker.com/get-docker/) & [Docker Compose](https://docs.docker.com/compose/install/)

### Running Tests

Execute the unit tests for core logic (indexing and hashing):

```bash
cd distributed-search-go
go test ./...
```

### Deploying the Cluster

Start the distributed cluster (1 Coordinator, 2 Primary Shards, 2 Replica Shards):

```bash
cd distributed-search-go
docker-compose up --build
```

The Coordinator will be available at `http://localhost:8080`.

## 🔍 Usage Examples

### 1. Indexing a Document
Send a POST request to the Coordinator to index a new document. The Coordinator will route it to the appropriate shard(s) based on the terms.

```bash
curl -X POST http://localhost:8080/index \
  -H "Content-Type: application/json" \
  -d '{"id": 1, "text": "Golang is perfect for distributed systems"}'
```

### 2. Searching
Send a GET request to the Coordinator to search for terms. The Coordinator performs a scatter-gather across relevant shards.

```bash
# Search for a single term
curl "http://localhost:8080/search?q=golang"

# Search for multiple terms (AND logic)
curl "http://localhost:8080/search?q=distributed+golang"
```

## 🛡️ Fault Tolerance

The system automatically detects node failures. If a primary shard goes down, the Coordinator will attempt to route search requests to its replica. The background health check loop (running every 5 seconds) monitors node status and updates the active hash ring dynamically.

---

## 🛠️ Project Deep Dive & Technical Guide

### 🎯 Main Purpose
Distributed search engine in Go featuring consensus and clustering.

### 🧰 Tecnologias Utilizadas (Tech Stack)
- **Primary Technologies:** Docker / Docker Compose, Go 1.24.3
- **Deployment Strategy:** Container-ready with standard Docker/Docker Compose integrations where applicable.

### 🚀 Como Implantar e Executar (How to Deploy & Run)
#### Go CLI / Service
1. Build the module: `go build`.
2. Execute: `./distributed-search-go` (or project executable name).

---
