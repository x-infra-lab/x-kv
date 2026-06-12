# Deployment Guide

[English](./deployment.md) | [中文](./deployment_zh.md)

## Prerequisites

- **JDK 17+** (Temurin recommended)
- **Maven 3.8+** (build only)
- **Docker & Docker Compose** (for containerized deployments)
- Disk: SSD recommended for RocksDB; minimum 10 GB per KV node for
  production workloads

## Port Layout

| Service | Port | Protocol | Description |
|---------|------|----------|-------------|
| PD client | 2379 | gRPC | Client-facing PD API |
| PD raft | 2380 | gRPC | Inter-PD Raft replication |
| PD metrics | 9190 | HTTP | Prometheus `/metrics`, `/healthz`, `/readyz` |
| KV client | 20160 | gRPC | Client-facing KV/Tikv API |
| KV raft | 20170 | gRPC | Inter-KV Raft replication |
| KV metrics | 9191+ | HTTP | Prometheus `/metrics`, `/healthz`, `/readyz` |

---

## Docker Compose (Recommended)

The `docker/` directory contains a production-like 3 PD + 3 KV topology.

```bash
cd docker
docker compose up -d

# Verify all services are healthy
docker compose ps

# Inspect the cluster
docker compose exec kv1 java -jar /opt/x-kv/x-kv-ctl.jar \
    --pd pd1:2379 cluster members
```

### Endpoints

| Service | Exposed ports |
|---------|---------------|
| PD | `localhost:2379`, `localhost:2381`, `localhost:2383` |
| KV | `localhost:20160`, `localhost:20161`, `localhost:20162` |
| Prometheus | `localhost:9090` |
| Grafana | `localhost:3000` (admin/admin) |

### Volumes

Docker Compose creates named volumes for each node's data directory. To
reset the cluster:

```bash
docker compose down -v
```

---

## Bare Metal

### 1. Build

```bash
mvn package -DskipTests -q
```

Produces:
- `pd/target/x-pd-server.jar` — PD fat JAR
- `kv/target/x-kv-server.jar` — KV fat JAR
- `ctl/target/x-kv-ctl.jar` — CLI fat JAR

### 2. Start PD Cluster (3 nodes)

On each PD node:

```bash
java -jar x-pd-server.jar \
    --node-id 1 \
    --cluster-id 1 \
    --client-address 0.0.0.0:2379 \
    --raft-address 0.0.0.0:2380 \
    --data-dir /var/lib/x-pd \
    --metrics-port 9190 \
    --peer "1=pd1:2380,pd1:2379;2=pd2:2380,pd2:2379;3=pd3:2380,pd3:2379"
```

Repeat for nodes 2 and 3 with adjusted `--node-id` and hostnames.

### 3. Start KV Stores (3 nodes)

On each KV node:

```bash
java -jar x-kv-server.jar \
    --store-id 1 \
    --pd pd1:2379,pd2:2379,pd3:2379 \
    --client-address 0.0.0.0:20160 \
    --raft-address 0.0.0.0:20170 \
    --data-dir /var/lib/x-kv \
    --metrics-port 9191
```

### 4. Systemd Unit (Example)

```ini
[Unit]
Description=x-kv Store
After=network.target

[Service]
Type=simple
User=xkv
ExecStart=/usr/bin/java -jar /opt/x-kv/x-kv-server.jar \
    --store-id 1 \
    --pd pd1:2379,pd2:2379,pd3:2379 \
    --client-address 0.0.0.0:20160 \
    --raft-address 0.0.0.0:20170 \
    --data-dir /var/lib/x-kv \
    --metrics-port 9191
Restart=always
RestartSec=5
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
```

---

## TLS / mTLS

Both client and raft planes support TLS via `GrpcChannelFactory`. Provide
PEM-encoded cert/key/CA files via CLI args or YAML config:

```bash
java -jar x-kv-server.jar \
    --client-tls-cert /etc/x-kv/tls/server.crt \
    --client-tls-key /etc/x-kv/tls/server.key \
    --client-tls-ca /etc/x-kv/tls/ca.crt \
    --raft-tls-cert /etc/x-kv/tls/peer.crt \
    --raft-tls-key /etc/x-kv/tls/peer.key \
    --raft-tls-ca /etc/x-kv/tls/ca.crt \
    ...
```

For mutual TLS (mTLS), set the CA on both sides so each endpoint
validates the other's certificate.

---

## Monitoring

### Prometheus

Each PD and KV node exposes a metrics HTTP server. Configure Prometheus to
scrape these endpoints — see `docker/prometheus/prometheus.yml` for a
reference configuration.

### Grafana

Pre-built dashboards are provided in `docker/grafana/dashboards/`:

- **xkv-overview** — KV store request rates, latency percentiles, active
  requests, error rates
- **xpd-overview** — PD request rates, TSO throughput, heartbeat rates

The Docker Compose setup auto-provisions these dashboards via Grafana's
file-based provisioning.

### Health Checks

- `/healthz` — returns 200 if the process is alive
- `/readyz` — returns 200 if the node is ready to serve traffic

---

## Operational Notes

### Graceful Drain

KV stores support graceful drain via `SIGTERM`. The server stops accepting
new requests and waits for in-flight requests to complete (configurable
via `drainTimeoutMs`, default 10 s).

### Scaling

To add a new KV store:
1. Start the store with a new `--store-id` pointing to the existing PD
   cluster
2. The PD will automatically discover the new store via heartbeat
3. Region balance schedulers will redistribute regions to the new store

### Configuration Reference

See [Configuration Reference](./configuration.md) for all available
settings.
