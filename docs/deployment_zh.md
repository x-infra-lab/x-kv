# 部署指南

[English](./deployment.md) | [中文](./deployment_zh.md)

## 前置条件

- **JDK 17+**（推荐 Temurin）
- **Maven 3.8+**（仅构建需要）
- **Docker & Docker Compose**（容器化部署）
- 磁盘：RocksDB 推荐 SSD；生产环境每个 KV 节点至少 10 GB

## 端口规划

| 服务 | 端口 | 协议 | 说明 |
|------|------|------|------|
| PD client | 2379 | gRPC | 客户端 PD API |
| PD raft | 2380 | gRPC | PD 节点间 Raft 复制 |
| PD metrics | 9190 | HTTP | Prometheus `/metrics`、`/healthz`、`/readyz` |
| KV client | 20160 | gRPC | 客户端 KV/Tikv API |
| KV raft | 20170 | gRPC | KV 节点间 Raft 复制 |
| KV metrics | 9191+ | HTTP | Prometheus `/metrics`、`/healthz`、`/readyz` |

---

## Docker Compose（推荐）

`docker/` 目录包含生产级 3 PD + 3 KV 拓扑。

```bash
cd docker
docker compose up -d

# 验证所有服务健康
docker compose ps

# 查看集群状态
docker compose exec kv1 java -jar /opt/x-kv/x-kv-ctl.jar \
    --pd pd1:2379 cluster members
```

### 访问端点

| 服务 | 暴露端口 |
|------|----------|
| PD | `localhost:2379`、`localhost:2381`、`localhost:2383` |
| KV | `localhost:20160`、`localhost:20161`、`localhost:20162` |
| Prometheus | `localhost:9090` |
| Grafana | `localhost:3000`（admin/admin） |

### 数据卷

Docker Compose 为每个节点创建命名卷。重置集群：

```bash
docker compose down -v
```

---

## 裸机部署

### 1. 构建

```bash
mvn package -DskipTests -q
```

产出：
- `pd/target/x-pd-server.jar` — PD 可执行 JAR
- `kv/target/x-kv-server.jar` — KV 可执行 JAR
- `ctl/target/x-kv-ctl.jar` — CLI 工具

### 2. 启动 PD 集群（3 节点）

在每个 PD 节点上执行：

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

节点 2 和 3 调整 `--node-id` 和主机名。

### 3. 启动 KV 存储（3 节点）

```bash
java -jar x-kv-server.jar \
    --store-id 1 \
    --pd pd1:2379,pd2:2379,pd3:2379 \
    --client-address 0.0.0.0:20160 \
    --raft-address 0.0.0.0:20170 \
    --data-dir /var/lib/x-kv \
    --metrics-port 9191
```

### 4. Systemd 单元（示例）

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

客户端和 Raft 通道均支持 TLS，通过 `GrpcChannelFactory` 实现。提供
PEM 格式的证书/密钥/CA 文件：

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

---

## 监控

### Prometheus

每个 PD 和 KV 节点暴露 metrics HTTP 端点。配置 Prometheus 抓取，参考
`docker/prometheus/prometheus.yml`。

### Grafana

预置仪表盘位于 `docker/grafana/dashboards/`：

- **xkv-overview** — KV 请求速率、延迟分位数、活跃请求、错误率
- **xpd-overview** — PD 请求速率、TSO 吞吐量、心跳速率

Docker Compose 通过 Grafana 文件供给自动加载这些仪表盘。

### 健康检查

- `/healthz` — 进程存活返回 200
- `/readyz` — 节点就绪返回 200

---

## 运维说明

### 优雅下线

KV 节点收到 `SIGTERM` 后停止接受新请求，等待进行中的请求完成
（`drainTimeoutMs` 可配置，默认 10 秒）。

### 扩容

添加新 KV 节点：
1. 使用新 `--store-id` 启动，指向已有 PD 集群
2. PD 通过心跳自动发现新节点
3. Region 均衡调度器自动迁移 Region 到新节点

### 配置参考

详见[配置参考](./configuration_zh.md)。
