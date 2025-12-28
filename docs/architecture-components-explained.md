# High-Level Architecture - Component Deep Dive

This document explains each component in the Real-Time Video Analytics Platform architecture in detail.

---

## Architecture Overview Diagram

```
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                                    DATA SOURCES                                          │
├─────────────────┬─────────────────┬─────────────────┬─────────────────┬─────────────────┤
│   Video Players │      CDNs       │ Backend Services│   Encoders      │  Third-Party    │
└────────┬────────┴────────┬────────┴────────┬────────┴────────┬────────┴────────┬────────┘
         │                 │                 │                 │                 │
         ▼                 ▼                 ▼                 ▼                 ▼
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                              INGESTION LAYER                                             │
│         Kafka / Kinesis  |  Schema Registry  |  API Gateway  |  SDK Collectors          │
└─────────────────────────────────────────────────────────────────────────────────────────┘
                                        │
                                        ▼
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                           STREAM PROCESSING LAYER                                        │
│    Event Enrichment | Session Aggregation | Metric Computation | Anomaly Detection      │
└─────────────────────────────────────────────────────────────────────────────────────────┘
                                        │
                    ┌───────────────────┼───────────────────┐
                    ▼                   ▼                   ▼
┌───────────────────────┐ ┌───────────────────────┐ ┌───────────────────────┐
│    HOT STORAGE        │ │    WARM STORAGE       │ │    COLD STORAGE       │
│    (Redis/Druid)      │ │  (ClickHouse/ES)      │ │    (S3/Data Lake)     │
└───────────────────────┘ └───────────────────────┘ └───────────────────────┘
                                        │
                                        ▼
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                              APPLICATION LAYER                                           │
│   Dashboards | Alerting | APIs | ML Pipeline | Session Explorer | Reports               │
└─────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 1. DATA SOURCES LAYER

These are the origin points of all telemetry data in the system.

### 1.1 Video Players

```
┌─────────────────────────────────────────────────────────────────┐
│                      VIDEO PLAYERS                               │
│                                                                  │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐              │
│  │   Web       │  │   Mobile    │  │  Smart TV   │              │
│  │  (Browser)  │  │ (iOS/Android│  │  (Tizen/    │              │
│  │             │  │   Apps)     │  │   WebOS)    │              │
│  └─────────────┘  └─────────────┘  └─────────────┘              │
│                                                                  │
│  ┌─────────────┐  ┌─────────────┐                               │
│  │ Set-Top Box │  │  Gaming     │                               │
│  │   (IPTV)    │  │  Consoles   │                               │
│  └─────────────┘  └─────────────┘                               │
└─────────────────────────────────────────────────────────────────┘
```

**What it is:**
Video players are the client-side applications that users interact with to watch content. Each player has an embedded telemetry SDK that captures events during playback.

**What data it generates:**
| Event Type | Description | Example Data |
|------------|-------------|--------------|
| `PLAY_REQUEST` | User clicked play | content_id, timestamp, device_info |
| `FIRST_FRAME` | Video started rendering | time_to_first_frame, initial_bitrate |
| `REBUFFER_START/END` | Buffering occurred | duration, playhead_position |
| `BITRATE_CHANGE` | Quality switched | old_bitrate, new_bitrate, reason |
| `ERROR` | Playback error | error_code, error_message, is_fatal |
| `HEARTBEAT` | Periodic health ping | buffer_length, current_bitrate, framerate |
| `SESSION_END` | Playback ended | total_watch_time, exit_reason |

**Why it's important:**
- Only source of true user experience data (what the user actually sees)
- Captures client-side issues (device performance, network conditions)
- Enables per-session and per-user analytics

---

### 1.2 CDNs (Content Delivery Networks)

```
┌─────────────────────────────────────────────────────────────────┐
│                         CDN LAYER                                │
│                                                                  │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │                    EDGE SERVERS                          │    │
│  │   ┌─────┐  ┌─────┐  ┌─────┐  ┌─────┐  ┌─────┐          │    │
│  │   │ DEL │  │ MUM │  │ BLR │  │ HYD │  │ CHE │          │    │
│  │   │ POP │  │ POP │  │ POP │  │ POP │  │ POP │          │    │
│  │   └─────┘  └─────┘  └─────┘  └─────┘  └─────┘          │    │
│  └─────────────────────────────────────────────────────────┘    │
│                              │                                   │
│                              ▼                                   │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │                    CDN LOGS                              │    │
│  │  • Request logs (every segment/manifest request)        │    │
│  │  • Cache hit/miss status                                │    │
│  │  • Response times                                       │    │
│  │  • Bytes transferred                                    │    │
│  │  • Error codes                                          │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                  │
│  Providers: Akamai, CloudFront, Fastly, In-house CDN            │
└─────────────────────────────────────────────────────────────────┘
```

**What it is:**
CDNs are globally distributed networks of servers that cache and deliver video content (segments, manifests) to users from the nearest edge location.

**What data it generates:**
| Metric | Description | Use Case |
|--------|-------------|----------|
| `cache_status` | HIT/MISS/REFRESH | Measure cache efficiency |
| `response_time` | Time to serve request | Identify slow edge nodes |
| `bytes_sent` | Data transferred | Bandwidth analysis |
| `http_status` | 200, 404, 503, etc. | Error rate monitoring |
| `origin_fetch_time` | Time to fetch from origin | Origin health |
| `edge_location` | Which POP served request | Geographic analysis |
| `request_url` | Content being requested | Content popularity |

**Why it's important:**
- Identifies CDN-level issues before they impact users
- Cache hit ratio directly affects video start time
- Helps with CDN cost optimization and vendor comparison
- Geographic performance analysis

---

### 1.3 Backend Services

```
┌─────────────────────────────────────────────────────────────────┐
│                     BACKEND SERVICES                             │
│                                                                  │
│  ┌──────────────────┐  ┌──────────────────┐                     │
│  │  Authentication  │  │   Entitlement    │                     │
│  │     Service      │  │     Service      │                     │
│  │                  │  │                  │                     │
│  │ • User login     │  │ • Subscription   │                     │
│  │ • Token issue    │  │   validation     │                     │
│  │ • Session mgmt   │  │ • Content access │                     │
│  └──────────────────┘  └──────────────────┘                     │
│                                                                  │
│  ┌──────────────────┐  ┌──────────────────┐                     │
│  │  Metadata        │  │   DRM License    │                     │
│  │  Service         │  │     Server       │                     │
│  │                  │  │                  │                     │
│  │ • Content info   │  │ • Widevine       │                     │
│  │ • Recommendations│  │ • FairPlay       │                     │
│  │ • Search         │  │ • PlayReady      │                     │
│  └──────────────────┘  └──────────────────┘                     │
│                                                                  │
│  ┌──────────────────┐  ┌──────────────────┐                     │
│  │  Playback URL    │  │   Analytics      │                     │
│  │  Service         │  │   Collector      │                     │
│  │                  │  │                  │                     │
│  │ • URL signing    │  │ • Event ingestion│                     │
│  │ • CDN selection  │  │ • Batch upload   │                     │
│  └──────────────────┘  └──────────────────┘                     │
└─────────────────────────────────────────────────────────────────┘
```

**What it is:**
Backend services are the APIs and microservices that support the video streaming platform - from user authentication to content metadata to DRM license delivery.

**What data it generates:**
| Service | Metrics | Why It Matters |
|---------|---------|----------------|
| **Auth Service** | Login latency, failure rate, active sessions | Slow auth = delayed playback |
| **Entitlement** | Check latency, denial rate | Blocks content access if slow |
| **Metadata API** | Response time, error rate | Affects app browsing experience |
| **DRM Server** | License acquisition time, failures | Critical for playback start |
| **URL Service** | Response time, CDN selection accuracy | Affects initial content routing |

**Why it's important:**
- Backend latency directly adds to video start time
- DRM failures cause playback failures
- Enables correlation: "Was the error client-side or server-side?"

---

### 1.4 Encoders/Transcoders

```
┌─────────────────────────────────────────────────────────────────┐
│                    ENCODING PIPELINE                             │
│                                                                  │
│  ┌─────────┐    ┌─────────────┐    ┌─────────────┐              │
│  │ Source  │───▶│ Transcoder  │───▶│  Packager   │───▶ CDN     │
│  │ Content │    │ (FFmpeg/    │    │ (HLS/DASH)  │              │
│  │         │    │  Elemental) │    │             │              │
│  └─────────┘    └─────────────┘    └─────────────┘              │
│                                                                  │
│  For LIVE:                                                       │
│  ┌─────────┐    ┌─────────────┐    ┌─────────────┐              │
│  │ Live    │───▶│ Live        │───▶│ Live        │───▶ CDN     │
│  │ Feed    │    │ Encoder     │    │ Origin      │              │
│  └─────────┘    └─────────────┘    └─────────────┘              │
└─────────────────────────────────────────────────────────────────┘
```

**What it is:**
The encoding pipeline converts source video into multiple bitrate renditions (ABR ladder) and packages them for streaming (HLS/DASH formats).

**What data it generates:**
| Metric | Description |
|--------|-------------|
| `encoding_latency` | Time from input to output |
| `queue_depth` | Pending encoding jobs |
| `output_quality` | VMAF/PSNR scores |
| `segment_duration` | Actual vs target segment length |
| `encoder_health` | CPU, memory, GPU utilization |
| `error_rate` | Failed encoding jobs |

**Why it's important:**
- For LIVE: Encoding delays = stream latency
- Encoding errors = content unavailability
- Quality metrics affect user experience

---

### 1.5 Third-Party Systems

```
┌─────────────────────────────────────────────────────────────────┐
│                   THIRD-PARTY INTEGRATIONS                       │
│                                                                  │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐              │
│  │   APM       │  │  Cloud      │  │  Network    │              │
│  │  (Datadog/  │  │  Provider   │  │  Monitoring │              │
│  │   NewRelic) │  │  (AWS/GCP)  │  │  (ISP data) │              │
│  └─────────────┘  └─────────────┘  └─────────────┘              │
│                                                                  │
│  ┌─────────────┐  ┌─────────────┐                               │
│  │  Ad Server  │  │  Payment    │                               │
│  │  Analytics  │  │  Gateway    │                               │
│  └─────────────┘  └─────────────┘                               │
└─────────────────────────────────────────────────────────────────┘
```

**What it is:**
External systems that provide additional context and data for comprehensive analysis.

**Why it's important:**
- Correlate video issues with infrastructure problems
- Ad insertion impact on playback
- Network-level visibility beyond your control

---

## 2. INGESTION LAYER

This layer is responsible for collecting, validating, and routing all incoming telemetry data.

### 2.1 Apache Kafka / Amazon Kinesis

```
┌─────────────────────────────────────────────────────────────────┐
│                    MESSAGE STREAMING PLATFORM                    │
│                                                                  │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │                   KAFKA CLUSTER                          │    │
│  │                                                          │    │
│  │   ┌─────────────────────────────────────────────────┐   │    │
│  │   │  Topic: player-events                            │   │    │
│  │   │  Partitions: 128 | Replication: 3 | Retention: 7d│   │    │
│  │   │  ┌────┬────┬────┬────┬────┬────┬────┬────┐      │   │    │
│  │   │  │ P0 │ P1 │ P2 │ P3 │...│P125│P126│P127│      │   │    │
│  │   │  └────┴────┴────┴────┴────┴────┴────┴────┘      │   │    │
│  │   └─────────────────────────────────────────────────┘   │    │
│  │                                                          │    │
│  │   ┌─────────────────────────────────────────────────┐   │    │
│  │   │  Topic: cdn-logs                                 │   │    │
│  │   │  Partitions: 64 | Replication: 3                 │   │    │
│  │   └─────────────────────────────────────────────────┘   │    │
│  │                                                          │    │
│  │   ┌─────────────────────────────────────────────────┐   │    │
│  │   │  Topic: backend-events                           │   │    │
│  │   │  Partitions: 32 | Replication: 3                 │   │    │
│  │   └─────────────────────────────────────────────────┘   │    │
│  │                                                          │    │
│  │   ┌─────────────────────────────────────────────────┐   │    │
│  │   │  Topic: alerts (output)                          │   │    │
│  │   │  Topic: enriched-events (output)                 │   │    │
│  │   └─────────────────────────────────────────────────┘   │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                  │
│  Brokers: 15-20 nodes | Throughput: 10M events/sec              │
└─────────────────────────────────────────────────────────────────┘
```

**What it is:**
A distributed streaming platform that acts as the central nervous system for all event data. All data flows through Kafka before being processed.

**Key responsibilities:**
| Function | Description |
|----------|-------------|
| **Decoupling** | Separates data producers from consumers |
| **Buffering** | Handles traffic spikes without data loss |
| **Durability** | Persists data with replication |
| **Ordering** | Maintains event order within partitions |
| **Replay** | Allows reprocessing of historical data |

**Design decisions:**
- **Partitioning**: By `session_id` ensures all events for a session go to same partition (important for stateful processing)
- **Replication Factor 3**: Survives loss of 2 brokers
- **Retention 7 days**: Allows replay for debugging/reprocessing

**Why it's critical:**
- Single source of truth for all events
- Enables multiple consumers (real-time + batch)
- Provides backpressure handling during traffic spikes
- Fault tolerance - no data loss

---

### 2.2 Schema Registry

```
┌─────────────────────────────────────────────────────────────────┐
│                      SCHEMA REGISTRY                             │
│                                                                  │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │                                                          │    │
│  │   Schema: PlayerEvent                                    │    │
│  │   ├── v1 (deprecated)                                   │    │
│  │   ├── v2 (deprecated)                                   │    │
│  │   ├── v3 (active) ◄── Current                           │    │
│  │   └── v4 (draft)                                        │    │
│  │                                                          │    │
│  │   Compatibility Mode: BACKWARD                           │    │
│  │                                                          │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                  │
│  Supported Formats: Avro, Protobuf, JSON Schema                 │
└─────────────────────────────────────────────────────────────────┘
```

**What it is:**
A centralized repository that stores and validates schemas for all events flowing through Kafka.

**Key responsibilities:**
| Function | Description |
|----------|-------------|
| **Schema Storage** | Central repository of all event schemas |
| **Validation** | Ensures events conform to schema |
| **Versioning** | Tracks schema evolution over time |
| **Compatibility** | Prevents breaking changes |

**Why it's important:**
- Prevents "garbage in, garbage out" - malformed events are rejected
- Enables schema evolution without breaking consumers
- Documentation of data contracts
- Reduces data quality issues downstream

---

### 2.3 API Gateway / Collector

```
┌─────────────────────────────────────────────────────────────────┐
│                    COLLECTOR / API GATEWAY                       │
│                                                                  │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │              LOAD BALANCER (L7)                          │    │
│  │         ┌─────────────────────────────┐                  │    │
│  │         │   Geographic DNS Routing    │                  │    │
│  │         │   (Route to nearest region) │                  │    │
│  │         └─────────────────────────────┘                  │    │
│  └─────────────────────────────────────────────────────────┘    │
│                            │                                     │
│              ┌─────────────┼─────────────┐                      │
│              ▼             ▼             ▼                      │
│  ┌───────────────┐ ┌───────────────┐ ┌───────────────┐         │
│  │  Collector    │ │  Collector    │ │  Collector    │         │
│  │  (Region 1)   │ │  (Region 2)   │ │  (Region 3)   │         │
│  │               │ │               │ │               │         │
│  │ • Auth/API Key│ │ • Auth/API Key│ │ • Auth/API Key│         │
│  │ • Rate Limit  │ │ • Rate Limit  │ │ • Rate Limit  │         │
│  │ • Validation  │ │ • Validation  │ │ • Validation  │         │
│  │ • Batching    │ │ • Batching    │ │ • Batching    │         │
│  └───────────────┘ └───────────────┘ └───────────────┘         │
│                            │                                     │
│                            ▼                                     │
│                    ┌───────────────┐                            │
│                    │    KAFKA      │                            │
│                    └───────────────┘                            │
└─────────────────────────────────────────────────────────────────┘
```

**What it is:**
The HTTP endpoint that receives events from video players and other data sources. It validates, authenticates, and forwards events to Kafka.

**Key responsibilities:**
| Function | Description |
|----------|-------------|
| **Authentication** | Validates API keys/tokens |
| **Rate Limiting** | Prevents abuse and DDoS |
| **Validation** | Basic payload validation |
| **Compression** | Accepts gzip-compressed payloads |
| **Batching** | Efficiently writes to Kafka |
| **Response** | Returns success/failure to client |

**Design considerations:**
- Deployed in multiple regions for low latency
- Stateless for horizontal scaling
- Async writes to Kafka (don't block on Kafka)
- Circuit breaker patterns for resilience

---

### 2.4 SDK/Agent Collectors

```
┌─────────────────────────────────────────────────────────────────┐
│                    SDK / AGENT COLLECTORS                        │
│                                                                  │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  PLAYER SDK (embedded in video player apps)              │    │
│  │                                                          │    │
│  │  • Automatic event capture (play, buffer, error, etc.)  │    │
│  │  • Offline buffering (for mobile apps)                  │    │
│  │  • Batching and compression                             │    │
│  │  • Retry logic with exponential backoff                 │    │
│  │  • Sampling configuration                               │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                  │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  SERVER-SIDE AGENTS (for backend services)               │    │
│  │                                                          │    │
│  │  • OpenTelemetry SDK integration                        │    │
│  │  • Automatic trace/metric collection                    │    │
│  │  • Log forwarding                                       │    │
│  │  • Low overhead (<1% CPU)                               │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                  │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  CDN LOG SHIPPER                                         │    │
│  │                                                          │    │
│  │  • Kafka Connect connectors                             │    │
│  │  • S3 log polling                                       │    │
│  │  • Real-time log streaming (where available)            │    │
│  └─────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
```

**What it is:**
Lightweight libraries and agents embedded in applications to automatically collect and send telemetry data.

**Why it's important:**
- Standardizes data collection across platforms
- Reduces integration effort for development teams
- Ensures consistent event formats
- Handles edge cases (offline, network issues)

---

## 3. STREAM PROCESSING LAYER

This layer transforms raw events into actionable insights in real-time.

### 3.1 Apache Flink

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         APACHE FLINK CLUSTER                             │
│                                                                          │
│  ┌────────────────────────────────────────────────────────────────┐     │
│  │                      JOB MANAGER (HA)                           │     │
│  │   • Coordinates distributed execution                          │     │
│  │   • Manages checkpoints                                        │     │
│  │   • Handles job submission                                     │     │
│  └────────────────────────────────────────────────────────────────┘     │
│                                    │                                     │
│          ┌─────────────────────────┼─────────────────────────┐          │
│          ▼                         ▼                         ▼          │
│  ┌──────────────┐         ┌──────────────┐         ┌──────────────┐    │
│  │ Task Manager │         │ Task Manager │         │ Task Manager │    │
│  │              │         │              │         │              │    │
│  │ ┌──────────┐ │         │ ┌──────────┐ │         │ ┌──────────┐ │    │
│  │ │  Slot 1  │ │         │ │  Slot 1  │ │         │ │  Slot 1  │ │    │
│  │ │  Slot 2  │ │         │ │  Slot 2  │ │         │ │  Slot 2  │ │    │
│  │ │  Slot 3  │ │         │ │  Slot 3  │ │         │ │  Slot 3  │ │    │
│  │ │  Slot 4  │ │         │ │  Slot 4  │ │         │ │  Slot 4  │ │    │
│  │ └──────────┘ │         │ └──────────┘ │         │ └──────────┘ │    │
│  └──────────────┘         └──────────────┘         └──────────────┘    │
│                                                                          │
│  State Backend: RocksDB | Checkpointing: S3 (every 60s)                 │
│  Parallelism: 256 | Exactly-once semantics enabled                      │
└─────────────────────────────────────────────────────────────────────────┘
```

**What it is:**
A distributed stream processing framework that processes millions of events per second with exactly-once guarantees.

**Why Flink (vs alternatives):**
| Feature | Flink | Spark Streaming | Kafka Streams |
|---------|-------|-----------------|---------------|
| Latency | Milliseconds | Seconds (micro-batch) | Milliseconds |
| State Management | Excellent | Good | Good |
| Exactly-once | Native | With effort | Native |
| Complex Event Processing | Excellent | Limited | Limited |
| Scaling | Excellent | Excellent | Per-partition |

---

### 3.2 Event Enrichment

```
┌─────────────────────────────────────────────────────────────────┐
│                     EVENT ENRICHMENT JOB                         │
│                                                                  │
│  Input Event                                                     │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │ {                                                        │    │
│  │   "session_id": "abc123",                               │    │
│  │   "client_ip": "203.0.113.45",                          │    │
│  │   "user_agent": "Mozilla/5.0...",                       │    │
│  │   "content_id": "movie_12345",                          │    │
│  │   ...                                                   │    │
│  │ }                                                        │    │
│  └─────────────────────────────────────────────────────────┘    │
│                              │                                   │
│          ┌───────────────────┼───────────────────┐              │
│          ▼                   ▼                   ▼              │
│  ┌──────────────┐   ┌──────────────┐   ┌──────────────┐        │
│  │   GeoIP      │   │   Device     │   │   Content    │        │
│  │   Lookup     │   │   Parser     │   │   Metadata   │        │
│  │              │   │              │   │   Join       │        │
│  │ • Country    │   │ • Device     │   │ • Title      │        │
│  │ • State      │   │   type       │   │ • Genre      │        │
│  │ • City       │   │ • OS         │   │ • Duration   │        │
│  │ • ISP        │   │ • Browser    │   │ • Bitrate    │        │
│  │ • ASN        │   │ • App version│   │   ladder     │        │
│  └──────────────┘   └──────────────┘   └──────────────┘        │
│                              │                                   │
│                              ▼                                   │
│  Enriched Event                                                  │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │ {                                                        │    │
│  │   "session_id": "abc123",                               │    │
│  │   "geo": {"country": "IN", "state": "MH", "city": "MUM"}│    │
│  │   "isp": "Jio", "asn": "AS55836",                       │    │
│  │   "device": {"type": "mobile", "os": "Android 13"},     │    │
│  │   "content": {"title": "Movie X", "genre": "Action"},   │    │
│  │   ...                                                   │    │
│  │ }                                                        │    │
│  └─────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
```

**What it is:**
Adds contextual information to raw events to enable dimensional analysis.

**Enrichment sources:**
| Enrichment | Source | Update Frequency |
|------------|--------|-----------------|
| GeoIP | MaxMind DB | Weekly |
| Device/UA | Device Atlas or custom | Monthly |
| ISP/ASN | IP database | Weekly |
| Content Metadata | Internal API/cache | Real-time |

**Why it's important:**
- Enables "rebuffer rate by ISP" or "errors by device type" analysis
- Raw IP/UA strings are not queryable efficiently
- Standardizes dimension values across all events

---

### 3.3 Session Aggregation

```
┌─────────────────────────────────────────────────────────────────┐
│                   SESSION AGGREGATION JOB                        │
│                                                                  │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │              SESSION STATE MACHINE                       │    │
│  │                                                          │    │
│  │    ┌──────────┐    play    ┌──────────┐                 │    │
│  │    │  INIT    │──────────▶│ PLAYING  │◀───┐             │    │
│  │    └──────────┘           └──────────┘    │             │    │
│  │         │                      │          │ resume      │    │
│  │         │ error           buffer│    ┌────┴────┐        │    │
│  │         ▼                      ▼    │ PAUSED  │        │    │
│  │    ┌──────────┐         ┌──────────┐└─────────┘        │    │
│  │    │  FAILED  │         │BUFFERING │                    │    │
│  │    └──────────┘         └──────────┘                    │    │
│  │                               │                          │    │
│  │                          end  │                          │    │
│  │                               ▼                          │    │
│  │                         ┌──────────┐                     │    │
│  │                         │  ENDED   │                     │    │
│  │                         └──────────┘                     │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                  │
│  Per-Session Computed Metrics:                                   │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │ • video_start_time: 1.82s                               │    │
│  │ • total_rebuffer_time: 3.2s                             │    │
│  │ • rebuffer_count: 2                                     │    │
│  │ • total_play_time: 45m 30s                              │    │
│  │ • avg_bitrate: 4.2 Mbps                                 │    │
│  │ • bitrate_switches: 3                                   │    │
│  │ • error_count: 0                                        │    │
│  │ • exit_state: COMPLETED                                 │    │
│  │ • content_completion: 95%                               │    │
│  └─────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
```

**What it is:**
Maintains stateful tracking of each playback session, computing aggregate metrics when the session ends.

**Key capabilities:**
| Feature | Description |
|---------|-------------|
| **Keyed State** | State is partitioned by session_id |
| **Session Windows** | Groups events by session with gap detection |
| **Late Event Handling** | Handles out-of-order events |
| **State TTL** | Automatically expires old sessions |

**Why it's important:**
- Individual events are too granular for analysis
- Session-level metrics are what matter for QoE
- Enables "sessions with rebuffering > 5s" type queries

---

### 3.4 Metric Computation (Real-Time Aggregation)

```
┌─────────────────────────────────────────────────────────────────┐
│                REAL-TIME AGGREGATION JOB                         │
│                                                                  │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │                 TUMBLING WINDOWS                         │    │
│  │                                                          │    │
│  │   10 seconds ──▶ Real-time dashboards                   │    │
│  │   1 minute   ──▶ Alerting                               │    │
│  │   5 minutes  ──▶ Trend analysis                         │    │
│  │   1 hour     ──▶ Hourly reports                         │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                  │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │                 DIMENSIONS                               │    │
│  │                                                          │    │
│  │   • content_id        • device_type                     │    │
│  │   • region (state)    • os                              │    │
│  │   • city              • app_version                     │    │
│  │   • isp               • cdn_provider                    │    │
│  │   • content_type      • player_version                  │    │
│  │     (VOD/LIVE)                                          │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                  │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │                 COMPUTED METRICS                         │    │
│  │                                                          │    │
│  │   • concurrent_viewers (count distinct session_id)      │    │
│  │   • play_attempts                                       │    │
│  │   • video_starts                                        │    │
│  │   • video_start_failures                                │    │
│  │   • avg_video_start_time (p50, p95, p99)               │    │
│  │   • rebuffer_ratio                                      │    │
│  │   • avg_bitrate                                         │    │
│  │   • error_rate                                          │    │
│  │   • exit_before_video_start_rate                        │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                  │
│  Output: Redis (real-time) + ClickHouse (historical)            │
└─────────────────────────────────────────────────────────────────┘
```

**What it is:**
Computes aggregate metrics over sliding/tumbling time windows, grouped by various dimensions.

**Example output:**
```json
{
  "window_start": "2024-01-15T10:30:00Z",
  "window_end": "2024-01-15T10:31:00Z",
  "dimensions": {
    "region": "Maharashtra",
    "isp": "Jio",
    "device_type": "mobile"
  },
  "metrics": {
    "concurrent_viewers": 45230,
    "avg_video_start_time_ms": 1820,
    "p95_video_start_time_ms": 3200,
    "rebuffer_ratio": 0.0034,
    "error_rate": 0.0012
  }
}
```

---

### 3.5 Anomaly Detection

```
┌─────────────────────────────────────────────────────────────────┐
│                   ANOMALY DETECTION JOB                          │
│                                                                  │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │              DETECTION METHODS                           │    │
│  │                                                          │    │
│  │  ┌─────────────────┐  ┌─────────────────┐               │    │
│  │  │   STATISTICAL   │  │   ML-BASED      │               │    │
│  │  │                 │  │                 │               │    │
│  │  │ • Z-score       │  │ • Isolation     │               │    │
│  │  │ • MAD (Median   │  │   Forest        │               │    │
│  │  │   Abs Deviation)│  │ • LSTM          │               │    │
│  │  │ • Percentile    │  │   (for trends)  │               │    │
│  │  │   thresholds    │  │ • Prophet       │               │    │
│  │  │ • Rate of change│  │   (seasonality) │               │    │
│  │  └─────────────────┘  └─────────────────┘               │    │
│  └─────────────────────────────────────────────────────────┘    │
│                              │                                   │
│                              ▼                                   │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │              CORRELATION ENGINE                          │    │
│  │                                                          │    │
│  │  "High rebuffer in MH region"                           │    │
│  │           +                                              │    │
│  │  "CDN latency spike in Mumbai POP"                      │    │
│  │           +                                              │    │
│  │  "ISP: Jio showing 3x normal errors"                    │    │
│  │           ▼                                              │    │
│  │  CORRELATED INCIDENT: CDN issue affecting Jio users     │    │
│  │  in Maharashtra                                          │    │
│  └─────────────────────────────────────────────────────────┘    │
│                              │                                   │
│                              ▼                                   │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │              ALERT ROUTING                               │    │
│  │                                                          │    │
│  │  • Severity classification (P1/P2/P3/P4)                │    │
│  │  • Deduplication (same root cause)                      │    │
│  │  • Routing rules (CDN team, NOC, Engineering)           │    │
│  │  • Escalation policies                                  │    │
│  └─────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
```

**What it is:**
Automatically detects unusual patterns in metrics that may indicate issues.

**Detection approaches:**
| Method | Best For | Example |
|--------|----------|---------|
| **Z-score** | Sudden spikes | Error rate jumps 5 std devs |
| **Percentile** | Absolute thresholds | VST > 5s (P95 threshold) |
| **Rate of change** | Gradual degradation | 20% increase over 10 mins |
| **ML (Isolation Forest)** | Multi-dimensional | Unusual combination of metrics |
| **Seasonality-aware** | Time-based patterns | Low traffic but not Sunday 3am |

**Why it's important:**
- Humans can't watch all metrics 24/7
- Early detection = faster resolution
- Reduces alert fatigue with correlation

---

## 4. STORAGE LAYER

Different storage systems optimized for different access patterns.

### 4.1 Hot Storage (Redis)

```
┌─────────────────────────────────────────────────────────────────┐
│                      REDIS CLUSTER                               │
│                                                                  │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  USE CASES                                               │    │
│  │                                                          │    │
│  │  1. ACTIVE SESSIONS                                      │    │
│  │     Key: session:{session_id}                           │    │
│  │     Value: {state, start_time, last_heartbeat, ...}     │    │
│  │     TTL: 30 minutes (auto-expire idle sessions)         │    │
│  │                                                          │    │
│  │  2. REAL-TIME COUNTERS                                   │    │
│  │     Key: viewers:content:{content_id}                   │    │
│  │     Type: HyperLogLog (for unique count)                │    │
│  │     Operations: PFADD, PFCOUNT                          │    │
│  │                                                          │    │
│  │  3. RECENT AGGREGATES                                    │    │
│  │     Key: metrics:1m:{timestamp}:{dimension}             │    │
│  │     Type: Hash                                          │    │
│  │     TTL: 1 hour                                         │    │
│  │                                                          │    │
│  │  4. PUB/SUB FOR DASHBOARDS                              │    │
│  │     Channel: dashboard:updates                          │    │
│  │     Subscribers: WebSocket servers                      │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                  │
│  Configuration: Cluster mode, 6 masters + 6 replicas            │
│  Memory: 256GB total | Latency: < 1ms                          │
└─────────────────────────────────────────────────────────────────┘
```

**What it is:**
In-memory data store for ultra-low latency access to real-time data.

**Why it's needed:**
- Sub-millisecond reads for live dashboards
- Active session tracking for concurrent viewer count
- Pub/Sub for pushing updates to dashboards

---

### 4.2 Hot Storage (Apache Druid)

```
┌─────────────────────────────────────────────────────────────────┐
│                      APACHE DRUID                                │
│                                                                  │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  ARCHITECTURE                                            │    │
│  │                                                          │    │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐   │    │
│  │  │   Broker     │  │ Coordinator  │  │  Overlord    │   │    │
│  │  │  (queries)   │  │  (segments)  │  │  (ingestion) │   │    │
│  │  └──────────────┘  └──────────────┘  └──────────────┘   │    │
│  │                                                          │    │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐   │    │
│  │  │  Historical  │  │  Historical  │  │MiddleManager │   │    │
│  │  │  (old data)  │  │  (old data)  │  │ (real-time)  │   │    │
│  │  └──────────────┘  └──────────────┘  └──────────────┘   │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                  │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  DATA MODEL                                              │    │
│  │                                                          │    │
│  │  Datasource: video_metrics                              │    │
│  │  Timestamp: __time (event time)                         │    │
│  │  Dimensions: region, isp, device_type, content_id, ...  │    │
│  │  Metrics: sum_play_time, count_sessions, sum_rebuffer..│    │
│  │                                                          │    │
│  │  Granularity: MINUTE (rolled up)                        │    │
│  │  Retention: 48 hours                                    │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                  │
│  Query Latency: < 100ms for most queries                        │
└─────────────────────────────────────────────────────────────────┘
```

**What it is:**
Real-time OLAP database designed for sub-second queries on time-series data.

**Why Druid:**
- Sub-second query performance at scale
- Real-time ingestion from Kafka
- Automatic rollup reduces storage
- SQL support

---

### 4.3 Warm Storage (ClickHouse)

```
┌─────────────────────────────────────────────────────────────────┐
│                      CLICKHOUSE CLUSTER                          │
│                                                                  │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  CLUSTER TOPOLOGY                                        │    │
│  │                                                          │    │
│  │  Shard 1          Shard 2          Shard 3              │    │
│  │  ┌─────────┐      ┌─────────┐      ┌─────────┐          │    │
│  │  │Replica 1│      │Replica 1│      │Replica 1│          │    │
│  │  │Replica 2│      │Replica 2│      │Replica 2│          │    │
│  │  └─────────┘      └─────────┘      └─────────┘          │    │
│  │                                                          │    │
│  │  Sharding: By toYYYYMMDD(timestamp)                     │    │
│  │  Replication: 2 replicas per shard                      │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                  │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  TABLES                                                  │    │
│  │                                                          │    │
│  │  1. sessions (MergeTree)                                │    │
│  │     - All session-level data                            │    │
│  │     - Partitioned by day                                │    │
│  │     - 90 days retention                                 │    │
│  │                                                          │    │
│  │  2. events (MergeTree)                                  │    │
│  │     - Raw event data (sampled or filtered)              │    │
│  │     - For deep debugging                                │    │
│  │     - 7 days retention                                  │    │
│  │                                                          │    │
│  │  3. metrics_1m (SummingMergeTree)                       │    │
│  │     - Pre-aggregated minute-level metrics               │    │
│  │     - Fast for dashboards                               │    │
│  │     - 90 days retention                                 │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                  │
│  Storage: 50TB+ | Query Latency: 100ms - 5s | Compression: 10x  │
└─────────────────────────────────────────────────────────────────┘
```

**What it is:**
Columnar OLAP database optimized for analytical queries on large datasets.

**Why ClickHouse:**
| Feature | Benefit |
|---------|---------|
| Columnar storage | Fast aggregations, 10x compression |
| Distributed queries | Scales horizontally |
| SQL support | Familiar query language |
| Materialized views | Pre-computed aggregations |
| Cost-effective | Handles TBs affordably |

---

### 4.4 Warm Storage (Elasticsearch)

```
┌─────────────────────────────────────────────────────────────────┐
│                      ELASTICSEARCH CLUSTER                       │
│                                                                  │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  USE CASES                                               │    │
│  │                                                          │    │
│  │  1. SESSION EXPLORER                                     │    │
│  │     "Find all sessions for user X in last 7 days"       │    │
│  │     "Sessions with error code ABC123"                   │    │
│  │     Full-text search on error messages                  │    │
│  │                                                          │    │
│  │  2. LOG SEARCH                                           │    │
│  │     CDN access logs                                     │    │
│  │     Backend service logs                                │    │
│  │     Correlated by request_id                            │    │
│  │                                                          │    │
│  │  3. ERROR ANALYSIS                                       │    │
│  │     Group errors by message pattern                     │    │
│  │     Identify new error types                            │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                  │
│  Index Strategy:                                                 │
│  - Daily indices: sessions-2024.01.15                           │
│  - ILM Policy: Hot (7d) → Warm (30d) → Delete                  │
│  - Replicas: 1 (2 copies of data)                               │
└─────────────────────────────────────────────────────────────────┘
```

**What it is:**
Full-text search engine for log analysis and session lookup.

**Why Elasticsearch:**
- Full-text search capabilities
- Great for debugging specific sessions
- Kibana for log visualization
- Handles semi-structured data well

---

### 4.5 Cold Storage (S3 / Data Lake)

```
┌─────────────────────────────────────────────────────────────────┐
│                      COLD STORAGE (S3)                           │
│                                                                  │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  BUCKET STRUCTURE                                        │    │
│  │                                                          │    │
│  │  s3://airtel-video-analytics/                           │    │
│  │  ├── raw-events/                                        │    │
│  │  │   └── year=2024/month=01/day=15/hour=10/            │    │
│  │  │       └── events_00000.parquet                       │    │
│  │  │                                                       │    │
│  │  ├── sessions/                                          │    │
│  │  │   └── year=2024/month=01/day=15/                    │    │
│  │  │       └── sessions_00000.parquet                     │    │
│  │  │                                                       │    │
│  │  ├── aggregates/                                        │    │
│  │  │   └── hourly/                                        │    │
│  │  │       └── 2024/01/15/                               │    │
│  │  │                                                       │    │
│  │  └── ml-training/                                       │    │
│  │      └── anomaly-detection/                             │    │
│  │          └── training_data.parquet                      │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                  │
│  Format: Parquet (columnar, compressed)                         │
│  Catalog: AWS Glue / Apache Hive Metastore                     │
│  Query Engine: Athena / Presto / Spark                         │
│  Retention: 1+ years                                            │
│  Storage Class: S3 Standard → Glacier (after 90 days)          │
└─────────────────────────────────────────────────────────────────┘
```

**What it is:**
Object storage for long-term retention of historical data.

**Why it's needed:**
| Use Case | Description |
|----------|-------------|
| **Compliance** | Regulatory requirements for data retention |
| **ML Training** | Historical data for model training |
| **Trend Analysis** | Year-over-year comparisons |
| **Disaster Recovery** | Backup of processed data |
| **Cost Efficiency** | Cheapest storage option |

---

## 5. APPLICATION LAYER

User-facing components that consume the data.

### 5.1 Real-Time Dashboards

```
┌─────────────────────────────────────────────────────────────────┐
│                    DASHBOARD ARCHITECTURE                        │
│                                                                  │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  FRONTEND (React/Vue)                                    │    │
│  │                                                          │    │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐   │    │
│  │  │   NOC        │  │  Engineering │  │  Executive   │   │    │
│  │  │  Dashboard   │  │   Dashboard  │  │  Dashboard   │   │    │
│  │  └──────────────┘  └──────────────┘  └──────────────┘   │    │
│  └─────────────────────────────────────────────────────────┘    │
│                              │                                   │
│                              ▼                                   │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  REAL-TIME UPDATE MECHANISM                              │    │
│  │                                                          │    │
│  │  Option 1: WebSocket                                     │    │
│  │  Dashboard ◄──── WebSocket Server ◄──── Redis Pub/Sub   │    │
│  │                                                          │    │
│  │  Option 2: Server-Sent Events (SSE)                     │    │
│  │  Dashboard ◄──── SSE Endpoint ◄──── Polling Redis       │    │
│  │                                                          │    │
│  │  Option 3: Polling (simplest)                           │    │
│  │  Dashboard ──── Poll API every 5s ──── Query Druid      │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                  │
│  Tools: Grafana (OSS) or Custom React Dashboard                 │
└─────────────────────────────────────────────────────────────────┘
```

**What it is:**
Visual interfaces that display real-time and historical metrics to various user personas.

**Dashboard types:**
| Dashboard | Audience | Refresh Rate | Key Metrics |
|-----------|----------|--------------|-------------|
| NOC Live | Operations | 5-10 seconds | Concurrent viewers, errors, alerts |
| Engineering | Developers | 1 minute | Error details, latency distributions |
| Executive | Leadership | Daily/Weekly | Trend summaries, SLA compliance |
| Content Ops | Content team | 1 minute | Per-content performance |

---

### 5.2 Alerting & Notification

```
┌─────────────────────────────────────────────────────────────────┐
│                    ALERTING SYSTEM                               │
│                                                                  │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  ALERT PIPELINE                                          │    │
│  │                                                          │    │
│  │  Anomaly      Alert       Dedup &     Route &    Notify  │    │
│  │  Detection ──▶ Queue ──▶ Correlate ──▶ Escalate ──▶      │    │
│  │  (Flink)     (Kafka)                                     │    │
│  │                                                          │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                  │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  NOTIFICATION CHANNELS                                   │    │
│  │                                                          │    │
│  │  ┌────────┐  ┌────────┐  ┌────────┐  ┌────────┐         │    │
│  │  │ Slack  │  │PagerDuty│  │ Email  │  │  SMS   │         │    │
│  │  └────────┘  └────────┘  └────────┘  └────────┘         │    │
│  │                                                          │    │
│  │  ┌────────┐  ┌────────┐                                 │    │
│  │  │ Teams  │  │Webhook │                                 │    │
│  │  │        │  │(Custom)│                                 │    │
│  │  └────────┘  └────────┘                                 │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                  │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  SEVERITY LEVELS                                         │    │
│  │                                                          │    │
│  │  P1 (Critical): >10% users affected, immediate page     │    │
│  │  P2 (High): >5% users affected, page within 15 min      │    │
│  │  P3 (Medium): Degradation, notify during business hours │    │
│  │  P4 (Low): Informational, ticket creation               │    │
│  └─────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
```

**What it is:**
System that detects issues and notifies the right teams through appropriate channels.

---

### 5.3 API Service

```
┌─────────────────────────────────────────────────────────────────┐
│                       API SERVICE                                │
│                                                                  │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  ENDPOINTS                                               │    │
│  │                                                          │    │
│  │  GET /api/v1/metrics/realtime                           │    │
│  │      Query real-time aggregates from Redis/Druid        │    │
│  │                                                          │    │
│  │  GET /api/v1/metrics/historical                         │    │
│  │      Query historical data from ClickHouse              │    │
│  │                                                          │    │
│  │  GET /api/v1/sessions/{session_id}                      │    │
│  │      Get details for a specific session                 │    │
│  │                                                          │    │
│  │  GET /api/v1/sessions/search                            │    │
│  │      Search sessions by user, content, time range       │    │
│  │                                                          │    │
│  │  GET /api/v1/alerts                                     │    │
│  │      List active and recent alerts                      │    │
│  │                                                          │    │
│  │  POST /api/v1/alerts/{id}/acknowledge                   │    │
│  │      Acknowledge an alert                               │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                  │
│  Features: Rate limiting, caching, authentication (OAuth2)      │
│  Format: REST or GraphQL                                        │
└─────────────────────────────────────────────────────────────────┘
```

**What it is:**
Backend APIs that power dashboards, enable integrations, and provide programmatic access to data.

---

### 5.4 ML Pipeline

```
┌─────────────────────────────────────────────────────────────────┐
│                       ML PIPELINE                                │
│                                                                  │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  TRAINING PIPELINE (Batch)                               │    │
│  │                                                          │    │
│  │  S3 (historical ──▶ Feature ──▶ Model ──▶ Model         │    │
│  │       data)        Engineering   Training   Registry     │    │
│  │                    (Spark)       (Python)   (MLflow)     │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                  │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  INFERENCE PIPELINE (Real-time)                          │    │
│  │                                                          │    │
│  │  Flink ──▶ Feature ──▶ Model ──▶ Prediction ──▶ Alert   │    │
│  │  Events    Vector      Serving    Output       if needed │    │
│  │                       (Triton/                           │    │
│  │                        TensorFlow                        │    │
│  │                        Serving)                          │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                  │
│  USE CASES:                                                      │
│  • Anomaly detection (unusual metric patterns)                  │
│  • Predictive alerting (predict issues before they occur)       │
│  • Root cause analysis (auto-suggest probable causes)           │
│  • Capacity forecasting (predict peak traffic)                  │
└─────────────────────────────────────────────────────────────────┘
```

**What it is:**
Infrastructure for training and deploying machine learning models that enhance the platform's intelligence.

---

### 5.5 Session Explorer

```
┌─────────────────────────────────────────────────────────────────┐
│                    SESSION EXPLORER                              │
│                                                                  │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  SEARCH INTERFACE                                        │    │
│  │                                                          │    │
│  │  ┌────────────────────────────────────────────────────┐ │    │
│  │  │ User ID: [__________]  Session ID: [__________]    │ │    │
│  │  │ Time Range: [Last 24 hours ▼]                      │ │    │
│  │  │ Filters: [Error sessions ▼] [Device: Mobile ▼]     │ │    │
│  │  │                                        [Search]    │ │    │
│  │  └────────────────────────────────────────────────────┘ │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                  │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  SESSION DETAIL VIEW                                     │    │
│  │                                                          │    │
│  │  Session: abc123xyz                                     │    │
│  │  User: user_456 | Device: iPhone 14 | ISP: Jio         │    │
│  │  Content: "Movie Title" | Duration: 2h 15m             │    │
│  │                                                          │    │
│  │  TIMELINE:                                               │    │
│  │  ──●────●─────●──────●───────●────────●───▶             │    │
│  │   Play  First  Buffer Buffer  Bitrate  End              │    │
│  │   Req   Frame  Start  End     Change                    │    │
│  │  0s    1.8s   45m    45.3s   1h 2m   2h 10m             │    │
│  │                                                          │    │
│  │  METRICS:                                                │    │
│  │  • Video Start Time: 1.82s                              │    │
│  │  • Total Rebuffer: 0.3s (1 event)                       │    │
│  │  • Avg Bitrate: 5.2 Mbps                                │    │
│  │  • Completion: 97%                                      │    │
│  └─────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
```

**What it is:**
A tool for customer support and engineering to investigate individual user sessions.

**Why it's important:**
- Diagnose customer complaints ("my video kept buffering")
- Debug specific issues reported by users
- Verify fix effectiveness for reported issues

---

## Summary: Data Flow Through Components

```
USER CLICKS PLAY
       │
       ▼
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│ Video Player │────▶│  Collector   │────▶│    Kafka     │
│   (SDK)      │     │  (API GW)    │     │   (Queue)    │
└──────────────┘     └──────────────┘     └──────────────┘
                                                 │
       ┌─────────────────────────────────────────┘
       ▼
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│    Flink     │────▶│   Storage    │────▶│  Dashboard   │
│ (Processing) │     │ (Redis/CH)   │     │   (Grafana)  │
└──────────────┘     └──────────────┘     └──────────────┘
       │
       ▼
┌──────────────┐     ┌──────────────┐
│   Anomaly    │────▶│   Alerting   │────▶ PagerDuty/Slack
│  Detection   │     │   System     │
└──────────────┘     └──────────────┘

END-TO-END LATENCY: < 5 SECONDS
```

---

*This component deep-dive complements the main design document and provides detailed understanding of each architectural element.*
