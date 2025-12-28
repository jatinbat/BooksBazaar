# Real-Time Performance Analytics & Experience Monitoring Platform

## Executive Summary

This document outlines the architecture and approach for building an in-house real-time analytics and monitoring platform for Airtel's digital video products (Xstream Play and IPTV). The platform will enable proactive insights, faster decision-making, and enhanced user experience across devices, regions, and networks.

---

## 1. Objectives & Scope

### 1.1 Platform Objectives

| Objective | Description |
|-----------|-------------|
| **Real-Time Visibility** | Provide instant visibility into video streaming performance across all platforms |
| **Proactive Issue Detection** | Identify and alert on degradations before they impact user experience at scale |
| **Data-Driven Decisions** | Enable teams to make informed decisions based on quantitative metrics |
| **Root Cause Analysis** | Rapidly identify the source of issues (CDN, network, device, content) |
| **Capacity Planning** | Inform infrastructure scaling decisions based on usage patterns |

### 1.2 Experience Metrics (Quality of Experience - QoE)

These metrics measure how users perceive the video streaming service:

| Metric | Description | Target |
|--------|-------------|--------|
| **Video Start Time (VST)** | Time from play request to first frame rendered | < 2 seconds |
| **Rebuffering Ratio** | Percentage of playback time spent buffering | < 0.5% |
| **Rebuffer Frequency** | Number of rebuffer events per hour of playback | < 1 per hour |
| **Video Start Failures (VSF)** | Percentage of play attempts that fail to start | < 1% |
| **Exit Before Video Start (EBVS)** | Users who abandon before playback begins | < 3% |
| **Playback Failure Rate** | Percentage of sessions with fatal errors | < 0.5% |
| **Average Bitrate** | Mean video quality delivered to users | Track by segment |
| **Bitrate Switches** | Frequency of quality changes during playback | < 2 per session |
| **Seek Latency** | Time to resume after user seeks | < 1 second |
| **Audio-Video Sync** | Lip sync issues detection | < 50ms drift |
| **Frame Drops** | Percentage of frames not rendered | < 0.1% |
| **Engagement Score** | Composite metric of watch time vs. content duration | Track trends |

### 1.3 Performance Metrics (Quality of Service - QoS)

These metrics measure the technical performance of the delivery infrastructure:

| Metric | Description | Source |
|--------|-------------|--------|
| **CDN Response Time** | Latency for manifest and segment requests | Player/CDN |
| **CDN Cache Hit Ratio** | Percentage of requests served from edge cache | CDN Logs |
| **Throughput** | Bytes per second during segment downloads | Player |
| **TCP Connection Time** | Time to establish connections | Player |
| **DNS Resolution Time** | Time for DNS lookups | Player |
| **TLS Handshake Time** | Time for secure connection setup | Player |
| **Origin Offload** | Percentage of traffic served from CDN vs origin | CDN/Origin |
| **Error Rates by Type** | HTTP 4xx, 5xx, timeout distributions | CDN/Player |
| **Segment Download Time** | Time to fetch each video/audio segment | Player |
| **Manifest Fetch Time** | Time to retrieve HLS/DASH manifests | Player |

### 1.4 Infrastructure & Network Metrics

| Metric | Description | Source |
|--------|-------------|--------|
| **Server CPU/Memory** | Resource utilization of transcoding/packaging servers | Backend |
| **Encoder Health** | Transcoding latency, queue depth, error rates | Encoding Pipeline |
| **DRM License Latency** | Time for license acquisition | DRM Server |
| **API Response Times** | Latency of metadata, auth, entitlement APIs | Backend Services |
| **ISP Performance** | QoE breakdown by Internet Service Provider | Aggregated Player Data |
| **Geographic Performance** | Metrics segmented by state/city/region | Player + GeoIP |
| **Device Performance** | Metrics by device type, OS, app version | Player |

### 1.5 Business & Engagement Metrics

| Metric | Description |
|--------|-------------|
| **Concurrent Viewers** | Real-time count of active streams |
| **Peak Viewership** | Maximum concurrent streams (by content, region, time) |
| **Session Duration** | Average and distribution of watch time |
| **Content Popularity** | Real-time trending content identification |
| **Churn Correlation** | Correlation between QoE issues and user churn |
| **Revenue Impact** | Estimated revenue impact of outages/degradations |

### 1.6 Key Users & Stakeholders

| User Group | Primary Use Cases | Key Dashboards/Features |
|------------|------------------|------------------------|
| **NOC (Network Operations Center)** | Real-time monitoring, incident detection, alerting | Live dashboards, alert management, runbooks |
| **Engineering Teams** | Root cause analysis, debugging, performance optimization | Drill-down analytics, log correlation, A/B testing |
| **Product Management** | Feature impact analysis, user experience trends | Engagement metrics, cohort analysis, funnel analysis |
| **Content Operations** | Content delivery health, encoding quality | Per-title analytics, encoding profiles comparison |
| **CDN/Infrastructure Team** | CDN performance, capacity planning, cost optimization | CDN analytics, traffic patterns, origin health |
| **Executive Leadership** | Business KPIs, SLA compliance, investment decisions | Executive summaries, trend reports, SLA dashboards |
| **Customer Support** | Individual user issue diagnosis | Session lookup, user journey replay |
| **Data Science Team** | Predictive models, anomaly detection, ML features | Raw data access, feature store, model serving |

---

## 2. Data & Architecture Overview

### 2.1 High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                                    DATA SOURCES                                          │
├─────────────────┬─────────────────┬─────────────────┬─────────────────┬─────────────────┤
│   Video Players │      CDNs       │ Backend Services│   Encoders      │  Third-Party    │
│  (Web/Mobile/   │  (Akamai/CF/    │  (Auth, DRM,    │  (Transcoding)  │  (APM, Logs)    │
│   STB/Smart TV) │   In-house)     │   Metadata)     │                 │                 │
└────────┬────────┴────────┬────────┴────────┬────────┴────────┬────────┴────────┬────────┘
         │                 │                 │                 │                 │
         ▼                 ▼                 ▼                 ▼                 ▼
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                              INGESTION LAYER                                             │
│  ┌─────────────────────────────────────────────────────────────────────────────────┐    │
│  │                        Apache Kafka / Amazon Kinesis                             │    │
│  │   ┌──────────────┐ ┌──────────────┐ ┌──────────────┐ ┌──────────────┐           │    │
│  │   │player-events │ │ cdn-logs     │ │backend-events│ │encoder-events│           │    │
│  │   │   (topic)    │ │   (topic)    │ │   (topic)    │ │   (topic)    │           │    │
│  │   └──────────────┘ └──────────────┘ └──────────────┘ └──────────────┘           │    │
│  └─────────────────────────────────────────────────────────────────────────────────┘    │
│                                                                                          │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐                          │
│  │ Schema Registry │  │  API Gateway    │  │  SDK/Agent      │                          │
│  │ (Avro/Protobuf) │  │  (REST/gRPC)    │  │  Collectors     │                          │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘                          │
└─────────────────────────────────────────────────────────────────────────────────────────┘
                                        │
                                        ▼
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                           STREAM PROCESSING LAYER                                        │
│  ┌─────────────────────────────────────────────────────────────────────────────────┐    │
│  │                    Apache Flink / Apache Spark Streaming                         │    │
│  │                                                                                   │    │
│  │   ┌────────────────┐  ┌────────────────┐  ┌────────────────┐                     │    │
│  │   │ Event          │  │ Session        │  │ Metric         │                     │    │
│  │   │ Enrichment     │  │ Aggregation    │  │ Computation    │                     │    │
│  │   │ (GeoIP, Device)│  │ (Windows)      │  │ (QoE/QoS)      │                     │    │
│  │   └────────────────┘  └────────────────┘  └────────────────┘                     │    │
│  │                                                                                   │    │
│  │   ┌────────────────┐  ┌────────────────┐  ┌────────────────┐                     │    │
│  │   │ Anomaly        │  │ Alerting       │  │ Real-time      │                     │    │
│  │   │ Detection      │  │ Rules Engine   │  │ Aggregates     │                     │    │
│  │   └────────────────┘  └────────────────┘  └────────────────┘                     │    │
│  └─────────────────────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────────────────────┘
                                        │
                    ┌───────────────────┼───────────────────┐
                    ▼                   ▼                   ▼
┌───────────────────────┐ ┌───────────────────────┐ ┌───────────────────────┐
│    HOT STORAGE        │ │    WARM STORAGE       │ │    COLD STORAGE       │
│  (Real-time Access)   │ │  (Interactive Query)  │ │  (Historical/Archive) │
├───────────────────────┤ ├───────────────────────┤ ├───────────────────────┤
│ • Redis/Druid         │ │ • ClickHouse          │ │ • S3/HDFS             │
│ • Time-series metrics │ │ • Apache Pinot        │ │ • Parquet files       │
│ • Active sessions     │ │ • Elasticsearch       │ │ • Long-term retention │
│ • Real-time counters  │ │ • 30-90 day retention │ │ • Compliance/Audit    │
│ • Sub-second queries  │ │ • Ad-hoc analytics    │ │ • ML Training Data    │
└───────────────────────┘ └───────────────────────┘ └───────────────────────┘
                    │                   │                   │
                    └───────────────────┼───────────────────┘
                                        ▼
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                              APPLICATION LAYER                                           │
│                                                                                          │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐     │
│  │  Real-time      │  │  Alerting &     │  │  API Service    │  │  ML Pipeline    │     │
│  │  Dashboards     │  │  Notification   │  │  (GraphQL/REST) │  │  (Predictions)  │     │
│  │  (Grafana/      │  │  (PagerDuty,    │  │                 │  │                 │     │
│  │   Custom UI)    │  │   Slack, Email) │  │                 │  │                 │     │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘  └─────────────────┘     │
│                                                                                          │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐     │
│  │  Session        │  │  Report         │  │  Data Export    │  │  Embedded       │     │
│  │  Explorer       │  │  Generator      │  │  (BI Tools)     │  │  Analytics      │     │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘  └─────────────────┘     │
└─────────────────────────────────────────────────────────────────────────────────────────┘
```

### 2.2 Data Collection Strategy

#### 2.2.1 Player Telemetry (Client-Side)

The video player SDK will be instrumented to collect and emit events:

```
┌─────────────────────────────────────────────────────────────────┐
│                     VIDEO PLAYER SDK                             │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │                  Telemetry Module                          │  │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐        │  │
│  │  │ Event       │  │ Metric      │  │ Buffer      │        │  │
│  │  │ Collector   │  │ Calculator  │  │ Manager     │        │  │
│  │  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘        │  │
│  │         │                │                │                │  │
│  │         ▼                ▼                ▼                │  │
│  │  ┌─────────────────────────────────────────────────────┐  │  │
│  │  │              Event Queue (In-Memory)                 │  │  │
│  │  └──────────────────────┬──────────────────────────────┘  │  │
│  │                         │                                  │  │
│  │                         ▼                                  │  │
│  │  ┌─────────────────────────────────────────────────────┐  │  │
│  │  │  Batch & Compress (gzip) → HTTPS POST to Collector  │  │  │
│  │  └─────────────────────────────────────────────────────┘  │  │
│  └───────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

**Player Events Schema (Protobuf/Avro):**

```protobuf
message PlayerEvent {
    string event_id = 1;           // Unique event identifier
    string session_id = 2;         // Playback session ID
    string user_id = 3;            // Anonymous or authenticated user ID
    string device_id = 4;          // Unique device identifier
    int64 timestamp = 5;           // Event timestamp (ms since epoch)
    
    // Event Type
    EventType event_type = 6;
    enum EventType {
        PLAYER_READY = 0;
        PLAY_REQUEST = 1;
        FIRST_FRAME = 2;
        REBUFFER_START = 3;
        REBUFFER_END = 4;
        BITRATE_CHANGE = 5;
        SEEK_START = 6;
        SEEK_END = 7;
        ERROR = 8;
        PAUSE = 9;
        RESUME = 10;
        SESSION_END = 11;
        HEARTBEAT = 12;
    }
    
    // Context
    string content_id = 7;
    string content_type = 8;       // VOD, LIVE, DVR
    string player_version = 9;
    string app_version = 10;
    
    // Device Info
    DeviceInfo device = 11;
    message DeviceInfo {
        string platform = 1;       // web, ios, android, stb, smarttv
        string os = 2;
        string os_version = 3;
        string device_model = 4;
        string browser = 5;
        int32 screen_width = 6;
        int32 screen_height = 7;
    }
    
    // Network Info
    NetworkInfo network = 12;
    message NetworkInfo {
        string connection_type = 1;  // wifi, cellular, ethernet
        string isp = 2;
        float downlink_mbps = 3;
        float rtt_ms = 4;
    }
    
    // Playback State
    PlaybackMetrics metrics = 13;
    message PlaybackMetrics {
        int64 playhead_position_ms = 1;
        int64 buffer_length_ms = 2;
        int32 current_bitrate_kbps = 3;
        int32 rendered_framerate = 4;
        int64 dropped_frames = 5;
        int64 total_bytes_downloaded = 6;
    }
    
    // Error Details (if event_type = ERROR)
    ErrorInfo error = 14;
    message ErrorInfo {
        string error_code = 1;
        string error_message = 2;
        string error_stack = 3;
        bool is_fatal = 4;
    }
    
    // Segment Timing (for granular CDN analysis)
    SegmentTiming segment = 15;
    message SegmentTiming {
        string segment_url = 1;
        int64 dns_time_ms = 2;
        int64 connect_time_ms = 3;
        int64 ttfb_ms = 4;
        int64 download_time_ms = 5;
        int64 segment_size_bytes = 6;
        int32 http_status = 7;
        string cdn_node = 8;
    }
}
```

#### 2.2.2 CDN Telemetry

**Approach 1: Real-time Log Streaming**
```
CDN Edge Servers → Kafka Connect → Kafka Topics
                   (Syslog/HTTP)
```

**Approach 2: CDN API Polling**
```
Scheduled Jobs → CDN Analytics API → Parse & Transform → Kafka
(every 1 min)    (Akamai, CloudFront)
```

**CDN Log Schema:**
```json
{
    "timestamp": "2024-01-15T10:30:00.123Z",
    "edge_location": "DEL",
    "pop_id": "DEL-E1",
    "client_ip": "hashed/anonymized",
    "request_url": "/content/12345/segment_001.ts",
    "http_method": "GET",
    "http_status": 200,
    "bytes_sent": 2048576,
    "time_taken_ms": 45,
    "cache_status": "HIT",
    "origin_fetch_time_ms": null,
    "user_agent": "ExoPlayer/2.18.1",
    "referer": "https://xstreamplay.airtel.in",
    "content_type": "video/MP2T",
    "cdn_request_id": "abc123"
}
```

#### 2.2.3 Backend Services Telemetry

```
┌─────────────────────────────────────────────────────────────────┐
│                    BACKEND SERVICES                              │
│                                                                  │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐              │
│  │ Auth/       │  │ Entitlement │  │ Metadata    │              │
│  │ Identity    │  │ Service     │  │ Service     │              │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘              │
│         │                │                │                      │
│         │    OpenTelemetry SDK / Micrometer / StatsD            │
│         │                │                │                      │
│         ▼                ▼                ▼                      │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │              OpenTelemetry Collector                       │  │
│  │  (Traces, Metrics, Logs → OTLP Export to Kafka)           │  │
│  └───────────────────────────────────────────────────────────┘  │
│                                                                  │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐              │
│  │ DRM License │  │ Packaging   │  │ Transcoding │              │
│  │ Server      │  │ Origin      │  │ Pipeline    │              │
│  └─────────────┘  └─────────────┘  └─────────────┘              │
└─────────────────────────────────────────────────────────────────┘
```

### 2.3 Stream Processing Architecture

#### 2.3.1 Processing Pipeline (Apache Flink)

```
                    Raw Events (Kafka)
                           │
                           ▼
            ┌──────────────────────────────┐
            │     Event Validation &       │
            │     Schema Enforcement       │
            └──────────────┬───────────────┘
                           │
                           ▼
            ┌──────────────────────────────┐
            │     Event Enrichment         │
            │  • GeoIP lookup              │
            │  • Device classification     │
            │  • Content metadata join     │
            │  • ISP identification        │
            └──────────────┬───────────────┘
                           │
              ┌────────────┼────────────┐
              │            │            │
              ▼            ▼            ▼
    ┌─────────────┐ ┌─────────────┐ ┌─────────────┐
    │   Session   │ │  Real-time  │ │   Anomaly   │
    │ Aggregation │ │  Counters   │ │  Detection  │
    │ (5s/1m/5m)  │ │  (per dim)  │ │    (ML)     │
    └──────┬──────┘ └──────┬──────┘ └──────┬──────┘
           │               │               │
           ▼               ▼               ▼
    ┌─────────────┐ ┌─────────────┐ ┌─────────────┐
    │ ClickHouse  │ │   Redis     │ │   Alerts    │
    │   (OLAP)    │ │  (Counters) │ │  (Kafka)    │
    └─────────────┘ └─────────────┘ └─────────────┘
```

#### 2.3.2 Key Processing Jobs

**1. Session State Machine**
```
Maintains stateful session tracking using Flink's keyed state:

Session States: INITIALIZING → PLAYING → BUFFERING → PAUSED → ENDED

Computes per-session:
- Total watch time
- Total rebuffer time  
- Rebuffer count
- Bitrate changes
- Error occurrences
- Exit state (completed, error, user_exit)
```

**2. Real-Time Aggregations**
```
Tumbling Windows: 10s, 1m, 5m, 1h

Dimensions:
- Content ID
- Device Type
- Region (State/City)
- ISP
- CDN Node
- App Version

Metrics:
- Concurrent viewers (count distinct sessions)
- Avg video start time
- Rebuffer ratio
- Error rate
- Avg bitrate
```

**3. Anomaly Detection Pipeline**
```
┌─────────────────────────────────────────────────┐
│              Anomaly Detection                   │
│                                                  │
│  Input: Real-time metric aggregates              │
│                                                  │
│  ┌─────────────┐  ┌─────────────┐               │
│  │ Statistical │  │ ML-Based    │               │
│  │ (Z-score,   │  │ (Isolation  │               │
│  │  MAD)       │  │  Forest)    │               │
│  └──────┬──────┘  └──────┬──────┘               │
│         │                │                       │
│         ▼                ▼                       │
│  ┌─────────────────────────────────────┐        │
│  │     Correlation Engine              │        │
│  │  (Group related anomalies)          │        │
│  └──────────────────┬──────────────────┘        │
│                     │                            │
│                     ▼                            │
│  ┌─────────────────────────────────────┐        │
│  │     Alert Deduplication & Routing   │        │
│  └─────────────────────────────────────┘        │
└─────────────────────────────────────────────────┘
```

### 2.4 Storage Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         STORAGE LAYER                                    │
│                                                                          │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │                    HOT TIER (0-1 hours)                          │    │
│  │  ┌─────────────────────────┐  ┌─────────────────────────┐       │    │
│  │  │        Redis            │  │     Apache Druid        │       │    │
│  │  │  • Active sessions      │  │  • Sub-second OLAP      │       │    │
│  │  │  • Real-time counters   │  │  • Time-series metrics  │       │    │
│  │  │  • Rate limiting        │  │  • Real-time dashboards │       │    │
│  │  │  • Pub/Sub for live     │  │                         │       │    │
│  │  │    dashboard updates    │  │                         │       │    │
│  │  └─────────────────────────┘  └─────────────────────────┘       │    │
│  └─────────────────────────────────────────────────────────────────┘    │
│                                                                          │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │                   WARM TIER (1 hour - 90 days)                   │    │
│  │  ┌─────────────────────────┐  ┌─────────────────────────┐       │    │
│  │  │      ClickHouse         │  │    Elasticsearch        │       │    │
│  │  │  • Session aggregates   │  │  • Log search           │       │    │
│  │  │  • Dimensional analysis │  │  • Error analysis       │       │    │
│  │  │  • Ad-hoc queries       │  │  • Full-text search     │       │    │
│  │  │  • Historical trends    │  │  • Session explorer     │       │    │
│  │  └─────────────────────────┘  └─────────────────────────┘       │    │
│  └─────────────────────────────────────────────────────────────────┘    │
│                                                                          │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │                   COLD TIER (90+ days)                           │    │
│  │  ┌─────────────────────────┐  ┌─────────────────────────┐       │    │
│  │  │      S3 / HDFS          │  │     Data Lakehouse      │       │    │
│  │  │  • Raw events (Parquet) │  │  (Delta Lake / Iceberg) │       │    │
│  │  │  • Compliance archive   │  │  • ML training data     │       │    │
│  │  │  • Disaster recovery    │  │  • Historical analysis  │       │    │
│  │  └─────────────────────────┘  └─────────────────────────┘       │    │
│  └─────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────┘
```

### 2.5 Data Flow Summary

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              END-TO-END DATA FLOW                                │
│                                                                                  │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐  │
│  │  Player  │───▶│ Collector│───▶│  Kafka   │───▶│  Flink   │───▶│  Storage │  │
│  │  Event   │    │  (API)   │    │  Topic   │    │  Job     │    │  Layer   │  │
│  └──────────┘    └──────────┘    └──────────┘    └──────────┘    └──────────┘  │
│       │                                │                               │         │
│       │                                │                               │         │
│       │         Latency Targets:       │                               │         │
│       │         ─────────────────      │                               │         │
│       │         • Player → Kafka: < 500ms                              │         │
│       │         • Kafka → Flink processing: < 1s                       │         │
│       │         • Flink → Dashboard: < 2s                              │         │
│       │         • End-to-end: < 5 seconds                              │         │
│       │                                                                │         │
│       ▼                                                                ▼         │
│  ┌──────────────────────────────────────────────────────────────────────────┐   │
│  │                         REAL-TIME DASHBOARD                               │   │
│  │   ┌─────────────────────────────────────────────────────────────────┐    │   │
│  │   │  Concurrent Viewers: 1.2M  │  Avg VST: 1.8s  │  Rebuffer: 0.3%  │    │   │
│  │   └─────────────────────────────────────────────────────────────────┘    │   │
│  └──────────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 2.6 Scalability Considerations

| Component | Scaling Strategy | Target Scale |
|-----------|-----------------|--------------|
| **Kafka** | Partition by session_id, horizontal scaling | 10M events/second |
| **Flink** | Keyed streams, checkpointing, auto-scaling | 5M sessions concurrent |
| **Redis** | Cluster mode, read replicas | 1M ops/second |
| **ClickHouse** | Sharding by time, distributed queries | 100B+ rows |
| **API Layer** | Kubernetes HPA, CDN caching | 100K req/second |

### 2.7 Reliability & Fault Tolerance

```
┌─────────────────────────────────────────────────────────────────┐
│                    RELIABILITY ARCHITECTURE                      │
│                                                                  │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  INGESTION                                               │    │
│  │  • Client-side buffering & retry                        │    │
│  │  • Multi-region collectors (active-active)              │    │
│  │  • Kafka replication factor = 3                         │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                  │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  PROCESSING                                              │    │
│  │  • Flink exactly-once semantics                         │    │
│  │  • Checkpointing to S3 (every 1 minute)                 │    │
│  │  • Job failure auto-restart                             │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                  │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  STORAGE                                                 │    │
│  │  • ClickHouse replication (2 replicas per shard)        │    │
│  │  • Redis Cluster with automatic failover                │    │
│  │  • S3 cross-region replication                          │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                  │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  MONITORING THE MONITOR                                  │    │
│  │  • Platform health dashboards (meta-monitoring)         │    │
│  │  • Data freshness checks                                │    │
│  │  • Lag monitoring for Kafka consumers                   │    │
│  │  • Synthetic monitoring (test streams)                  │    │
│  └─────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
```

---

## 3. Implementation Phases

### Phase 1: Foundation (Months 1-3)
- [ ] Deploy Kafka cluster and define event schemas
- [ ] Implement player SDK telemetry module
- [ ] Set up basic Flink processing jobs
- [ ] Deploy ClickHouse and Redis clusters
- [ ] Build core dashboards (concurrent viewers, VST, rebuffer)

### Phase 2: Enrichment (Months 4-6)
- [ ] Integrate CDN log streaming
- [ ] Add GeoIP and ISP enrichment
- [ ] Implement session-level aggregation
- [ ] Build alerting infrastructure
- [ ] Deploy session explorer for support teams

### Phase 3: Intelligence (Months 7-9)
- [ ] Implement anomaly detection
- [ ] Build correlation engine
- [ ] Add predictive capabilities
- [ ] Integrate with incident management
- [ ] Build self-service analytics portal

### Phase 4: Optimization (Months 10-12)
- [ ] Performance tuning at scale
- [ ] Cost optimization
- [ ] Advanced ML models
- [ ] A/B testing integration
- [ ] Full production rollout

---

## 4. Success Metrics

| KPI | Target |
|-----|--------|
| Data freshness (event to dashboard) | < 5 seconds |
| Platform availability | 99.9% |
| Alert false positive rate | < 5% |
| Mean time to detect (MTTD) | < 2 minutes |
| Query latency (P95) | < 500ms |
| Cost per million events | Defined post-baseline |

---

## Appendix A: Technology Stack Summary

| Layer | Primary Technology | Alternatives |
|-------|-------------------|--------------|
| Message Queue | Apache Kafka | Amazon Kinesis, Pulsar |
| Stream Processing | Apache Flink | Spark Streaming, Kafka Streams |
| OLAP Database | ClickHouse | Apache Druid, Apache Pinot |
| Cache/Real-time | Redis Cluster | Apache Ignite |
| Search/Logs | Elasticsearch | OpenSearch |
| Object Storage | S3 | MinIO, HDFS |
| Visualization | Grafana + Custom UI | Superset, Metabase |
| Alerting | Custom + PagerDuty | OpsGenie, VictorOps |
| Container Orchestration | Kubernetes | ECS |
| Observability | OpenTelemetry + Prometheus | Datadog, New Relic |

---

## Appendix B: Sample Dashboard Layouts

### NOC Real-Time Dashboard
```
┌─────────────────────────────────────────────────────────────────────────┐
│  AIRTEL VIDEO ANALYTICS - NOC DASHBOARD                    🟢 HEALTHY   │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐ │
│  │ CONCURRENT   │  │ VIDEO START  │  │ REBUFFER     │  │ ERROR RATE   │ │
│  │ VIEWERS      │  │ TIME         │  │ RATIO        │  │              │ │
│  │              │  │              │  │              │  │              │ │
│  │  1,245,892   │  │   1.82s      │  │   0.34%      │  │   0.12%      │ │
│  │   ▲ 12.3%    │  │   ▼ 0.1s     │  │   ▼ 0.02%    │  │   ▼ 0.01%    │ │
│  └──────────────┘  └──────────────┘  └──────────────┘  └──────────────┘ │
│                                                                          │
│  ┌─────────────────────────────────────────────────────────────────────┐ │
│  │                    VIEWERS BY REGION (INDIA MAP)                    │ │
│  │                         [Heat Map Visual]                           │ │
│  └─────────────────────────────────────────────────────────────────────┘ │
│                                                                          │
│  ┌──────────────────────────────┐  ┌──────────────────────────────────┐ │
│  │  TOP CONTENT (LIVE)          │  │  ACTIVE ALERTS                   │ │
│  │  1. IPL Match - 450K         │  │  ⚠ High rebuffer - MH region    │ │
│  │  2. Movie XYZ - 120K         │  │  ⚠ CDN latency spike - DEL      │ │
│  │  3. News Live - 85K          │  │                                  │ │
│  └──────────────────────────────┘  └──────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────┘
```

---

*Document Version: 1.0*  
*Last Updated: December 2024*  
*Author: Platform Architecture Team*
