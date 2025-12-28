# Data Flow Through the Pipeline - With Examples

This document traces how data flows through the entire analytics platform, showing the exact format and transformations at each stage.

---

## Overview: The Journey of a Single Play Event

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                           DATA TRANSFORMATION JOURNEY                                │
│                                                                                      │
│  STAGE 1          STAGE 2          STAGE 3          STAGE 4          STAGE 5       │
│  ────────         ────────         ────────         ────────         ────────       │
│                                                                                      │
│  Raw Events  ──▶  Validated   ──▶  Enriched    ──▶  Aggregated  ──▶  Visualized   │
│  (Various        Events           Events           Metrics          Insights        │
│   Formats)       (Kafka)          (Flink)          (Storage)        (Dashboard)     │
│                                                                                      │
│  JSON/Binary     Avro/Proto       Enriched         Time-series      Charts &        │
│  HTTP/gRPC       Unified          + Context        + Dimensional    Alerts          │
│  Logs            Schema                            Rollups                          │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

---

## Scenario: User Watches a Movie on Xstream Play

Let's follow a real user session through the entire pipeline:

**User Context:**
- Rahul in Mumbai opens Xstream Play on his Android phone
- He's on Jio 4G network
- He starts watching "Jawan" (movie)
- He experiences one buffering event
- He watches for 45 minutes and exits

---

## STAGE 1: Data Generation (Multiple Sources, Different Formats)

### 1.1 Player SDK Events (JSON over HTTPS)

The video player SDK generates events as the user interacts:

**Event 1: Play Request** (when user taps play)
```json
{
  "event_id": "evt_a1b2c3d4",
  "event_type": "PLAY_REQUEST",
  "timestamp": 1705312200000,
  "session_id": "sess_xyz789",
  "user_id": "user_rahul_123",
  "device_id": "dev_android_456",
  "content_id": "movie_jawan_2023",
  "player_version": "3.2.1",
  "app_version": "5.4.0",
  "device": {
    "platform": "android",
    "os": "Android 14",
    "model": "Samsung Galaxy S23",
    "screen_width": 1080,
    "screen_height": 2340
  },
  "network": {
    "connection_type": "cellular",
    "downlink_mbps": 15.2,
    "rtt_ms": 45
  },
  "client_ip": "49.36.128.45"
}
```

**Event 2: First Frame Rendered** (video starts playing)
```json
{
  "event_id": "evt_e5f6g7h8",
  "event_type": "FIRST_FRAME",
  "timestamp": 1705312201820,
  "session_id": "sess_xyz789",
  "user_id": "user_rahul_123",
  "metrics": {
    "time_to_first_frame_ms": 1820,
    "initial_bitrate_kbps": 4500,
    "initial_resolution": "1080p"
  },
  "segment_timing": {
    "manifest_fetch_ms": 120,
    "first_segment_fetch_ms": 450,
    "drm_license_ms": 380
  }
}
```

**Event 3: Heartbeat** (sent every 30 seconds during playback)
```json
{
  "event_id": "evt_h1h2h3h4",
  "event_type": "HEARTBEAT",
  "timestamp": 1705312230000,
  "session_id": "sess_xyz789",
  "metrics": {
    "playhead_position_ms": 28000,
    "buffer_length_ms": 15000,
    "current_bitrate_kbps": 4500,
    "rendered_framerate": 24,
    "dropped_frames": 2,
    "total_bytes_downloaded": 15728640
  }
}
```

**Event 4: Rebuffer Start** (buffering begins)
```json
{
  "event_id": "evt_r1r2r3r4",
  "event_type": "REBUFFER_START",
  "timestamp": 1705314000000,
  "session_id": "sess_xyz789",
  "metrics": {
    "playhead_position_ms": 1800000,
    "buffer_length_ms": 0,
    "current_bitrate_kbps": 4500
  }
}
```

**Event 5: Rebuffer End** (playback resumes)
```json
{
  "event_id": "evt_r5r6r7r8",
  "event_type": "REBUFFER_END",
  "timestamp": 1705314002300,
  "session_id": "sess_xyz789",
  "metrics": {
    "rebuffer_duration_ms": 2300,
    "playhead_position_ms": 1800000,
    "buffer_length_ms": 8000,
    "new_bitrate_kbps": 2500
  }
}
```

**Event 6: Session End** (user exits)
```json
{
  "event_id": "evt_s1s2s3s4",
  "event_type": "SESSION_END",
  "timestamp": 1705314900000,
  "session_id": "sess_xyz789",
  "exit_reason": "USER_EXIT",
  "metrics": {
    "total_play_time_ms": 2700000,
    "total_rebuffer_time_ms": 2300,
    "rebuffer_count": 1,
    "content_duration_ms": 9000000,
    "completion_percentage": 30
  }
}
```

---

### 1.2 CDN Logs (Different Format - Access Logs)

Simultaneously, the CDN generates access logs for every segment request:

**CDN Log Format (Akamai Extended Log)**
```
#Fields: date time cs-ip cs-method cs-uri sc-status sc-bytes time-taken cache-status
2024-01-15 10:30:01 49.36.128.45 GET /content/jawan/segment_0001.ts 200 2097152 45 HIT
2024-01-15 10:30:05 49.36.128.45 GET /content/jawan/segment_0002.ts 200 2097152 52 HIT
2024-01-15 10:30:09 49.36.128.45 GET /content/jawan/segment_0003.ts 200 2097152 1250 MISS
2024-01-15 10:30:14 49.36.128.45 GET /content/jawan/segment_0004.ts 504 0 30000 ERR
2024-01-15 10:30:16 49.36.128.45 GET /content/jawan/segment_0004.ts 200 2097152 89 HIT
```

**Parsed CDN Log (JSON)**
```json
{
  "timestamp": "2024-01-15T10:30:14.000Z",
  "edge_location": "MUM",
  "pop_id": "MUM-E2",
  "client_ip_hash": "a1b2c3d4e5",
  "http_method": "GET",
  "request_url": "/content/jawan/segment_0004.ts",
  "http_status": 504,
  "bytes_sent": 0,
  "time_taken_ms": 30000,
  "cache_status": "ERR",
  "origin_response_time_ms": null,
  "user_agent": "ExoPlayer/2.19.1",
  "cdn_request_id": "req_cdn_abc123"
}
```

---

### 1.3 Backend Service Logs (OpenTelemetry Format)

The DRM license server logs the license request:

**OpenTelemetry Trace Span**
```json
{
  "traceId": "abc123def456",
  "spanId": "span_789",
  "operationName": "drm.license.acquire",
  "startTime": 1705312200150,
  "duration": 380,
  "tags": {
    "service.name": "drm-license-server",
    "content.id": "movie_jawan_2023",
    "drm.type": "widevine",
    "user.id": "user_rahul_123",
    "device.type": "android",
    "license.type": "streaming",
    "http.status_code": 200
  },
  "logs": [
    {"timestamp": 1705312200200, "message": "License generated successfully"}
  ]
}
```

**Backend Metrics (Prometheus Format)**
```
# HELP drm_license_request_duration_seconds DRM license request duration
# TYPE drm_license_request_duration_seconds histogram
drm_license_request_duration_seconds_bucket{content_type="movie",drm_type="widevine",le="0.1"} 450
drm_license_request_duration_seconds_bucket{content_type="movie",drm_type="widevine",le="0.5"} 892
drm_license_request_duration_seconds_bucket{content_type="movie",drm_type="widevine",le="1.0"} 923
drm_license_request_duration_seconds_count{content_type="movie",drm_type="widevine"} 925
drm_license_request_duration_seconds_sum{content_type="movie",drm_type="widevine"} 285.5
```

---

## STAGE 2: Ingestion Layer (Normalization & Validation)

### 2.1 Collector API Receives Events

The player batches multiple events and sends them compressed:

**HTTP Request to Collector**
```http
POST /v1/events HTTP/1.1
Host: collector.xstreamplay.airtel.in
Content-Type: application/json
Content-Encoding: gzip
X-API-Key: ak_player_prod_xxxxx
X-Device-ID: dev_android_456

[
  {"event_id": "evt_a1b2c3d4", "event_type": "PLAY_REQUEST", ...},
  {"event_id": "evt_e5f6g7h8", "event_type": "FIRST_FRAME", ...},
  {"event_id": "evt_h1h2h3h4", "event_type": "HEARTBEAT", ...}
]
```

**Collector Processing:**
```
┌─────────────────────────────────────────────────────────────────┐
│                    COLLECTOR PROCESSING                          │
│                                                                  │
│  1. DECOMPRESS (gzip)                                           │
│     └─▶ Raw JSON array                                          │
│                                                                  │
│  2. AUTHENTICATE                                                 │
│     └─▶ Validate API key: ak_player_prod_xxxxx ✓                │
│                                                                  │
│  3. VALIDATE SCHEMA                                              │
│     └─▶ Check required fields exist ✓                           │
│     └─▶ Check data types correct ✓                              │
│     └─▶ Check timestamp not in future ✓                         │
│                                                                  │
│  4. NORMALIZE                                                    │
│     └─▶ Add server_received_at timestamp                        │
│     └─▶ Add collector_region: "ap-south-1"                      │
│                                                                  │
│  5. SERIALIZE TO AVRO                                            │
│     └─▶ Convert JSON to Avro binary format                      │
│                                                                  │
│  6. WRITE TO KAFKA                                               │
│     └─▶ Topic: player-events                                    │
│     └─▶ Partition: hash(session_id) % 128 = 42                  │
│     └─▶ Key: sess_xyz789                                        │
└─────────────────────────────────────────────────────────────────┘
```

### 2.2 Events in Kafka (Unified Avro Format)

All events are now in a consistent Avro schema:

**Kafka Record (player-events topic, partition 42)**
```
Key: sess_xyz789
Headers:
  - source: player-sdk
  - schema-id: 15
  - collector-region: ap-south-1

Value (Avro Binary - shown as JSON equivalent):
{
  "event_id": "evt_a1b2c3d4",
  "event_type": "PLAY_REQUEST",
  "timestamp": 1705312200000,
  "server_received_at": 1705312200450,
  "session_id": "sess_xyz789",
  "user_id": "user_rahul_123",
  "device_id": "dev_android_456",
  "content_id": "movie_jawan_2023",
  "client_ip": "49.36.128.45",
  "player_version": "3.2.1",
  "app_version": "5.4.0",
  "device": {
    "platform": "android",
    "os": "Android 14",
    "model": "Samsung Galaxy S23",
    "screen_width": 1080,
    "screen_height": 2340
  },
  "network": {
    "connection_type": "cellular",
    "downlink_mbps": 15.2,
    "rtt_ms": 45
  },
  "playback_metrics": null,
  "error_info": null,
  "segment_timing": null
}
```

### 2.3 CDN Logs in Kafka (Different Topic, Same Pipeline)

CDN logs are parsed and written to a separate topic:

**Kafka Record (cdn-logs topic)**
```
Key: MUM-E2
Value:
{
  "timestamp": 1705314014000,
  "source": "akamai",
  "edge_location": "MUM",
  "pop_id": "MUM-E2",
  "client_ip_hash": "a1b2c3d4e5",
  "request_path": "/content/jawan/segment_0004.ts",
  "content_id": "movie_jawan_2023",
  "segment_number": 4,
  "http_status": 504,
  "bytes_sent": 0,
  "time_taken_ms": 30000,
  "cache_status": "ERR",
  "is_error": true
}
```

---

## STAGE 3: Stream Processing (Apache Flink)

### 3.1 Event Enrichment Job

**Input:** Raw event from Kafka
**Output:** Enriched event with additional context

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         ENRICHMENT TRANSFORMATIONS                               │
│                                                                                  │
│  RAW EVENT                           ENRICHED EVENT                             │
│  ──────────                          ──────────────                             │
│                                                                                  │
│  "client_ip": "49.36.128.45"   ──▶   "geo": {                                  │
│                                         "country": "IN",                        │
│          GeoIP Lookup                   "country_name": "India",               │
│                                         "region": "MH",                         │
│                                         "region_name": "Maharashtra",          │
│                                         "city": "Mumbai",                       │
│                                         "latitude": 19.0760,                   │
│                                         "longitude": 72.8777,                  │
│                                         "isp": "Reliance Jio",                 │
│                                         "asn": "AS55836",                      │
│                                         "connection_type": "mobile"            │
│                                       }                                         │
│                                                                                  │
│  "device": {                     ──▶   "device_parsed": {                       │
│    "platform": "android",               "type": "smartphone",                  │
│    "model": "Samsung Galaxy S23"        "brand": "Samsung",                    │
│  }                                      "model": "Galaxy S23",                 │
│          Device Parsing                 "os_family": "Android",                │
│                                         "os_version": "14.0",                  │
│                                         "is_tablet": false,                    │
│                                         "is_smart_tv": false,                  │
│                                         "screen_category": "FHD+"             │
│                                       }                                         │
│                                                                                  │
│  "content_id":                   ──▶   "content": {                             │
│    "movie_jawan_2023"                   "title": "Jawan",                      │
│                                         "type": "movie",                        │
│          Content Metadata Join          "genre": "Action",                     │
│          (from Redis/API)               "language": "Hindi",                   │
│                                         "duration_ms": 9000000,                │
│                                         "release_year": 2023,                  │
│                                         "rating": "UA",                        │
│                                         "bitrate_ladder": [                    │
│                                           {"resolution": "480p", "kbps": 800}, │
│                                           {"resolution": "720p", "kbps": 2500},│
│                                           {"resolution": "1080p", "kbps": 4500}│
│                                         ]                                       │
│                                       }                                         │
└─────────────────────────────────────────────────────────────────────────────────┘
```

**Enriched Event (Complete)**
```json
{
  "event_id": "evt_a1b2c3d4",
  "event_type": "PLAY_REQUEST",
  "timestamp": 1705312200000,
  "server_received_at": 1705312200450,
  "processing_time": 1705312200520,
  
  "session_id": "sess_xyz789",
  "user_id": "user_rahul_123",
  "device_id": "dev_android_456",
  "content_id": "movie_jawan_2023",
  
  "geo": {
    "country": "IN",
    "region": "MH",
    "city": "Mumbai",
    "isp": "Reliance Jio",
    "asn": "AS55836"
  },
  
  "device_parsed": {
    "type": "smartphone",
    "brand": "Samsung",
    "os_family": "Android",
    "os_version": "14.0"
  },
  
  "content": {
    "title": "Jawan",
    "type": "movie",
    "genre": "Action",
    "duration_ms": 9000000
  },
  
  "network": {
    "connection_type": "cellular",
    "downlink_mbps": 15.2
  },
  
  "app_version": "5.4.0",
  "player_version": "3.2.1"
}
```

---

### 3.2 Session Aggregation Job

**Purpose:** Track state across all events for a session and compute session-level metrics

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                      SESSION STATE MACHINE (Flink Keyed State)                   │
│                                                                                  │
│  Key: session_id = "sess_xyz789"                                                │
│                                                                                  │
│  ┌─────────────────────────────────────────────────────────────────────────┐    │
│  │                         STATE EVOLUTION                                  │    │
│  │                                                                          │    │
│  │  Event 1: PLAY_REQUEST                                                  │    │
│  │  ─────────────────────                                                  │    │
│  │  State: {                                                               │    │
│  │    status: "INITIALIZING",                                              │    │
│  │    play_request_time: 1705312200000,                                    │    │
│  │    content_id: "movie_jawan_2023",                                      │    │
│  │    user_id: "user_rahul_123",                                           │    │
│  │    geo: {region: "MH", city: "Mumbai", isp: "Jio"},                    │    │
│  │    device: {type: "smartphone", os: "Android 14"}                       │    │
│  │  }                                                                      │    │
│  │                                      │                                   │    │
│  │                                      ▼                                   │    │
│  │  Event 2: FIRST_FRAME                                                   │    │
│  │  ────────────────────                                                   │    │
│  │  State Update: {                                                        │    │
│  │    status: "PLAYING",                                                   │    │
│  │    first_frame_time: 1705312201820,                                     │    │
│  │    video_start_time_ms: 1820,  // calculated                           │    │
│  │    initial_bitrate: 4500,                                               │    │
│  │    started: true                                                        │    │
│  │  }                                                                      │    │
│  │                                      │                                   │    │
│  │                                      ▼                                   │    │
│  │  Events 3-N: HEARTBEAT (every 30s)                                      │    │
│  │  ─────────────────────────────────                                      │    │
│  │  State Update: {                                                        │    │
│  │    last_heartbeat: <timestamp>,                                         │    │
│  │    last_playhead_position: <position>,                                  │    │
│  │    total_bytes_downloaded: <cumulative>,                                │    │
│  │    avg_bitrate: <running_avg>,                                          │    │
│  │    heartbeat_count: <count>                                             │    │
│  │  }                                                                      │    │
│  │                                      │                                   │    │
│  │                                      ▼                                   │    │
│  │  Event: REBUFFER_START                                                  │    │
│  │  ─────────────────────                                                  │    │
│  │  State Update: {                                                        │    │
│  │    status: "BUFFERING",                                                 │    │
│  │    rebuffer_start_time: 1705314000000,                                  │    │
│  │    rebuffer_count: 1  // incremented                                    │    │
│  │  }                                                                      │    │
│  │                                      │                                   │    │
│  │                                      ▼                                   │    │
│  │  Event: REBUFFER_END                                                    │    │
│  │  ───────────────────                                                    │    │
│  │  State Update: {                                                        │    │
│  │    status: "PLAYING",                                                   │    │
│  │    total_rebuffer_time_ms: 2300,  // accumulated                       │    │
│  │    rebuffer_events: [{start: ..., end: ..., duration: 2300}]           │    │
│  │  }                                                                      │    │
│  │                                      │                                   │    │
│  │                                      ▼                                   │    │
│  │  Event: SESSION_END                                                     │    │
│  │  ──────────────────                                                     │    │
│  │  State: FINAL (triggers output)                                         │    │
│  │                                                                          │    │
│  └─────────────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────────────┘
```

**Session Summary Output (when session ends)**
```json
{
  "session_id": "sess_xyz789",
  "user_id": "user_rahul_123",
  "device_id": "dev_android_456",
  
  "session_start": 1705312200000,
  "session_end": 1705314900000,
  "session_duration_ms": 2700000,
  
  "content": {
    "content_id": "movie_jawan_2023",
    "title": "Jawan",
    "type": "movie",
    "duration_ms": 9000000
  },
  
  "geo": {
    "country": "IN",
    "region": "MH",
    "city": "Mumbai",
    "isp": "Reliance Jio"
  },
  
  "device": {
    "type": "smartphone",
    "brand": "Samsung",
    "os": "Android 14"
  },
  
  "qoe_metrics": {
    "video_start_time_ms": 1820,
    "started": true,
    "total_play_time_ms": 2697700,
    "total_rebuffer_time_ms": 2300,
    "rebuffer_count": 1,
    "rebuffer_ratio": 0.00085,
    "avg_bitrate_kbps": 3800,
    "max_bitrate_kbps": 4500,
    "min_bitrate_kbps": 2500,
    "bitrate_switches": 1,
    "completion_percentage": 30,
    "exit_reason": "USER_EXIT"
  },
  
  "qos_metrics": {
    "manifest_fetch_time_ms": 120,
    "drm_license_time_ms": 380,
    "total_bytes_downloaded": 1258291200,
    "avg_segment_download_time_ms": 85,
    "error_count": 0
  },
  
  "flags": {
    "had_error": false,
    "had_rebuffer": true,
    "completed_content": false,
    "exit_before_video_start": false
  }
}
```

---

### 3.3 Real-Time Aggregation Job

**Purpose:** Compute metrics over time windows, grouped by dimensions

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                      TUMBLING WINDOW AGGREGATION                                 │
│                                                                                  │
│  Window: 1 MINUTE (10:30:00 - 10:31:00)                                         │
│  Group By: (region, isp, device_type, content_type)                             │
│                                                                                  │
│  ┌─────────────────────────────────────────────────────────────────────────┐    │
│  │                    EVENTS IN WINDOW                                      │    │
│  │                                                                          │    │
│  │  Session 1: PLAY_REQUEST (MH, Jio, smartphone, movie)                   │    │
│  │  Session 1: FIRST_FRAME (VST: 1820ms)                                   │    │
│  │  Session 2: PLAY_REQUEST (MH, Jio, smartphone, movie)                   │    │
│  │  Session 2: FIRST_FRAME (VST: 2100ms)                                   │    │
│  │  Session 3: PLAY_REQUEST (MH, Jio, smartphone, movie)                   │    │
│  │  Session 3: ERROR (failed to start)                                     │    │
│  │  Session 4: REBUFFER_START (MH, Jio, smartphone, movie)                 │    │
│  │  Session 5: HEARTBEAT (MH, Jio, smartphone, movie)                      │    │
│  │  ... (thousands more events)                                            │    │
│  │                                                                          │    │
│  └─────────────────────────────────────────────────────────────────────────┘    │
│                                      │                                          │
│                                      ▼                                          │
│                              AGGREGATION LOGIC                                  │
│                                                                                  │
│  concurrent_viewers = COUNT(DISTINCT session_id WHERE status=PLAYING)          │
│  play_attempts = COUNT(PLAY_REQUEST events)                                     │
│  video_starts = COUNT(FIRST_FRAME events)                                       │
│  video_start_failures = play_attempts - video_starts                            │
│  avg_vst = AVG(video_start_time_ms)                                             │
│  p95_vst = PERCENTILE(video_start_time_ms, 0.95)                               │
│  rebuffer_ratio = SUM(rebuffer_time) / SUM(play_time)                          │
│  error_rate = COUNT(ERROR events) / play_attempts                              │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

**Aggregated Metrics Output (per minute, per dimension combination)**
```json
{
  "window_start": "2024-01-15T10:30:00Z",
  "window_end": "2024-01-15T10:31:00Z",
  "window_duration_sec": 60,
  
  "dimensions": {
    "country": "IN",
    "region": "MH",
    "city": "Mumbai",
    "isp": "Reliance Jio",
    "device_type": "smartphone",
    "os": "Android",
    "content_type": "movie",
    "app_version": "5.4.0"
  },
  
  "metrics": {
    "concurrent_viewers": 45230,
    "play_attempts": 1250,
    "video_starts": 1235,
    "video_start_failures": 15,
    "video_start_failure_rate": 0.012,
    
    "video_start_time": {
      "avg_ms": 1890,
      "p50_ms": 1750,
      "p95_ms": 3200,
      "p99_ms": 4500,
      "min_ms": 800,
      "max_ms": 8500
    },
    
    "rebuffer": {
      "sessions_with_rebuffer": 312,
      "rebuffer_ratio": 0.0034,
      "avg_rebuffer_duration_ms": 2100,
      "total_rebuffer_events": 425
    },
    
    "bitrate": {
      "avg_kbps": 3850,
      "p50_kbps": 4500,
      "switches_per_session": 0.8
    },
    
    "errors": {
      "total_errors": 28,
      "error_rate": 0.0224,
      "fatal_errors": 15,
      "by_code": {
        "DRM_LICENSE_FAILED": 5,
        "NETWORK_ERROR": 8,
        "DECODE_ERROR": 2
      }
    },
    
    "engagement": {
      "avg_watch_time_sec": 1250,
      "avg_completion_pct": 45,
      "exits_before_video_start": 12
    }
  },
  
  "sample_sessions": ["sess_xyz789", "sess_abc123", "sess_def456"]
}
```

---

### 3.4 Anomaly Detection Job

**Purpose:** Detect unusual patterns by comparing current metrics to baselines

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         ANOMALY DETECTION FLOW                                   │
│                                                                                  │
│  INPUT: Aggregated metrics (1-minute windows)                                   │
│                                                                                  │
│  ┌─────────────────────────────────────────────────────────────────────────┐    │
│  │  CURRENT VALUE                         BASELINE (last 7 days same hour) │    │
│  │                                                                          │    │
│  │  MH + Jio + smartphone:                MH + Jio + smartphone:           │    │
│  │  rebuffer_ratio = 0.034 (3.4%)         avg = 0.005 (0.5%)               │    │
│  │                                         std_dev = 0.002                  │    │
│  │                                                                          │    │
│  │  Z-score = (0.034 - 0.005) / 0.002 = 14.5                               │    │
│  │                                                                          │    │
│  │  THRESHOLD: Z-score > 3 = ANOMALY                                       │    │
│  │  RESULT: 14.5 > 3 → ANOMALY DETECTED! 🚨                                │    │
│  └─────────────────────────────────────────────────────────────────────────┘    │
│                                      │                                          │
│                                      ▼                                          │
│  ┌─────────────────────────────────────────────────────────────────────────┐    │
│  │                      CORRELATION ENGINE                                  │    │
│  │                                                                          │    │
│  │  Looking for related anomalies...                                       │    │
│  │                                                                          │    │
│  │  Found:                                                                  │    │
│  │  1. CDN logs: MUM POP showing 504 errors spike (same timeframe)        │    │
│  │  2. Same ISP (Jio) affected across MH region                           │    │
│  │  3. Other ISPs in MH region: NORMAL                                    │    │
│  │                                                                          │    │
│  │  Correlation: CDN issue at Mumbai POP affecting Jio users              │    │
│  └─────────────────────────────────────────────────────────────────────────┘    │
│                                      │                                          │
│                                      ▼                                          │
│                              ALERT GENERATED                                    │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

**Alert Output**
```json
{
  "alert_id": "alert_2024011510301234",
  "created_at": "2024-01-15T10:31:05Z",
  "severity": "P2",
  "status": "FIRING",
  
  "title": "High Rebuffer Ratio - Maharashtra/Jio Users",
  "description": "Rebuffer ratio is 6.8x higher than normal for Jio users in Maharashtra on mobile devices",
  
  "detection": {
    "metric": "rebuffer_ratio",
    "current_value": 0.034,
    "baseline_value": 0.005,
    "threshold": 0.015,
    "z_score": 14.5,
    "method": "statistical_zscore"
  },
  
  "impact": {
    "affected_users_estimate": 45000,
    "affected_sessions_last_5min": 2500,
    "affected_dimensions": {
      "region": "MH",
      "isp": "Reliance Jio",
      "device_type": "smartphone"
    }
  },
  
  "correlation": {
    "related_alerts": ["alert_cdn_mum_504_spike"],
    "probable_cause": "CDN edge node issue at Mumbai POP",
    "confidence": 0.85
  },
  
  "suggested_actions": [
    "Check Mumbai CDN POP health",
    "Consider traffic failover to alternate POP",
    "Contact CDN vendor if issue persists"
  ],
  
  "runbook_url": "https://runbooks.airtel.in/video/cdn-rebuffer-spike",
  
  "routing": {
    "team": "noc-video",
    "escalation_policy": "video-p2",
    "notification_channels": ["slack-noc-alerts", "pagerduty-video"]
  }
}
```

---

## STAGE 4: Storage Layer

### 4.1 Hot Storage (Redis) - Real-Time State

**Active Session Tracking**
```redis
# Store active session
HSET session:sess_xyz789 
  status "PLAYING"
  user_id "user_rahul_123"
  content_id "movie_jawan_2023"
  start_time "1705312200000"
  last_heartbeat "1705314800000"
  current_bitrate "4500"

# Set TTL (auto-expire if no heartbeat)
EXPIRE session:sess_xyz789 300

# Count concurrent viewers for content
PFADD viewers:content:movie_jawan_2023 sess_xyz789
PFCOUNT viewers:content:movie_jawan_2023
# Result: 125000 (unique sessions)

# Real-time counters
HINCRBY metrics:1m:1705314860:MH:Jio:smartphone play_attempts 1
HINCRBY metrics:1m:1705314860:MH:Jio:smartphone video_starts 1
HINCRBYFLOAT metrics:1m:1705314860:MH:Jio:smartphone sum_vst 1820

# Pub/Sub for dashboard updates
PUBLISH dashboard:updates '{"type":"metric","data":{"concurrent_viewers":1250000}}'
```

### 4.2 Hot Storage (Druid) - Real-Time OLAP

**Druid Ingestion Spec**
```json
{
  "type": "kafka",
  "dataSchema": {
    "dataSource": "video_metrics_realtime",
    "timestampSpec": {
      "column": "window_start",
      "format": "iso"
    },
    "dimensionsSpec": {
      "dimensions": [
        "country", "region", "city", "isp",
        "device_type", "os", "content_type", "content_id"
      ]
    },
    "metricsSpec": [
      {"type": "count", "name": "event_count"},
      {"type": "longSum", "name": "play_attempts", "fieldName": "play_attempts"},
      {"type": "longSum", "name": "video_starts", "fieldName": "video_starts"},
      {"type": "doubleSum", "name": "sum_vst", "fieldName": "sum_vst"},
      {"type": "HLLSketch", "name": "unique_sessions", "fieldName": "session_id"}
    ],
    "granularitySpec": {
      "segmentGranularity": "HOUR",
      "queryGranularity": "MINUTE"
    }
  }
}
```

**Druid Query (for dashboard)**
```sql
SELECT
  TIME_FLOOR(__time, 'PT1M') AS minute,
  region,
  isp,
  SUM(play_attempts) AS plays,
  SUM(video_starts) AS starts,
  SUM(sum_vst) / SUM(video_starts) AS avg_vst,
  APPROX_COUNT_DISTINCT_DS_HLL(unique_sessions) AS concurrent
FROM video_metrics_realtime
WHERE __time >= CURRENT_TIMESTAMP - INTERVAL '1' HOUR
  AND country = 'IN'
GROUP BY 1, 2, 3
ORDER BY minute DESC
```

### 4.3 Warm Storage (ClickHouse) - Historical Analytics

**ClickHouse Table Schema**
```sql
CREATE TABLE sessions
(
    session_id String,
    user_id String,
    session_date Date,
    session_start DateTime64(3),
    session_end DateTime64(3),
    
    -- Dimensions
    country LowCardinality(String),
    region LowCardinality(String),
    city LowCardinality(String),
    isp LowCardinality(String),
    device_type LowCardinality(String),
    os LowCardinality(String),
    content_id String,
    content_type LowCardinality(String),
    
    -- Metrics
    video_start_time_ms UInt32,
    total_play_time_ms UInt64,
    total_rebuffer_time_ms UInt32,
    rebuffer_count UInt16,
    avg_bitrate_kbps UInt32,
    completion_percentage Float32,
    
    -- Flags
    had_error UInt8,
    had_rebuffer UInt8,
    exit_before_video_start UInt8,
    exit_reason LowCardinality(String)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(session_date)
ORDER BY (session_date, region, isp, session_start)
TTL session_date + INTERVAL 90 DAY;
```

**Insert Session Data**
```sql
INSERT INTO sessions VALUES (
    'sess_xyz789',
    'user_rahul_123',
    '2024-01-15',
    '2024-01-15 10:30:00.000',
    '2024-01-15 11:15:00.000',
    'IN', 'MH', 'Mumbai', 'Reliance Jio',
    'smartphone', 'Android 14',
    'movie_jawan_2023', 'movie',
    1820, 2700000, 2300, 1, 3800, 30.0,
    0, 1, 0, 'USER_EXIT'
);
```

**Analytical Queries**
```sql
-- Query 1: Video Start Time trends by region (last 7 days)
SELECT
    toDate(session_start) AS date,
    region,
    count() AS sessions,
    avg(video_start_time_ms) AS avg_vst,
    quantile(0.95)(video_start_time_ms) AS p95_vst
FROM sessions
WHERE session_date >= today() - 7
  AND country = 'IN'
GROUP BY date, region
ORDER BY date, region;

-- Query 2: Rebuffer analysis by ISP
SELECT
    isp,
    count() AS total_sessions,
    countIf(had_rebuffer = 1) AS rebuffer_sessions,
    countIf(had_rebuffer = 1) / count() AS rebuffer_session_rate,
    avg(total_rebuffer_time_ms) AS avg_rebuffer_ms
FROM sessions
WHERE session_date = today()
  AND region = 'MH'
GROUP BY isp
ORDER BY rebuffer_session_rate DESC;

-- Query 3: Content performance comparison
SELECT
    content_id,
    count() AS plays,
    avg(completion_percentage) AS avg_completion,
    avg(video_start_time_ms) AS avg_vst,
    countIf(exit_before_video_start = 1) / count() AS ebvs_rate
FROM sessions
WHERE session_date >= today() - 7
  AND content_type = 'movie'
GROUP BY content_id
ORDER BY plays DESC
LIMIT 20;
```

### 4.4 Warm Storage (Elasticsearch) - Session Search

**Elasticsearch Document**
```json
{
  "_index": "sessions-2024.01.15",
  "_id": "sess_xyz789",
  "_source": {
    "session_id": "sess_xyz789",
    "user_id": "user_rahul_123",
    "timestamp": "2024-01-15T10:30:00.000Z",
    
    "geo": {
      "country": "IN",
      "region": "MH", 
      "city": "Mumbai",
      "location": {"lat": 19.076, "lon": 72.877}
    },
    
    "device": {
      "type": "smartphone",
      "brand": "Samsung",
      "model": "Galaxy S23",
      "os": "Android 14"
    },
    
    "content": {
      "id": "movie_jawan_2023",
      "title": "Jawan",
      "type": "movie"
    },
    
    "metrics": {
      "video_start_time_ms": 1820,
      "rebuffer_count": 1,
      "total_rebuffer_time_ms": 2300,
      "completion_pct": 30
    },
    
    "errors": [],
    
    "events_summary": "PLAY_REQUEST -> FIRST_FRAME -> HEARTBEAT(x90) -> REBUFFER -> SESSION_END"
  }
}
```

**Search Queries**
```json
// Find sessions for a specific user
GET sessions-2024.01.*/_search
{
  "query": {
    "bool": {
      "must": [
        {"term": {"user_id": "user_rahul_123"}}
      ]
    }
  },
  "sort": [{"timestamp": "desc"}]
}

// Find sessions with errors containing specific message
GET sessions-2024.01.*/_search
{
  "query": {
    "bool": {
      "must": [
        {"match": {"errors.message": "DRM license"}}
      ],
      "filter": [
        {"range": {"timestamp": {"gte": "now-24h"}}}
      ]
    }
  }
}

// Aggregate errors by type for troubleshooting
GET sessions-2024.01.*/_search
{
  "size": 0,
  "query": {
    "range": {"timestamp": {"gte": "now-1h"}}
  },
  "aggs": {
    "error_types": {
      "terms": {"field": "errors.code.keyword"}
    }
  }
}
```

### 4.5 Cold Storage (S3) - Data Lake

**S3 File Structure**
```
s3://airtel-video-analytics-prod/
├── raw-events/
│   └── year=2024/month=01/day=15/hour=10/
│       ├── player-events-00000.parquet
│       ├── player-events-00001.parquet
│       └── player-events-00002.parquet
│
├── sessions/
│   └── year=2024/month=01/day=15/
│       ├── sessions-part-00000.parquet
│       └── sessions-part-00001.parquet
│
├── aggregates/
│   └── hourly/
│       └── year=2024/month=01/day=15/
│           └── hour=10/
│               └── metrics.parquet
│
└── ml-features/
    └── anomaly-detection/
        └── training-2024-01.parquet
```

**Parquet Schema (Sessions)**
```
message sessions {
  required binary session_id (STRING);
  required binary user_id (STRING);
  required int64 session_start (TIMESTAMP_MILLIS);
  required int64 session_end (TIMESTAMP_MILLIS);
  
  required group geo {
    required binary country (STRING);
    required binary region (STRING);
    required binary city (STRING);
    required binary isp (STRING);
  }
  
  required group device {
    required binary type (STRING);
    required binary os (STRING);
  }
  
  required group metrics {
    required int32 video_start_time_ms;
    required int64 total_play_time_ms;
    required int32 rebuffer_count;
    required float completion_pct;
  }
}
```

**Query with Athena/Presto**
```sql
-- Year-over-year comparison
SELECT 
    DATE_TRUNC('week', from_unixtime(session_start/1000)) as week,
    region,
    COUNT(*) as sessions,
    AVG(metrics.video_start_time_ms) as avg_vst,
    AVG(metrics.completion_pct) as avg_completion
FROM sessions
WHERE year = '2024' OR year = '2023'
GROUP BY 1, 2
ORDER BY 1;
```

---

## STAGE 5: Application Layer (Consumption)

### 5.1 Real-Time Dashboard Display

**Dashboard Data Request**
```javascript
// Frontend WebSocket connection
const ws = new WebSocket('wss://analytics.xstreamplay.airtel.in/ws/dashboard');

ws.onmessage = (event) => {
  const data = JSON.parse(event.data);
  
  // Update dashboard widgets
  if (data.type === 'concurrent_viewers') {
    document.getElementById('viewers-count').innerText = 
      formatNumber(data.value); // "1,245,892"
  }
  
  if (data.type === 'metrics_update') {
    updateChart('vst-chart', data.vst_timeseries);
    updateChart('rebuffer-chart', data.rebuffer_timeseries);
    updateHeatmap('geo-heatmap', data.geo_distribution);
  }
};
```

**Dashboard Widget Data**
```json
{
  "type": "metrics_update",
  "timestamp": "2024-01-15T10:31:00Z",
  
  "summary": {
    "concurrent_viewers": 1245892,
    "concurrent_viewers_delta": "+12.3%",
    "avg_video_start_time_ms": 1820,
    "avg_video_start_time_delta": "-50ms",
    "rebuffer_ratio": 0.0034,
    "rebuffer_ratio_status": "healthy",
    "error_rate": 0.0012,
    "error_rate_status": "healthy"
  },
  
  "vst_timeseries": [
    {"time": "10:25", "p50": 1750, "p95": 3100},
    {"time": "10:26", "p50": 1780, "p95": 3050},
    {"time": "10:27", "p50": 1820, "p95": 3200},
    {"time": "10:28", "p50": 1800, "p95": 3150},
    {"time": "10:29", "p50": 1790, "p95": 3100},
    {"time": "10:30", "p50": 1820, "p95": 3200}
  ],
  
  "top_content": [
    {"title": "Jawan", "viewers": 125000, "trend": "up"},
    {"title": "IPL Live", "viewers": 89000, "trend": "stable"},
    {"title": "Animal", "viewers": 45000, "trend": "down"}
  ],
  
  "geo_distribution": [
    {"region": "MH", "viewers": 285000, "status": "healthy"},
    {"region": "DL", "viewers": 198000, "status": "healthy"},
    {"region": "KA", "viewers": 167000, "status": "degraded"}
  ],
  
  "active_alerts": [
    {
      "id": "alert_123",
      "severity": "P2",
      "title": "High rebuffer in Karnataka",
      "since": "5 minutes ago"
    }
  ]
}
```

### 5.2 Alert Notification Flow

**Slack Alert Message**
```json
{
  "channel": "#noc-video-alerts",
  "attachments": [
    {
      "color": "#ff9900",
      "title": "🚨 P2 Alert: High Rebuffer Ratio - Maharashtra/Jio",
      "title_link": "https://dashboard.airtel.in/alerts/alert_2024011510301234",
      "fields": [
        {
          "title": "Metric",
          "value": "Rebuffer Ratio: 3.4% (normal: 0.5%)",
          "short": true
        },
        {
          "title": "Impact",
          "value": "~45,000 affected users",
          "short": true
        },
        {
          "title": "Affected Segment",
          "value": "Maharashtra • Jio • Mobile",
          "short": false
        },
        {
          "title": "Probable Cause",
          "value": "CDN edge issue at Mumbai POP (correlation: 85%)",
          "short": false
        }
      ],
      "actions": [
        {
          "type": "button",
          "text": "View Dashboard",
          "url": "https://dashboard.airtel.in/region/MH"
        },
        {
          "type": "button",
          "text": "Acknowledge",
          "style": "primary",
          "url": "https://api.airtel.in/alerts/alert_123/ack"
        },
        {
          "type": "button",
          "text": "Runbook",
          "url": "https://runbooks.airtel.in/video/cdn-rebuffer"
        }
      ],
      "footer": "Video Analytics Platform",
      "ts": 1705314665
    }
  ]
}
```

### 5.3 Session Explorer UI

**Session Detail API Response**
```json
{
  "session": {
    "id": "sess_xyz789",
    "user": {
      "id": "user_rahul_123",
      "name": "Rahul S.",
      "subscription": "Premium"
    },
    "device": {
      "type": "Samsung Galaxy S23",
      "os": "Android 14",
      "app_version": "5.4.0",
      "player_version": "3.2.1"
    },
    "network": {
      "isp": "Reliance Jio",
      "connection": "4G",
      "location": "Mumbai, Maharashtra"
    },
    "content": {
      "title": "Jawan",
      "type": "Movie",
      "duration": "2h 30m"
    }
  },
  
  "timeline": [
    {
      "time": "10:30:00.000",
      "event": "PLAY_REQUEST",
      "details": "User initiated playback"
    },
    {
      "time": "10:30:00.120",
      "event": "MANIFEST_LOADED",
      "details": "HLS manifest fetched in 120ms"
    },
    {
      "time": "10:30:00.500",
      "event": "DRM_LICENSE",
      "details": "Widevine license acquired in 380ms"
    },
    {
      "time": "10:30:01.820",
      "event": "FIRST_FRAME",
      "details": "Video started at 1080p/4.5Mbps",
      "metric": "VST: 1.82s ✓"
    },
    {
      "time": "10:30:01.820 - 11:00:00.000",
      "event": "PLAYING",
      "details": "Smooth playback for 30 minutes",
      "metric": "Avg bitrate: 4.5Mbps"
    },
    {
      "time": "11:00:00.000",
      "event": "REBUFFER_START",
      "details": "Buffer depleted at 30:00 mark",
      "severity": "warning"
    },
    {
      "time": "11:00:02.300",
      "event": "REBUFFER_END",
      "details": "Resumed at 720p/2.5Mbps",
      "metric": "Rebuffer: 2.3s"
    },
    {
      "time": "11:00:02.300 - 11:15:00.000",
      "event": "PLAYING",
      "details": "Continued playback for 15 minutes"
    },
    {
      "time": "11:15:00.000",
      "event": "SESSION_END",
      "details": "User exited voluntarily",
      "metric": "Watched: 45min (30%)"
    }
  ],
  
  "summary": {
    "video_start_time": "1.82s",
    "total_watch_time": "45 minutes",
    "rebuffer_events": 1,
    "total_rebuffer_time": "2.3s",
    "average_bitrate": "3.8 Mbps",
    "completion": "30%",
    "quality_score": "Good (7.5/10)"
  },
  
  "related_issues": [
    {
      "type": "CDN",
      "description": "Segment 450 had slow response (1.2s)",
      "time": "11:00:00"
    }
  ]
}
```

---

## Summary: Complete Data Transformation Flow

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                        COMPLETE DATA TRANSFORMATION                              │
│                                                                                  │
│  USER ACTION          RAW DATA              PROCESSED            INSIGHT        │
│  ───────────          ────────              ─────────            ───────        │
│                                                                                  │
│  Rahul taps    ──▶   JSON event      ──▶   Enriched       ──▶   Dashboard:     │
│  "Play" on           from player           event with           "1.2M viewers"  │
│  Jawan               SDK                   geo/device           "VST: 1.8s"     │
│                                            context                               │
│                                                 │                                │
│                                                 ▼                                │
│  Video          ──▶   Heartbeats     ──▶   Session        ──▶   Session:       │
│  plays for           every 30s             state machine        "45 min watch"  │
│  45 minutes                                tracking             "1 rebuffer"    │
│                                                 │                                │
│                                                 ▼                                │
│  Buffering      ──▶   REBUFFER       ──▶   Aggregated     ──▶   Alert:         │
│  happens             events +              metrics show         "High rebuffer  │
│                      CDN 504 logs          spike in MH+Jio      in MH region"   │
│                                                 │                                │
│                                                 ▼                                │
│  User exits     ──▶   SESSION_END    ──▶   Stored in      ──▶   Report:        │
│                      event                  ClickHouse +         "30% watched   │
│                                            Elasticsearch         Jawan today"   │
│                                                                                  │
│  ─────────────────────────────────────────────────────────────────────────────  │
│  LATENCY:  Event ──(500ms)──▶ Kafka ──(1s)──▶ Flink ──(2s)──▶ Dashboard       │
│            TOTAL END-TO-END: < 5 seconds                                        │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

*This document provides concrete examples of data at every stage of the pipeline, showing how raw events from multiple sources are transformed into actionable insights.*
