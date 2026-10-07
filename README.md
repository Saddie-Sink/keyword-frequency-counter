# Real-Time Keyword Frequency and Trend Analysis

**Domain:** Big Data Analytics  
**Core Stack:** Apache Kafka &bull; Apache Spark Structured Streaming &bull; Python &bull; FastAPI &bull; PostgreSQL &bull; React + TypeScript + Recharts &bull; Docker Compose

---

## Table of Contents

1. [Problem Statement](#1-problem-statement)
2. [Objectives](#2-objectives)
3. [Big Data Motivation](#3-big-data-motivation)
4. [Architecture](#4-architecture)
5. [Data Flow](#5-data-flow)
6. [Technology Stack](#6-technology-stack)
7. [Kafka Role](#7-kafka-role)
8. [Spark Role](#8-spark-role)
9. [Structured Streaming](#9-structured-streaming)
10. [Window Analytics](#10-window-analytics)
11. [Trending Algorithm](#11-trending-algorithm)
12. [Database Design](#12-database-design)
13. [API Documentation](#13-api-documentation)
14. [WebSocket](#14-websocket)
15. [Stream Lifecycle](#15-stream-lifecycle)
16. [Docker Setup](#16-docker-setup)
17. [Running the Project](#17-running-the-project)
18. [Testing](#18-testing)
19. [Dashboard Layout & Screenshots](#19-dashboard-layout--screenshots)
20. [Future Enhancements](#20-future-enhancements)
21. [6-Member Team Division & Viva Guide](#21-6-member-team-division--viva-guide)

---

## 1. Problem Statement

Modern digital platforms generate continuous, high-velocity streams of unstructured textual data—ranging from technology news feeds and social commentary to product reviews and system telemetry. Traditional batch-oriented analytics pipelines introduce minutes or hours of latency, making it impossible to detect emerging topics or sudden keyword surges as they happen.

This project designs and implements a **genuine end-to-end Real-Time Big Data Analytics system** that ingests live textual streams through **Apache Kafka**, processes and aggregates tokens across sliding time windows using **Apache Spark Structured Streaming**, persists analytical summaries in **PostgreSQL**, and delivers sub-second interactive visualizations and stream lifecycle controls (`START`, `PAUSE`, `RESUME`, `STOP`) to a **React + TypeScript** dashboard over **WebSockets**.

---

## 2. Objectives

* Build a configurable **Python Streaming Data Producer** capable of generating realistic multi-category textual streams (`Technology`, `Big Data`, `Artificial Intelligence`, `Data Science`, `News`, `Product Reviews`) with controlled throughput (`Messages/sec`).
* Deploy **Apache Kafka** with partitioned topic ingestion (`text_stream`) to decouple stream producers from analytical consumers.
* Implement **Apache Spark Structured Streaming** (`spark/keyword_streaming.py`) performing distributed text preprocessing, multi-horizon time-window aggregations (`1m`, `5m`, `15m`, `1h`), event-time watermarking, fault-tolerant checkpointing, and trending velocity detection.
* Persist aggregated time-windowed analytics in **PostgreSQL** (`keyword_statistics`, `trend_statistics`, `stream_statistics`) without storing unbounded raw messages.
* Expose a **FastAPI** backend providing REST endpoints, non-blocking stream lifecycle control (`START` / `PAUSE` / `RESUME` / `STOP`), component health monitoring, and a real-time **WebSocket (`WS /ws`)** event broadcaster.
* Deliver a responsive **React + TypeScript + Tailwind CSS + Recharts** dashboard that updates live without page refreshes.

---

## 3. Big Data Motivation

The system directly addresses the **5 Vs of Big Data**:

| Big Data V | How This System Addresses It |
| :--- | :--- |
| **Volume** | Aggregated time-window storage in PostgreSQL (`keyword_statistics`, `trend_statistics`, `stream_statistics`) and bounded ring buffers prevent unbounded raw message growth while processing continuous streams. |
| **Velocity** | Apache Kafka partitioned ingestion (`text_stream`) combined with Spark Structured Streaming micro-batches and WebSocket push notifications process and visualize up to 100 messages/sec in real time. |
| **Variety** | Multi-category unstructured natural language text (`Technology`, `Big Data`, `Artificial Intelligence`, `Data Science`, `News`, `Product Reviews`) is normalized into structured analytical schemas. |
| **Veracity** | A 6-stage Spark SQL text preprocessing pipeline (lowercasing, punctuation stripping, whitespace normalization, tokenization, stop-word filtering, short/numeric token removal) plus a **2-minute event-time watermark** ensures clean, accurate counts even with late-arriving data. |
| **Value** | Real-time keyword rankings, share percentages, and surge velocity (`Change %`) empower operators to spot emerging trends immediately. |

---

## 4. Architecture

```text
               REAL-TIME TEXT DATA
                        │
                        ▼
                 ┌─────────────┐
                 │   Producer  │  (Member 1: Python Async Background Task)
                 └──────┬──────┘
                        ▼
                 ┌─────────────┐
                 │    Kafka    │  (Member 2: Topic `text_stream`, 3 Partitions)
                 └──────┬──────┘
                        ▼
             ┌─────────────────────┐
             │ Spark Structured    │  (Member 3: PySpark readStream, Watermark,
             │ Streaming           │   Checkpointing, 6-Stage Preprocessing)
             └──────────┬──────────┘
                        ▼
               ┌────────────────┐
               │ Big Data       │
               │ Analytics      │
               ├────────────────┤
               │ Keyword Count  │
               │ Windows (1m-1h)│
               │ Trends (Δ %)   │
               └───────┬────────┘
                       ▼
                 ┌─────────────┐
                 │ PostgreSQL  │  (Member 4: Aggregated Analytics Tables)
                 └──────┬──────┘
                        ▼
                 ┌─────────────┐
                 │   FastAPI   │  (Member 4: REST API + Stream Control)
                 └──────┬──────┘
                        ▼
                 ┌─────────────┐
                 │  WebSocket  │  (Member 4 & 5: Live Event Push `/ws`)
                 └──────┬──────┘
                        ▼
                 ┌─────────────┐
                 │    React    │  (Member 5: TypeScript + Recharts UI)
                 │  Dashboard  │
                 └─────────────┘
```

---

## 5. Data Flow

1. **Stream Trigger**: The user configures `Messages/sec` (e.g., `10`) and `Data Category` (e.g., `Technology`) on the React Dashboard and clicks **`START`**.
2. **Producer Lifecycle**: `POST /api/stream/start` transitions Member 1's `StreamLifecycleManager` from `STOPPED` to `RUNNING` and launches a managed background `asyncio.Task`.
3. **Kafka Ingestion**: The producer serializes JSON messages (`id`, `sequence_id`, `text`, `category`, `timestamp`) and publishes them to the Kafka topic `text_stream`.
4. **Spark Stream Processing**: Spark Structured Streaming consumes `text_stream` via `spark.readStream.format("kafka")`, parses JSON payloads, applies a 2-minute watermark on `event_time`, cleans and explodes tokens using Spark SQL functions, and computes windowed frequencies (`1m`, `5m`, `15m`, `1h`), percentages, ranks, and trend `Change %`.
5. **PostgreSQL Persistence**: Aggregated window results are written to `keyword_statistics`, `trend_statistics`, and `stream_statistics`.
6. **FastAPI & WebSocket Broadcast**: FastAPI pushes `new_message`, `stats_update`, `keyword_update`, `trend_update`, `stream_status`, and `system_status` events over `WS /ws` to connected React clients.

---

## 6. Technology Stack

| Layer | Technology | Purpose |
| :--- | :--- | :--- |
| **Data Producer** | Python 3.12, `asyncio`, `kafka-python-ng` | Controlled background message generation & lifecycle state machine |
| **Message Broker** | Apache Kafka 3.7 (KRaft Mode) | Distributed, partitioned event streaming (`text_stream`) |
| **Stream Processing** | Apache Spark 3.5 / PySpark Structured Streaming | Distributed text preprocessing, windowing, watermarking, trend analytics |
| **Database** | PostgreSQL 16, SQLAlchemy 2.0 | Persistent storage of windowed keyword, trend, and stream statistics |
| **Backend API** | FastAPI, Pydantic v2, Uvicorn, WebSockets | REST endpoints, stream control API, health monitoring, live WebSocket server |
| **Frontend UI** | React 18, TypeScript, Vite, Tailwind CSS, Recharts | Real-time interactive analytics dashboard and charts |
| **Deployment & QA** | Docker, Docker Compose, Pytest | Multi-container orchestration and automated unit/integration testing |

---

## 7. Kafka Role

Apache Kafka serves as the high-throughput ingestion backbone (`kafka_infra/healthcheck.py`, `kafka/init-topics.sh`):

* **Decoupling**: The Python Producer publishes messages to `text_stream` independently of downstream Spark micro-batch execution speed.
* **Partitioning**: Configured with 3 partitions (`KAFKA_PARTITIONS=3`) so Spark executors can consume partitions in parallel.
* **Fault Tolerance**: Buffers incoming events during downstream restarts and provides health checks for broker connectivity and automatic topic creation.

---

## 8. Spark Role

Apache Spark (`spark/preprocessing.py`, `spark/analytics.py`, `spark/keyword_streaming.py`) executes all core analytical transformations using Catalyst-optimized Spark SQL DataFrame and `Window` operations (**never ordinary Python loops** for main analytics):

1. **Lowercase Conversion**: `F.lower(F.col("text"))`
2. **Punctuation Removal**: `F.regexp_replace(lowercased, r"[^a-z0-9\s]", " ")`
3. **Whitespace Normalization**: `F.trim(F.regexp_replace(no_punct, r"\s+", " "))`
4. **Tokenization**: `F.split(..., " ")` + `F.explode(...)`
5. **Stop-Word Removal**: Filters out common English stop words (`~F.col("keyword").isin(...)`)
6. **Short & Numeric Token Filtering**: Retains tokens with `F.length(F.col("keyword")) >= 3` and `~F.col("keyword").rlike(r"^\d+$")`.

---

## 9. Structured Streaming

In `spark/keyword_streaming.py`:

* **Source**: `spark.readStream.format("kafka").option("subscribe", "text_stream")`
* **Checkpointing**: Configured via `.option("checkpointLocation", CHECKPOINT_LOCATION)` so offset progress and stateful metadata survive process restarts.
* **Micro-Batch Sink**: `.writeStream.foreachBatch(processor.process_batch).trigger(processingTime="3 seconds")` persists multi-window analytics to PostgreSQL in each micro-batch.

---

## 10. Window Analytics

Implemented in `spark/analytics.py` (`compute_windowed_keyword_analytics_df`):

* **Supported Windows**:
  * `1 minute` (`1m`)
  * `5 minutes` (`5m`)
  * `15 minutes` (`15m`)
  * `1 hour` (`1h`)
* **Watermarking Rationale (`2 minutes`)**:
  `df.withWatermark("event_time", "2 minutes")` balances two competing distributed streaming requirements:
  1. **Late-Data Accuracy**: Tolerates messages arriving up to 120 seconds out-of-order due to network jitter or Kafka partition rebalancing.
  2. **Bounded State Memory**: Instructs Spark's state store to evict finalized window state older than `max_event_time - 2 minutes`, preventing unbounded executor memory growth (`OutOfMemoryError`).

---

## 11. Trending Algorithm

Implemented in `spark/analytics.py` (`compute_trending_keywords_df`) and `backend/app/analytics_engine.py`:

$$\text{Change \%} = \left( \frac{\text{Current Frequency} - \text{Previous Frequency}}{\text{Previous Frequency}} \right) \times 100$$

* **Zero-Division Safe Handling (`Previous = 0`)**:
  * When `Previous == 0` and `Current > 0` (newly emerging keyword): `Change % = +100.0%` (flagged with direction `"new"`).
  * When `Previous == 0` and `Current == 0`: `Change % = 0.0%`.

---

## 12. Database Design

Defined in `backend/init.sql` and `backend/app/models.py`:

### 1. `keyword_statistics`
| Column | Type | Description |
| :--- | :--- | :--- |
| `id` | `SERIAL PRIMARY KEY` | Auto-incrementing primary key |
| `keyword` | `VARCHAR(128)` | Cleaned keyword token |
| `window_label` | `VARCHAR(16)` | Window horizon (`1m`, `5m`, `15m`, `1h`) |
| `window_start` | `TIMESTAMPTZ` | Window start timestamp |
| `window_end` | `TIMESTAMPTZ` | Window end timestamp |
| `frequency` | `INTEGER` | Keyword occurrences in window |
| `percentage` | `DOUBLE PRECISION` | Share percentage within window |
| `created_at` | `TIMESTAMPTZ` | Record creation timestamp |

### 2. `trend_statistics`
| Column | Type | Description |
| :--- | :--- | :--- |
| `id` | `SERIAL PRIMARY KEY` | Auto-incrementing primary key |
| `keyword` | `VARCHAR(128)` | Cleaned keyword token |
| `window_start` | `TIMESTAMPTZ` | Current window start timestamp |
| `current_frequency` | `INTEGER` | Current window count |
| `previous_frequency` | `INTEGER` | Previous window count |
| `change_percentage` | `DOUBLE PRECISION` | Computed `Change %` |
| `created_at` | `TIMESTAMPTZ` | Record creation timestamp |

### 3. `stream_statistics`
| Column | Type | Description |
| :--- | :--- | :--- |
| `id` | `SERIAL PRIMARY KEY` | Auto-incrementing primary key |
| `timestamp` | `TIMESTAMPTZ` | Snapshot timestamp |
| `message_count` | `INTEGER` | Cumulative messages processed |
| `word_count` | `INTEGER` | Cumulative preprocessed words |
| `unique_keyword_count` | `INTEGER` | Distinct keywords observed |
| `processing_rate` | `DOUBLE PRECISION` | Throughput in messages/second |

---

## 13. API Documentation

### Analytics & Health Endpoints (`backend/app/routes/api.py`)
| Method | Endpoint | Description |
| :--- | :--- | :--- |
| `GET` | `/api/health` | Overall backend & component health summary |
| `GET` | `/api/status` | Detailed status for Kafka, Spark, PostgreSQL, Backend, WebSocket |
| `GET` | `/api/stats` | Summary metrics (`total_messages`, `total_words`, `unique_keywords`, `messages_per_sec`, `processing_rate`, `frequency_over_time`) |
| `GET` | `/api/keywords?window=1m&limit=10` | Ranked top keywords (`rank`, `keyword`, `frequency`, `percentage`) |
| `GET` | `/api/keywords/{keyword}` | Multi-window breakdown (`1m`, `5m`, `15m`, `1h`) and trend history for a specific keyword |
| `GET` | `/api/trending?limit=10` | Trending keywords sorted by `change_percentage` |
| `GET` | `/api/messages?limit=20` | Bounded list of recent streamed messages (max 50) |
| `GET` | `/api/stream/status` | Producer lifecycle state (`STOPPED`, `RUNNING`, `PAUSED`), rate, and category |

### Stream Control Endpoints (`backend/app/routes/stream.py`)
| Method | Endpoint | Payload | Description |
| :--- | :--- | :--- | :--- |
| `POST` | `/api/stream/start` | `{"rate": 10, "category": "Technology"}` | Starts stream (`STOPPED -> RUNNING`) |
| `POST` | `/api/stream/pause` | None | Pauses stream (`RUNNING -> PAUSED`) |
| `POST` | `/api/stream/resume` | None | Resumes stream (`PAUSED -> RUNNING`) |
| `POST` | `/api/stream/stop` | None | Stops stream (`RUNNING/PAUSED -> STOPPED`) |

---

## 14. WebSocket

* **Endpoint**: `WS /ws`
* **Server-to-Client Event Types**:
  * `system_status`: Component health indicators (`Kafka`, `Spark`, `PostgreSQL`, `Backend`, `WebSocket`)
  * `stream_status`: Current producer lifecycle state (`STOPPED`, `RUNNING`, `PAUSED`), rate, and category
  * `stats_update`: Summary counters and `frequency_over_time` chart series
  * `keyword_update`: Ranked top keywords across `1m`, `5m`, `15m`, and `1h` windows
  * `trend_update`: Trending keywords with `current_frequency`, `previous_frequency`, and `change_percentage`
  * `new_message`: Latest streamed message for the bounded Live Messages feed
  * `error`: Error notifications

---

## 15. Stream Lifecycle

Enforced by `StreamLifecycleManager` (`producer/lifecycle.py`):

```text
STOPPED ──(START)──▶ RUNNING ──(PAUSE)──▶ PAUSED ──(RESUME)──▶ RUNNING ──(STOP)──▶ STOPPED
   ▲                                                                                  │
   └───────────────────────────(Can START again without restart)──────────────────────┘
```

* The producer runs in a managed background `asyncio.Task` (`producer/producer.py`)—never an uncontrolled `while True:` blocking a FastAPI request handler.

---

## 16. Docker Setup

`docker-compose.yml` orchestrates all 5 services:
* `postgres` (`postgres:16-alpine`, port `5432`, auto-initializes `backend/init.sql`)
* `kafka` (`apache/kafka:3.7.0` KRaft mode, port `9092`, topic `text_stream`)
* `spark` (`spark/Dockerfile`, PySpark Structured Streaming job `spark/keyword_streaming.py`)
* `backend` (`backend/Dockerfile`, FastAPI + Uvicorn on port `8000`)
* `frontend` (`frontend/Dockerfile`, React + Vite on port `5173`)

---

## 17. Running the Project

### Option A: Using Docker Compose (Full Containerized Stack)

```bash
docker compose up --build
```

* **React Dashboard**: [http://localhost:5173](http://localhost:5173)
* **FastAPI Docs (Swagger UI)**: [http://localhost:8000/docs](http://localhost:8000/docs)

### Option B: Running Locally (Native Python + Node.js)

1. **Start the FastAPI Backend** (from project root):
   ```powershell
   .\.venv\Scripts\python.exe -m uvicorn backend.app.main:app --host 0.0.0.0 --port 8000
   ```
2. **Start the React Dashboard** (in a second terminal):
   ```powershell
   cd frontend
   npm run dev
   ```
3. Open [http://localhost:5173](http://localhost:5173), set `Messages/sec = 10` and `Category = Technology`, and click **`START`**.

---

## 18. Testing

Run the complete automated test suite (covering Producer Lifecycle, Spark Text Preprocessing, Window & Trend Analytics, Zero-Division Handling, REST & WebSocket APIs, and System Fault Tolerance):

```powershell
.\.venv\Scripts\python.exe -m pytest -v
```

---

## 19. Dashboard Layout & Screenshots

The React dashboard (`frontend/src/App.tsx`) implements the specification layout:

```text
┌───────────────────────────────────────────────────────────┐
│ Real-Time Keyword Analytics                               │
├───────────────────────────────────────────────────────────┤
│ System Status: Kafka ● | Spark ● | PostgreSQL ● | WS ●    │
├───────────────────────────────────────────────────────────┤
│ Total Messages | Total Words | Keywords | Rate | Proc Rate│
├───────────────────────────────────────────────────────────┤
│ Stream Controls: [Rate] [Category] [START][PAUSE][RESUME] │
├──────────────────────────────┬────────────────────────────┤
│ Top Keywords (Top 5/10/20/50)│ Live Keyword Bar Chart     │
├──────────────────────────────┼────────────────────────────┤
│ Trending Keywords (Change %) │ Frequency Over Time Chart  │
├──────────────────────────────┴────────────────────────────┤
│ Live Messages (Bounded Recent Feed)                       │
└───────────────────────────────────────────────────────────┘
```

---

## 20. Future Enhancements

1. **Multi-Broker Kafka Cluster**: Scale Kafka from single-node KRaft to a 3-broker cluster with replication factor 3.
2. **Semantic Topic Clustering**: Integrate word embeddings and streaming k-means in Spark MLlib to group related keywords automatically.
3. **Custom Alert Webhooks**: Trigger Slack/Webhook alerts when any keyword's `Change %` exceeds a configurable surge threshold.
4. **Historical Parquet Archival**: Sink hourly window rollups to Apache Parquet / Delta Lake for long-term historical trend comparison.

---

## 21. 6-Member Team Division & Viva Guide

| Member | Module | Key Files |
| :--- | :--- | :--- |
| **Member 1** | Data Producer & Stream Control | `producer/producer.py`, `producer/lifecycle.py`, `producer/sample_datasets.py`, `producer/config.py` |
| **Member 2** | Kafka & Streaming Infrastructure | `kafka_infra/healthcheck.py`, `kafka/init-topics.sh`, `docker-compose.yml` |
| **Member 3** | Spark Structured Streaming & Analytics | `spark/keyword_streaming.py`, `spark/preprocessing.py`, `spark/analytics.py`, `spark/Dockerfile` |
| **Member 4** | PostgreSQL & FastAPI Backend | `backend/init.sql`, `backend/app/main.py`, `backend/app/models.py`, `backend/app/routes/`, `backend/app/websocket_manager.py` |
| **Member 5** | React Dashboard & Visualization | `frontend/src/App.tsx`, `frontend/src/components/`, `frontend/src/services/` |
| **Member 6** | Integration, Testing, Docker & Docs | `docker-compose.yml`, `tests/`, `README.md` |
