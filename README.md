# IP-SAKTI Sahayak — Admin & Legal Data Ingestion Platform

<div align="center">

![Python](https://img.shields.io/badge/Python-3.10%2B-blue?logo=python&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-0.115%2B-009688?logo=fastapi&logoColor=white)
![Qdrant](https://img.shields.io/badge/Qdrant-Cloud%20%7C%20Vector%20DB-DC2626?logo=qdrant&logoColor=white)
![Neo4j](https://img.shields.io/badge/Neo4j-AuraDB%20Cloud-008CC1?logo=neo4j&logoColor=white)
![Embedding](https://img.shields.io/badge/BAAI-bge--small--en--v1.5-7C3AED)
![Tests](https://img.shields.io/badge/Tests-17%20Passed-emerald?logo=pytest&logoColor=white)
![License](https://img.shields.io/badge/License-Proprietary%20%2F%20SIH-amber)

<p align="center">
  <strong>High-performance legal document ETL pipeline, hierarchical statutory chunker, dense vector embedding engine, and dual cloud explorer for the IP-SAKTI Sahayak RAG system.</strong>
</p>

[Key Features](#-key-features) • [Architecture](#-architecture) • [Cloud Explorers](#-cloud-database-explorers-gui) • [Quick Start](#-quick-start) • [API Reference](#-http-api-reference) • [Configuration](#-configuration) • [Testing](#-testing)

</div>

---

## 📖 Overview

**IP-SAKTI Sahayak (Admin Console & Ingestion Engine)** is an enterprise-grade legal data ingestion pipeline purpose-built for Indian Intellectual Property statutes, Patent Gazettes, the Biological Diversity Act (BDA), and the Traditional Knowledge Digital Library (TKDL).

It processes complex multi-format statutory documents (`.pdf`, `.txt`, `.md`), decomposes them into legally cohesive hierarchical chunks with contextual breadcrumbs, embeds them into 384-dimensional dense vectors using `BAAI/bge-small-en-v1.5`, and constructs provenance knowledge graph triples in Neo4j.

The admin console provides **real-time NDJSON streaming progress tracking**, **deduplication and vector overwrite safeguards**, and **dedicated Cloud GUIs** for live inspection of vector chunks in Qdrant Cloud and knowledge graph relationships in Neo4j AuraDB.

---

## ⚡ Key Features

### 1. Hierarchical Legal Chunker & Contextual Breadcrumbs
- **Statutory Boundary Awareness**: Intelligently partitions Indian Acts, Rules, and Gazettes by Chapter, Section, Sub-section, and Proviso.
- **Context Injection**: Each chunk carries prepended structural breadcrumbs (e.g., `[Act: The Patents Act, 1970 | Chapter: Chapter II | Section: Section 3(p)]`), ensuring high-precision semantic retrieval even when passages are queried in isolation.
- **Smart Fallback for Non-Statutory Files**: Automatically applies semantic heading and passage partitioning to patent filings, gazettes, and guidelines (e.g., `EP0436257B1.pdf`).

### 2. Real-Time 6-Stage Streaming Progress Tracker
- **Low-Latency Streaming**: Powered by `POST /api/ingest/file-stream` using server-sent NDJSON events.
- **Visual 6-Stage Stepper**:
  1. **Upload & Clean** — Gazette header/footer normalization, hyphenation repair, formatting cleanup.
  2. **Text Extract** — Dual-format text extraction (`pypdf` for gazettes, raw parser for text).
  3. **Hierarchical Chunking** — Live chunk generation counter (e.g. `Generated 23 chunks`).
  4. **Dense Vectors** — 384-d normalized vector generation via `BAAI/bge-small-en-v1.5`.
  5. **Qdrant & Neo4j Sync** — Batch insertion into Qdrant Cloud and knowledge graph linkage.
  6. **Indexed** — Success status with green checkmarks and active corpus state.

### 3. Idempotent Deduplication & Overwrite Safeguard
- **Zero Duplicate Chunks**: Re-uploading or updating an existing document automatically triggers `delete_document_vectors` before upserting new vectors, preventing stale or duplicate chunks from returning during retrieval.
- **Graph Tree Refresh**: Executes atomic Cypher `DETACH DELETE` on existing chapter and section nodes for the target `doc_id` while preserving shared regulatory authorities (`IPO`, `NBA`, `TKDL`).
- **Synchronized Registry**: Updates rows in-place in `corpus_index.csv`.

### 4. Dedicated Cloud Database Explorers (GUI)
- **2 Main Header Buttons**: Instant navigation via `🧠 Qdrant Cloud` and `🕸️ Neo4j Cloud` buttons.
- **Direct Cloud Connectivity**: Fetches live data from remote cloud instances over HTTPS/WSS (port 443 with API keys), not local caches.
- **Qdrant Cloud Explorer**:
  - Displays total vector points from AWS cluster (`collection: legal_chunks`).
  - Search filter across verbatim text, sections, and document names.
  - Raw JSON point payload modal inspector with one-click `📋 Copy JSON`.
- **Neo4j Aura Cloud Explorer**:
  - Live AuraDB connection status with direct links to `console.neo4j.io`.
  - **Interactive Cypher Query Console** with instant-run presets (`Statutory Hierarchy`, `Section 3(p) TKDL`, `Herb Prior Art`).
  - Visual graph ontology triples stream (`Subject ➔ Predicate ➔ Object`).

### 5. Resilient Hybrid Engine (Cloud & Local Fallbacks)
- Seamlessly falls back to an embedded in-memory/local Qdrant instance if network or cloud cluster timeouts occur during offline development.

---

## 🏛 Architecture

```
┌────────────────────────────────────────────────────────────────────────┐
│               IP-SAKTI Sahayak Admin Web Console                       │
│       HTML5 • Vanilla CSS Design System • Asynchronous JavaScript      │
│  [🧠 Qdrant Cloud Explorer]   [🕸️ Neo4j Cloud Explorer]   [Upload Zone] │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │ HTTP / NDJSON Stream
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│                     FastAPI Application Server (:8000)                 │
│  ├── /api/ingest/file-stream (NDJSON SSE Progress Pipeline)            │
│  ├── /api/cloud/qdrant/chunks (Live AWS Cloud Scroll & Payload Query)  │
│  ├── /api/cloud/neo4j/graph   (Live AuraDB Graph & Cypher Runner)      │
│  └── /api/corpus/*            (Corpus Registry, Overwrite & Delete)   │
└───────────────┬──────────────────────────────────────┬─────────────────┘
                │                                      │
       Dense Vectors (384-d)               Graph Triples & Ontology
                │                                      │
                ▼                                      ▼
┌────────────────────────────────┐    ┌──────────────────────────────────┐
│         Qdrant Cloud           │    │         Neo4j AuraDB Cloud       │
│    AWS us-west-1 (Port 443)    │    │      neo4j+s:// Protocol         │
│   Collection: legal_chunks     │    │  (:Statute)-[:CONTAINS]->(:Sec)  │
│  Dense Cosine L2-Normalized    │    │  (:Section)-[:CROSS_REFS]->(TKDL)│
└────────────────────────────────┘    └──────────────────────────────────┘
                ▲                                      ▲
                │ (Fallback)                           │ (Standby)
┌────────────────────────────────┐    ┌──────────────────────────────────┐
│   Embedded Local Vector DB     │    │   Statutory Domain Ontology      │
│     (Offline Development)      │    │       (In-Memory Triples)        │
└────────────────────────────────┘    └──────────────────────────────────┘
```

---

## 🧠 Cloud Database Explorers (GUI)

The console features dedicated modals for visually inspecting your cloud database state:

### 1. Qdrant Cloud • Vector Chunks Explorer
- **Cluster**: AWS us-west-1 (`https://<cluster-id>.aws.cloud.qdrant.io:443`)
- **Collection**: `legal_chunks` (384-dimensional cosine metric)
- **Features**:
  - Live point counter badge (e.g. `Total Points: 23`).
  - Verbatim text preview with statutory breadcrumbs.
  - Section title, document origin, and vector dimension tags.
  - **Raw Payload Modal**: Inspect and copy full JSON payload stored in Qdrant points.
  - Live search filtering by keyword or section.

### 2. Neo4j Cloud • Knowledge Graph Explorer
- **Cluster**: Neo4j AuraDB Cloud (`neo4j+s://<instance-id>.databases.neo4j.io`)
- **Features**:
  - Real-time AuraDB connection status (`Online` / `Standby`).
  - **Interactive Cypher Console**: Type Cypher queries or click presets to inspect node relationships.
  - **Graph Triples Visual Stream**: Displays formatted cards for cross-references:
    - `The Patents Act, 1970` ➔ `HAS_CHAPTER` ➔ `Chapter II`
    - `Chapter II` ➔ `CONTAINS_SECTION` ➔ `Section 3(p)`
    - `Section 3(p)` ➔ `CROSS_REFERENCES_PRIOR_ART` ➔ `CSIR_TKDL`
    - `Section 3(p)` ➔ `MANDATES_COMPLIANCE_WITH` ➔ `BDA Section 6`
    - `BDA Section 6` ➔ `REQUIRES_STATUTORY_FORM` ➔ `Form III`

---

## 🚀 Quick Start

### 1. Prerequisites
- **Python**: 3.10 or 3.11
- **Cloud Accounts** (optional, for cloud sync):
  - [Qdrant Cloud](https://cloud.qdrant.io) cluster URL & API key
  - [Neo4j AuraDB](https://console.neo4j.io) instance URI & credentials
- **Docker Desktop** (optional, if running local databases via Docker Compose)

### 2. Clone & Setup Virtual Environment

```bash
# Clone the repository
git clone https://github.com/Gyan-Ranjan-01/ip-sahayak-admin.git
cd ip-sahayak-admin

# Create and activate Python virtual environment
python -m venv .venv
# On Windows:
.venv\Scripts\activate
# On Linux/macOS:
source .venv/bin/activate

# Install backend dependencies
cd backend
pip install -r requirements.txt
pip install -r requirements-test.txt
```

### 3. Configure Environment Variables

Create your `.env` file in the project root:

```bash
cp .env.example .env
```

Configure your cloud or local database credentials:

```ini
# Environment
ENVIRONMENT=development
LOG_LEVEL=INFO

# Embedding Model
EMBEDDING_MODEL_NAME=BAAI/bge-small-en-v1.5
EMBEDDING_DIMENSION=384

# Qdrant Vector DB (Cloud or Local)
QDRANT_URL=https://357d6adb-1dfc-45e3-85e1-9aae90d69332.us-west-1-0.aws.cloud.qdrant.io
QDRANT_API_KEY=your_qdrant_cloud_api_key_here
QDRANT_COLLECTION_NAME=legal_chunks

# Neo4j Knowledge Graph (AuraDB Cloud or Local Bolt)
NEO4J_URI=neo4j+s://78ac68a3.databases.neo4j.io
NEO4J_USER=neo4j
NEO4J_PASS=your_neo4j_password_here

# Celery & Redis (Optional for async background tasks)
REDIS_URL=redis://localhost:6379/0
```

> **Note**: For Qdrant Cloud HTTPS endpoints, the client automatically routes via port `443`.

### 4. Start the Admin Server

```bash
# Run from backend directory
python -m uvicorn app.main:app --reload --host 127.0.0.1 --port 8000
```

Access the application in your browser:
- 🌐 **Admin Console**: [http://127.0.0.1:8000](http://127.0.0.1:8000)
- 📚 **Interactive Swagger API Docs**: [http://127.0.0.1:8000/docs](http://127.0.0.1:8000/docs)
- 🔍 **Health Status**: [http://127.0.0.1:8000/health](http://127.0.0.1:8000/health)

---

## 🛠 Command-Line ETL Operations

You can also run ingestions directly via the CLI:

```bash
cd backend

# Ingest a single statute or patent PDF
python scripts/ingest_docs.py --file ../corpus_extracted/patents_act_1970.txt

# Ingest an entire directory with auto-detected formats
python scripts/ingest_docs.py --input-dir ../corpus_extracted --format auto

# Dry-run test (chunks and generates breadcrumbs without database writes)
python scripts/ingest_docs.py --file ../corpus_extracted/EP0436257B1.pdf --dry-run

# Purge collection and re-index clean
python scripts/ingest_docs.py --reset-db --input-dir ../corpus_extracted
```

---

## 📡 HTTP API Reference

All routes are versioned and accessible at both `/api` and `/api/v1`.

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/health` | Server liveness and basic runtime state |
| `GET` | `/api/health` | Comprehensive connection test for Qdrant Cloud, Neo4j, and Redis |
| `GET` | `/api/stats` | Live counts of corpus documents, chunks, and database readiness |
| `GET` | `/api/corpus/index` | Returns documents registered in `corpus_index.csv` |
| `POST` | `/api/upload` | Multipart file upload with auto-detection of Act name, year, and type |
| `POST` | `/api/ingest/file-stream` | **NDJSON streaming pipeline** with real-time 6-stage progress events |
| `POST` | `/api/ingest/file` | Single-file ingestion (`dry_run`, `async_mode` supported) |
| `POST` | `/api/ingest/stream` | Batch streaming ingestion for entire directory |
| `GET` | `/api/cloud/qdrant/chunks` | **Direct Qdrant Cloud fetcher** with pagination, doc filters, and search |
| `GET` | `/api/cloud/neo4j/graph` | **Direct Neo4j Cloud fetcher** for live knowledge graph triples |
| `POST` | `/api/cloud/neo4j/query` | Read-only Cypher query execution runner against AuraDB |
| `POST` | `/api/corpus/delete` | Deletes a document, purges its vectors from Qdrant and nodes from Neo4j |
| `POST` | `/api/corpus/clear` | Clears all registered documents from index for a clean slate |
| `POST` | `/api/reset-db` | Wipes and re-initializes Qdrant collections and Neo4j graph |

---

## ⚙️ Configuration Variables

| Variable | Type | Default | Description |
|---|---|---|---|
| `ENVIRONMENT` | string | `development` | Environment name (`development`, `production`) |
| `EMBEDDING_MODEL_NAME` | string | `BAAI/bge-small-en-v1.5` | Dense transformer embedding model |
| `EMBEDDING_DIMENSION` | int | `384` | Vector dimensionality |
| `QDRANT_URL` | string | `http://localhost:6333` | Local URL or Cloud endpoint (`https://...qdrant.io`) |
| `QDRANT_API_KEY` | string | `""` | API key for Qdrant Cloud clusters |
| `QDRANT_COLLECTION_NAME`| string | `legal_chunks` | Target Qdrant collection name |
| `NEO4J_URI` | string | `bolt://localhost:7687` | Bolt or Neo4j Aura URI (`neo4j+s://...`) |
| `NEO4J_USER` | string | `neo4j` | Neo4j database user |
| `NEO4J_PASS` | string | `ipsakti_secret_password` | Neo4j database password |
| `REDIS_URL` | string | `redis://localhost:6379/0`| Redis connection URL for Celery |
| `OPENAI_API_KEY` | string | `""` | Optional API key for LLM-based metadata enrichment |

---

## 🧪 Testing

The repository includes a comprehensive unit test suite covering chunking, breadcrumb generation, stream responses, and duplicate vector purging.

```bash
cd backend
python -m pytest tests/ -v
```

### Test Suite Highlights:
- `test_legal_chunker_basic`: Verifies chapter and section boundary detection.
- `test_text_cleaner_headers_and_hyphenation`: Validates legal gazette artifact cleaning.
- `test_hierarchical_chunker_structure_and_breadcrumbs`: Confirms breadcrumb injection format.
- `test_qdrant_payload_schema_compliance`: Validates vector payload keys and types.
- `test_neo4j_provenance_ontology_and_rules`: Asserts statutory ontology graph constraints.
- `test_duplicate_file_overwrite_deduplication`: Verifies that re-uploading a file wipes prior vectors and replaces them cleanly.

---

## 📁 Repository Structure

```
ip-sahayak-admin/
├── .github/workflows/         # CI/CD workflows for testing and linting
├── corpus_extracted/          # Source legal statutes, gazettes, and patents
├── corpus_index.csv           # Local corpus catalog and metadata registry
├── docker-compose.yml         # Local stack (Qdrant, Neo4j, Redis, Postgres)
├── .env.example               # Environment variables template
├── .gitignore                 # Excludes local caches, virtualenvs, and data
├── README.md                  # System documentation
└── backend/
    ├── app/
    │   ├── api/
    │   │   └── routes.py      # REST & streaming endpoints, cloud queries
    │   ├── core/
    │   │   └── config.py      # Pydantic Settings & environment loader
    │   ├── services/etl/
    │   │   ├── chunker.py     # LegalDocumentChunker & text cleaners
    │   │   ├── etl_ingestion_pipeline.py # Vector embedder & graph constructor
    │   │   └── tasks/         # Celery task definitions
    │   └── static/
    │       ├── index.html     # Admin Console & Cloud GUI modals
    │       ├── style.css      # Custom design system & dark Cypher terminal
    │       └── app.js         # Reactive streaming client & cloud explorers
    ├── scripts/
    │   ├── ingest_docs.py     # CLI batch and single-file ingestion tool
    │   └── setup_sample_corpus.py # Sample statutory data populator
    ├── tests/                 # 17 Unit & integration tests
    ├── pytest.ini             # Pytest configuration
    └── requirements.txt       # Production dependencies
```

---

## 🔒 Security & Best Practices

- **Never Commit Secrets**: Keep `.env` out of version control; `.gitignore` is pre-configured to ignore `.env`, `.env*.local`, and local database cache files.
- **L2-Normalized Cosine Metric**: The downstream retrieval service must query Qdrant with `BAAI/bge-small-en-v1.5` using the BGE query prefix `Represent this sentence for searching relevant passages: ` to match passage embeddings.
- **Idempotency**: All ingestion endpoints utilize deterministic point IDs and document-level vector purges before writing, guaranteeing database consistency.

---

<div align="center">
  <sub>Built for the Smart India Hackathon (SIH) • Ministry of AYUSH & Intellectual Property Domain</sub>
</div>
