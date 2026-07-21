# DataPipelineETL — Big Data Solution for Network & Service KPI Monitoring

[![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-000?style=flat&logo=apachekafka)](https://kafka.apache.org/)
[![Apache Spark](https://img.shields.io/badge/Apache_Spark-E25A1C?style=flat&logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Apache Airflow](https://img.shields.io/badge/Apache_Airflow-017CEE?style=flat&logo=apacheairflow&logoColor=white)](https://airflow.apache.org/)
[![Elasticsearch](https://img.shields.io/badge/Elasticsearch-005571?style=flat&logo=elasticsearch&logoColor=white)](https://www.elastic.co/elasticsearch/)
[![Docker](https://img.shields.io/badge/Docker-2496ED?style=flat&logo=docker&logoColor=white)](https://www.docker.com/)
[![Python](https://img.shields.io/badge/Python-3.11-3776AB?style=flat&logo=python&logoColor=white)](https://www.python.org/)

> End-to-end, fully automated Big Data pipeline designed and deployed at **Orange Tunisie** as part of an Engineering Final Year Project (PFE). The platform ingests, processes and visualises network & service KPIs in near-real-time, replacing legacy manual workflows with a scalable, container-based architecture.

---

## Table of Contents

1. [Project Overview](#project-overview)
2. [Architecture](#architecture)
3. [Tech Stack](#tech-stack)
4. [Repository Layout](#repository-layout)
5. [Quick Start](#quick-start)
6. [Pipelines & DAGs](#pipelines--dags)
7. [Kafka Topics](#kafka-topics)
8. [Web Interfaces](#web-interfaces)
9. [Visualisation](#visualisation)
10. [Roadmap](#roadmap)
11. [Authors & Acknowledgements](#authors--acknowledgements)

---

## Project Overview

Telecom operators generate massive volumes of operational data every second — cell-level KPIs, hardware health, call records, traffic volumes, handover events. At Orange Tunisie, these streams were historically fragmented across heterogeneous systems and processed manually, limiting visibility and slowing incident response.

**DataPipelineETL** addresses these limitations by providing:

- **Automated ingestion** of heterogeneous file formats (XML, CSV, GZIP archives).
- **Distributed processing** with Apache Spark for cleaning, validation and enrichment.
- **Real-time streaming** through Apache Kafka with topic-based isolation per source.
- **Centralised storage and search** in Elasticsearch, fed by Logstash.
- **Interactive dashboards** in Kibana for KPI monitoring, anomaly detection and decision support.
- **End-to-end orchestration** via Apache Airflow, with retry, backup and idempotency guarantees.

The result is a reproducible, container-native platform that converts raw telecom data into actionable insight while keeping operational cost low.

---

## Architecture

```
                          ┌───────────────────┐
                          │   Apache Airflow   │
                          │  (orchestration)   │
                          └─────────┬──────────┘
                                    │
                ┌───────────────────┼───────────────────┐
                ▼                   ▼                   ▼
        ┌──────────────┐    ┌──────────────┐    ┌──────────────┐
        │  XML / GZIP  │    │     CSV      │    │  Hardware    │
        │   (1 min)    │    │  (on-arrival)│    │   XML (1 min)│
        └──────┬───────┘    └──────┬───────┘    └──────┬───────┘
               └───────────┬───────┴───────────────────┘
                           ▼
                  ┌─────────────────┐
                  │  Apache Spark   │  ← cleaning, schema, enrichment
                  │   (3 workers)   │
                  └────────┬────────┘
                           ▼
                  ┌─────────────────┐
                  │  Apache Kafka   │  ← topic routing (KRaft mode)
                  └────────┬────────┘
                           ▼
                  ┌─────────────────┐
                  │     Logstash    │  ← conditional routing per topic
                  └────────┬────────┘
                           ▼
                  ┌─────────────────┐
                  │  Elasticsearch  │  ← indexed storage
                  └────────┬────────┘
                           ▼
                  ┌─────────────────┐
                  │      Kibana     │  ← dashboards & analytics
                  └─────────────────┘
```

Supporting services:

- **PostgreSQL 16** — Airflow metastore.
- **Redis 7.2** — Celery broker for the Airflow worker pool.
- **Kafdrop** — Kafka cluster inspection UI.
- **Flower** — Celery monitoring UI.

The full architecture is deployed via Docker Compose on a single Ubuntu 24.04 LTS host (16 GB RAM, 6 vCPU, 150 GB disk) and is horizontally scalable.

---

## Tech Stack

| Layer              | Technology                                  |
| ------------------ | ------------------------------------------- |
| Orchestration      | Apache Airflow 2.7.3 (CeleryExecutor)       |
| Distributed compute| Apache Spark 3.5.0 (1 master + 3 workers)   |
| Streaming          | Apache Kafka (KRaft, no Zookeeper)          |
| Search & storage   | Elasticsearch 7.16.2                        |
| Visualisation      | Kibana 7.16.2                               |
| Ingestion bridge   | Logstash (Bitnami)                          |
| Metadata DB        | PostgreSQL 16                               |
| Message broker     | Redis 7.2 Alpine                            |
| Containerisation   | Docker + Docker Compose                     |
| Language           | Python 3.11, PySpark                        |

---

## Repository Layout

```
DataPipelineETL/
├── dags/                        # Airflow DAG definitions
│   ├── xmlonly_to_kafka.py      # XML network KPIs (every 1 min)
│   ├── gzip_to_kafka.py         # GZIP → XML → Kafka (every 15 min)
│   ├── csv_to_kafka.py          # CSV ingestion (on-arrival)
│   └── hardware_to_kafka.py     # Hardware XML (every 1 min)
├── mypy/                        # PySpark jobs
│   ├── xmlonly.py
│   ├── streaming.py
│   ├── preprocessproduce.py
│   └── xmlhard.py
├── kibana/                      # Kibana configuration
├── gzip/    xmlonly/    csv/    xmlhard/   # Runtime data folders
│   ├── *input/      # incoming files
│   ├── *done/       # processed files
│   └── *backup/     # timestamped backups
├── Dockerfile                   # Custom Airflow image (PySpark + JDK)
├── docker-compose.yml           # Full stack definition
├── logstash.conf                # Kafka → Elasticsearch routing
├── setup.sh                     # One-shot bootstrap script
└── README.md
```

---

## Quick Start

### Prerequisites

- Ubuntu 24.04 LTS (or any Linux host with Docker support)
- 16 GB RAM minimum, 6 vCPU recommended
- Docker Engine and Docker Compose v2

### Installation

```bash
# 1. Clone the repository
git clone https://github.com/MohamedYoussefjo/DataPipelineETL.git
cd DataPipelineETL

# 2. Make the bootstrap script executable
chmod +x setup.sh

# 3. Launch the full stack
sudo ./setup.sh
```

`setup.sh` will:

1. Install required system packages and Docker Compose.
2. Generate a `.env` file with environment variables (host IP, credentials, Fernet key).
3. Create the full folder hierarchy for incoming, processed and backup data.
4. Pull/build all Docker images and start the entire cluster.

> **Security note (v1.0)** — Default credentials in `.env` are intended for local development only. Before any production rollout, rotate all passwords, regenerate the Fernet key, and enable TLS/SSL plus proper authentication/authorisation on Kafka, Elasticsearch and Airflow. Hardening of these layers is planned for v2.0.

### Verifying the deployment

```bash
docker compose ps              # all services should be Up / healthy
docker compose logs -f airflow_scheduler
```

---

## Pipelines & DAGs

Four Airflow DAGs orchestrate the platform:

| DAG                          | Frequency      | Source format       | Kafka topic   | Description                                                |
| ---------------------------- | -------------- | ------------------- | ------------- | ---------------------------------------------------------- |
| `xmlonly_to_kafka`           | Every 1 min    | XML (network KPIs)  | `xmlt_fast`   | Network performance metrics from base stations.            |
| `gzip_to_kafka_pipeline`     | Every 15 min   | GZIP-archived XML   | `xmlt`        | Bulk ingestion of compressed XML archives.                 |
| `csv_to_kafka`               | On file arrival| CSV                 | `csv`         | Cleansed and validated tabular telemetry.                  |
| `hardware_to_kafka`          | Every 1 min    | XML (server health) | `xmlhard`     | CPU, RAM, ICMP error counters from hardware probes.        |

Each DAG follows the same robust pattern:

```
ensure_directories → process_source → check_files_processed
                              │
                  ┌───────────┴───────────┐
                  ▼                       ▼
            spark_to_kafka           skip_spark
                  └───────────┬───────────┘
                              ▼
                       pipeline_success
```

Key guarantees:

- **Idempotent backups** — timestamped, MD5-deduplicated copies of every source file.
- **At-least-once delivery** to Kafka via Spark Structured Streaming checkpoints.
- **Conditional execution** — Spark jobs are skipped when no new files are present.
- **Automatic retries** with exponential backoff on transient failures.

---

## Kafka Topics

Four topics provide source-aware isolation:

| Topic        | Partitions | Producer       | Consumer (Logstash → Elasticsearch index) |
| ------------ | ---------- | -------------- | ----------------------------------------- |
| `xmlt_fast`  | 1          | Spark (XML)    | `filesxmlonly`                            |
| `xmlt`       | 1          | Spark (GZIP)   | `filesgziphere`                           |
| `csv`        | 1          | Spark (CSV)    | `filescsv`                                |
| `xmlhard`    | 1          | Spark (Hard.)  | `xmlhardware`                             |

Topics can be inspected or recreated through **Kafdrop** at <http://localhost:8900>.

---

## Web Interfaces

| Service           | URL                         | Default credentials       |
| ----------------- | --------------------------- | ------------------------- |
| Airflow Webserver | <http://localhost:8080>     | `admin` / `admin`         |
| Spark Master UI   | <http://localhost:9090>     | —                         |
| Kibana            | <http://localhost:5601>     | configured at first start |
| Kafdrop           | <http://localhost:8900>     | —                         |
| Flower (Celery)   | <http://localhost:5555>     | —                         |

> Default credentials must be changed before any non-local deployment.

---

## Visualisation

Once data flows into Elasticsearch, open **Kibana → Stack Management → Index Patterns** and create one pattern per index (`filesxmlonly*`, `filescsv*`, `filesgziphere*`, `xmlhardware*`) using `begin_time` as the time field.

The provided dashboard surfaces the most operationally relevant KPIs:

- Top congested LTE cells over time.
- Top 30 base stations by connected users.
- LTE call-establishment success rate per eNodeB.
- E-RAB session creation failures and DL/UL traffic volumes.
- RRC success vs. resource congestion.
- Handover success/failure analysis across X2 / S1 interfaces.
- Hardware health KPIs (memory, CPU, ICMP errors) with control-panel filtering.

These visualisations transform raw telemetry into proactive monitoring tools for the NOC team.

---

## Roadmap

**v1.0 (current)** — Functional pipeline, dashboards, full automation.

**v2.0 (planned)** — Production hardening:

- TLS/SSL across Kafka, Elasticsearch, Kibana and Airflow.
- SASL/SCRAM authentication on Kafka, role-based access on Elasticsearch.
- Secrets externalised to Vault / Docker secrets (no `.env` for sensitive material).
- Horizontal scaling profiles (multi-broker Kafka, multi-node Elasticsearch).
- Predictive ML models (anomaly detection on KPI time series) deployed via MLflow.
- Alerting through Kibana Watcher and PagerDuty/Slack integration.

---

## Authors & Acknowledgements

**Authors**
- **Mohamed Youssef Jouini** — [GitHub](https://github.com/MohamedYoussefjo) · [LinkedIn](https://www.linkedin.com/in/mohamed-youssef-jouini-b05740269/)
- **Youssef Alouane**

**Supervisors**
- *Academic*: Mrs. Fatma Kaabi — Université Centrale, École d'Informatique IT
- *Industrial*: Mr. Mohamed Rahal — Orange Tunisie, Direction Réseaux & Services

This project was conducted as a Final Year Engineering Project (PFE) in the **Big Data & Data Analytics** programme during the 2024 / 2025 academic year, in collaboration with **Orange Tunisie**.

---

## License

Released under the MIT License. See [`LICENSE`](LICENSE) for details.
