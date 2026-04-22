# 🐋 WhaleRadar — CryptoWhaleTracker

Real-time cryptocurrency whale movement detection and alert system powered by big data + AI agents.

## Architecture

```
Binance WS ──┐                                    ┌── Telegram Bot
Blockchair ──┤──► Kafka ──► Spark Streaming ──► LangGraph Agents ──► Grafana/Streamlit
Mempool.space┘       │          (ML model)        (3 agents + MCP)     Dashboard API
                     │                                  │
                     └──────► Hadoop (HDFS) ◄───────────┘
                                   ▲
                                   │
                              Airflow DAGs
```

## Stack

| Component | Technology | Port |
|-----------|-----------|------|
| Message Bus | Apache Kafka (KRaft) | 9092/9094 |
| Kafka UI | Kafdrop | 9000 |
| Stream Processing | Apache Spark | 4040/8080 |
| Distributed Storage | Hadoop HDFS | 9870 |
| Cluster Manager | YARN | 8088 |
| AI Agents | LangGraph + OpenAI | — |
| Workflow Scheduler | Apache Airflow | 8081 |
| Real-time Dashboard | Grafana | 3000 |
| AI Dashboard | Streamlit | 8501 |
| Alerts | Telegram Bot | — |

## Quick Start

### 1. Environment Setup

```bash
cp .env.example .env
# Edit .env with your API keys:
#   OPENAI_API_KEY, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, BLOCKCHAIR_API_KEY
```

### 2. Start Infrastructure

```bash
docker compose up -d
```

### 3. Install Python Dependencies

```bash
uv sync
```

### 4. Run WhaleRadar

```bash
uv run python main.py
```

### 5. Launch Streamlit Dashboard

```bash
uv run streamlit run src/whaleradar/dashboard/streamlit_app.py
```

## UIs

After `docker compose up`, open:

- **Kafdrop** (Kafka topics): http://localhost:9000
- **Airflow**: http://localhost:8081 (admin/admin)
- **HDFS NameNode**: http://localhost:9870
- **YARN ResourceManager**: http://localhost:8088
- **Spark Master**: http://localhost:8080
- **Grafana**: http://localhost:3000 (admin/admin)
- **Streamlit**: http://localhost:8501

## Project Structure

```
whaleradar/
├── main.py                          # Entry point — orchestrator
├── docker-compose.yml               # All infrastructure services
├── pyproject.toml                   # Dependencies
├── .env.example                     # Environment template
│
├── src/whaleradar/
│   ├── config.py                    # Pydantic Settings
│   ├── collectors/
│   │   ├── binance_ws.py            # Binance WebSocket stream
│   │   ├── blockchain_monitor.py    # Blockchair + Mempool.space
│   │   └── exchange_addresses.py    # Known exchange wallets
│   ├── kafka/
│   │   ├── producer.py              # Generic Kafka producer
│   │   ├── consumer.py              # Generic Kafka consumer
│   │   └── topics.py                # Topic constants + admin
│   ├── spark/
│   │   ├── streaming_job.py         # Structured Streaming pipeline
│   │   └── ml_model.py              # GBT whale intent classifier
│   ├── agents/
│   │   ├── state.py                 # LangGraph state schema
│   │   ├── whale_profiler.py        # Agent 1: Wallet profiling
│   │   ├── impact_predictor.py      # Agent 2: Market impact analysis
│   │   ├── signal_generator.py      # Agent 3: Alert generation
│   │   └── graph.py                 # LangGraph pipeline wiring
│   ├── mcp/
│   │   ├── telegram_tool.py         # Telegram alert delivery
│   │   ├── dashboard_tool.py        # Dashboard data writer
│   │   └── blockchain_explorer_tool.py  # Mid-reasoning wallet lookup
│   ├── storage/
│   │   ├── models.py                # Pydantic domain models
│   │   └── hdfs_client.py           # HDFS read/write operations
│   └── dashboard/
│       └── streamlit_app.py         # Streamlit AI narrative dashboard
│
├── airflow/dags/
│   ├── retrain_model.py             # Nightly ML retraining
│   ├── daily_summary.py             # Morning activity report
│   └── weekly_backtest.py           # Weekly signal accuracy check
│
├── grafana/provisioning/
│   ├── dashboards/
│   │   ├── dashboard.yml
│   │   └── whale_dashboard.json     # Pre-built Grafana panels
│   └── datasources/
│       └── datasource.yml
│
├── hadoop/
│   ├── core-site.xml
│   └── hdfs-site.xml
│
└── spark/
    └── spark-defaults.conf
```

## Agents (LangGraph)

1. **Whale Profiler** 🔍 — Checks HDFS for wallet history, builds behavioral profiles
2. **Impact Predictor** 📊 — Multi-iteration reasoning loop assessing market impact
3. **Signal Generator** 🚦 — Produces clean, actionable alerts for delivery

## Airflow DAGs

| DAG | Schedule | Purpose |
|-----|----------|---------|
| `retrain_model` | Midnight UTC | Retrain ML model on latest data |
| `daily_summary` | 08:00 UTC | 24h whale activity report |
| `weekly_backtest` | Sunday 10:00 UTC | Signal accuracy evaluation |
