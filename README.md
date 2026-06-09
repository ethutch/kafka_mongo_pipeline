# Kafka → MongoDB Pipeline


An enterprise-grade Python service that consumes messages from a Kafka topic and persists them to a MongoDB collection.

## Architecture

```
kafka_mongo_pipeline/
├── charts/             # Helm Charts
├── scripts/            # generations scripts
├── kafka_mongo_pipeline/
│   ├── config/         # Settings, env management
│   ├── models/         # Data models
│   ├── services/       # Major functions
│   └── utils/          # Logging, helpers
├── tests/
│   ├── unit/
├── pyproject.toml
├── Dockerfile
└── docker-compose.yml
```

## Quick Start

```bash
# 1. Install dependencies
pip install -e ".[dev]"

# 2. Copy and configure environment
cp .env.example .env
# Edit .env with your Kafka/MongoDB connection details

# 3. Run locally (requires Kafka + MongoDB)
python -m kafka_mongo_pipeline.main

# 4. Run via Docker Compose (spins up Kafka + MongoDB too)
docker-compose up --build
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker address(es) |
| `KAFKA_TOPIC` | `pipeline.events` | Topic to consume |
| `KAFKA_GROUP_ID` | `pipeline-consumer-group` | Consumer group ID |
| `KAFKA_AUTO_OFFSET_RESET` | `earliest` | Offset reset policy |
| `MONGODB_URI` | `mongodb://localhost:27017` | MongoDB connection string |
| `MONGODB_DATABASE` | `pipeline_db` | Target database |
| `MONGODB_COLLECTION` | `events` | Target collection |
| `LOG_LEVEL` | `INFO` | Logging level |
| `BATCH_SIZE` | `100` | Messages per batch write |
| `BATCH_TIMEOUT_MS` | `5000` | Max ms to wait before flushing batch |


## Environment setup
To see dependency tree use
```pipdeptree```

to update the dependencies use
```pip install .```
## Building


Build the pb2.py class and pb2.pyi helper for proto specification file 

``` protoc -I=. --python_out=. --pyi_out=. order_event.proto```
## Running Tests

The raw event is needed to put in unit test cases.  To get it directly out of the kafka Topic use

```
kcat -b localhost:9094 -t order-events -c 1 -f '%s' | xxd
```

```bash
pytest tests/unit -v
pytest tests/integration -v   # requires running Kafka + MongoDB
```
