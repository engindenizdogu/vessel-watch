# Targets are added as their scripts land; see the Commands section in CLAUDE.md.
KAFKA_TOPICS = docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092

.PHONY: env kafka-up kafka-down test live-probe live stream app clean-data profile download

env:
	uv sync
	mkdir -p data/raw data/clean data/checkpoints data/serving data/models

kafka-up:
	docker compose up -d
	@until $(KAFKA_TOPICS) --list >/dev/null 2>&1; do echo "waiting for Kafka..."; sleep 2; done
	$(KAFKA_TOPICS) --create --if-not-exists --topic ais.raw --partitions 3 --replication-factor 1
	$(KAFKA_TOPICS) --create --if-not-exists --topic ais.alerts --partitions 3 --replication-factor 1

kafka-down:
	docker compose down

test:
	uv run pytest

live-probe:
	uv run python -m ingest.live_probe --seconds $(or $(DURATION),120)

live:
	uv run python -m ingest.live_client

stream:
	uv run python -m pipeline.stream_job --run $(or $(RUN),live)

# python -m keeps the repo root on sys.path so the app can import pipeline/
app:
	uv run python -m streamlit run app/dashboard.py

clean-data:
	uv run python -m pipeline.batch_job

profile:
	uv run python -m pipeline.data_profile

download:
	uv run python -m ingest.noaa_download --start $(START) --end $(END)
