# Targets are added as their scripts land; see the Commands section in CLAUDE.md.
KAFKA_TOPICS = docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092

.PHONY: env kafka-up kafka-down test live-probe

env:
	uv sync
	mkdir -p data/raw data/clean data/stream_in data/checkpoints data/serving data/models

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
