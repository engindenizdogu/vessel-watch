# Targets are added as their scripts land; see the Commands section in CLAUDE.md.
KAFKA_TOPICS = docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092
APP_PORT = 8501

.PHONY: env kafka-up kafka-down test live-probe live stream app tunnel clean-data profile download history gaps gates compact

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

# Live history: merge finished days into data/live/history and trim old serving data (safe while streaming).
compact:
	uv run python -m pipeline.compact_job

# python -m keeps the repo root on sys.path so the app can import pipeline/.
# The port is pinned so Streamlit fails instead of moving to another port the tunnel doesn't point at.
app:
	uv run python -m streamlit run app/dashboard.py --server.port $(APP_PORT)

# Public demo link: Cloudflare Quick Tunnel to the local dashboard (new random URL on each start).
tunnel:
	cloudflared tunnel --url http://localhost:$(APP_PORT)

clean-data:
	uv run python -m pipeline.batch_job

profile:
	uv run python -m pipeline.data_profile

download:
	uv run python -m ingest.noaa_download --start $(START) --end $(END)

history:
	uv run python -m ingest.noaa_history

gaps:
	uv run python -m pipeline.gap_job

gates:
	uv run python -m pipeline.gate_job
