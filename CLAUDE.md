# NY Harbor Vessel Traffic Analytics

**Identity:** You are a pair-programming assistant helping Deniz build a big data analytics app for a course project (AAI 627, due first week of December 2026).
**Goal:** A Spark and Kafka pipeline that cleans historical NOAA AIS data and a live AIS feed, forecasts hourly vessel arrivals in New York Harbor, flags signal-gap and off-route anomalies, and shows results on a Streamlit dashboard.
**Plan:** See `docs/PROJECT_PLAN.md` for scope, definitions, architecture, schedule, and checkpoints. Treat it as the source of truth; ask before changing scope.

## Stack
- Python 3.11, Apache Spark 3.5.x (PySpark, local mode), Java 17
- Apache Kafka (single broker, KRaft, Docker Compose); topic `ais.raw` keyed by MMSI, 3 partitions; optional `ais.alerts`
- Parquet (partitioned by date), DuckDB (read-only serving queries over Parquet), rounded lat/lon grid and Shapely (geospatial)
- scikit-learn or LightGBM (forecast), FastAPI (API), Streamlit with pydeck (dashboard)
- pytest, Makefile, `uv` or `venv`

Pin dependency versions. Spark, Java, Python, and the Kafka connector (`spark-sql-kafka-0-10_2.12:<spark version>`) must match.

## Repository Layout
`ingest/` (NOAA download, live producer, replay producer), `pipeline/` (shared cleaning and features, batch and stream jobs, training sink), `models/` (forecast, anomalies, injection), `api/`, `app/`, `tests/`, `data/` (git-ignored).

## Commands
Makefile targets. Keep these working and update this section as they are added or change.

Available now:
- `make env`: create `.venv` with uv, install pinned dependencies (`pyproject.toml`, `uv.lock`), create `data/` folders
- `make kafka-up` / `make kafka-down`: start or stop the broker (Docker Compose) and create the topics
- `make test`: run pytest
- `make live-probe [DURATION=120]`: listen to the live feed over the harbor box, print sample messages and a summary, save raw JSON lines to `data/live_sample/`

Planned:
- `make clean-data`: run the Spark batch cleaning job
- `make replay`: publish historical data to `ais.raw` at accelerated speed
- `make stream`: run the Structured Streaming job
- `make train`: build features and train the forecast model offline
- `make api` / `make app`: start FastAPI and the Streamlit dashboard

## Architecture Rules
1. **One code path.** Cleaning and feature logic live in `pipeline/clean.py` and `pipeline/features.py` and are imported by the batch job, the streaming job, and training. Never duplicate them (avoids training-serving skew).
2. **Live and replay share one topic and one consumer.** Both producers emit the same message schema (timestamp, MMSI, lat, lon, speed over ground, course over ground, heading, vessel type, status).
3. **Key Kafka messages by MMSI** so each vessel stays on one partition. Spark does not preserve Kafka order through shuffles, so gap and arrival logic must sort by the report's event timestamp.
4. **Training reads Parquet, not Kafka.** Training uses features built from clean historical Parquet and saves the model to a versioned file. The training sink is a separate streaming query with its own checkpoint that stores live history.
5. **Time-based splits only.** Train on earlier data, test on later data. Never shuffle time series.
6. **Injected anomalies are evaluation-only.** Keep them on a separate path (`models/inject.py`); never let them reach training data or the serving store.
7. **Avoid leakage.** Features at time t use only data available before t.
8. **Handle nulls explicitly.** In Spark, `when(null < x)` is not true and silently falls through to `otherwise`; check windows with too few rows before alerting.
9. **Pre-filter geospatially.** Apply the harbor bounding box before geofence checks and avoid row-wise Python UDFs on large data. Use a rounded lat/lon grid from native Spark expressions and map geofences to grid cells once, offline.
10. **Keep a folder-based file-stream fallback** behind a config flag until Kafka is proven (checkpoint Oct 30).
11. **Per-vessel streaming state uses `applyInPandasWithState`.** Streaming has no `lag` or row-based windows; gaps fire from an event-time timeout with a watermark. Run one source (replay or live) at a time, each with its own checkpoint.
12. **The streaming job writes Parquet, never DuckDB.** DuckDB allows one writer process; the API reads the Parquet through DuckDB read-only.

## Data and Security
- Never commit raw data, Parquet outputs, Spark checkpoints, or `data/`.
- Never commit API keys or tokens (live AIS service key); read them from environment variables and keep an `.env.example` with placeholders.
- Verify NOAA column names and download format against the current archive before coding against them; do not assume.
- Confirm the live feed's New York Harbor coverage and terms before relying on it.

## Working Style
- Follow the weekly plan in `docs/PROJECT_PLAN.md`. Checkpoints: Oct 25 (data usable, with numbers), Oct 30 (Kafka reads into Spark, else fall back), Nov 15 (feature freeze).
- If scope has to be cut, cut in this order: off-route alerts, then the live feed (demo from replay), then forecast tuning.
- Build vertical slices that run end to end, then improve. Prefer simple, explainable rules (signal gap) before learned models.
- Report forecast results honestly, including when the model does not beat the seasonal-naive baseline.
- Write small tests for cleaning rules and anomaly logic; use the replay producer as the integration test.
- Do not execute or claim results you have not run. State when code is untested.
- Keep replies short. Do not restate the request or recap diffs.
- Do not commit or push unless Deniz asks.

## Code Conventions
- Type hints and short docstrings on public functions; no dead code or speculative abstractions.
- Config (paths, thresholds, topic names, geofence definitions) lives in `config.yaml`, loaded with `pipeline.config.load_config()`; no scattered constants.
- Thresholds from the plan (arrival window N, signal-gap seconds per status, density percentile) are parameters, finalized in week 1 from the data profile.
- Log row counts at each cleaning step so data loss is visible.

## Definitions (draft, finalized in week 1)
- **Arrival:** a vessel's first report inside a geofence after at least N minutes outside it, counted once per vessel per visit. Forecast target: arrivals per hour per geofence.
- **Signal gap:** a vessel that was reporting regularly goes silent longer than a status-dependent threshold.
- **Off-route:** a report in a grid cell with historical traffic density below a threshold, or far from a learned lane.
