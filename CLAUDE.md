# NY Harbor Vessel Traffic Analytics

**Identity:** You are a pair-programming assistant helping Deniz build a big data analytics app for a course project (AAI 627, due first week of December 2026).
**Goal:** A Spark and Kafka pipeline that cleans historical NOAA AIS data and a live AIS feed, forecasts hourly vessel arrivals in New York Harbor, flags signal-gap and off-route anomalies, and shows results on a Streamlit dashboard.
**Plan:** See `docs/PROJECT_PLAN.md` for scope, definitions, architecture, schedule, and checkpoints. Treat it as the source of truth; ask before changing scope.

## Stack
- Python 3.11, Apache Spark 3.5.9 (PySpark, local mode), Java 17
- Apache Kafka 4.3 (single broker, KRaft, Docker Compose); topic `ais.raw` keyed by MMSI, 3 partitions; optional `ais.alerts`; producers use `confluent-kafka`, the live feed is AISStream over `websockets`
- Parquet (partitioned by date), DuckDB (read-only serving queries over Parquet), rounded lat/lon grid (geospatial)
- pandas (forecast target, lags, baselines), scikit-learn or LightGBM (forecast model, week 5), Streamlit with pydeck and Altair (dashboard), FastAPI (API, planned)
- pytest, Makefile, `uv`
- Hosting: one Azure VM (Standard_B2s, Azure for Students credit) runs the live side for the public demo at https://vessel-watch.northcentralus.cloudapp.azure.com (setup in `docs/DEPLOY.md`); batch jobs and training stay on the laptop

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
- `make live`: run the live producer (AISStream over the harbor box to `ais.raw`, keyed by MMSI) until Ctrl+C; needs `make kafka-up` and `AISSTREAM_API_KEY` in `.env`
- `make replay START=YYYY-MM-DD [END=...] [SPEED=10]`: the replay producer; sends `data/clean/` days to `ais.raw` in event-time order with original timestamps, keyed by MMSI, `SPEED` times faster than real time (`replay.speed`; 0 = as fast as possible); the range must be one block of consecutive clean days. Start `make stream RUN=replay` first and wait for its "Ready" line
- `make stream [RUN=live|replay]`: run the Structured Streaming job; applies the stateless cleaning steps and writes three queries (own checkpoint each) to the run's serving folder (`data/serving/`, or `data/serving/replay/`): `positions/`, `gap_events/` (streaming signal gaps, `models.gaps.track_gaps` with `applyInPandasWithState`; read them through `confirmed_gaps`, which applies the feed-outage guard), and `minute_counts/` (reports per minute for that guard), and `offroute/` (off-route reports, once `make offroute` has written the lanes). Each run keeps only its own reports by age (`stream.live_max_age_days`: live keeps the last 8 days, replay older ones). `RUN=replay` starts fresh every time: it clears `data/serving/replay/` and its checkpoints and reads only messages produced after it starts. With `RUN=live` a second query (own checkpoint) also writes live history to `data/live/incoming/`, and every hour (`stream.compact_minutes`) it compacts that into `data/live/history/` (one file per day, today included; duplicate and spike steps applied) and deletes serving days older than `stream.serving_days`
- `make compact` (optional): run that compaction once, only when `make stream` is not running
- `make app`: start the Streamlit dashboard on port 8501: a Live page (latest positions from serving Parquet; a Feed switch shows the replay output instead) a History page (map at a chosen time on a cleaned NOAA day, vessels per hour), both read through DuckDB, and a Data Dictionary page that renders `docs/DATA_DICTIONARY.md`
- `make tunnel`: public demo link; a Cloudflare Quick Tunnel to the local dashboard (needs `cloudflared`; random URL per start, works only while the laptop runs the pipeline)
- `make history`: the main way to get NOAA history. Samples random blocks of consecutive days (config `history`), then downloads, cleans into `data/clean/` (Parquet by date, row counts per step), and deletes each raw day in turn; skips days already cleaned
- `make download START=YYYY-MM-DD END=YYYY-MM-DD` (optional): download specific NOAA days into `data/raw/` and keep the CSVs (about 1 GB per day unzipped; existing days skipped)
- `make clean-data` (optional): clean every `data/raw/AIS_*.csv` with Spark into `data/clean/`, e.g. to re-clean kept CSVs after a rule change
- `make gaps`: find signal gaps in `data/clean/` with Spark (rule in `models/gaps.py`), learn shutdown spots from them (`data/models/shutdown_spots.parquet`, drawn on the dashboard maps), and write the labeled gaps to `data/gaps/` (Parquet by date), printing counts by state
- `make gates`: find gate crossings in `data/clean/` with Spark (rule in `pipeline/gates.py`), write them to `data/crossings/` (Parquet by date) and crossings per hour, gate, direction, and vessel group, zeros included, to `data/crossings_hourly.parquet` (the forecast target table, with each hour's feed-outage minutes)
- `make baseline`: score the forecast baselines (same time last week and yesterday, training time-of-day mean, always zero) on the test block (`forecast.test_start`), hourly and in 3-hour bins, for the target's vessel groups and wider ones (`forecast` in `config.yaml`); target and lag code in `pipeline/features.py`, scoring in `models/forecast.py`
- `make offroute`: learn ship lanes from the training days (`split.test_start`) into `data/models/ship_lanes.parquet` (read by `make stream`, which then writes off-route reports to its serving folder) and flag off-route reports on the test days into `data/offroute/` (Parquet by date), printing the alert rate and the alerts
- `make profile`: profile `data/clean/` with DuckDB (traffic per hour, report intervals by status, implied speeds) and write `docs/DATA_PROFILE.md`

Planned:
- `make train`: build features and train the forecast model offline
- `make api`: start FastAPI

## Architecture Rules
1. **One code path.** Cleaning and feature logic live in `pipeline/clean.py` and `pipeline/features.py` and are imported by the batch job, the streaming job, and training. Never duplicate them (avoids training-serving skew).
2. **Live and replay share one topic and one consumer.** Both producers emit the same message schema (timestamp, MMSI, vessel name, lat, lon, speed over ground, course over ground, heading, vessel type, status).
3. **Key Kafka messages by MMSI** so each vessel stays on one partition. Spark does not preserve Kafka order through shuffles, so gap and arrival logic must sort by the report's event timestamp.
4. **Training reads Parquet, not Kafka.** Training uses features built from clean historical Parquet and saves the model to a versioned file. The training sink is a separate streaming query with its own checkpoint that stores live history.
5. **Time-based splits only.** Train on earlier data, test on later data. Never shuffle time series.
6. **Injected anomalies are evaluation-only.** Keep them on a separate path (`models/inject.py`); never let them reach training data or the serving store.
7. **Avoid leakage.** Features at time t use only data available before t.
8. **Handle nulls explicitly.** In Spark, `when(null < x)` is not true and silently falls through to `otherwise`; check windows with too few rows before alerting.
9. **Pre-filter geospatially.** Apply the harbor bounding box before gate and grid checks and avoid row-wise Python UDFs on large data. Use a rounded lat/lon grid from native Spark expressions and map geofences to grid cells once, offline.
10. **Per-vessel streaming state uses `applyInPandasWithState`.** Streaming has no `lag` or row-based windows; gaps fire from an event-time timeout with a watermark. Run one source (replay or live) at a time, each with its own checkpoint.
11. **The streaming job writes Parquet, never DuckDB.** DuckDB allows one writer process; the API reads the Parquet through DuckDB read-only.

## Data and Security
- Never commit raw data, Parquet outputs, Spark checkpoints, or `data/`.
- Never commit API keys or tokens (live AIS service key); read them from environment variables and keep an `.env.example` with placeholders.
- Verify NOAA column names and download format against the current archive before coding against them; do not assume.
- Confirm the live feed's New York Harbor coverage and terms before relying on it.

## Working Style
- Follow the weekly plan in `docs/PROJECT_PLAN.md`. Checkpoints: Oct 25 (data usable, with numbers), Oct 30 (Kafka reads into Spark; passed Oct 28), feature freeze in week 5 (Nov 16-22, date to be decided; moved from Nov 15 on Nov 11).
- If scope has to be cut, cut in this order: off-route alerts, then the live feed (demo from replay), then forecast tuning.
- Build vertical slices that run end to end, then improve. Prefer simple, explainable rules (signal gap) before learned models.
- Report forecast results honestly, including when the model does not beat the seasonal-naive baseline.
- Write small tests for cleaning rules and anomaly logic; use the replay producer as the integration test.
- Do not execute or claim results you have not run. State when code is untested.
- Keep replies short. Do not restate the request or recap diffs.
- Do not commit or push unless Deniz asks.

## Code Conventions
- Type hints and short docstrings on public functions; no dead code or speculative abstractions.
- Config (paths, thresholds, topic names, gate definitions) lives in `config.yaml`, loaded with `pipeline.config.load_config()`; no scattered constants.
- Thresholds from the plan (signal-gap seconds per status, density percentile) are parameters, finalized from the data profile.
- Log row counts at each cleaning step so data loss is visible.

## Definitions (draft, finalized in week 1)
- **Gate crossing (arrival):** a vessel's consecutive reports, at most 30 min apart, whose track crosses a gate line at a harbor entrance; "in" if it ends on the harbor side. Rule in `pipeline/gates.py`, gates and vessel groups in `config.yaml` (`gates`, `vessel_groups`). Forecast target: inbound cargo ships and tankers per hour at the sea entrance (`gates.target`).
- **Signal gap:** a Class A vessel that was reporting regularly goes silent longer than max(minimum for its state, 8 x its median recent interval); state is moving (>= 1 knot, 10 min minimum) or stopped (30 min, includes anchored and moored). Skip vessels last seen near the box edge; never alert past the end of the data or across a feed outage (a minute with under 25% of the day's median reports). Gaps starting at a learned shutdown spot (a berth where vessels routinely switch AIS off) are labeled `known_spot`. Rule in `models/gaps.py`, parameters in `config.yaml` (`signal_gap`).
- **Off-route:** a cargo ship or tanker under way (2 knots or more) in a grid cell where no such ship moved, in the cell or its 8 neighbours, during the training days (before `split.test_start`); flagged reports of one ship at most 30 min apart are one alert. Rule in `models/offroute.py`, parameters in `config.yaml` (`offroute`).
