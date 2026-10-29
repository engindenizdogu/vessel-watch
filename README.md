# vessel-watch

NY Harbor vessel traffic analytics for AAI 627: a Spark and Kafka pipeline over NOAA AIS data and a live AIS feed, with an hourly arrival forecast, signal-gap and off-route alerts, and a Streamlit dashboard.

- Plan, scope, and schedule: [docs/PROJECT_PLAN.md](docs/PROJECT_PLAN.md)
- Data source links: [docs/SOURCES.md](docs/SOURCES.md)
- Data profile (generated): [docs/DATA_PROFILE.md](docs/DATA_PROFILE.md)

## Prerequisites

- [uv](https://docs.astral.sh/uv/) (installs Python 3.11 and the pinned dependencies)
- Java 17 (for Spark), e.g. `brew install openjdk@17`
- Docker with Compose (for Kafka), e.g. Docker Desktop
- A free [AISStream](https://aisstream.io/) API key (for the live feed)

## Setup

```
cp .env.example .env      # then add your AISStream API key
make env                  # create .venv and data/ folders
make test
```

## Historical data

NOAA daily CSVs (about 1 GB per day unzipped) go in `data/raw/` (git-ignored).

```
make download START=2024-07-01 END=2024-07-07   # download days into data/raw/ (existing days are skipped)
make clean-data                                  # Spark: clean data/raw/ into data/clean/ (Parquet by date)
make profile                                     # summarize data/clean/ into docs/DATA_PROFILE.md
```

`make clean-data` prints the row count after each cleaning step.

## Live dashboard

Start the broker once, then run the other three in their own terminals and leave them running:

```
make kafka-up      # start the Kafka broker (Docker) and create the topics; returns when ready
make live          # live producer: AISStream over the harbor box to the ais.raw topic
make stream        # Spark Structured Streaming: ais.raw to data/serving/positions/
make app           # Streamlit dashboard at http://localhost:8501
```

The map shows each vessel's latest position from the last 15 minutes and refreshes every 30 seconds. The first `make stream` downloads the Spark Kafka connector, so it takes a minute to start. Stop each one with Ctrl+C, and the broker with `make kafka-down`.

`make live-probe [DURATION=120]` listens to the live feed without Kafka and saves the raw messages to `data/live_sample/`, which is useful for checking the feed and the API key.

## Configuration

Paths, topic names, the harbor bounding box, cleaning and gap thresholds, and dashboard settings live in `config.yaml`. Values marked "draft" are calibrated from the data profile.
