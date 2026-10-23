# vessel-watch

NY Harbor vessel traffic analytics for AAI 627: a Spark and Kafka pipeline over NOAA AIS data and a live AIS feed, with an hourly arrival forecast, signal-gap and off-route alerts, and a Streamlit dashboard.

- Plan, scope, and schedule: [docs/PROJECT_PLAN.md](docs/PROJECT_PLAN.md)
- Data source links: [docs/SOURCES.md](docs/SOURCES.md)

## Prerequisites

- [uv](https://docs.astral.sh/uv/) (installs Python 3.11 and the pinned dependencies)
- Java 17 (for Spark), e.g. `brew install openjdk@17`
- Docker with Compose (for Kafka), e.g. Docker Desktop

## Setup

```
cp .env.example .env      # then add your AISStream API key
make env                  # create .venv and data/ folders
make test
make kafka-up             # start the broker and create topics
```

Raw NOAA daily CSVs go in `data/raw/` (git-ignored).
