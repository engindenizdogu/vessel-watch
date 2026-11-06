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

```
make history    # sample NOAA days, download, clean into data/clean/ (Parquet by date), delete the raw files
make profile    # summarize data/clean/ into docs/DATA_PROFILE.md
make gaps       # find signal gaps in data/clean/ and write them to data/gaps/
```

`make history` draws random blocks of consecutive days; the `history` section of `config.yaml` sets the date ranges, the number of blocks, the days per block, and the seed. It downloads, cleans, and deletes one raw day at a time (NOAA daily CSVs are about 1 GB unzipped), so only about 1.4 GB of raw data is on disk at once. It prints the row count after each cleaning step. Days already in `data/clean/` are skipped, so it can be stopped with Ctrl+C and rerun. The default sample (4 blocks of 14 days) takes about 35-45 minutes.

Optional, for specific days or to keep the raw CSVs (for example, to re-clean after changing a cleaning rule without downloading again):

```
make download START=2024-07-01 END=2024-07-07   # download days into data/raw/ and keep them (existing days are skipped)
make clean-data                                  # clean every CSV in data/raw/ into data/clean/
```

## Live dashboard

Start the broker once, then run the other three in their own terminals and leave them running:

```
make kafka-up      # start the Kafka broker (Docker) and create the topics; returns when ready
make live          # live producer: AISStream over the harbor box to the ais.raw topic
make stream        # Spark Structured Streaming: ais.raw to data/serving/positions/
make app           # Streamlit dashboard at http://localhost:8501
```

The Live page shows each vessel's latest position from the last 15 minutes and refreshes every 30 seconds. The Data Dictionary page shows [docs/DATA_DICTIONARY.md](docs/DATA_DICTIONARY.md). The History page shows the harbor at a chosen time on any day in `data/clean/`, with a Play button and a vessels-per-hour chart; it needs only `make app`, not Kafka. Times are New York time. NOAA files are UTC days, so the last 4-5 hours of a day are empty unless the next day is also downloaded. The first `make stream` downloads the Spark Kafka connector, so it takes a minute to start. Stop each one with Ctrl+C, and the broker with `make kafka-down`.

`make live-probe [DURATION=120]` listens to the live feed without Kafka and saves the raw messages to `data/live_sample/`, which is useful for checking the feed and the API key.

## Public demo link

With the live dashboard running, `make tunnel` (needs `brew install cloudflared`) opens a [Cloudflare Quick Tunnel](https://developers.cloudflare.com/cloudflare-one/networks/connectors/cloudflare-tunnel/do-more-with-tunnels/trycloudflare/) to `localhost:8501` and prints a public `https://<random>.trycloudflare.com` URL. No Cloudflare account is needed.

- The link works only while the laptop is awake and running all four commands above.
- The URL changes every time the tunnel restarts, so share the new one each time.
- Anyone with the URL can view the dashboard. Only the dashboard is exposed, not Kafka.

## Configuration

Paths, topic names, the harbor bounding box, cleaning and gap thresholds, and dashboard settings live in `config.yaml`. Values marked "draft" are calibrated from the data profile.
