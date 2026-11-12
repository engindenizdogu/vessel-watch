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
make gaps       # find signal gaps in data/clean/, learn shutdown spots, write both (data/gaps/, data/models/)
make gates      # vessels crossing the harbor entrances, in and out, and crossings per hour (the forecast target)
make baseline   # score the forecast baselines on the test block (needs make gates)
make offroute   # learn ship lanes from the training days, flag off-route ships on the test days
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
make stream        # Spark Structured Streaming: ais.raw to data/serving/ (positions, signal gaps, reports per minute), and live history to data/live/
make app           # Streamlit dashboard at http://localhost:8501
```

The Live page shows each vessel's latest position from the last 15 minutes and refreshes every 30 seconds. The Data Dictionary page shows [docs/DATA_DICTIONARY.md](docs/DATA_DICTIONARY.md). The History page shows the harbor at a chosen time on any day in `data/clean/`, with a Play button and a vessels-per-hour chart; it needs only `make app`, not Kafka. Times are New York time. NOAA files are UTC days, so the last 4-5 hours of a day are empty unless the next day is also downloaded. The first `make stream` downloads the Spark Kafka connector, so it takes a minute to start. Stop each one with Ctrl+C, and the broker with `make kafka-down`.

Live history: `make stream` also stores every live report and, every hour, merges them into one file per day in `data/live/history/`, cleaned like the NOAA history; it also deletes serving data older than 2 days. `make compact` does the same once, for when the stream is not running.

## Replay

Replay sends days from `data/clean/` through the same Kafka topic and Spark job as the live feed, with their original timestamps, at an accelerated speed. It is the demo fallback when the live feed is quiet, and the test bed for the streaming detectors. Run one source at a time:

```
make kafka-up
make stream RUN=replay                         # wait for "Ready"; each replay run clears data/serving/replay/
make replay START=2023-02-24 [END=...] [SPEED=10]   # SPEED: times faster than real time (default 10); 0 = as fast as possible
make app                                       # Live page, Feed: Replay
```

On the Live page, **Replay** shows the latest 15 minutes before the newest replayed report. A replay run reads only messages produced after it starts, so start it before `make replay`. The range must lie inside one block of consecutive clean days (`make replay` lists them otherwise), since pacing follows event time and would wait out the months between blocks. The stream keeps live and replayed reports apart by age (live keeps the last 8 days, replay anything older), so a later `make stream` on the live feed skips replayed messages still in the topic. Replaying 2023-02-24 at `SPEED=0` (237,808 reports, sent in about 3 s) gave serving rows identical to `data/clean/`. For the gap detector use `SPEED=3600` or slower: at full speed the three Kafka partitions can drift more than the 2-minute watermark apart in event time, and late reports are dropped (the stream prints how many). Replaying 2023-02-24 to 2023-03-09 at 3600x (3.0 M reports, 6 minutes) reproduced 1,405 of the 1,407 batch gaps exactly; the other 2 would have fired after the data ended.

`make live-probe [DURATION=120]` listens to the live feed without Kafka and saves the raw messages to `data/live_sample/`, which is useful for checking the feed and the API key.

## Public demo link

The live dashboard is hosted at https://vessel-watch.northcentralus.cloudapp.azure.com on one Azure VM that runs Kafka, the live producer, the streaming job, and the dashboard. Setup and upkeep are in [docs/DEPLOY.md](docs/DEPLOY.md).

For a temporary link from the laptop instead: with the live dashboard running, `make tunnel` (needs `brew install cloudflared`) opens a [Cloudflare Quick Tunnel](https://developers.cloudflare.com/cloudflare-one/networks/connectors/cloudflare-tunnel/do-more-with-tunnels/trycloudflare/) to `localhost:8501` and prints a public `https://<random>.trycloudflare.com` URL. No Cloudflare account is needed.

- The link works only while the laptop is awake and running all four commands above.
- The URL changes every time the tunnel restarts, so share the new one each time.
- Anyone with the URL can view the dashboard. Only the dashboard is exposed, not Kafka.

## Configuration

Paths, topic names, the harbor bounding box, cleaning and gap thresholds, and dashboard settings live in `config.yaml`. Values marked "draft" are calibrated from the data profile.
