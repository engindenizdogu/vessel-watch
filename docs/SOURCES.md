# Data Sources

Links and findings for the project's data sources. Add new ones as they come up, with the date checked.

## Historical AIS: NOAA MarineCadastre

Checked 2026-10-22. No account or request needed for bulk downloads.

| What | Link | Notes |
| :-- | :-- | :-- |
| 2024 daily CSV index | https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2024/ | 366 files, nationwide, about 360 MB zipped each. **Chosen raw source.** |
| Example daily file | https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2024/AIS_2024_07_01.zip | Pattern: `AIS_2024_MM_DD.zip` |
| 2024 daily GeoParquet (experimental) | https://ocmgeodatastor1.blob.core.windows.net/marinecadastre/ais2024/ais-2024-07-01.parquet | Pattern: `ais-2024-MM-DD.parquet`, about 280 MB each. Same publisher, hosted on Azure. Columns not yet checked. |
| Official README | https://github.com/ocm-marinecadastre/ais-vessel-traffic/blob/main/README.md | Lists download options and GeoParquet file names |
| AIS FAQ | https://coast.noaa.gov/data/marinecadastre/ais/faq.pdf | |
| AccessAIS custom orders | https://hub.marinecadastre.gov/datasets/accessais | Map-based ordering; was unavailable when checked. Not needed. |
| AIS Vessel Tracks 2026 | https://www.fisheries.noaa.gov/inport/item/80415 | Track lines, not point reports |

2025: no daily point files in the bulk folder (`.../AISDataHandler/2025/` returns 404). Only monthly track files exist, which don't support gap detection.

CSV columns (from `AIS_2024_07_01.zip`):
```
MMSI,BaseDateTime,LAT,LON,SOG,COG,Heading,VesselName,IMO,CallSign,VesselType,Status,Length,Width,Draft,Cargo,TransceiverClass
```

## Live AIS: AISStream.io

Checked 2026-10-22. The website blocks scripted access, so the terms, coverage, and rate limits below are **not yet verified**; read them in a browser.

| What | Link | Notes |
| :-- | :-- | :-- |
| Website (sign up, API key) | https://aisstream.io/ | Free; sign in with GitHub to get a key |
| Documentation | https://aisstream.io/documentation | Message schema, subscription options |
| Official examples | https://github.com/aisstream/example | Python example in `python/main.py` (updated 2026-09) |
| Message models | https://github.com/aisstream/ais-message-models | |

From the official Python example:
- Endpoint: `wss://stream.aisstream.io/v0/stream`, with `compression="deflate"` (required for full bandwidth).
- First message: `{"APIKey": "...", "BoundingBoxes": [[[lat1, lon1], [lat2, lon2]]]}`. Corners are lat/lon order.
- Position reports arrive as `MessageType == "PositionReport"`; fields include `UserID` (the MMSI), `Latitude`, `Longitude`.

First probe (`make live-probe`, 60 s, 2026-10-23 02:55 UTC, harbor box):
- Works with our key; New York Harbor is covered. 69 messages, 61 distinct vessels.
- Message types seen: `PositionReport` (Class A), `StandardClassBPositionReport`, `ShipStaticData` (has `Type`, the vessel type), `StaticDataReport` (Class B name), and one `SubscriptionConfirmation` first.
- Every message has `MetaData` with `MMSI`, `ShipName`, `latitude`, `longitude`, `time_utc`. Speed, course, heading, and status are inside `Message.<type>` as `Sog`, `Cog`, `TrueHeading`, `NavigationalStatus`.
- Vessel type is not in position reports; it comes only from `ShipStaticData`.
- About one position report per vessel per minute. Not yet known whether that is feed throttling or receiver coverage; needs a longer probe.

NOAA 2024-07-01, same box and minute: 165 rows from 165 vessels, exactly one per vessel. The NOAA archive appears to be downsampled to one report per vessel per minute.

### Other live options (not chosen)

| What | Link | Notes |
| :-- | :-- | :-- |
| AISHub | https://www.aishub.net/join-us | API access only if you contribute your own receiver feed (at least 10 vessels, 90% uptime). Not practical. |
| Own receiver | | RTL-SDR dongle and AIS-catcher software. Only useful with a line of sight to the harbor. |
| Paid APIs (MarineTraffic, Spire, etc.) | | Good coverage, not free. |

## Reference geometry (geofences and lanes)

Checked 2026-10-22; both downloads return HTTP 200. Contents not yet inspected.

| What | Link | Notes |
| :-- | :-- | :-- |
| Shipping fairways, lanes, and zones | https://encdirect.noaa.gov/theme_layers/data/shipping_lanes/shippinglanes.zip | NOAA, from nautical charts, CC0. Covers US traffic lanes and separation zones (NY coverage not yet checked). Candidate off-route reference and geofence source. |
| Lanes metadata | https://catalog.data.gov/dataset/shipping-fairways-lanes-and-zones-for-us-waters | |
| Anchorage areas | https://marinecadastre.gov/downloads/data/mc/Anchorage.zip | Candidate anchorage geofences |
| MarineCadastre data registry | https://marinecadastre.gov/data/ | Other layers (ports, terminals) |

## Context features (optional, for the forecast)

| What | Link | Notes |
| :-- | :-- | :-- |
| NOAA tides, The Battery (8518750) | https://api.tidesandcurrents.noaa.gov/api/prod/datagetter?station=8518750&product=water_level&datum=MLLW&date=latest&units=metric&time_zone=gmt&format=json&application=vessel-watch | Free, no key. Water level works; wind is not offered at this station. |
| Open-Meteo historical weather | https://archive-api.open-meteo.com/v1/archive?latitude=40.70&longitude=-74.01&start_date=2024-07-01&end_date=2024-07-01&hourly=wind_speed_10m | Free, no key. Hourly wind works; visibility came back undefined. |
