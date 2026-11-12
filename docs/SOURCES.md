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
| Vessel Traffic landing page | https://hub.marinecadastre.gov/pages/vesseltraffic | Checked 2026-11-06: returns 200, title "Vessel Traffic - Marine Cadastre". Content is rendered by JavaScript, so not read by script. Linked from the dashboard as the NOAA overview. |
| AIS Vessel Tracks 2026 | https://www.fisheries.noaa.gov/inport/item/80415 | Track lines, not point reports |

2025: no daily point files in the bulk folder (`.../AISDataHandler/2025/` returns 404). Only monthly track files exist, which don't support gap detection.

Availability, checked 2026-11-06 with a HEAD request for every day of 2023 and 2024:
- 2024: only July 1 to December 31 work (184 days). January 1 to June 30 are listed in the index but return 404.
- 2023: all 365 days return 200 (https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2023/), about 280-330 MB zipped each. Same file pattern (`AIS_2023_MM_DD.zip`) and the same 17 CSV columns and timestamp format as 2024 (header read from `AIS_2023_03_14.zip`).
- 2022 and 2021 folders exist but were not checked.

CSV columns (from `AIS_2024_07_01.zip`):
```
MMSI,BaseDateTime,LAT,LON,SOG,COG,Heading,VesselName,IMO,CallSign,VesselType,Status,Length,Width,Draft,Cargo,TransceiverClass
```

Vessel type codes: "AIS Vessel Type and Group Codes used by the Marine Cadastre Project" (2018-05-23, sources U.S. Coast Guard, NOAA, BOEM): https://coast.noaa.gov/data/marinecadastre/ais/VesselTypeCodes2018.pdf (read 2026-11-06). Codes 0-99 are the AIS "ship and cargo type" each vessel broadcasts in its static data (the ITU-R M.1371 table): 30 fishing, 31-32 towing, 33 dredging, 35 military, 36 sailing, 37 pleasure craft, 50-59 service vessels (50 pilot, 52 tug, 55 law enforcement), 60-69 passenger, 70-79 cargo, 80-89 tanker, 90-99 other; the second digit marks the hazardous cargo category (e.g. 71 = cargo, category A). NOAA's own groups also put 21 and 22 under "Tug Tow", and older files used codes 1001-1025; in our 58 days only one vessel has 22 and one has 107. 84 vessels have no type and 46 have 0 (not available).

## Live AIS: AISStream.io

Checked 2026-10-22. The website blocks scripted access (HTTP 403, again on 2026-11-11), so the site itself was read in a browser.

**Terms and limits (checked 2026-11-11).** Deniz read the privacy policy in a browser and found nothing that blocks our use: https://aisstream.io/privacypolicy. AISStream publishes no terms of service or data licence: users report that `/terms` returns 404 and that the repositories carry no licence for the data, and many requests for written permission (commercial use, public display, storage) have no maintainer reply. What the maintainers have said, in their issue tracker (https://github.com/aisstream/issues):
- "We have a limit on updates a second" (2026-09-14): https://github.com/aisstream/issues/issues/291. The number is not published.
- Concurrent connections are capped; "the new 4th connection would be rejected" with HTTP 429, existing connections keep priority (2026-09-03): https://github.com/aisstream/issues/issues/292. One user measured 2, unconfirmed: https://github.com/aisstream/issues/issues/300. We use 1 (`make live`); `make live-probe` alongside it makes 2.
- The service filters no vessels, but individual receiving stations may downsample and drop small vessels (2023-09-04): https://github.com/aisstream/issues/issues/29.
- Near bandwidth capacity, and an outage on 2026-08-22 caused by load (circuit-breaker bug): https://github.com/aisstream/issues/issues/291, https://github.com/aisstream/issues/issues/273.
- Unanswered terms questions: https://github.com/aisstream/issues/issues/290 (storage and redistribution), https://github.com/aisstream/issues/issues/226 (commercial app).

Our use: a non-commercial course project, one server-side connection with a private key, attribution and a link on the Live page, a temporary public demo link, live history stored locally and never redistributed or committed.

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
- About one position report per vessel per minute. Not yet known whether that is feed throttling or receiver coverage; needs a longer probe. (Nov 6: median 180 s for Class A. Nov 11: the maintainers confirm both a per-second update limit and station downsampling exist, see above; which one sets our rate is still open.) Test 2026-11-12 03:50-04:02 UTC: two connections at once for 10 minutes, one on the whole harbor box and one filtered (`FiltersShipMMSI`) to the 6 Class A vessels reporting most; the filter worked, and both received exactly the same 17 reports from those vessels, none extra. So our subscription is not being throttled; the sparse rate comes from upstream (receiving stations or AISStream's own processing before the fan-out). Caveat: it was night and the feed carried only about 0.4 reports a second, so a per-second limit could still bind at busy daytime hours; a daytime rerun would settle that.

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

## Hosting (public demo link)

Checked 2026-11-05. The professor asked for a public link; chosen: Cloudflare Quick Tunnel to the local dashboard (`make tunnel`).

| What | Link | Notes |
| :-- | :-- | :-- |
| Cloudflare Quick Tunnels | https://developers.cloudflare.com/cloudflare-one/networks/connectors/cloudflare-tunnel/do-more-with-tunnels/trycloudflare/ | No account or domain needed. Limits from the page: no uptime guarantee, 200 in-flight requests, no Server-Sent Events, hostname changes on each start. WebSockets (which Streamlit needs) are not mentioned; test before relying on it. |
| Streamlit Community Cloud dependencies | https://docs.streamlit.io/deploy/streamlit-community-cloud/deploy-your-app/app-dependencies | Not chosen. Would need a committed data snapshot; cannot run Kafka or Spark. |
