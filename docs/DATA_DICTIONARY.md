# Data Dictionary

Both sources are cleaned into one shared schema (Table 1), which is what the dashboard shows. Tables 2 and 3 list the raw fields it is built from, and Table 4 describes the signal gaps found in the historical data.

## Sources

- **Historical:** NOAA and BOEM MarineCadastre AIS vessel traffic data ([overview](https://hub.marinecadastre.gov/pages/vesseltraffic), [documentation](https://github.com/ocm-marinecadastre/ais-vessel-traffic)). Daily files used here: [2023](https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2023/) and [2024](https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2024/) (2024 only from July 1; earlier 2024 days are listed but do not download).
- **Live:** [AISStream.io](https://aisstream.io/), a free WebSocket feed of AIS messages ([documentation](https://aisstream.io/documentation)).

## Table 1. Cleaned position reports (shown on the dashboard)

One row per position report inside the harbor box (latitude 40.40 to 40.95, longitude -74.30 to -73.70). Historical days are stored by date in `data/clean/` and live reports in `data/serving/positions/`. Historical data also has duplicate reports and isolated position spikes (implied speed over 50 knots to both neighbouring reports) removed; the live stream skips those two steps because they need each vessel's earlier reports.

| Column        | Type      | Description                                                                                                                                                                  | From NOAA (Table 2) | From AISStream (Table 3)  |
| :------------ | :-------- | :--------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :------------------ | :------------------------ |
| `ts`          | timestamp | Time of the report, stored in UTC. The dashboard shows it in New York time.                                                                                                  | `BaseDateTime`      | `MetaData.time_utc`       |
| `mmsi`        | integer   | Vessel's 9-digit radio ID. Only ship MMSIs are kept (first digit 2-7); base stations, aids to navigation, and search-and-rescue units are dropped.                           | `MMSI`              | `MetaData.MMSI`           |
| `name`        | text      | Vessel name, trimmed; empty when the vessel sent none (shown as "(no name)"). Live reports saved before this column was added (early Nov 2026) have none.                                            | `VesselName`        | `MetaData.ShipName`       |
| `lat`         | decimal   | Latitude in decimal degrees.                                                                                                                                                 | `LAT`               | `Latitude`                |
| `lon`         | decimal   | Longitude in decimal degrees (negative = west).                                                                                                                              | `LON`               | `Longitude`               |
| `sog`         | decimal   | Speed over ground, in knots. Empty when the vessel sent "not available" (102.3).                                                                                             | `SOG`               | `Sog`                     |
| `cog`         | decimal   | Course over ground (direction of travel), in degrees from true north. Empty when not available (360).                                                                         | `COG`               | `Cog`                     |
| `heading`     | integer   | Direction the bow points, in degrees from true north. Empty when not available (511).                                                                                        | `Heading`           | `TrueHeading`             |
| `vessel_type` | integer   | AIS ship type code (see Table 2). Live: taken from the vessel's latest static data message, so it is empty until one has been received since the live feed started.          | `VesselType`        | `Type` or `ReportB.ShipType` |
| `status`      | integer   | Navigational status code, e.g. 0 under way using engine, 1 at anchor, 5 moored. Empty for Class B transmitters, which do not send it.                                        | `Status`            | `NavigationalStatus`      |
| `date`        | date      | UTC date of `ts`; the Parquet files are partitioned by it.                                                                                                                   |                     |                           |

## Table 2. Historical AIS data (NOAA MarineCadastre, 2023 and 2024 daily files)

One CSV per day covering all US waters, one row per vessel position report. The archive keeps about one report per vessel per minute. Example: 2024-07-01 has 8,999,649 rows from 22,651 vessels. The 2023 files have the same columns.

| Column             | Type      | Description                                                                                                                                           |
| :----------------- | :-------- | :---------------------------------------------------------------------------------------------------------------------------------------------------- |
| `MMSI`             | integer   | Maritime Mobile Service Identity, the vessel's 9-digit radio ID. Main vessel key.                                                                     |
| `BaseDateTime`     | timestamp | Time of the report, in UTC (`YYYY-MM-DDTHH:MM:SS`).                                                                                                   |
| `LAT`              | decimal   | Latitude in decimal degrees.                                                                                                                          |
| `LON`              | decimal   | Longitude in decimal degrees (negative = west).                                                                                                       |
| `SOG`              | decimal   | Speed over ground, in knots. `102.3` means not available.                                                                                             |
| `COG`              | decimal   | Course over ground (direction of travel), in degrees from true north. `360` means not available.                                                      |
| `Heading`          | decimal   | True heading (direction the bow points), in degrees. `511` means not available.                                                                       |
| `VesselName`       | text      | Name broadcast by the vessel.                                                                                                                         |
| `IMO`              | text      | International Maritime Organization number, a permanent hull ID (e.g. `IMO9190705`). Often missing or `IMO0000000` for small craft.                   |
| `CallSign`         | text      | Radio call sign.                                                                                                                                      |
| `VesselType`       | integer   | AIS ship type code, e.g. 30 fishing, 31-32 towing, 37 pleasure craft, 52 tug, 60-69 passenger, 70-79 cargo, 80-89 tanker.                             |
| `Status`           | integer   | Navigational status code, e.g. 0 under way using engine, 1 at anchor, 5 moored, 15 not defined. Empty for Class B transmitters, which do not send it. |
| `Length`           | integer   | Vessel length, in metres.                                                                                                                             |
| `Width`            | integer   | Vessel width (beam), in metres.                                                                                                                       |
| `Draft`            | decimal   | Draft (depth below the waterline), in metres. Mostly empty for Class B.                                                                               |
| `Cargo`            | integer   | Cargo type code from the vessel's static data. Mostly empty for Class B.                                                                              |
| `TransceiverClass` | text      | AIS equipment class: `A` (large commercial vessels, required by law) or `B` (smaller vessels, fewer fields, less frequent reports).                   |

## Table 3. Live AIS feed (AISStream.io WebSocket)

Each message is a JSON object with a `MessageType`, a `MetaData` block, and a `Message` block holding the decoded AIS fields. Position reports and static data (vessel type, size) arrive as separate message types. The last column shows the matching NOAA column.

| Field                                     | Message type       | Description                                                                                                                            | NOAA column                     |
| :---------------------------------------- | :----------------- | :------------------------------------------------------------------------------------------------------------------------------------- | :------------------------------ |
| `MessageType`                             | all                | Kind of message: `PositionReport` (Class A), `StandardClassBPositionReport`, `ShipStaticData` (Class A), `StaticDataReport` (Class B). |                                 |
| `MetaData.MMSI`                           | all                | Vessel's 9-digit radio ID. Also given as `UserID` inside `Message`.                                                                    | `MMSI`                          |
| `MetaData.time_utc`                       | all                | Message time stamped by AISStream, in UTC, to the nanosecond.                                                                          | `BaseDateTime`                  |
| `MetaData.latitude`, `MetaData.longitude` | all                | Vessel position in decimal degrees, rounded to 5 places. The exact values are in `Message` as `Latitude` and `Longitude`.              | `LAT`, `LON`                    |
| `MetaData.ShipName`                       | all                | Vessel name, if known (can be empty or padded with spaces).                                                                            | `VesselName`                    |
| `Sog`                                     | position reports   | Speed over ground, in knots.                                                                                                           | `SOG`                           |
| `Cog`                                     | position reports   | Course over ground, in degrees from true north.                                                                                        | `COG`                           |
| `TrueHeading`                             | position reports   | Direction the bow points, in degrees. `511` means not available.                                                                       | `Heading`                       |
| `NavigationalStatus`                      | `PositionReport`   | Navigational status code (same codes as NOAA). Not sent by Class B.                                                                    | `Status`                        |
| `RateOfTurn`                              | `PositionReport`   | Rate of turn as an encoded AIS value (0 = not turning).                                                                                |                                 |
| `Timestamp`                               | position reports   | Second of the minute (0-59) when the position was fixed; `60` means not available.                                                     |                                 |
| `PositionAccuracy`                        | position reports   | `true` if the position fix is accurate to within 10 m.                                                                                 |                                 |
| `Type`                                    | `ShipStaticData`   | AIS ship type code (same codes as NOAA).                                                                                               | `VesselType`                    |
| `ImoNumber`, `CallSign`, `Name`           | `ShipStaticData`   | IMO number (0 if none), call sign, and vessel name.                                                                                    | `IMO`, `CallSign`, `VesselName` |
| `Dimension.A`, `.B`, `.C`, `.D`           | `ShipStaticData`   | Distance in metres from the GPS antenna to bow, stern, port, and starboard. Length = A + B, width = C + D.                             | `Length`, `Width`               |
| `MaximumStaticDraught`                    | `ShipStaticData`   | Draft as reported by the vessel (0 = not available).                                                                                   | `Draft`                         |
| `Destination`, `Eta`                      | `ShipStaticData`   | Destination typed in by the crew, and estimated arrival time (month, day, hour, minute).                                               |                                 |
| `ReportA.Name` / `ReportB.ShipType`       | `StaticDataReport` | Class B static data sent in two parts: name, then ship type and call sign.                                                             | `VesselName`, `VesselType`      |

Other fields in the messages (`RepeatIndicator`, `Raim`, `CommunicationState`, `Spare`, and similar) are radio-protocol details and are not used.

## Table 4. Signal gaps (`make gaps`, stored in `data/gaps/`)

One row per gap: a Class A vessel that was reporting regularly went silent longer than its threshold (see the signal-gap definition in the project plan). Built from the cleaned historical data (Table 1).

| Column        | Type      | Description                                                                                                       |
| :------------ | :-------- | :---------------------------------------------------------------------------------------------------------------- |
| `mmsi`, `name`, `vessel_type`, `status` | | The vessel, as in Table 1. `status` is the status it declared in its last report.                   |
| `state`       | text      | `moving` (1 knot or more in the last report) or `stopped` (slower, or no speed; includes anchored and moored).    |
| `last_ts`     | timestamp | Time of the last report before the silence (UTC).                                                                 |
| `lat`, `lon`, `sog` | decimal | Position and speed in that last report.                                                                    |
| `threshold_s` | decimal   | Silence allowed, in seconds: the larger of the state minimum and 8 times the vessel's median recent interval.      |
| `alert_ts`    | timestamp | `last_ts` plus the threshold: when the gap is raised.                                                             |
| `resumed_ts`  | timestamp | Time of the next report; empty if the vessel was not seen again before the data ends.                             |
| `gap_s`       | decimal   | Length of the silence in seconds; empty when `resumed_ts` is empty.                                               |
| `date`        | date      | UTC date of `last_ts`; the Parquet files are partitioned by it.                                                   |
