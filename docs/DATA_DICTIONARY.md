# Data Dictionary

## Table 1. Historical AIS data (NOAA MarineCadastre, 2024 daily files)

One CSV per day covering all US waters, one row per vessel position report. The archive keeps about one report per vessel per minute. Example: 2024-07-01 has 8,999,649 rows from 22,651 vessels.

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

## Table 2. Live AIS feed (AISStream.io WebSocket)

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
