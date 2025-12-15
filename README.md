# time-series weather data

Use the ABC's Aurora GraphQL database to fetch and collate weather data.

## Setup

To set up this repo, you must create a geojson object with the locations you wish to track.

1. `npm run fetch:geonames` # Download the Australian GeoNames dump
1. `npm run process:geonames-to-geojson` # Filter and convert to GeoJSON
1. `npm run fetch:aurora-ids` # Rectify locations with Aurora IDs

## Usage (gather data)

Once the setup steps are done, you can run the following on a cron job to
populate weather.sqlite.

1. `npm run fetch:weather-cron`

## Usage (collate data)

Output a JSON file containing a series of the given measurements for every
Aurora location. You can use this to correlate with the GeoJSON file.

1. `node src/generate-dataset.js -c tempC -o tempc.json`

This generates a file in this format, with each item in the array corresponding
with the timestamps object:

```json
{
  "timestamps": [
    "2025-12-12T05:00:00Z",
    "2025-12-12T06:10:00Z",
    "2025-12-12T06:40:00Z",
    "2025-12-12T07:00:00Z"
  ],
  "tempC": {
    "aurora://location/loc004e26acc9fa": [
      30.4,
      30.8,
      null,
      32.7
    ],
    …
```
