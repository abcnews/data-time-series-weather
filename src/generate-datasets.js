import { getTimeSeriesForColumn } from "./generate-dataset.js";
import fs from "node:fs/promises";
import path from "node:path";

const datasets = [
  "tempC",
  "averageWindSpeedKm",
  "maximumGustKmh",
  "relativeHumidityPct",
  "precipitationSince9amMM",
];

const daysToGenerate = [0, -1]; // today and yesterday
for (let i = -2; i > -14; i--) {
  daysToGenerate.push(i);
}
console.log(daysToGenerate);

for (const dataset of datasets) {
  for (const dayOffset of daysToGenerate) {
    console.log(`Generating ${dataset} for day offset ${dayOffset}...`);

    const data = await getTimeSeriesForColumn({
      column: dataset,
      dayStart: dayOffset,
    });

    if (data.timestamps.length === 0) {
      console.warn(`No data for ${dataset} on day offset ${dayOffset}`);
      continue;
    }

    // Extract date portion from first timestamp and use substr for filename
    // Example: "2026-01-13T00:00:00+10:00" -> "2026-01-13"
    const firstTimestamp = data.timestamps[0];
    const dateSubstr = firstTimestamp.substring(0, 10); // Gets "YYYY-MM-DD"

    const filename = `${dateSubstr}.json`;
    const outputPath = path.join("data/assets/", dataset, filename);

    console.log(`Writing ${outputPath}...`);
    await fs.mkdir(path.dirname(outputPath), { recursive: true });
    await fs.writeFile(outputPath, JSON.stringify(data));
  }
}

console.log("✅ All datasets generated successfully");
