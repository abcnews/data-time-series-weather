import { getTimeSeriesForColumn } from "./generate-dataset.js";
import fs from "node:fs/promises";
import path from "node:path";

const datasets = process.env.GENERATE_DATASETS.split(",");

const daysToGenerate = []; // today and yesterday
// generate the last 14 days actually
for (let i = 0; i > 0 - Number(process.env.GENERATE_DAYS || 1); i--) {
  daysToGenerate.push(i);
}

for (const dataset of datasets) {
  for (const dayOffset of daysToGenerate) {
    console.log(`Generating ${dataset} for day offset ${dayOffset}...`);

    const data = await getTimeSeriesForColumn({
      column: dataset,
      dayStart: dayOffset,
    });

    // Extract date portion from first timestamp and use substr for filename
    // Example: "2026-01-13T00:00:00+10:00" -> "2026-01-13"
    const dateSubstr = data.startDate.substring(0, 10); // Gets "YYYY-MM-DD"

    const filename = `${dateSubstr}.json`;
    const outputPath = path.join("data/assets/", dataset, filename);

    console.log(`Writing ${outputPath}...`);
    await fs.mkdir(path.dirname(outputPath), { recursive: true });
    await fs.writeFile(outputPath, JSON.stringify(data));
  }
}

console.log("✅ All datasets generated successfully");
