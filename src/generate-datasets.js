import { getTimeSeriesForColumn } from "./generate-dataset.js";
import fs from "node:fs/promises";
import path from "node:path";
import logger from "./logger.js";

export default async function generateDatasets(options) {
  const datasets = options.columns.split(",");
  const daysToGenerate = [];

  for (let i = 0; i > 0 - Number(options.days || 1); i--) {
    daysToGenerate.push(i);
  }

  for (const dataset of datasets) {
    for (const dayOffset of daysToGenerate) {
      const data = await getTimeSeriesForColumn({
        column: dataset,
        dayStart: dayOffset,
      });

      // Extract date portion from first timestamp and use substr for filename
      // Example: "2026-01-13T00:00:00+10:00" -> "2026-01-13"
      const dateSubstr = data.startDate.substring(0, 10); // Gets "YYYY-MM-DD"

      const filename = `${dateSubstr}.json`;
      const outputPath = path.join("data/assets/", dataset, filename);

      await fs.mkdir(path.dirname(outputPath), { recursive: true });
      await fs.writeFile(outputPath, JSON.stringify(data));
      logger.info(
        "Generated %s for day offset %d -> %s",
        dataset,
        dayOffset,
        outputPath,
      );
    }
  }

  logger.info("All datasets generated successfully");
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const datasets = process.env.GENERATE_DATASETS.split(",");
  const daysToGenerate = [];

  for (let i = 0; i > 0 - Number(process.env.GENERATE_DAYS || 1); i--) {
    daysToGenerate.push(i);
  }

  for (const dataset of datasets) {
    for (const dayOffset of daysToGenerate) {
      const data = await getTimeSeriesForColumn({
        column: dataset,
        dayStart: dayOffset,
      });

      // Extract date portion from first timestamp and use substr for filename
      // Example: "2026-01-13T00:00:00+10:00" -> "2026-01-13"
      const dateSubstr = data.startDate.substring(0, 10); // Gets "YYYY-MM-DD"

      const filename = `${dateSubstr}.json`;
      const outputPath = path.join("data/assets/", dataset, filename);

      await fs.mkdir(path.dirname(outputPath), { recursive: true });
      await fs.writeFile(outputPath, JSON.stringify(data));
      logger.info(
        "Generated %s for day offset %d -> %s",
        dataset,
        dayOffset,
        outputPath,
      );
    }
  }

  logger.info("All datasets generated successfully");
}
