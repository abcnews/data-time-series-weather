import { program } from "commander";
import { TABLE_NAME } from "./migrations/01-create-weather_data.js";
import fs from "node:fs/promises";
import path from "node:path";
import { initializeDatabase } from "./sqlite.js";

const db = await initializeDatabase();
/**
 * Fetches time-series data aligned to 10-minute windows.
 * @param {string} column - The column name to extract.
 * @returns {Object} { timestamps: string[], series: { [location]: (number|null)[] } }
 */
export function getTimeSeriesForColumn({ column = "tempC" }) {
  try {
    // 1. Fetch all unique locations
    const locationsList = db
      .prepare(`SELECT DISTINCT auroraId FROM ${TABLE_NAME}`)
      .all()
      .map((r) => r.auroraId);

    // 2. Fetch data with normalized 10-minute timestamps.
    // 'YYYY-MM-DDTHH:M0:00Z' logic:
    // We take the first 15 characters of the ISO string (e.g., "2023-10-01T12:3")
    // and append "0:00Z" to bucket everything into 10-minute intervals.
    const querySql = `
      SELECT
        substr(fetchTime, 1, 15) || '0:00Z' as timeBucket,
        auroraId,
        ${column} as value
      FROM ${TABLE_NAME}
      WHERE fetchTime >= datetime('now', '-7 days')
      ORDER BY timeBucket ASC
    `;

    const rows = db.prepare(querySql).all();
    if (rows.length === 0) return { timestamps: [], series: {} };

    // 3. Identify all unique 10-minute buckets across the dataset
    const uniqueBuckets = [...new Set(rows.map((r) => r.timeBucket))];

    // 4. Create a lookup map for quick access: map[bucket][auroraId] = value
    const dataMap = {};
    for (const row of rows) {
      if (!dataMap[row.timeBucket]) dataMap[row.timeBucket] = {};
      dataMap[row.timeBucket][row.auroraId] = row.value;
    }

    // 5. Build the final structure
    const series = {};
    locationsList.forEach((loc) => {
      series[loc] = uniqueBuckets.map((bucket) => {
        const val = dataMap[bucket][loc];
        return val !== undefined ? val : null;
      });
    });

    return {
      timestamps: uniqueBuckets,
      series,
    };
  } catch (e) {
    console.error(`❌ Error generating time-boxed series: ${e.message}`);
    return { timestamps: [], series: {} };
  }
}

program
  .option("-c, --column <columnName>", "Which column to return", "tempC")
  .option("-o, --output <filename>", "Where to write this json");

program.parse();

const { output, column } = program.opts();
const outputFile = path.resolve(process.cwd(), output);
console.log("fetching data for", column);
const datas = getTimeSeriesForColumn({ column: column });
console.log("writing to", outputFile);
await fs.writeFile(outputFile, JSON.stringify(datas));
