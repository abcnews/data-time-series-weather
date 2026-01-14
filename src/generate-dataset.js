import { program } from "commander";
import { TABLE_NAME } from "./migrations/01-create-weather_data.js";
import fs from "node:fs/promises";
import path from "node:path";
import { initializeDatabase } from "./sqlite.js";
import { startOfDay, endOfDay, addDays, parseISO } from "date-fns";
import { toZonedTime, fromZonedTime } from "date-fns-tz";

const TIMEZONE_HOURS = +10;

const toAEST = (dateStr) => {
  const d = new Date(dateStr);
  return new Date(d.getTime() + TIMEZONE_HOURS * 60 * 60 * 1000)
    .toISOString()
    .replace("Z", "+10:00");
};

/**
 * Calculates the start and end of a day in Brisbane timezone, returned as UTC
 * ISO strings. This should be possible in SQL but I couldn't get it to work.
 */
const getDayBoundaries = (dayOffset = 0) => {
  const timezone = "Australia/Brisbane";
  const now = new Date();

  // Get current date in Brisbane timezone
  const brisbaneNow = toZonedTime(now, timezone);

  // Add day offset and get start of that day in Brisbane
  const targetDay = addDays(brisbaneNow, dayOffset);
  const brisbaneDayStart = startOfDay(targetDay);
  const brisbaneDayEnd = endOfDay(targetDay);

  // Convert Brisbane times back to UTC for database query
  const startDate = fromZonedTime(brisbaneDayStart, timezone).toISOString();
  const endDate = fromZonedTime(brisbaneDayEnd, timezone).toISOString();

  return { startDate, endDate };
};

/**
 * Buckets a timestamp to 10-minute intervals by rounding down the minutes.
 * @param {string} isoString - ISO 8601 timestamp with timezone
 * @returns {string} ISO 8601 timestamp rounded to 10-minute bucket in UTC
 */
const bucketTo10Minutes = (isoString) => {
  const date = parseISO(isoString);
  const minutes = Math.floor(date.getMinutes() / 30) * 30;
  date.setMinutes(minutes, 0, 0);
  return date.toISOString();
};

/**
 * Fetches time-series data aligned to 10-minute windows.
 * @param {object} options
 * @param {string} options.column - The DB column name to extract.
 * @param {string} options.dayStart - Limit to results for the day relative to today (0, -1, -2)
 * @returns {Object} { timestamps: string[], series: { [location]: (number|null)[] } }
 */
export async function getTimeSeriesForColumn({
  column = "tempC",
  dayStart = 0,
}) {
  const db = await initializeDatabase();

  // 1. Fetch all unique locations
  const locationsList = db
    .prepare(`SELECT DISTINCT auroraId FROM ${TABLE_NAME}`)
    .all()
    .map((r) => r.auroraId);

  // 2. Calculate the start and end of the target day in Brisbane timezone
  const { startDate, endDate } = getDayBoundaries(dayStart);
  console.log(`Date range (UTC): ${startDate} to ${endDate}`);
  console.log(`Date range (AEST): ${toAEST(startDate)} to ${toAEST(endDate)}`);

  // 3. Fetch raw data and bucket timestamps in Node.js
  const querySql = `
    SELECT
      generationTime,
      auroraId,
      ${column} as value
    FROM ${TABLE_NAME}
    WHERE datetime(generationTime) >= datetime('${startDate}')
      AND datetime(generationTime) < datetime('${endDate}')
    ORDER BY generationTime ASC
  `;

  console.log(querySql);

  const rows = db.prepare(querySql).all();
  if (rows.length === 0) return { timestamps: [], series: {} };

  // 4. Bucket timestamps and create data structure
  const rowsWithBucket = rows.map((row) => ({
    ...row,
    timeBucket: bucketTo10Minutes(row.generationTime),
  }));

  // 5. Identify all unique 10-minute buckets across the dataset
  const uniqueBuckets = [...new Set(rowsWithBucket.map((r) => r.timeBucket))];

  // 6. Create a lookup map for quick access: map[bucket][auroraId] = value
  const dataMap = rowsWithBucket.reduce((acc, row) => {
    if (!acc[row.timeBucket]) acc[row.timeBucket] = {};
    acc[row.timeBucket][row.auroraId] = row.value;
    return acc;
  }, {});

  // 7. Build the final structure
  const series = locationsList.reduce((acc, loc) => {
    const data = uniqueBuckets.map((bucket) => {
      const val = dataMap[bucket]?.[loc];
      return val !== undefined ? val : null;
    });

    // Only include locations with at least some data
    const hasData = data.some((value) => value !== null);
    if (hasData) {
      acc[loc] = data;
    }

    return acc;
  }, {});

  return {
    createdDate: new Date().toISOString(),
    timestamps: uniqueBuckets.map(toAEST),
    series,
  };
}

// Only run CLI logic when this file is executed directly
if (import.meta.url === `file://${process.argv[1]}`) {
  program
    .option("-c, --column <columnName>", "Which column to return", "tempC")
    .option("-d, --dayStart <number>", "Day relative to today (0, -1, -2)", "0")
    .option(
      "-o, --output <filename>",
      "Where to write this json",
      "output.json"
    );

  program.parse();

  const options = program.opts();
  const outputFile = path.resolve(process.cwd(), options.output);

  console.log(`Fetching ${options.column} for day: ${options.dayStart}`);

  const datas = await getTimeSeriesForColumn({
    column: options.column,
    dayStart: parseInt(options.dayStart),
  });

  console.log(`Writing to ${outputFile}`);
  await fs.writeFile(outputFile, JSON.stringify(datas));
}
