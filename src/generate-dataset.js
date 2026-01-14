import { program } from "commander";
import {
  SCHEMA_MAPPING,
  TABLE_NAME,
} from "./migrations/01-create-weather_data.js";
import fs from "node:fs/promises";
import path from "node:path";
import { initializeDatabase } from "./sqlite.js";
import { startOfDay, endOfDay, addDays, parseISO } from "date-fns";
import { toZonedTime, fromZonedTime, formatInTimeZone } from "date-fns-tz";

const TZ = "Australia/Brisbane";

const BUCKET_SIZE_MIN = 30;

/**
 * Calculates day boundaries based on Brisbane time, returned as UTC strings.
 */
const getDayBoundaries = (offset = 0) => {
  const brisbaneNow = toZonedTime(new Date(), TZ);
  const target = addDays(brisbaneNow, offset);

  return {
    start: fromZonedTime(startOfDay(target), TZ).toISOString(),
    end: fromZonedTime(endOfDay(target), TZ).toISOString(),
  };
};

/**
 * Rounds a date down to the nearest 10-minute interval.
 */
const bucketDate = (isoString) => {
  const date = parseISO(isoString);
  date.setMinutes(
    Math.floor(date.getMinutes() / BUCKET_SIZE_MIN) * BUCKET_SIZE_MIN,
    0,
    0
  );
  return date.toISOString();
};

export async function getTimeSeriesForColumn({
  column = "tempC",
  dayStart = 0,
}) {
  if (!Object.keys(SCHEMA_MAPPING).includes(column)) {
    throw new Error(`Invalid column: ${column}`);
  }

  const db = await initializeDatabase();
  const { start, end } = getDayBoundaries(dayStart);

  // The DB returns sorted by generationTime, but those times are in various
  // timezones, so we must use unixepoch
  const rows = db
    .prepare(
      `
    SELECT generationTime, auroraId, ${column} as value
    FROM ${TABLE_NAME}
    /* Convert both the column and the parameters to unix timestamps for comparison */
    WHERE unixepoch(generationTime) BETWEEN unixepoch(?) AND unixepoch(?)
    ORDER BY unixepoch(generationTime) ASC
  `
    )
    .all(start, end);

  const dataMap = new Map();
  const uniqueBuckets = new Set();
  const locations = new Set();

  for (const row of rows) {
    const bucket = bucketDate(row.generationTime);
    uniqueBuckets.add(bucket);
    locations.add(row.auroraId);

    if (!dataMap.has(bucket)) dataMap.set(bucket, {});
    dataMap.get(bucket)[row.auroraId] = row.value;
  }

  const sortedBuckets = Array.from(uniqueBuckets).sort();

  // Pivot the data: { [location]: [values_per_bucket] }
  const series = Object.fromEntries(
    Array.from(locations)
      .map((loc) => [
        loc,
        sortedBuckets.map((b) => dataMap.get(b)[loc] ?? null),
      ])
      .filter(([_, values]) => values.some((v) => v !== null)) // Remove empty series
  );

  return {
    createdDate: new Date().toISOString(),
    timestamps: sortedBuckets.map((b) =>
      formatInTimeZone(new Date(b), TZ, "yyyy-MM-dd'T'HH:mm:ssXXX")
    ),
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
