import { program } from "commander";
import { TABLE_NAME } from "./migrations/01-create-weather_data.js";
import fs from "node:fs/promises";
import path from "node:path";
import { initializeDatabase } from "./sqlite.js";
import { startOfDay, endOfDay, addDays } from "date-fns";
import { toZonedTime, fromZonedTime, formatInTimeZone } from "date-fns-tz";

/**
 * All output times are in Brisbane (UTC+10) since it doesn't have DST. To
 * ensure we always grab a full "Brisbane Day" , we calculate local midnight and
 * convert those boundaries back to UTC for DB querying.
 */
const TZ = "Australia/Brisbane";

const getDayBoundaries = (offset = 0) => {
  // Determine "Now" in Brisbane and apply the day offset (0 = today, -1 = yesterday)
  const brisbaneNow = toZonedTime(new Date(), TZ);
  const target = addDays(brisbaneNow, offset);

  // Return UTC ISO strings representing 00:00:00 and 23:59:59 in Brisbane
  return {
    start: fromZonedTime(startOfDay(target), TZ).toISOString(),
    end: fromZonedTime(endOfDay(target), TZ).toISOString(),
  };
};

export async function getTimeSeriesForColumn({
  column = "tempC",
  dayStart = 0,
}) {
  const db = await initializeDatabase();
  const { start, end } = getDayBoundaries(dayStart);

  /**
   * The database contains generationTime strings with varying timezones.
   * 'unixepoch()' is used to convert these to a standard UTC seconds.
   * This ensures the 'BETWEEN' filter and the 'ORDER BY' are chronologically accurate
   * even if the input strings are formatted differently.
   */
  const rows = db
    .prepare(
      `
      SELECT 
        generationTime, 
        auroraId, 
        ${column} as value
      FROM ${TABLE_NAME}
      WHERE unixepoch(generationTime) BETWEEN unixepoch(?) AND unixepoch(?)
      ORDER BY unixepoch(generationTime) ASC
    `
    )
    .all(start, end);

  const series = {};
  const startMs = new Date(start).getTime();

  /**
   * To keep the JSON payload smallish:
   * Instead of ISO strings/datestamps, we store 'x' as 'minutes elapsed since midnight'.
   * This results in a tight [x, y] coordinate array that compresses well
   */
  for (const row of rows) {
    // Calculate the absolute millisecond difference from the start of the day
    const currentMs = new Date(row.generationTime).getTime();

    // Map absolute time to a minute index (0 to 1439 for a standard day)
    const x = Math.floor((currentMs - startMs) / (1000 * 60));

    if (!series[row.auroraId]) {
      series[row.auroraId] = [];
    }

    // Final format: [minuteIndex, readingValue]
    series[row.auroraId].push([x, row.value]);
  }

  return {
    updatedDate: formatInTimeZone(new Date(), TZ, "yyyy-MM-dd'T'HH:mm:ssXXX"),
    startDate: formatInTimeZone(
      new Date(start),
      TZ,
      "yyyy-MM-dd'T'HH:mm:ssXXX"
    ),
    series,
  };
}

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
