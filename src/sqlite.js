import { DatabaseSync } from "node:sqlite";
import * as path from "node:path";

const __dirname = path.dirname(new URL(import.meta.url).pathname);
const DATABASE_FILE = path.resolve(__dirname, "../data/weather.sqlite");
export const TABLE_NAME = "weather_data";

// Define the schema mapping your field names to SQLite data types
const SCHEMA_MAPPING = {
  auroraId: "TEXT NOT NULL",
  fetchTime: "TEXT NOT NULL", // Stored as ISO 8601 string
  averageWindSpdKnots: "REAL",
  averageWindSpeedKm: "REAL",
  cloud: "TEXT",
  cloudOktas: "TEXT",
  dayName: "TEXT",
  dewPointC: "REAL",
  endTime: "TEXT",
  feelsLikeTempC: "REAL",
  generationTime: "TEXT",
  gustKmh: "REAL",
  maximumGustDir: "TEXT",
  maximumGustKmh: "REAL",
  maximumGustSpdKnots: "REAL",
  maximumTempC: "REAL",
  maximumTempLocalTime: "TEXT",
  minimumTempC: "REAL",
  minimumTempLocalTime: "TEXT",
  precipitationSince9amMM: "REAL",
  pressure: "REAL",
  pressureMSLP: "REAL",
  qnhPressure: "REAL",
  rainHour: "REAL",
  rainTen: "REAL",
  rainfall24hr: "REAL",
  relativeHumidityPct: "REAL",
  startTime: "TEXT",
  tempC: "REAL",
  visibilityKm: "REAL",
  wetBulbTemp: "REAL",
  windDir: "TEXT",
  windDirDeg: "REAL",
  windGustSpdKnots: "REAL",
};

// --- Single Global Database Connection ---
/**
 * The single, reused database connection instance.
 * @type {DatabaseSync | null}
 */
let dbInstance = null;

// --- Database Functions ---

/**
 * Initializes, creates the table/index if necessary, and returns
 * the single database connection instance.
 * @returns {DatabaseSync} The active database connection.
 */
export function initializeDatabase() {
  if (dbInstance) {
    return dbInstance;
  }

  try {
    // Connect/create the database file
    dbInstance = new DatabaseSync(DATABASE_FILE);

    // 1. Generate the SQL for the table columns
    const columnsSql = Object.entries(SCHEMA_MAPPING)
      .map(([columnName, dataType]) => `${columnName} ${dataType}`)
      .join(", \n  ");

    // 2. Construct and execute the CREATE TABLE statement
    const createTableSql = `
CREATE TABLE IF NOT EXISTS ${TABLE_NAME} (
  ${columnsSql},
  UNIQUE (auroraId, fetchTime)
) STRICT;`;

    dbInstance.exec(createTableSql);

    // 3. Add a non-unique index for fast querying by location and time
    dbInstance.exec(`
CREATE INDEX IF NOT EXISTS idx_timeseries ON ${TABLE_NAME} (auroraId, fetchTime);
`);

    console.log(
      `✅ Database '${DATABASE_FILE}' initialized and table '${TABLE_NAME}' ready with time-series index.`
    );
    return dbInstance;
  } catch (e) {
    console.error(
      `❌ Fatal error during database initialization: ${e.message}`
    );
    // If connection fails, close and re-throw
    if (dbInstance) dbInstance.close();
    throw e;
  }
}

/**
 * Appends a single object to the weather_data table using the persistent connection.
 * @param {Object<string, any>} dataObject - The data to insert.
 */
export function append(dataObject) {
  // 1. Get the persistent connection
  const db = initializeDatabase();

  // Safety check for required keys (auroraId and fetchTime must exist and be NOT NULL)
  if (!dataObject.auroraId || !dataObject.fetchTime) {
    console.error(
      "❌ Data object must contain non-null 'auroraId' and 'fetchTime'."
    );
    return;
  }

  try {
    const columnNames = Object.keys(SCHEMA_MAPPING);
    const placeholders = columnNames.map(() => "?").join(", ");
    const colsListSql = columnNames.join(", ");

    const insertSql = `
INSERT OR IGNORE INTO ${TABLE_NAME} (${colsListSql}) 
VALUES (${placeholders})
`;

    const insertStmt = db.prepare(insertSql);

    // --- KEY CHANGE: Explicitly map undefined to null ---
    // Although node-sqlite usually handles this, being explicit is safer.
    // If a key is not present in dataObject, its value will be undefined,
    // which the sqlite binder will treat as NULL. We can confirm this
    // behavior or explicitly set it to null if the key is missing.
    const values = columnNames.map((col) => {
      const value = dataObject[col];
      // Treat explicit undefined as null for the database
      return value === undefined ? null : value;
    });

    // Execute the statement
    const result = insertStmt.run(...values);

    if (result.changes === 0) {
      console.log(
        `⚠️ Record for auroraId='${dataObject.auroraId}' at fetchTime='${dataObject.fetchTime}' already exists (ignored).`
      );
    } else {
      console.log(
        `➕ Successfully appended data for auroraId='${dataObject.auroraId}'.`
      );
    }
  } catch (e) {
    console.error(`❌ An error occurred during data append: ${e.message}`);
  }
}

/**
 * Fetches all records for a specific auroraId since a given timestamp.
 * @param {string} targetAuroraId - The ID of the location to fetch.
 * @param {Date} [since] - The minimum fetchTime (exclusive). Defaults to 7 days ago.
 * @returns {Array<Object<string, any>>} An array of weather objects.
 */
export function fetchLocation(
  targetAuroraId,
  since = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000)
) {
  // 1. Get the persistent connection
  const db = initializeDatabase();

  try {
    // Convert Date object to ISO 8601 string for comparison
    const sinceIsoTime = since.toISOString();

    const querySql = `
SELECT * FROM ${TABLE_NAME} 
WHERE auroraId = ? AND fetchTime > ?
ORDER BY fetchTime ASC
`;
    const query = db.prepare(querySql);

    const results = query.all(targetAuroraId, sinceIsoTime);

    console.log(
      `🔍 Found ${results.length} records for ${targetAuroraId} since ${sinceIsoTime}.`
    );

    // Note: NO db.close() call. The connection remains open.
    return results;
  } catch (e) {
    console.error(`❌ An error occurred during data fetch: ${e.message}`);
    return [];
  }
}

/**
 * Utility function to close the persistent connection when the application exits.
 */
export function closeDatabase() {
  if (dbInstance) {
    dbInstance.close();
    dbInstance = null;
    console.log(`\n🛑 Database connection closed.`);
  }
}
