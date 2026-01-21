import { DatabaseSync } from "node:sqlite";
import * as path from "node:path";
import { createAuroraMap } from "./migrations/00-create-aurora_map.js";
import {
  createWeatherData,
  SCHEMA_MAPPING,
  TABLE_NAME,
} from "./migrations/01-create-weather_data.js";
import { updateColumns } from "./migrations/02-update-columns.js";
import { removeOldLocations } from "./migrations/04-remove-old-locations.js";

const __dirname = path.dirname(new URL(import.meta.url).pathname);
const DEFAULT_DATABASE_FILE = path.resolve(__dirname, "../data/weather.sqlite");

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
 * @param {string} [dbPath] - Optional path to the database file.
 * @param {string} [geojsonPath] - Optional path to the geojson file for migrations.
 * @returns {Promise<DatabaseSync>} The active database connection.
 */
export async function initializeDatabase(
  dbPath = DEFAULT_DATABASE_FILE,
  geojsonPath,
) {
  if (dbInstance) {
    return dbInstance;
  }

  try {
    // Connect/create the database file
    dbInstance = new DatabaseSync(dbPath);

    await createAuroraMap(dbInstance);
    await createWeatherData(dbInstance);
    await updateColumns(dbInstance);
    await removeOldLocations(dbInstance, geojsonPath);

    console.log(`✅ Database '${dbPath}' loaded.`);
    return dbInstance;
  } catch (e) {
    console.error(
      `❌ Fatal error during database initialization: ${e.message}`,
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
export async function append(dataObject) {
  // 1. Get the persistent connection
  const db = await initializeDatabase();

  // Safety check for required keys (auroraId and fetchTime must exist and be NOT NULL)
  if (!dataObject.auroraId || !dataObject.fetchTime) {
    console.error(
      "❌ Data object must contain non-null 'auroraId' and 'fetchTime'.",
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
        `⚠️ Record for auroraId='${dataObject.auroraId}' at fetchTime='${dataObject.fetchTime}' already exists (ignored).`,
      );
    } else {
      console.log(
        `➕ Successfully appended data for auroraId='${dataObject.auroraId}'.`,
      );
    }
  } catch (e) {
    console.error(`❌ An error occurred during data append: ${e.message}`);
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
