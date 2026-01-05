export const TABLE_NAME = "weather_data";

// Define the schema mapping your field names to SQLite data types
export const SCHEMA_MAPPING = {
  auroraId: "TEXT NOT NULL",
  fetchTime: "TEXT NOT NULL", // Stored as ISO 8601 string
  startTime: "TEXT",
  endTime: "TEXT",
  generationTime: "TEXT",
  averageWindSpdKnots: "REAL",
  averageWindSpeedKm: "REAL",
  dewPointC: "REAL",
  feelsLikeTempC: "REAL",
  pressure: "REAL",
  pressureMSLP: "REAL",
  qnhPressure: "REAL",
  rainfall24hr: "REAL",
  relativeHumidityPct: "REAL",
  tempC: "REAL",
  wetBulbTemp: "REAL",
  windDir: "TEXT",
  windDirDeg: "REAL",
};

export function createWeatherData(dbInstance) {
  // 1. Generate the SQL for the table columns
  const columnsSql = Object.entries(SCHEMA_MAPPING)
    .map(([columnName, dataType]) => `${columnName} ${dataType}`)
    .join(", \n  ");

  // 2. Construct and execute the CREATE TABLE statement
  const createTableSql = `
CREATE TABLE IF NOT EXISTS ${TABLE_NAME} (
  ${columnsSql},
  UNIQUE (auroraId, generationTime)
) STRICT;`;

  dbInstance.exec(createTableSql);

  // 3. Add a non-unique index for fast querying by location and time
  dbInstance.exec(`
CREATE INDEX IF NOT EXISTS idx_timeseries ON ${TABLE_NAME} (auroraId, fetchTime);
`);
}
