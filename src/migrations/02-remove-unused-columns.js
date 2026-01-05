import { SCHEMA_MAPPING } from "./01-create-weather_data.js";

const TABLE_NAME = "weather_data";

/**
 * Gets the current table schema from SQLite
 */
function getCurrentSchema(db, tableName) {
  const query = db.prepare(`PRAGMA table_info(${tableName})`);
  return query.all();
}

/**
 * Compares current schema with expected schema
 */
function compareSchemas(currentColumns, expectedMapping) {
  const currentColumnNames = currentColumns.map((col) => col.name);
  const expectedColumns = Object.keys(expectedMapping);

  const missingColumns = expectedColumns.filter(
    (col) => !currentColumnNames.includes(col)
  );
  const extraColumns = currentColumnNames.filter(
    (col) => !expectedColumns.includes(col)
  );

  return {
    needsMigration: missingColumns.length > 0 || extraColumns.length > 0,
    missingColumns,
    extraColumns,
  };
}

/**
 * Migrates table to new schema using temp table approach
 */
function migrateTable(db, tableName, expectedMapping) {
  const tempTableName = `${tableName}_temp_${Date.now()}`;

  console.log(`🔄 Starting schema migration for ${tableName}...`);

  // 1. Create temp table with new schema
  const columnsSql = Object.entries(expectedMapping)
    .map(([columnName, dataType]) => `${columnName} ${dataType}`)
    .join(", \n  ");

  const createTempTableSql = `
CREATE TABLE ${tempTableName} (
  ${columnsSql},
  UNIQUE (auroraId, generationTime)
) STRICT;`;

  db.exec(createTempTableSql);

  // 2. Copy existing data (only columns that exist in both schemas)
  const currentSchema = getCurrentSchema(db, tableName);
  const commonColumns = Object.keys(expectedMapping).filter((col) =>
    currentSchema.some((currentCol) => currentCol.name === col)
  );

  if (commonColumns.length > 0) {
    const columnsList = commonColumns.join(", ");

    const copyDataSql = `
INSERT OR IGNORE INTO ${tempTableName} (${columnsList})
SELECT ${columnsList} FROM ${tableName}`;

    const copyStmt = db.prepare(copyDataSql);
    const result = copyStmt.run();
    console.log(`📊 Copied ${result.changes} rows to temp table`);
  }

  // 3. Drop old table
  db.exec(`DROP TABLE ${tableName}`);

  // 4. Rename temp table to original name
  db.exec(`ALTER TABLE ${tempTableName} RENAME TO ${tableName}`);

  // 5. Recreate indexes
  db.exec(`
CREATE INDEX IF NOT EXISTS idx_timeseries ON ${tableName} (auroraId, fetchTime);
`);

  console.log(`✅ Schema migration completed for ${tableName}`);
}

/**
 * Main migration function
 */
export function removeUnusedColumns(db) {
  // Check if table exists and get current schema
  let currentSchema = getCurrentSchema(db, TABLE_NAME);

  // Compare schemas
  const schemaComparison = compareSchemas(currentSchema, SCHEMA_MAPPING);

  if (!schemaComparison.needsMigration) {
    return;
  }

  migrateTable(db, TABLE_NAME, SCHEMA_MAPPING);

  // Run VACUUM to optimize database after schema changes
  db.exec("VACUUM");

  // Close database connection
  db.close();
  console.log(`✅ Migration completed successfully`);
}
