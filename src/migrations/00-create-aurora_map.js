import fs from "node:fs/promises";
import path from "node:path";

const __dirname = path.dirname(new URL(import.meta.url).pathname);
const TABLE_NAME = "aurora_map";

export const SCHEMA_MAPPING = {
  auroraId: "TEXT NOT NULL",
  name: "TEXT NOT NULL",
};

export async function createAuroraMap(dbInstance) {
  // 1. Generate the SQL for the table columns
  const columnsSql = Object.entries(SCHEMA_MAPPING)
    .map(([columnName, dataType]) => `${columnName} ${dataType}`)
    .join(", \n  ");

  // 2. Construct and execute the CREATE TABLE statement
  const createTableSql = `
CREATE TABLE IF NOT EXISTS ${TABLE_NAME} (
  ${columnsSql},
  UNIQUE (auroraId)
) STRICT;`;

  dbInstance.exec(createTableSql);

  // 3. Check if table is empty and populate if needed
  const countQuery = dbInstance.prepare(
    `SELECT COUNT(*) as count FROM ${TABLE_NAME}`
  );
  const result = countQuery.get();

  if (!result.count) {
    await populateAuroraMap(dbInstance);
  }
}

async function getAuroraIds() {
  const text = await fs.readFile(
    path.resolve(__dirname, "../../data/au.geo.json")
  );
  const geojson = JSON.parse(text);
  return geojson.features.map((feature) => [
    feature.properties.auroraId,
    feature.properties.name,
  ]);
}

async function populateAuroraMap(dbInstance) {
  console.log(`Starting migration: add aurora map`);
  const insertStmt = dbInstance.prepare(`
INSERT OR IGNORE INTO ${TABLE_NAME} (auroraId, name) 
VALUES (?, ?)
`);

  const ids = await getAuroraIds();
  ids.forEach(([auroraId, name]) => insertStmt.run(String(auroraId), name));
}
