/**
 * @file
 * 2026-01-16 - We received a canonical list of weather sites and updated
 * au.geo.json to match. This means most if not all of our Aurora IDs have
 * changed, and we need to start over. This migration removes data for IDs that
 * don't exist in au.geo.json.
 */
import fs from "node:fs/promises";
import path from "node:path";

const TABLE_NAME = "weather_data";
const __dirname = path.dirname(new URL(import.meta.url).pathname);
const DEFAULT_GEOJSON_FILE = path.resolve(__dirname, "../../data/au.geo.json");

/**
 * Main migration function
 */
export async function removeOldLocations(
  db,
  geojsonPath = DEFAULT_GEOJSON_FILE,
) {
  console.log(
    `🔄 Starting migration to remove old locations from ${TABLE_NAME}...`,
  );

  // 1. Read au.geo.json to get valid Aurora IDs
  const geojsonText = await fs.readFile(geojsonPath, "utf8");
  const geojson = JSON.parse(geojsonText);
  const validAuroraIds = geojson.features
    .map((f) => f.properties.auroraId)
    .filter(Boolean);

  if (validAuroraIds.length === 0) {
    console.warn(
      "⚠️ No valid Aurora IDs found in au.geo.json. Skipping deletion to be safe.",
    );
    return;
  }

  // 2. Delete rows with auroraId not in the valid list
  // Using a temp table or a large IN clause might be slow if there are many IDs,
  // but for a few hundred/thousand it should be fine.
  const placeholders = validAuroraIds.map(() => "?").join(",");
  const deleteSql = `DELETE FROM ${TABLE_NAME} WHERE auroraId NOT IN (${placeholders})`;

  const deleteStmt = db.prepare(deleteSql);
  const result = deleteStmt.run(...validAuroraIds);

  console.log(`🗑️ Removed ${result.changes} rows with old Aurora IDs`);

  // 3. Run VACUUM to optimize database
  db.exec("VACUUM");

  console.log(`✅ Migration completed successfully`);
}
