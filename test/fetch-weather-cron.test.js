import assert from "node:assert";
import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import fetchWeatherCron from "../src/fetch-weather-cron.js";
import { initializeDatabase, closeDatabase } from "../src/sqlite.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

describe("fetchWeatherCron", () => {
  const tempGeojsonPath = path.resolve(__dirname, "temp.geo.json");
  const tempDbPath = path.resolve(__dirname, "temp.sqlite");

  beforeEach(async () => {
    // Create a temp geojson with a few features
    const geojson = {
      type: "FeatureCollection",
      features: [
        {
          type: "Feature",
          properties: {
            name: "Test Location 1",
            auroraId: "0a9a5834157b",
          },
          geometry: { type: "Point", coordinates: [0, 0] },
        },
        {
          type: "Feature",
          properties: {
            name: "Test Location 2",
            auroraId: "0aee89e74496",
          },
          geometry: { type: "Point", coordinates: [1, 1] },
        },
      ],
    };
    await fs.writeFile(tempGeojsonPath, JSON.stringify(geojson));
  });

  afterEach(async () => {
    closeDatabase();
    await fs.unlink(tempGeojsonPath).catch(() => {});
    await fs.unlink(tempDbPath).catch(() => {});
  });

  it("should fetch weather and insert into DB for multiple locations", async () => {
    const mockQueryFn = async (query) => {
      const match = query.match(/aurora:\/\/location\/([a-z0-9]+)/);
      const id = match ? match[1] : null;
      if (!id) throw new Error("Could not find auroraId in query");

      const sampleDataPath = path.resolve(
        __dirname,
        `aurora-observations/loc${id}.json`,
      );
      const content = await fs.readFile(sampleDataPath, "utf8");
      return JSON.parse(content);
    };

    await fetchWeatherCron({
      geojsonPath: tempGeojsonPath,
      databasePath: tempDbPath,
      queryFn: mockQueryFn,
    });

    // Verify data in DB
    const db = await initializeDatabase(tempDbPath, tempGeojsonPath);

    const row1 = db
      .prepare("SELECT * FROM weather_data WHERE auroraId = ?")
      .get("0a9a5834157b");
    assert.ok(row1, "Row 1 should exist in DB");
    assert.strictEqual(row1.auroraId, "0a9a5834157b");
    assert.strictEqual(row1.averageWindSpeedKm, 4);
    assert.strictEqual(row1.tempC, 26.8);
    assert.strictEqual(row1.maximumTempLocalTime, "10:22 am");
    assert.strictEqual(
      row1.maximumTempLocalTimeUTC,
      "2026-01-21T10:22:00+10:00",
    );
    assert.strictEqual(row1.minimumTempLocalTime, "5:53 am");
    assert.strictEqual(
      row1.minimumTempLocalTimeUTC,
      "2026-01-21T05:53:00+10:00",
    );

    const row2 = db
      .prepare("SELECT * FROM weather_data WHERE auroraId = ?")
      .get("0aee89e74496");
    assert.ok(row2, "Row 2 should exist in DB");
    assert.strictEqual(row2.auroraId, "0aee89e74496");
    assert.ok(row2.tempC !== null);
  });
});
