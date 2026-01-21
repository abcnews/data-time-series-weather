import assert from "node:assert";
import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { resolveLocalTimeToUtc } from "../src/utils.aurora-dates.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

describe("utils.aurora-dates", () => {
  describe("resolveLocalTimeToUtc", () => {
    it("should resolve morning time correctly", () => {
      const localTime = "10:22 am";
      const referenceIso = "2025-12-18T17:30:00+11:00";
      const expected = "2025-12-18T10:22:00+11:00";
      assert.strictEqual(
        resolveLocalTimeToUtc(localTime, referenceIso),
        expected,
      );
    });

    it("should resolve afternoon time correctly", () => {
      const localTime = "3:45 pm";
      const referenceIso = "2025-12-18T17:30:00+11:00";
      const expected = "2025-12-18T15:45:00+11:00";
      assert.strictEqual(
        resolveLocalTimeToUtc(localTime, referenceIso),
        expected,
      );
    });

    it("should resolve midnight correctly", () => {
      const localTime = "12:00 am";
      const referenceIso = "2025-12-18T17:30:00+11:00";
      const expected = "2025-12-18T00:00:00+11:00";
      assert.strictEqual(
        resolveLocalTimeToUtc(localTime, referenceIso),
        expected,
      );
    });

    it("should resolve noon correctly", () => {
      const localTime = "12:00 pm";
      const referenceIso = "2025-12-18T17:30:00+11:00";
      const expected = "2025-12-18T12:00:00+11:00";
      assert.strictEqual(
        resolveLocalTimeToUtc(localTime, referenceIso),
        expected,
      );
    });

    it("should handle different timezone offsets", () => {
      const localTime = "9:30 am";
      const referenceIso = "2025-12-18T17:30:00+09:30";
      const expected = "2025-12-18T09:30:00+09:30";
      assert.strictEqual(
        resolveLocalTimeToUtc(localTime, referenceIso),
        expected,
      );
    });

    it("should handle UTC (Z) offset", () => {
      const localTime = "11:00 pm";
      const referenceIso = "2025-12-18T17:30:00Z";
      const expected = "2025-12-18T23:00:00+00:00";
      assert.strictEqual(
        resolveLocalTimeToUtc(localTime, referenceIso),
        expected,
      );
    });

    it("should return null if inputs are missing", () => {
      assert.strictEqual(
        resolveLocalTimeToUtc(null, "2025-12-18T17:30:00+11:00"),
        null,
      );
      assert.strictEqual(resolveLocalTimeToUtc("10:22 am", null), null);
    });

    it("should throw error for invalid reference ISO", () => {
      assert.throws(() => {
        resolveLocalTimeToUtc("10:22 am", "invalid-date");
      }, /Invalid reference ISO string/);
    });

    it("should throw error for invalid local time format", () => {
      assert.throws(() => {
        resolveLocalTimeToUtc("10:22", "2025-12-18T17:30:00+11:00");
      }, /Invalid local time format/);
    });

    it("should ensure resulting dates parse correctly", () => {
      const localTime = "10:22 am";
      const referenceIso = "2025-12-18T17:30:00+11:00";
      const result = resolveLocalTimeToUtc(localTime, referenceIso);
      const date = new Date(result);
      assert.ok(!isNaN(date.getTime()), "Resulting date should be valid");
    });

    it("should parse all Aurora observation files without errors", async () => {
      const observationsDir = path.resolve(__dirname, "aurora-observations");
      const files = await fs.readdir(observationsDir);
      const jsonFiles = files.filter((f) => f.endsWith(".json"));

      for (const file of jsonFiles) {
        const content = await fs.readFile(
          path.join(observationsDir, file),
          "utf8",
        );
        const json = JSON.parse(content);
        const data =
          json.data?.locations?.byId?.weather?.detailedHistoricConditions?.[0]
            ?.values?.[0];

        if (!data) continue;

        if (data.maximumTempLocalTime && data.endTime) {
          const result = resolveLocalTimeToUtc(
            data.maximumTempLocalTime,
            data.endTime,
          );
          assert.ok(result, `Failed to parse max temp for ${file}`);
          assert.ok(
            !isNaN(new Date(result).getTime()),
            `Invalid date for max temp in ${file}`,
          );
        }

        if (data.minimumTempLocalTime && data.endTime) {
          const result = resolveLocalTimeToUtc(
            data.minimumTempLocalTime,
            data.endTime,
          );
          assert.ok(result, `Failed to parse min temp for ${file}`);
          assert.ok(
            !isNaN(new Date(result).getTime()),
            `Invalid date for min temp in ${file}`,
          );
        }
      }
    });
  });
});
