import assert from "node:assert";
import {
  formatIsoDate,
  calculateRowDate,
} from "../dataBomRiver/utils.dates.js";

describe("bom utils.dates", () => {
  describe("formatIsoDate", () => {
    it("should format HH:mm AM/PM correctly", () => {
      assert.strictEqual(
        formatIsoDate(2026, 1, 20, "03:24 PM", "+09:30"),
        "2026-01-20T15:24:00+09:30",
      );
      assert.strictEqual(
        formatIsoDate(2026, 1, 20, "09:15 AM", "+10:00"),
        "2026-01-20T09:15:00+10:00",
      );
    });

    it("should format HH.mmAM/PM correctly", () => {
      assert.strictEqual(
        formatIsoDate(2026, 1, 20, "03.10PM", "+09:30"),
        "2026-01-20T15:10:00+09:30",
      );
    });

    it("should handle 12 AM/PM correctly", () => {
      assert.strictEqual(
        formatIsoDate(2026, 1, 20, "12:00 PM", "+10:00"),
        "2026-01-20T12:00:00+10:00",
      );
      assert.strictEqual(
        formatIsoDate(2026, 1, 20, "12:00 AM", "+10:00"),
        "2026-01-20T00:00:00+10:00",
      );
    });
  });

  describe("calculateRowDate", () => {
    it("should find the same day if it matches", () => {
      const base = { year: 2026, month: 1, day: 20 }; // Tuesday
      const result = calculateRowDate(base, "Tue");
      assert.deepStrictEqual(result, { year: 2026, month: 1, day: 20 });
    });

    it("should find the previous day", () => {
      const base = { year: 2026, month: 1, day: 20 }; // Tuesday
      const result = calculateRowDate(base, "Mon");
      assert.deepStrictEqual(result, { year: 2026, month: 1, day: 19 });
    });

    it("should handle year rollover", () => {
      const base = { year: 2026, month: 1, day: 1 }; // Thursday
      const result = calculateRowDate(base, "Wed");
      assert.deepStrictEqual(result, { year: 2025, month: 12, day: 31 });
    });
  });
});
