import assert from "node:assert";
import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { parseRiverHeights } from "../dataBomRiver/parse-rivers.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

describe("parseRiverHeights", () => {
  it("should parse the example HTML file correctly", async () => {
    const htmlPath = path.resolve(__dirname, "bom-river-levels/IDD60022.html");
    const html = await fs.readFile(htmlPath, "utf8");

    const { issuedAt, records } = await parseRiverHeights(html);

    assert.strictEqual(issuedAt, "2026-01-20T15:24:00+09:30");
    assert.ok(Array.isArray(records), "Records should be an array");
    assert.ok(records.length > 0, "Records should not be empty");

    // Check the first record
    // Todd River at Bond Springs 	Automatic 	03.10PM Tue 	1.00 	LGH 	steady 	  	  	Plot | Table
    const first = records[0];
    assert.strictEqual(first.id, "515008");
    assert.strictEqual(first.stationName, "Todd River at Bond Springs");
    assert.strictEqual(first.timestamp, "2026-01-20T15:10:00+09:30");
    assert.strictEqual(first.heightM, 1.0);
  });

  it("should parse all HTML files and verify timezone coverage", async () => {
    const dirPath = path.resolve(__dirname, "bom-river-levels");
    const files = (await fs.readdir(dirPath)).filter((f) =>
      f.endsWith(".html"),
    );

    for (const file of files) {
      const htmlPath = path.join(dirPath, file);
      const html = await fs.readFile(htmlPath, "utf8");

      try {
        const { issuedAt, records } = await parseRiverHeights(html);
        assert.ok(issuedAt, `Should have issuedAt for ${file}`);
        assert.ok(records.length > 0, `Should have records for ${file}`);

        records.forEach((r) => {
          assert.ok(r.id, `Should have id for record in ${file}`);
          assert.strictEqual(
            typeof r.heightM,
            r.heightM === null ? "object" : "number",
          );
          assert.ok(
            r.timestamp.endsWith(issuedAt.slice(-6)),
            "Record offset should match issuedAt offset",
          );
        });
      } catch (err) {
        err.message = `Error parsing ${file}: ${err.message}`;
        throw err;
      }
    }
  });
});
