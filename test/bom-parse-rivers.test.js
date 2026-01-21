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

    const result = parseRiverHeights(html);

    assert.ok(Array.isArray(result), "Result should be an array");
    assert.ok(result.length > 0, "Result should not be empty");

    // Check the first record
    // Todd River at Bond Springs 	Automatic 	03.10PM Tue 	1.00 	LGH 	steady 	  	  	Plot | Table
    const first = result[0];
    assert.strictEqual(first.stationName, "Todd River at Bond Springs");
    assert.strictEqual(first.stationType, "Automatic");
    assert.strictEqual(first.timeDay, "03.10PM Tue");
    assert.strictEqual(first.heightM, "1.00");
    assert.strictEqual(first.gaugeDatum, "LGH");
    assert.strictEqual(first.tendency, "steady");
    assert.strictEqual(first.crossingM, "");
    assert.strictEqual(first.floodClassification, "");

    // Check a record with crossing info
    // Charles River at Big Dipper 	Automatic 	03.10PM Tue 	0.33 	LGH 	steady 	0.13 below CTF 	  	Plot | Table
    const bigDipper = result.find(
      (r) => r.stationName === "Charles River at Big Dipper",
    );
    assert.ok(bigDipper, "Should find Charles River at Big Dipper");
    assert.strictEqual(bigDipper.crossingM, "0.13 below CTF");

    // Check a record with flood classification
    // Todd River at Wigley Gorge 	Automatic 	03.10PM Tue 	0.14 	LGH 	steady 	  	Below Flood Level 	Plot | Table
    const wigleyGorge = result.find(
      (r) => r.stationName === "Todd River at Wigley Gorge",
    );
    assert.ok(wigleyGorge, "Should find Todd River at Wigley Gorge");
    assert.strictEqual(wigleyGorge.floodClassification, "Below Flood Level");
  });

  it("should return an empty array if no table is found", () => {
    const result = parseRiverHeights("<html><body></body></html>");
    assert.deepStrictEqual(result, []);
  });

  it("should parse all HTML files in test/bom-river-levels/ correctly", async () => {
    const dirPath = path.resolve(__dirname, "bom-river-levels");
    const files = (await fs.readdir(dirPath)).filter((f) =>
      f.endsWith(".html"),
    );

    assert.ok(files.length > 0, "Should find at least one HTML file");

    for (const file of files) {
      const htmlPath = path.join(dirPath, file);
      const html = await fs.readFile(htmlPath, "utf8");

      try {
        const result = parseRiverHeights(html);

        assert.ok(
          Array.isArray(result),
          `Result for ${file} should be an array`,
        );
        assert.ok(result.length > 0, `Result for ${file} should not be empty`);

        result.forEach((record, index) => {
          const requiredKeys = [
            "stationName",
            "stationType",
            "timeDay",
            "heightM",
            "gaugeDatum",
            "tendency",
            "crossingM",
            "floodClassification",
            "recentData",
          ];
          requiredKeys.forEach((key) => {
            assert.ok(
              key in record,
              `Record ${index} in ${file} should have key ${key}`,
            );
            assert.strictEqual(
              typeof record[key],
              "string",
              `Key ${key} in record ${index} of ${file} should be a string`,
            );
          });
        });
      } catch (err) {
        err.message = `Error parsing ${file}: ${err.message}`;
        throw err;
      }
    }
  });
});
