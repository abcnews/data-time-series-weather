import { JSDOM } from "jsdom";
import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

/**
 * Parses a river height HTML file to a JS object.
 * @param {string} html - The HTML content of the river height page, e.g.  "IDD60022 Latest River Heights for the NT Rivers".
 * @returns {Array<Object>} - An array of objects representing the river height data.
 */
export function parseRiverHeights(html) {
  const dom = new JSDOM(html);
  const document = dom.window.document;
  const table = document.querySelector("table.rhb");

  if (!table) {
    return [];
  }

  const rows = table.querySelectorAll("tbody tr");
  const data = [];

  rows.forEach((row) => {
    const cells = row.querySelectorAll("td");

    // Ignore rows with rowspan because these are subheadings, not data.
    const hasRowspan = Array.from(row.querySelectorAll("td, th")).some((cell) =>
      cell.hasAttribute("rowspan"),
    );
    if (hasRowspan) {
      return;
    }

    if (cells.length === 9) {
      const record = {
        stationName: cells[0].textContent.trim(),
        stationType: cells[1].textContent.trim(),
        timeDay: cells[2].textContent.trim(),
        heightM: cells[3].textContent.trim(),
        gaugeDatum: cells[4].textContent.trim(),
        tendency: cells[5].textContent.trim(),
        crossingM: cells[6].textContent
          .trim()
          .replace(/&nbsp;/g, "")
          .trim(),
        floodClassification: cells[7].textContent
          .trim()
          .replace(/&nbsp;/g, "")
          .trim(),
        recentData: cells[8].textContent.trim(),
      };

      // Clean up &nbsp; and other whitespace
      for (const key in record) {
        if (typeof record[key] === "string") {
          record[key] = record[key].replace(/\u00a0/g, " ").trim();
        }
      }

      data.push(record);
    }
  });

  return data;
}

/**
 * Loads all HTML river files from data/bom-products/ and parses them into a single array.
 * @returns {Promise<Array<Object>>} - A promise that resolves to an array of all river height records.
 */
export async function parseAllRiverHeights() {
  const dirPath = path.resolve(__dirname, "../test/bom-river-levels");
  const files = (await fs.readdir(dirPath)).filter((f) => f.endsWith(".html"));

  let allData = [];

  for (const file of files) {
    const htmlPath = path.join(dirPath, file);
    const html = await fs.readFile(htmlPath, "utf8");
    const result = parseRiverHeights(html);
    allData = allData.concat(result);
  }

  return allData;
}

const riversJsonPath = path.resolve(__dirname, "../rivers.json");
await fs.writeFile(
  riversJsonPath,
  JSON.stringify(await parseAllRiverHeights()),
);
