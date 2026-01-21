import { JSDOM } from "jsdom";
import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  parseIssuedAt,
  calculateRowDate,
  formatIsoDate,
} from "./utils.dates.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

/**
 * Parses a river height HTML file to a JS object.
 * @param {string} html - The HTML content of the river height page.
 * @returns {Promise<Object>} - An object containing issuedAt and records.
 */
export async function parseRiverHeights(html) {
  const dom = new JSDOM(html);
  const document = dom.window.document;

  // Parse "Issued at"
  const issuedAtElement = Array.from(document.querySelectorAll("p")).find((p) =>
    p.textContent.includes("Issued at"),
  );

  if (!issuedAtElement) {
    throw new Error("Could not find 'Issued at' element");
  }

  const issuedAt = await parseIssuedAt(issuedAtElement.textContent.trim());

  const table = document.querySelector("table.rhb");
  if (!table) {
    return { issuedAt: issuedAt.iso, records: [] };
  }

  const rows = Array.from(table.querySelectorAll("tbody tr"));
  const records = rows
    .filter((row) => {
      // Ignore rows with rowspan because these are subheadings, not data.
      const hasRowspan = Array.from(row.querySelectorAll("td, th")).some(
        (cell) => cell.hasAttribute("rowspan"),
      );
      const cells = row.querySelectorAll("td");
      return !hasRowspan && cells.length === 9;
    })
    .map((row) => {
      const cells = row.querySelectorAll("td");
      const timeDayStr = cells[2].textContent.trim().replace(/\u00a0/g, " ");
      if (!timeDayStr) {
        return null; // Will be filtered out
      }

      // Parse row time and day (e.g., "03.10PM Tue")
      const timeDayMatch = timeDayStr.match(
        /(\d{1,2}[:.]\d{2}\s*[AP]M)\s+(\w+)/i,
      );
      if (!timeDayMatch) {
        console.error(`Failed to parse row time/day: "${timeDayStr}"`);
        return null;
      }

      const [_, rowTime, rowDayAbbr] = timeDayMatch;
      const rowDate = calculateRowDate(issuedAt, rowDayAbbr);
      const timestamp = formatIsoDate(
        rowDate.year,
        rowDate.month,
        rowDate.day,
        rowTime,
        issuedAt.offset,
      );

      const heightStr = cells[3].textContent.trim().replace(/\u00a0/g, " ");
      const heightM = heightStr ? parseFloat(heightStr) : null;

      return {
        stationName: cells[0].textContent.trim().replace(/\u00a0/g, " "),
        stationType: cells[1].textContent.trim().replace(/\u00a0/g, " "),
        timeDay: timeDayStr,
        timestamp,
        issuedAt: issuedAt.iso,
        heightM,
        gaugeDatum: cells[4].textContent.trim().replace(/\u00a0/g, " "),
        tendency: cells[5].textContent.trim().replace(/\u00a0/g, " "),
        crossingM: cells[6].textContent
          .trim()
          .replace(/\u00a0/g, " ")
          .trim(),
        floodClassification: cells[7].textContent
          .trim()
          .replace(/\u00a0/g, " ")
          .trim(),
        recentData: cells[8].textContent.trim().replace(/\u00a0/g, " "),
      };
    })
    .filter((record) => record !== null);

  return {
    issuedAt: issuedAt.iso,
    records,
  };
}

/**
 * Loads all HTML river files from data/bom-products/ and parses them into a single array.
 * @returns {Promise<Array<Object>>} - A promise that resolves to an array of all river height records.
 */
export async function parseAllRiverHeights() {
  const dirPath = path.resolve(__dirname, "../test/bom-river-levels");
  const files = (await fs.readdir(dirPath)).filter((f) => f.endsWith(".html"));

  const results = await Promise.all(
    files.map(async (file) => {
      const htmlPath = path.join(dirPath, file);
      const html = await fs.readFile(htmlPath, "utf8");
      try {
        const result = await parseRiverHeights(html);
        return result.records;
      } catch (err) {
        console.error(`Error parsing ${file}: ${err.message}`);
        return [];
      }
    }),
  );

  return results.flat();
}

const riversJsonPath = path.resolve(__dirname, "../rivers.json");
await fs.writeFile(
  riversJsonPath,
  JSON.stringify(await parseAllRiverHeights(), null, 2),
);
