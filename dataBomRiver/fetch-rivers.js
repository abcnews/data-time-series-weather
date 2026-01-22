import ftp from "basic-ftp";
import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { Writable } from "node:stream";
import { parseRiverHeights } from "./parse-rivers.js";
import logger from "../src/logger.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

/**
 * Helper to download a file to a buffer using basic-ftp
 */
async function downloadToBuffer(client, remotePath) {
  let buffer = Buffer.alloc(0);
  const writable = new Writable({
    write(chunk, encoding, callback) {
      buffer = Buffer.concat([buffer, chunk]);
      callback();
    },
  });

  await client.downloadTo(writable, remotePath);
  return buffer.toString("utf8");
}

async function fetchRequestedProducts() {
  const productsPath = path.join(__dirname, "bom-products.json");
  const products = JSON.parse(await fs.readFile(productsPath, "utf8"));

  const timestamp = new Date().toISOString();
  const outputDir = path.resolve(__dirname, "../data/rivers");
  const outputFile = path.join(
    outputDir,
    `${timestamp.replace(/:/g, "-")}.json`,
  );

  try {
    await fs.mkdir(outputDir, { recursive: true });
  } catch (err) {
    // Ignore if directory exists
  }

  const client = new ftp.Client();
  const allRecords = [];

  try {
    await client.access({
      host: "ftp.bom.gov.au",
      user: "anonymous",
      password: "guest",
      secure: false,
    });

    logger.info("Connected to BOM FTP. Navigating to /anon/gen/fwo/...");
    await client.cd("anon/gen/fwo");

    for (const product of products) {
      if (!product.filename) {
        continue;
      }

      logger.info(`Fetching and parsing ${product.filename}...`);
      try {
        const html = await downloadToBuffer(client, product.filename);
        const result = await parseRiverHeights(html);
        allRecords.push(...result.records);
      } catch (err) {
        logger.error(`Failed to process ${product.filename}: %s`, err.message);
      }
    }

    if (allRecords.length > 0) {
      await fs.writeFile(outputFile, JSON.stringify(allRecords, null, 2));
      logger.info(`Saved ${allRecords.length} records to ${outputFile}`);
    } else {
      logger.info("No records found to save.");
    }
  } catch (err) {
    logger.error("FTP Error: %O", err);
  } finally {
    client.close();
  }
}

fetchRequestedProducts();
