import ftp from "basic-ftp";
import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

async function fetchRequestedProducts() {
  const productsPath = path.join(__dirname, "bom-products.json");
  const products = JSON.parse(await fs.readFile(productsPath, "utf8"));

  const downloadDir = path.resolve(__dirname, "../test/bom-river-levels");
  try {
    await fs.access(downloadDir);
  } catch {
    // If access fails, the directory likely doesn't exist; create it
    await fs.mkdir(downloadDir, { recursive: true });
  }

  const client = new ftp.Client();
  try {
    await client.access({
      host: "ftp.bom.gov.au",
      user: "anonymous",
      password: "guest",
      secure: false,
    });

    console.log("Connected to BOM FTP. Navigating to /anon/gen/fwo/...");
    await client.cd("anon/gen/fwo");

    for (const product of products) {
      if (!product.filename) {
        continue;
      }
      const localPath = path.join(downloadDir, product.filename);
      console.log(`Fetching ${product.filename} to ${localPath}...`);
      try {
        await client.downloadTo(localPath, product.filename);
      } catch (err) {
        console.error(`Failed to download ${product.filename}:`, err.message);
      }
    }
  } catch (err) {
    console.error("FTP Error:", err);
  } finally {
    client.close();
  }
}

fetchRequestedProducts();
