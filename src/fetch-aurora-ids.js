/**
 * @file
 * For each location in the GeoJSON file, look up the corresponding Aurora ID
 */
import { eachLimit } from "async";
import fs from "node:fs/promises";
import path from "node:path";
import { graphqlQuery } from "./graphql.js";
import logger from "./logger.js";

const __dirname = path.dirname(new URL(import.meta.url).pathname);
const GEOJSON_FILE = path.resolve(__dirname, "../data/au.geo.json");
const geojsonText = await fs.readFile(GEOJSON_FILE, "utf8").catch((e) => {
  logger.error(
    "Error: %s must first be created by process:geonames-to-geojson",
    GEOJSON_FILE,
  );
  process.exit();
});
const geojson = JSON.parse(geojsonText);

async function rectifyLocation(location) {
  const [longitude, latitude] = location.geometry.coordinates;
  const { name, auroraId } = location.properties;
  if (auroraId) {
    return;
  }

  const auroraLocationsFiveKm = await graphqlQuery(`query ByLatLongWithRadius {
  locations {
      byLatLongWithRadius(
      lat: ${Number(latitude)},
      long: ${Number(longitude)},
      radius: FiveKm
      ) {
      id,
      suburb
      }
  }
  }`);

  const locations = auroraLocationsFiveKm.data?.locations?.byLatLongWithRadius;
  if (!locations?.length) {
    logger.error("No locations found for name %s", name);
  } else {
    location.properties.auroraId = locations[0].id.replace(
      "aurora://location/",
      "",
    );
    const auroraName = locations[0].suburb;
    if (location.properties.name !== auroraName) {
      location.properties.auroraName = auroraName;
    }
    logger.info("Rectified %s", name);
  }
  await fs.writeFile(GEOJSON_FILE, JSON.stringify(geojson, null, 2));
}

export default async function fetchAuroraIds() {
  logger.info("Fetching %d IDs", geojson.features.length);
  await eachLimit(geojson.features, 3, async (feature) => {
    await rectifyLocation(feature);
  });
  logger.info("Done fetching Aurora IDs");
}

if (import.meta.url === `file://${process.argv[1]}`) {
  await fetchAuroraIds();
}
