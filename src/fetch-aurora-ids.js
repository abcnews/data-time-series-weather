/**
 * @file
 * For each location in the GeoJSON file, look up the corresponding Aurora ID
 */
import { eachLimit } from "async";
import fs from "node:fs/promises";
import path from "node:path";
import { graphqlQuery } from "./graphql.js";

const __dirname = path.dirname(new URL(import.meta.url).pathname);
const GEOJSON_FILE = path.resolve(__dirname, "../data/au.geo.json");
const geojsonText = await fs.readFile(GEOJSON_FILE, "utf8").catch((e) => {
  console.error(
    "Error: ",
    GEOJSON_FILE,
    "must first be created by process:geonames-to-geojson"
  );
  process.exit();
});
const geojson = JSON.parse(geojsonText);

async function rectifyLocation(location) {
  const [longitude, latitude] = location.geometry.coordinates;
  const { geonameid, name, auroraId } = location.properties;
  if (auroraId) {
    console.log("Already fetched", auroraId);
    return;
  }
  console.log("Rectifying", name);

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
    console.error(`No locations found for geoname ${name} (#${geonameid})`);
  } else {
    location.properties.auroraId = locations[0].id.replace(
      "aurora://location/",
      ""
    );
    const auroraName = locations[0].suburb;
    if (location.properties.name !== auroraName) {
      location.properties.auroraName = auroraName;
    }
  }
  await fs.writeFile(GEOJSON_FILE, JSON.stringify(geojson, null, 2));
}

export default async function fetchAuroraIds() {
  console.log("Fetching", geojson.features.length, "IDs");
  await eachLimit(geojson.features, 3, async (feature) => {
    await rectifyLocation(feature);
    console.log("did ", feature.properties.name);
  });
  console.log("done");
}

if (import.meta.url === `file://${process.argv[1]}`) {
  await fetchAuroraIds();
}
