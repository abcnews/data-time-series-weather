/**
 * @file
 * Create a geojson file from a GeoNames data dump. Places are limited to at
 * most DISTANCE_THRESHOLD_KM apart, with more populous places taking precedence
 */
import fs from "node:fs";
import path from "node:path";
import * as turf from "@turf/turf";

const __dirname = path.dirname(new URL(import.meta.url).pathname);
const input = path.resolve(__dirname, "../data/AU.txt");
const output = path.resolve(__dirname, "../data/au.geo.json");
const DISTANCE_THRESHOLD_KM = 50;

// 1. Parse the schema to find fields
const fieldsSchema = `
geonameid         : integer id of record in geonames database
name              : name of geographical point (utf8) varchar(200)
asciiname         : name of geographical point in plain ascii characters, varchar(200)
alternatenames    : alternatenames, comma separated, ascii names automatically transliterated, convenience attribute from alternatename table, varchar(10000)
latitude          : latitude in decimal degrees (wgs84)
longitude         : longitude in decimal degrees (wgs84)
feature class     : see http://www.geonames.org/export/codes.html, char(1)
feature code      : see http://www.geonames.org/export/codes.html, varchar(10)
country code      : ISO-3166 2-letter country code, 2 characters
cc2               : alternate country codes, comma separated, ISO-3166 2-letter country code, 200 characters
admin1 code       : fipscode (subject to change to iso code), see exceptions below, see file admin1Codes.txt for display names of this code; varchar(20)
admin2 code       : code for the second administrative division, a county in the US, see file admin2Codes.txt; varchar(80) 
admin3 code       : code for third level administrative division, varchar(20)
admin4 code       : code for fourth level administrative division, varchar(20)
population        : bigint (8 byte int) 
elevation         : in meters, integer
dem               : digital elevation model, srtm3 or gtopo30, average elevation of 3''x3'' (ca 90mx90m) or 30''x30'' (ca 900mx900m) area in meters, integer. srtm processed by cgiar/ciat.
timezone          : the iana timezone id (see file timeZone.txt) varchar(40)
modification date : date of last modification in yyyy-MM-dd format
`;

const fieldsToKeep = ["geonameid", "name"];

const fields = fieldsSchema
  .trim()
  .split("\n")
  .map((line) => line.split(":")[0].trim());

console.log("Loading file into memory...");

// 2. Load and Parse Data
const rawData = fs.readFileSync(input, "utf-8");
const lines = rawData.split("\n").filter((l) => l.trim().length > 0);

console.log(`Parsing ${lines.length} locations...`);

const locations = lines
  .map((line) => {
    const columns = line.split("\t");
    const props = {};

    fields.forEach((field, index) => {
      props[field] = columns[index];
    });

    // Convert critical fields to numbers for sorting/calculating
    props.population = parseInt(props.population || "0", 10);
    props.latitude = parseFloat(props.latitude);
    props.longitude = parseFloat(props.longitude);

    return props;
  })
  .filter(
    (location) =>
      location.population > 0 && location["feature code"].includes("PPL")
  )
  .toSorted((a, b) => b.population - a.population);

console.log(`Filtered ${locations.length} locations...`);

console.log("Filtering nearby points (Greedy Spatial Filter)...");

// 4. Spatial Filter
const acceptedLocations = [];

for (const candidate of locations) {
  let isTooClose = false;

  const candidatePoint = turf.point([candidate.longitude, candidate.latitude]);

  // Compare candidate against points we have ALREADY accepted
  // (which we know are larger because of the sort order)
  for (const existing of acceptedLocations) {
    const existingPoint = turf.point([existing.longitude, existing.latitude]);

    const distance = turf.distance(candidatePoint, existingPoint, {
      units: "kilometers",
    });

    if (distance < DISTANCE_THRESHOLD_KM) {
      isTooClose = true;
      break; // Stop checking, this candidate is rejected
    }
  }

  if (!isTooClose) {
    acceptedLocations.push(candidate);
  }
}

console.log(
  `Filtered down to ${acceptedLocations.length} locations (removed ${
    locations.length - acceptedLocations.length
  }).`
);

console.log(
  locations.reduce((obj, location) => {
    obj[location["feature code"]] = obj[location["feature code"]]
      ? obj[location["feature code"]] + 1
      : 1;
    return obj;
  }, {})
);

// 5. Convert to GeoJSON FeatureCollection
const geojson = {
  type: "FeatureCollection",
  features: acceptedLocations.map((loc) => {
    // Extract lat/long to keep properties clean (optional, but standard practice)
    const { latitude, longitude, ...properties } = loc;

    return {
      type: "Feature",
      geometry: {
        type: "Point",
        coordinates: [longitude, latitude],
      },
      properties: Object.entries(properties).reduce((obj, [key, value]) => {
        if (fieldsToKeep.includes(key)) {
          obj[key] = value;
        }
        return obj;
      }, {}),
    };
  }),
};

// 6. Write to disk
fs.writeFileSync(output, JSON.stringify(geojson, null, 2));
console.log(`Written to ${output}`);
