/**
 * Fetch the latest weather from Aurora and append it to the sqlite db
 */
import fs from "node:fs/promises";
import path from "node:path";
import { eachLimit } from "async";
import { append, closeDatabase, initializeDatabase } from "./sqlite.js";
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

function everythingQuery(auroraId) {
  return `query ByLatLongWithRadius {
  locations {
    byId(id: ${JSON.stringify(auroraId)}) {
      weather {
        detailedHistoricConditions(hours: 1) {
          values {
            averageWindSpdKnots,
            averageWindSpeedKm,
            cloud,
            cloudOktas,
            dayName,
            dewPointC,
            endTime,
            feelsLikeTempC,
            generationTime,
            gustKmh,
            maximumGustDir,
            maximumGustKmh,
            maximumGustSpdKnots,
            maximumTempC,
            maximumTempLocalTime,
            minimumTempC,
            minimumTempLocalTime,
            precipitationSince9amMM,
            pressure,
            pressureMSLP,
            qnhPressure,
            rainfall24hr,
            rainHour,
            rainTen,
            relativeHumidityPct,
            startTime,
            tempC,
            visibilityKm,
            wetBulbTemp,
            windDir,
            windDirDeg,
            windGustSpdKnots
          }
        },
      }
    }
  }
}`.replace(/\n\s*/g, "");
}

async function fetchWeatherForLocation(location) {
  const { geonameid, name, auroraId } = location.properties;
  if (!auroraId) {
    console.warn(`Missing aurora ID for ${name} (#${geonameid})`);
    return;
  }
  const query = everythingQuery(auroraId);
  console.log("QUERYING --- ", query);
  const res = await graphqlQuery(query);
  const data =
    res.data?.locations?.byId?.weather?.detailedHistoricConditions?.[0]
      ?.values?.[0];
  if (!data) {
    console.error(`No data fetched for ${name} (${auroraId})`);
    console.log(JSON.stringify(res));
    return;
  }
  append({
    auroraId,
    fetchTime: new Date().toISOString(),
    ...data,
  });
  await fs.writeFile(
    path.resolve(__dirname, "../data/taketheweather.json"),
    JSON.stringify(res, null, 2)
  );
  return res;
}

initializeDatabase();

let i = 0;
await eachLimit(geojson.features, 3, async (feature) => {
  console.log(
    "STARTING - ",
    `${i++}/${geojson.features.length}`,
    feature.properties.name,
    feature.properties.auroraId
  );
  await fetchWeatherForLocation(feature).catch((e) => {
    console.log(e);
  });
});

closeDatabase();
