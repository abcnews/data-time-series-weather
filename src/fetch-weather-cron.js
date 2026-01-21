/**
 * Fetch the latest weather from Aurora and append it to the sqlite db
 */
import fs from "node:fs/promises";
import path from "node:path";
import { eachLimit } from "async";
import { append, closeDatabase, initializeDatabase } from "./sqlite.js";
import { graphqlQuery } from "./graphql.js";

const __dirname = path.dirname(new URL(import.meta.url).pathname);

export function everythingQuery(auroraId) {
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

export async function fetchWeatherForLocation(
  location,
  queryFn = graphqlQuery,
) {
  const { name, auroraId } = location.properties;
  if (!auroraId) {
    console.warn(`Missing aurora ID for "${name}"`);
    return;
  }
  const query = everythingQuery("aurora://location/" + auroraId);
  const res = await queryFn(query);
  const data =
    res.data?.locations?.byId?.weather?.detailedHistoricConditions?.[0]
      ?.values?.[0];
  if (!data) {
    console.error(`No data fetched for ${name} (${auroraId})`);
    console.log(JSON.stringify(res));
    return;
  }
  await append({
    auroraId,
    fetchTime: new Date().toISOString(),
    ...data,
  });
  return res;
}

export default async function fetchWeatherCron({
  geojsonPath = path.resolve(__dirname, "../data/au.geo.json"),
  databasePath,
  queryFn = graphqlQuery,
} = {}) {
  await initializeDatabase(databasePath, geojsonPath);

  const geojsonText = await fs.readFile(geojsonPath, "utf8").catch((e) => {
    console.error(
      "Error: ",
      geojsonPath,
      "must first be created by process:geonames-to-geojson",
    );
    process.exit();
  });
  const geojson = JSON.parse(geojsonText);

  let i = 0;
  await eachLimit(geojson.features, 3, async (feature) => {
    console.log(
      "STARTING - ",
      `${i++}/${geojson.features.length}`,
      feature.properties.name,
      feature.properties.auroraId,
    );
    await fetchWeatherForLocation(feature, queryFn).catch((e) => {
      console.log(e);
    });
  });

  closeDatabase();
}

if (import.meta.url === `file://${process.argv[1]}`) {
  await fetchWeatherCron();
}
