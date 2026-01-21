/**
 * Fetch the latest weather from Aurora and append it to the sqlite db
 */
import fs from "node:fs/promises";
import path from "node:path";
import { eachLimit } from "async";
import { append, closeDatabase, initializeDatabase } from "./sqlite.js";
import { graphqlQuery } from "./graphql.js";
import { resolveLocalTimeToUtc } from "./utils.aurora-dates.js";
import logger from "./logger.js";

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
    logger.warn("Missing aurora ID for %s", name);
    return;
  }
  const query = everythingQuery("aurora://location/" + auroraId);
  const res = await queryFn(query);
  const data =
    res.data?.locations?.byId?.weather?.detailedHistoricConditions?.[0]
      ?.values?.[0];
  if (!data) {
    logger.error("No data fetched for %s (%s)", name, auroraId);
    logger.debug("Response: %j", res);
    return;
  }

  const processedData = {
    ...data,
    maximumTempLocalTimeUTC: resolveLocalTimeToUtc(
      data.maximumTempLocalTime,
      data.endTime,
    ),
    minimumTempLocalTimeUTC: resolveLocalTimeToUtc(
      data.minimumTempLocalTime,
      data.endTime,
    ),
  };

  await append({
    auroraId,
    fetchTime: new Date().toISOString(),
    ...processedData,
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
    logger.error(
      "Error: %s must first be created by process:geonames-to-geojson",
      geojsonPath,
    );
    process.exit();
  });
  const geojson = JSON.parse(geojsonText);

  let i = 0;
  await eachLimit(geojson.features, 3, async (feature) => {
    await fetchWeatherForLocation(feature, queryFn).catch((e) => {
      logger.error(
        "Failed to fetch weather for %s: %s",
        feature.properties.name,
        e.message,
      );
    });
    i++;
    if (i % 10 === 0 || i === geojson.features.length) {
      logger.info(
        "Progress: %d/%d locations processed",
        i,
        geojson.features.length,
      );
    }
  });

  closeDatabase();
}

if (import.meta.url === `file://${process.argv[1]}`) {
  await fetchWeatherCron();
}
