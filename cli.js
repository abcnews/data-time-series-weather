#!/usr/bin/env node
import { program } from "commander";
import dotenv from "dotenv";

// Load environment variables
dotenv.config();

// Define subcommands
program
  .command("fetch-aurora-ids")
  .description("Fetch Aurora IDs for locations in the GeoJSON file")
  .action(async () => {
    const { default: fetchAuroraIds } = await import(
      "./src/fetch-aurora-ids.js"
    );
    await fetchAuroraIds();
  });

program
  .command("fetch-weather-cron")
  .description(
    "Fetch the latest weather data and append it to the SQLite database"
  )
  .action(async () => {
    const { default: fetchWeatherCron } = await import(
      "./src/fetch-weather-cron.js"
    );
    await fetchWeatherCron();
  });

program
  .command("generate-dataset")
  .description("Generate a time-series dataset for a specified column and day")
  .option("-c, --column <columnName>", "Which column to return", "tempC")
  .option("-d, --dayStart <number>", "Day relative to today (0, -1, -2)", "0")
  .option("-o, --output <filename>", "Where to write this json", "output.json")
  .action(async (options) => {
    const { default: generateDataset } = await import(
      "./src/generate-dataset.js"
    );
    await generateDataset(options);
  });

program
  .command("generate-datasets")
  .description("Generate multiple datasets based on specified columns and days")
  .option(
    "-c, --columns <columns>",
    "Comma-separated list of columns to generate",
    process.env.GENERATE_DATASETS || "tempC"
  )
  .option(
    "-d, --days <days>",
    "Number of days to generate data for",
    process.env.GENERATE_DAYS || "1"
  )
  .action(async (options) => {
    const { default: generateDatasets } = await import(
      "./src/generate-datasets.js"
    );
    await generateDatasets(options);
  });

program
  .command("geonames-to-geojson")
  .description("Convert GeoNames data to a GeoJSON file")
  .action(async () => {
    const { default: geonamesToGeojson } = await import(
      "./src/geonames-to-geojson.js"
    );
    await geonamesToGeojson();
  });

program
  .command("upload-s3")
  .description("Upload files to S3")
  .option("-e, --end-point <server>", "S3 endpoint", process.env.S3_END_POINT)
  .option("-p, --port <port>", "Port", process.env.S3_PORT || 443)
  .option(
    "-a, --access-key <accessKey>",
    "s3 access key",
    process.env.S3_ACCESS_KEY
  )
  .option(
    "-k, --secret-key <secretKey>",
    "s3 secret key",
    process.env.S3_SECRET_KEY
  )
  .option("-b --bucket <bucket>", "Bucket", process.env.S3_BUCKET)
  .option(
    "-s, --src <srcDir>",
    "source directory",
    process.env.S3_SRC || "data/"
  )
  .option("-d, --dest <destDir>", "destination directory", process.env.S3_DEST)
  .action(async (options) => {
    const { default: uploadS3 } = await import("./src/upload-s3.mjs");
    await uploadS3(options);
  });

program.parse();
