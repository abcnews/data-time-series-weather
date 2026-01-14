#!/usr/bin/env bash

# Fetch and process weather data pipeline
npm run get-db && \
npm run fetch:weather-cron && \
npm run prepare:tempC && \
npm run prepare:averageWindSpeedKm && \
npm run prepare:maximumGustKmh && \
npm run prepare:relativeHumidityPct && \
npm run prepare:precipitationSince9amMM && \
gzip data/weather.sqlite && \
npm run upload