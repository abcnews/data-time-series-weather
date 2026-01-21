import fs from "node:fs";

function convertCsvToGeoJson(inputPath, outputPath) {
  const csvData = fs.readFileSync(inputPath, "utf8");
  const lines = csvData.trim().split(/\r?\n/);

  // Clean headers
  const headers = ["metraId", "wmoId", "bomId", "name", "point"];

  const features = lines.slice(1).map((line) => {
    // Note: If your CSV uses quotes around fields with spaces,
    // a simple .split(",") might shift columns.
    // This regex ensures "POINT (long lat)" stays as one piece.
    const values = line.split(",");

    const properties = {};
    let geometry = null;

    headers.forEach((header, index) => {
      let value = values[index];
      if (value.slice(0, 1) === '"') {
        value = value.slice(1, -1);
      }

      if (header === "point" && value) {
        value = value.replace("POINT (", "").replace(")", "");
        const coords = value.split(" ");
        if (coords) {
          geometry = {
            type: "Point",
            // GeoJSON coordinates are [longitude, latitude]
            coordinates: [parseFloat(coords[0]), parseFloat(coords[1])],
          };
        }
      } else {
        // Store all other fields (IDs, labels) in properties
        properties[header] = value;
      }
    });

    return {
      type: "Feature",
      geometry: geometry,
      properties: properties,
    };
  });

  const geoJson = {
    type: "FeatureCollection",
    features: features,
  };

  fs.writeFileSync(outputPath, JSON.stringify(geoJson, null, 4));
  console.log(
    `Successfully converted ${features.length} features to ${outputPath}`
  );
}

convertCsvToGeoJson("weather-stations-fulltable-export.csv", "au.geo.json");
