import * as Minio from "minio";
import fs from "fs/promises";
import { program } from "commander";
import { createReadStream as fsCreateReadStream } from "fs";
import process from "process";
import path from "path";
import mime from "mime";
import zlib from "zlib";

const BROTLI_EXTENSIONS = ["json", "xml"];

/**
 * Check if a file should be compressed based on its extension
 * @param {string} filename - The filename to check
 * @returns {boolean} True if the file should be compressed
 */
function shouldCompress(filename) {
  const ext = path.extname(filename).slice(1); // Remove leading dot
  return BROTLI_EXTENSIONS.includes(ext);
}

program
  .requiredOption(
    "-e, --end-point <server>",
    "S3 endpoint, e.g. sgp1.digitaloceanspaces.com"
  )
  .option("-p, --port <port>", "Port to connect over", 443)
  .requiredOption("-a, --access-key <accessKey>", "s3 access key")
  .requiredOption("-k, --secret-key <secretKey>", "s3 secret key")
  .requiredOption("-b --bucket <bucket>", "Bucket to put files in")
  .option("-s, --src <srcDir>", "source directory to publish", "dist/")
  .requiredOption(
    "-d, --dest <destDir>",
    "directory on the S3 server to publish to"
  );

program.parse();

/**
 * Recursively list directories to create an array of src/dest files to upload
 * @param {string} src - Source directory, an absolute path
 * @param {string} dest - Dest dir on the s3 server
 * @returns Array of objects with local & remote filenames
 **/
async function syncDir({ src, dest }) {
  const listing = await fs.readdir(src, { withFileTypes: true });
  const files = [];
  const dirs = [];
  listing.forEach((entry) => {
    if (entry.isDirectory()) {
      dirs.push(entry.name + "/");
    } else {
      files.push({
        local: path.join(entry.parentPath, entry.name),
        remote: path.join(dest, entry.name),
        contentType: mime.getType(entry.name),
      });
    }
  });
  const extraFiles = await Promise.all(
    dirs.map((dir) =>
      syncDir({
        src: path.join(src, dir),
        dest: path.join(dest, dir),
      })
    )
  );
  extraFiles.forEach((newFiles) => {
    files.push(...newFiles);
  });
  return files;
}

async function go() {
  const opts = program.opts();
  const errors = [];
  [
    "bucket",
    "endPoint",
    "port",
    "accessKey",
    "secretKey",
    "src",
    "dest",
  ].forEach((key) => {
    if (!opts[key]) {
      errors.push(`${key} is required`);
    }
  });

  if (errors.length) {
    console.log(program.opts());
    errors.forEach((e) => console.error(e));
    throw new Error("Required options were missing.");
  }

  // Set up s3 client
  const { src, dest, bucket, ...s3Options } = opts;
  const minioClient = new Minio.Client(s3Options);

  const srcAbsolute = path.resolve(process.cwd(), src);
  const actions = await syncDir({ src: srcAbsolute, dest });

  // Transfer each file with given options
  for (const action of actions) {
    const { local, remote, contentType } = action;
    const useCompression = shouldCompress(remote);

    const metadata = {
      "Content-Type": contentType,
      "x-amz-acl": "public-read",
      "cache-control": "max-age=60",
    };

    if (useCompression) {
      metadata["Content-Encoding"] = "br";
    }

    console.log(
      useCompression ? "putObject (brotli)" : "fPutObject",
      remote,
      contentType
    );

    try {
      if (useCompression) {
        const readStream = fsCreateReadStream(local);
        const brotliStream = zlib.createBrotliCompress({
          params: {
            // BROTLI_PARAM_QUALITY: 11 is the maximum compression level
            [zlib.constants.BROTLI_PARAM_QUALITY]: 11,
          },
        });
        const compressedStream = readStream.pipe(brotliStream);

        await minioClient.putObject(bucket, remote, compressedStream, metadata);
      } else {
        // For non-compressed files, use the simpler fPutObject
        await minioClient.fPutObject(bucket, remote, local, metadata);
      }
    } catch (e) {
      console.error(e);
      process.exit(1);
    }
  }
}

go();
