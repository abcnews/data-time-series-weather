import * as Minio from "minio";
import os from "node:os";
import fs from "node:fs/promises";
import { createReadStream as fsCreateReadStream } from "fs";
import process from "process";
import path from "path";
import mime from "mime";
import zlib from "zlib";
import { performance } from "perf_hooks";
import { eachLimit } from "async";

const BROTLI_EXTENSIONS = ["json", "xml"];

function shouldCompress(filename) {
  const ext = path.extname(filename).slice(1);
  return BROTLI_EXTENSIONS.includes(ext);
}

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
        contentType: mime.getType(entry.name) || "application/octet-stream",
      });
    }
  });
  const extraFiles = await Promise.all(
    dirs.map((dir) =>
      syncDir({ src: path.join(src, dir), dest: path.join(dest, dir) })
    )
  );
  return [...files, ...extraFiles.flat()];
}

async function uploadS3(options) {
  const { src, dest, bucket, ...s3Options } = options;
  const minioClient = new Minio.Client(s3Options);

  const srcAbsolute = path.resolve(process.cwd(), src);
  const actions = await syncDir({ src: srcAbsolute, dest });

  const availableParallelism = os.availableParallelism();
  console.log(
    `🚀 Starting parallel upload (${actions.length} files, concurrency: ${availableParallelism})...\n`
  );

  const globalStart = performance.now();

  try {
    await eachLimit(actions, availableParallelism, async (action) => {
      const { local, remote, contentType } = action;
      const useCompression = shouldCompress(remote);
      const methodLabel = useCompression ? "[Brotli]" : "[Static]";

      const metadata = {
        "Content-Type": contentType,
        "x-amz-acl": "public-read",
        "cache-control": "max-age=60",
        ...(useCompression && { "Content-Encoding": "br" }),
      };

      const fileStart = performance.now();

      try {
        if (useCompression) {
          const readStream = fsCreateReadStream(local);
          const brotliStream = zlib.createBrotliCompress({
            params: { [zlib.constants.BROTLI_PARAM_QUALITY]: 11 },
          });
          await minioClient.putObject(
            bucket,
            remote,
            readStream.pipe(brotliStream),
            metadata
          );
        } else {
          await minioClient.fPutObject(bucket, remote, local, metadata);
        }

        const fileEnd = performance.now();
        const duration = ((fileEnd - fileStart) / 1000).toFixed(2);

        // Print the entire line at once now that it's finished
        console.log(`${methodLabel.padEnd(8)} ${remote} ✅ (${duration}s)`);
      } catch (fileError) {
        // Log individual file failure but allow the overall process to handle the error
        console.error(
          `${methodLabel.padEnd(8)} ${remote} ❌ Failed: ${fileError.message}`
        );
        throw fileError;
      }
    });

    const globalEnd = performance.now();
    const totalTime = ((globalEnd - globalStart) / 1000).toFixed(2);

    console.log(`\n✨ Deployment complete! Total time: ${totalTime}s`);
  } catch (err) {
    console.error(`\n🛑 Upload process halted due to error.`);
    process.exit(1);
  }
}

export default uploadS3;
