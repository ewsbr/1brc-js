import fs from 'fs/promises';
import { setTimeout } from 'timers/promises';
import { Worker } from 'node:worker_threads';

const LOG_LINES = 10_000_000;
const CHUNK_SIZE = 4096 * 1024;
const CHUNKS_PER_BATCH = 8;
const BATCH_SIZE = CHUNK_SIZE * CHUNKS_PER_BATCH;
const WORKER_POOL_SIZE = 8;
const MEASUREMENTS_FILE = 'measurements.txt'
const WORKER_FILE = './worker.js'

type HeadTailPairs = Array<[string, string]>;

function max(first: bigint, second: bigint): bigint {
  return first > second ? first : second;
}

function fixObjectKeys(obj: Map<string, any>) {
  for (const [key, value] of obj.entries()) {
    // const cityName = value.at(-1);
    const newKey = Buffer.from(key, 'binary').toString('utf-8');
    if (newKey !== key) {
      obj.set(newKey, obj.get(key));
      obj.delete(key);
    }
  }
}

function processBoundaryLines(pairs: HeadTailPairs, measurements: Map<string, any>) {
  let previousTail = '';
  for (const pair of pairs) {
    const [head, tail] = pair;
    const line = previousTail + head;
    previousTail = tail;

    const [city, temp] = line.split(';');
    const temperature = Number(temp);

    const measurement = measurements.get(city);
    if (measurement === undefined) {
      measurements.set(city, [temperature, temperature, temperature, 1, city]);
      continue;
    }

    measurement[0] = Math.min(measurement[0], temperature);
    measurement[1] = Math.max(measurement[1], temperature);
    measurement[2] = measurement[2] + temperature;
    measurement[3] += 1;
  }
}

const start = process.hrtime.bigint();
const measurements: Map<string, any> = new Map();
const unprocessedLines: HeadTailPairs = [];

const { size } = await fs.stat(MEASUREMENTS_FILE);
const totalChunks = Math.ceil(size / BATCH_SIZE);
const remainingBatches = Array.from({ length: totalChunks }, (_, i) => i)

const workerPool: Worker[] = [];
const freeWorkers: number[] = [];
for (let i = 0; i < WORKER_POOL_SIZE; i++) {
  const worker = new Worker(WORKER_FILE);
  worker.on('message', async (value: any) => {
    const { workerId, batchIndex, head, tail, result, lineCount } = value;
    for (const [city, temps] of result.entries()) {
      const current = measurements.get(city);
      if (!current) {
        measurements.set(city, temps)
        continue;
      }

      current[0] = Math.min(current[0], temps[0]);
      current[1] = Math.max(current[1], temps[1]);
      current[2] += temps[2];
      current[3] += temps[3];
    }

    unprocessedLines[batchIndex] = [head, tail];
    freeWorkers.push(workerId);
  });

  workerPool.push(worker);
  freeWorkers.push(i);
}

while (remainingBatches.length > 0) {
  await setTimeout(0);
  if (freeWorkers.length === 0) {
    continue;
  }

  const workerId = freeWorkers.shift()!!;
  const worker = workerPool[workerId];

  const batchIndex = remainingBatches.shift()!!;
  if (batchIndex % 100 === 0) {
    console.log(`${workerId} takes ${batchIndex}`);
  }
  worker.postMessage({
    workerId,
    batchIndex,
    batchSize: BATCH_SIZE,
    chunksPerBatch: CHUNKS_PER_BATCH,
    chunkSize: CHUNK_SIZE,
    fileName: MEASUREMENTS_FILE,
  })
}

while (freeWorkers.length < WORKER_POOL_SIZE) {
  await setTimeout(0);
}

const timeTaken = Number(process.hrtime.bigint() - start) / 1_000_000_000;
const totalCount = unprocessedLines.length +  [...measurements.entries()].map(([_, value]) => value[3]).reduce((prev, curr) => prev + curr, 0)
console.log({
  ...process.memoryUsage(),
  timeTaken,
  totalCount: totalCount,
  linesPerSec: totalCount / timeTaken,
  throughput: (size / 1e6) / timeTaken
});
await Promise.all(workerPool.map(worker => worker.terminate()))

processBoundaryLines(unprocessedLines, measurements);
fixObjectKeys(measurements);

const result: any = {};
for (const [city, temps] of measurements.entries()) {
  const [min, max, sum, count] = temps;
  result[city] = {
    min,
    max,
    avg: sum / count
  }
}

await fs.writeFile('measurements.json', JSON.stringify(result));