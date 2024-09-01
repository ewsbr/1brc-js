import fs from 'fs/promises';
import { setTimeout } from 'timers/promises';
import { Worker } from 'node:worker_threads';

const CHUNK_SIZE = 64 * 1024;
const CHUNKS_PER_BATCH = 512;
const BATCH_SIZE = CHUNK_SIZE * CHUNKS_PER_BATCH;
const WORKER_POOL_SIZE = 10;
const MEASUREMENTS_FILE = 'measurements.txt'
const WORKER_FILE = './worker.js'

type HeadTailPairs = Array<[string, string]>;

function max(first: bigint, second: bigint): bigint {
  return first > second ? first : second;
}

function exec<T>(worker: Worker, message: any): Promise<T> {
  return new Promise((resolve, reject) => {
    worker.once('message', resolve);
    worker.once('error', reject);
  });
}

function fixObjectKeys(obj: Map<string, any>) {
  for (const [key, value] of obj.entries()) {
    const newKey = Buffer.from(value.city, 'binary').toString('utf-8');
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
      measurements.set(city, {
        min: temperature,
        max: temperature,
        sum: temperature,
        count: 1,
        city
      });
      continue;
    }

    measurement.min = Math.min(measurement.min, temperature);
    measurement.max = Math.max(measurement.max, temperature);
    measurement.sum = measurement.sum + temperature;
    measurement.count += 1;
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
    const { workerId, batchIndex, head, tail, result } = value;
    for (const [city, temps] of result.entries()) {
      const current = measurements.get(city);
      if (!current) {
        measurements.set(city, temps)
        continue;
      }

      current.min = Math.min(current.min, temps.min);
      current.max  = Math.max(current.max, temps.max);
      current.sum += temps.sum;
      current.count += temps.count;
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
const totalCount = unprocessedLines.length +  [...measurements.entries()].map(([_, value]) => value.count).reduce((prev, curr) => prev + curr, 0)
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

const results = [];
const entries = [...measurements.entries()].sort((left, right) => left[0] > right[0] ? 1 : -1);

for (const [city, temps] of entries) {
  const { min, max, sum, count } = temps;
  const avg = sum / count;
  results.push(`${city}=${min.toFixed(1)}/${avg.toFixed(1)}/${max.toFixed(1)}`)
}

const output = `{${results.join(', ')}}`
await fs.writeFile('results.txt', output);