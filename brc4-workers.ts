import fs from 'fs/promises';
import { Worker } from 'node:worker_threads';

const CHUNK_SIZE = 64 * 1024;
const CHUNKS_PER_BATCH = 512;
const BATCH_SIZE = CHUNK_SIZE * CHUNKS_PER_BATCH;
const WORKER_POOL_SIZE = 12;
const MEASUREMENTS_FILE = 'measurements.txt'
const WORKER_FILE = './worker.js'

type HeadTailPairs = Array<[string, string]>;

class TaskQueue {

  private readonly queue: any[] = [];
  private readonly availableWorkers: Worker[] = [];

  constructor(
    private readonly workerPool: Worker[],
  ) {
    this.availableWorkers = [...workerPool]
  }

  #runJob(job: any) {
    return new Promise((resolve, reject) => {
      const worker = this.availableWorkers.shift();
      if (worker != null) {
        const onMessage = (value: any) => {
          resolve(value);
          this.availableWorkers.push(worker);
          this.#processQueue();
          worker.removeListener('error', onError);
        }

        const onError = (err: unknown) => {
          reject(err)
          this.availableWorkers.push(worker);
          this.#processQueue();
          worker.removeListener('message', onMessage);
        };

        worker.once('message', onMessage);
        worker.once('error', onError)

        worker.postMessage(job);
      } else {
        this.queue.push({ job, resolve, reject })
      }
    });
  }

  #processQueue() {
    if (this.queue.length === 0) {
      return;
    }

    const { job, resolve, reject } = this.queue.shift();
    console.log('process queue', job.batchIndex);
    this.#runJob(job).then(resolve).catch(reject);
  }

  public async run(jobs: any[]): Promise<any[]> {
    return await Promise.all(jobs.map(job => this.#runJob(job)))
  }

  public close(): Promise<number[]> {
    return Promise.all(this.workerPool.map(worker => worker.terminate()))
  }

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

const workerPool: Worker[] = Array.from({ length: WORKER_POOL_SIZE }, () => new Worker(WORKER_FILE));

const taskQueue = new TaskQueue(workerPool);
const jobs = Array.from({ length: totalChunks }, (_, i) => ({
  batchIndex: i,
  batchSize: BATCH_SIZE,
  chunksPerBatch: CHUNKS_PER_BATCH,
  chunkSize: CHUNK_SIZE,
  fileName: MEASUREMENTS_FILE,
}))

const values = await taskQueue.run(jobs);
for (const value of values) {
  const { batchIndex, head, tail, result } = value;
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
}
console.log(values);

const timeTaken = Number(process.hrtime.bigint() - start) / 1_000_000_000;
const totalCount = unprocessedLines.length +  [...measurements.entries()].map(([_, value]) => value.count).reduce((prev, curr) => prev + curr, 0)
console.log({
  ...process.memoryUsage(),
  timeTaken,
  totalCount: totalCount,
  linesPerSec: totalCount / timeTaken,
  throughput: (size / 1e6) / timeTaken
});

processBoundaryLines(unprocessedLines, measurements);
fixObjectKeys(measurements);

await taskQueue.close();

const results = [];
const entries = [...measurements.entries()].sort((left, right) => left[0] > right[0] ? 1 : -1);

for (const [city, temps] of entries) {
  const { min, max, sum, count } = temps;
  const avg = sum / count;
  results.push(`${city}=${min.toFixed(1)}/${avg.toFixed(1)}/${max.toFixed(1)}`)
}

const output = `{${results.join(', ')}}`
await fs.writeFile('results.txt', output);