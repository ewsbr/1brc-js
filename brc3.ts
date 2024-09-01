import fs from 'fs/promises';
import { Worker } from 'node:worker_threads';

const LOG_LINES = 10_000_000;
const BUFF_SIZE = 64 * 1024;

function max(first: bigint, second: bigint): bigint {
  return first > second ? first : second;
}

function* readLines(tail: string, bytesRead: number, buf: Buffer): Generator<string> {
  let start = 0;

  const subarray = bytesRead === buf.length ? buf : buf.subarray(0, bytesRead);
  const str = subarray.toString('binary');
  do {
    const idx = str.indexOf('\n', start);
    if (idx === -1) {
      return str.slice(start);
    }

    if (str.substring(start, idx).startsWith('furt')) {
      console.log(str.substring(start, idx), tail, start, start === 0 && tail !== '');
    }
    if (start === 0 && tail !== '') {
      yield tail + str.substring(start, idx);
    } else {
      yield str.substring(start, idx);
    }
    start = idx + 1;
  } while (start < bytesRead);
}

function fixObjectKeys(obj: Map<string, any>) {
  for (const [key, value] of obj.entries()) {
    const cityName = value.at(-1);
    const newKey = Buffer.from(cityName, 'binary').toString('utf-8');
    if (newKey !== key) {
      obj.set(newKey, obj.get(key));
      obj.delete(key);
    }
  }
}

const handle = await fs.open('measurements-100m.txt', 'r');

let lines = 0;
const start = process.hrtime.bigint();
const measurements: Map<string, any> = new Map();

const workerPool = new Array(8).map(() => new Worker('worker.js'));

let tail = '';
const buff = Buffer.alloc(BUFF_SIZE);
while (true) {
  const { bytesRead } = await handle.read(buff, 0, BUFF_SIZE);
  if (bytesRead === 0) {
    break;
  }

  const lineGen = readLines(tail, bytesRead, buff);
  while (true) {
    const { value, done } = lineGen.next()
    if (done) {
      tail = value ?? '';
      break;
    }

    const semi = value.indexOf(';');
    const city = value.slice(0, semi);
    const temp = value.slice(semi + 1);
    const temperature = Number(temp);

    if (Number.isNaN(temperature)) {
      console.log('Invalid temperature:', value);
    }

    if (lines % LOG_LINES === 0) {
      const diff = (process.hrtime.bigint() - start) / 1_000_000n;
      const throughput = BigInt(lines) / max(diff, 1n);

      console.log(lines, city, temperature, diff, throughput);
    }

    lines += 1;

    const measurement = measurements.get(city);
    if (measurement === undefined) {
      measurements.set(city, [temperature, temperature, temperature, 1]);
      continue;
    }

    // measurements[city][0] = Math.min(measurements[city][0], temperature);
    // measurements[city][1] = Math.max(measurements[city][1], temperature);
    // measurements[city][2] += temperature;
    // measurements[city][3] += 1;
    measurement[0] = Math.min(measurement[0], temperature);
    measurement[1] = Math.max(measurement[1], temperature);
    measurement[2] = measurement[2] + temperature;
    measurement[3] += 1;
  }
}

await handle.close();

await fs.writeFile('measurements.json', JSON.stringify(Object.fromEntries(measurements.entries())));