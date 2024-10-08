import fs from 'fs/promises';
import { parentPort, isMainThread } from 'worker_threads'

if (isMainThread) {
  throw new Error('This worker cannot be run on the main thread')
}

function* readLines(tail, bytesRead, buf) {
  let start = 0;

  const subarray = bytesRead === buf.length ? buf : buf.subarray(0, bytesRead);
  const str = subarray.toString('binary');
  do {
    const idx = str.indexOf('\n', start);
    if (idx === -1) {
      return str.slice(start);
    }

    if (start === 0 && tail !== '') {
      yield tail + str.substring(start, idx);
    } else {
      yield str.substring(start, idx);
    }
    start = idx + 1;
  } while (start < bytesRead);
}

parentPort.on('message', async (message) => {
  const { batchIndex, batchSize, chunksPerBatch, fileName, chunkSize } = message;
  const buf = Buffer.alloc(chunkSize);
  const handle = await fs.open(fileName, 'r');
  const result = new Map();

  let head = '';
  let tail = '';

  for (let i = 0; i < chunksPerBatch; i++) {
    const { bytesRead } = await handle.read(buf, 0, chunkSize, (batchIndex * batchSize) + (i * chunkSize));
    if (bytesRead === 0) {
      parentPort.postMessage({
        batchIndex,
        result,
        head,
        tail,
      })
      return;
    }

    const lineGen = readLines(tail, bytesRead, buf);
    if (i === 0) {
      const { value } = lineGen.next();
      head = value;
    }

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

      const measurement = result.get(city);
      if (measurement === undefined) {
        result.set(city, {
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

  await handle.close();
  parentPort.postMessage({
    batchIndex,
    head,
    tail,
    result
  })
});