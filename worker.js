import fs from 'fs/promises';
import { parentPort, isMainThread } from 'worker_threads'

if (isMainThread) {
  throw new Error('This worker cannot be run on the main thread')
}

function* readLines(bytesRead, buf) {
  let start = 0;

  const subarray = bytesRead === buf.length ? buf : buf.subarray(0, bytesRead);
  const str = subarray.toString('binary');
  do {
    const idx = str.indexOf('\n', start);
    if (idx === -1) {
      return str.slice(start);
    }

    yield str.substring(start, idx);
    start = idx + 1;
  } while (start < bytesRead);
}

parentPort.on('message', async (message) => {
  const { workerId, chunkIndex, chunksPerBatch, fileName, chunkSize } = message;
  const buf = Buffer.alloc(chunkSize);
  const handle = await fs.open(fileName, 'r');
  const result = new Map();

  let tail = '';
  let lineCount = 1; // head

  const { bytesRead } = await handle.read(buf, 0, chunkSize, chunkIndex * chunkSize);
  if (bytesRead === 0) {
    parentPort.postMessage({
      workerId,
      chunkIndex,
      result,
      lineCount: 0
    })
    return;
  }

  const lineGen = readLines(bytesRead, buf);
  const { value: head } = lineGen.next();

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

    lineCount += 1;

    const measurement = result.get(city);
    if (measurement === undefined) {
      result.set(city, [temperature, temperature, temperature, 1, city]);
      continue;
    }

    measurement[0] = Math.min(measurement[0], temperature);
    measurement[1] = Math.max(measurement[1], temperature);
    measurement[2] = measurement[2] + temperature;
    measurement[3] += 1;
  }

  await handle.close();

  parentPort.postMessage({
    workerId,
    chunkIndex,
    head,
    tail,
    result,
    lineCount
  })
});