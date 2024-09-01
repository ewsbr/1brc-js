import fs from 'fs/promises';

const LOG_LINES = 10_000_000;
const BUFF_SIZE = 64 * 1024;

console.log('running with', BUFF_SIZE);

const handle = await fs.open('measurements-100m.txt', 'r');

let lines = 0;
const start = process.hrtime.bigint();
const measurements = new Map()

let lastLine = '';
const buff = Buffer.alloc(BUFF_SIZE);
while (true) {
  const { bytesRead } = await handle.read(buff, 0, BUFF_SIZE);
  if (bytesRead === 0) {
    break;
  }

  let str = buff.toString('binary');
  if (lastLine.length > 0) {
    str = lastLine + str;
  }

  let i = 0;
  let prev = 0;
  let indexStart = 0;
  while (true) {
    const idx = str.indexOf('\n', indexStart);
    if (idx === -1) {
      lastLine = str.slice(prev, str.length);
      break;
    }

    prev = indexStart;
    indexStart = idx + 1;
    lines += 1;
    let line = str.slice(prev, idx);

    const semi = line.indexOf(';');
    const city = line.slice(0, semi);
    const temp = line.slice(semi + 1);
    const temperature = Number(temp);

    if (Number.isNaN(temperature)) {
      console.log('Invalid temperature:', line);
    }

    if (lines % LOG_LINES === 0) {
      const diff = (process.hrtime.bigint() - start) / 1_000_000n;
      const throughput = BigInt(lines) / diff;

      console.log(lines, city, temperature, diff, throughput);
    }

    const measurement = measurements.get(city);
    if (measurement === undefined) {
      measurements.set(city, [temperature, temperature, temperature, 1])
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
    i += 1;
  }
}

await handle.close();

await fs.writeFile('measurements.json', JSON.stringify(Object.fromEntries(measurements.entries())));