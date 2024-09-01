import fs from 'fs/promises';

const LOG_LINES = 5_000_000;

const handle = await fs.open('measurements.txt', 'r');

let i = 0;
let start = process.hrtime.bigint();
const measurements: any = {}
for await (const line of handle.readLines()) {
  const [city, temp] = line.split(';');
  const temperature = Number(temp);

  i += 1;
  if (i % LOG_LINES === 0) {
    const diff = (process.hrtime.bigint() - start) / 1_000_000n;
    const throughput = BigInt(i) / diff;

    console.log(i, city, temperature, diff, throughput);
  }

  if (measurements[city] === undefined) {
    measurements[city] = [temperature, temperature, temperature, 1];
    continue;
  }

  measurements[city][0] = Math.min(measurements[city][0], temperature);
  measurements[city][1] = Math.max(measurements[city][1], temperature);
  measurements[city][2] += temperature;
  measurements[city][3] += 1;
}

await handle.close();

await fs.writeFile('measurements.json', JSON.stringify(measurements));