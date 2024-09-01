# 1 billion row challenge
This is my attempt at the 1 billion row challenge using Node.js.
It manages to achieve ~24.5s on my Ryzen 5700G using 14 threads, which imho is not bad for Javascript.

## What I learned
- Node is definitely not single-threaded nowadays, the worker threads scale very well
- At least for this challenge, Map was considerably faster than a plain object
- Arrays are slower than objects... not sure if it's because of the write operations themselves or the copies between threads
- `Buffer.toString('binary')` is ~10%+ faster than the `'utf-8'` version
- The challenge is probably responsible for 1TB of wear on my SSD

## Optimizations not implemented
- Custom HashMaps based on the max number of distinct cities or their max length
- Custom parsers for the CSV format (in my tests, indexOf beat everything else anyway)

## Cursed parts
- The UTF-8 encoding in my Map keys was breaking, even if I converted the Buffer to `utf-8`. So I just gave up and transformed the keys at the end :)
- Instead of doing a full pass in the file to find the lines in chunk boundaries, I just don't process the first line of each chunk. That way, I can just send this data to the main thread and merge it correctly there, given that the number of lines in chunk boundaries is negligible.

## Best run
- CPU threads: 14
- Memory used (rss): 773MB
- Time taken: 24.5s
- Lines per second: 41,266,378/s
- Throughput: 569MB/s

## 8 threads
- CPU threads: 8
- Memory used (rss): 464MB
- Time taken: 26.986
- Lines per second: 37,365,160/s
- Throughput: 515MB/s

# 4 threads
- CPU threads: 4
- Memory used (rss): 299MB
- Time taken: 42.573s
- Lines per second: 23,603,634/s
- Throughput: 325MB/s