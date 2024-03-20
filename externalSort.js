const fs = require('fs');
const readline = require('readline');

async function splitFile(inputFilePath, maxMemory) {
    const fileStreams = [];
    let [stream, size, count] = [[], 0, 0];

    const rl = readline.createInterface({
        input: fs.createReadStream(inputFilePath),
        crlfDelay: Infinity,
    });

    for await (const line of rl) {
        size += Buffer.byteLength(line);
        if (size >= maxMemory) {
            const tempFilePath = `${inputFilePath}_${count++}`;
            fileStreams.push(tempFilePath);
            fs.writeFileSync(tempFilePath, stream.sort().join('\n') + '\n');
            [stream, size] = [[], 0];
        }
        stream.push(line);
    }

    if (stream.length > 0) {
        const tempFilePath = `${inputFilePath}_${count}`;
        fileStreams.push(tempFilePath);
        fs.writeFileSync(tempFilePath, stream.sort().join('\n') + '\n');
    }

    return fileStreams;
}

async function mergeFiles(inputFilePaths, outputFilePath) {
    const streams = inputFilePaths.map((path) =>
        readline
            .createInterface({
                input: fs.createReadStream(path),
                crlfDelay: Infinity,
            })
            [Symbol.asyncIterator]()
    );

    const outputs = fs.createWriteStream(outputFilePath);
    const minHeap = [];

    async function initializeHeap() {
        for (const [index, writer] of streams.entries()) {
            const { value, done } = await writer.next();
            if (!done) minHeap.push({ value: value.toString(), index });
        }
        minHeap.sort((a, b) => a.value.localeCompare(b.value));
    }

    async function merge() {
        if (minHeap.length === 0) {
            outputs.end();
            return;
        }
        const { value, index } = minHeap.shift();
        outputs.write(`${value}\n`);
        const { value: nextValue, done } = await streams[index].next();
        if (!done) {
            minHeap.push({ value: nextValue.toString(), index });
            minHeap.sort((a, b) => a.value.localeCompare(b.value));
        }
        await merge();
    }

    await initializeHeap();
    await merge();
}

async function sortLargeFile(inputFilePath, outputFilePath, maxMemory) {
    const tempFilePaths = await splitFile(inputFilePath, maxMemory);
    await mergeFiles(tempFilePaths, outputFilePath);

    await Promise.all(tempFilePaths.map((path) => fs.promises.unlink(path)));
}

const inputFilePath = 'input.txt';
const outputFilePath = 'output.txt';
const maxMemory = 500 * 1024 * 1024;

sortLargeFile(inputFilePath, outputFilePath, maxMemory)
    .then(() => console.log('Success'))
    .catch((err) => console.error('Error:', err));
