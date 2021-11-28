import {open, mkdir} from 'fs/promises';
import {createReadStream, createWriteStream} from 'fs';
import {Buffer} from 'buffer';

const t1 = Date.now();

const path = './StockEtablissement_utf8.csv';
const endline = '\n'.charCodeAt(0);
const separator = ','.charCodeAt(0);
const fileMaxLen = 1024 * 1024; // 1 MB
const bufferLen = 1024 * 64; // 64 KB

const filehandle = await open(path);
const buf = Buffer.allocUnsafe(bufferLen);

let headerLen = 0;
// first read header line
for (; headerLen === 0 || buf[headerLen - 1] !== endline; headerLen++) {
  await filehandle.read(buf, headerLen, 1);
}

// create dir if needed
try {
  await mkdir('csv');
} catch (e) {
  if (e.code !== 'EEXIST') {
    throw e;
  }
}

// write header
let out = await open('csv/header.csv', 'w');
await out.write(buf, 0, headerLen);
out.close();


const batchPerFile = Math.floor(fileMaxLen / bufferLen); // number of read needed for one file
let currentBatch = 1;
let nbfiles = 0;
let fileopen = false;
let remainBytes = 0; // used when reaching end of one file
let bytesRead = 0;
while ((bytesRead = (await filehandle.read(buf, remainBytes, bufferLen - remainBytes)).bytesRead) !== 0 || remainBytes !== 0) { // read until eof, check if we have some remain bytes in buffer
  if (!fileopen) {
    out = await open(`csv/csv-${(nbfiles++).toString().padStart(4, '0')}.csv`, 'w');
    fileopen = true;
  }
  if (currentBatch !== batchPerFile) {
    await out.write(buf, 0, bytesRead + remainBytes);
    currentBatch++;
    remainBytes = 0;
  } else {
    // end file after last line
    let index = bufferLen - 1;
    while (buf[index] !== endline) { // truncate to the last endline
      index--;
    }
    await out.write(buf, 0, index + 1);
    out.close();
    fileopen = false;

    // move remainder at start of the buffer
    if (index !== bufferLen - 1) {
      remainBytes = buf.copy(buf, 0, index + 1);
    }
    currentBatch = 1;
  }
}

console.log(`splitted file in ${nbfiles} in ${Date.now() - t1}ms`);
