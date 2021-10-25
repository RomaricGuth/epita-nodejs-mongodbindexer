import {open, mkdir} from 'fs/promises';
import {createReadStream, createWriteStream} from 'fs';
import {Buffer} from 'buffer';

const t1 = new Date().getTime();

const path = './StockEtablissement_utf8.csv';
const endline = '\n'.charCodeAt(0);
const separator = ','.charCodeAt(0);
const fileMaxLen = 1024 * 1024 * 64; // 64 MB
const bufferLen = 1024 * 64; // 64 KB

const filehandle = await open(path);
const buf = Buffer.allocUnsafe(bufferLen);
/*
const rs = createReadStream(path);
const ws = (await open('header.csv', 'w')).createWriteStream();
rs.pipe(ws)
*/

let headerLen = 0;
for (; headerLen === 0 || buf[headerLen - 1] !== endline; headerLen++) {
  /*
  await filehandle.read({
    buffer: buf,
    offset: headerLen,
    position: headerLen,
    length: 1,
  });

   */
  await filehandle.read(buf, headerLen, 1);
}

try {
  await mkdir('csv');
} catch (e) {
  if (e.code !== 'EEXIST') {
    throw e;
  }
}

let out = await open('csv/header.csv', 'w');
await out.write(buf, 0, headerLen);
out.close();


const batchPerFile = Math.floor(fileMaxLen / bufferLen);
let currentBatch = 1;
let nbfiles = 0;
let fileopen = false;
let remainBytes = 0;
let bytesRead = 0;
while ((bytesRead = (await filehandle.read(buf, remainBytes, bufferLen - remainBytes)).bytesRead) !== 0 || remainBytes !== 0) {
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
    while (buf[index] !== endline) {
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

console.log(`splitted file in ${nbfiles} in ${new Date().getTime() - t1}ms`);
