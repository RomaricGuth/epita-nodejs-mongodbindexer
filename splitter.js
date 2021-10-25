import {open} from 'fs/promises';
import {createReadStream, createWriteStream} from 'fs';
import {Buffer} from 'buffer';

const path = './StockEtablissement_utf8.csv';
const endline = '\n'.charCodeAt(0);
const separator = ','.charCodeAt(0);

const filehandle = await open(path);
const buf = Buffer.allocUnsafe(1024 * 64);
//const rs = createReadStream(path);

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
  await filehandle.read(buf, headerLen, 1, headerLen);
}

const out = await open('header.csv', 'w');
await out.write(buf, 0, headerLen);

/*
const ws = (await open('header.csv', 'w')).createWriteStream();
rs.pipe(ws)

let buf;
*/
