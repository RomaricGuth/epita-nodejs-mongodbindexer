import {parseCsv} from './parser.js';

function sendDataToMaster(packet) {
  process.send({
    type: 'process:msg',
    data: packet,
  });
}

const t1 = Date.now();

const args = process.argv.slice(2);
const filenames = JSON.parse(args[0]);
const indexes = JSON.parse(args[1]);
const keys = JSON.parse(args[2]);

if (!filenames || !indexes || !keys) {
  sendDataToMaster({cmd: 'error', reason: 'invalid args'});
  process.exit(2);
}


let total_collections = 0;
for (let filename of filenames) {
  const t2 = Date.now()
  const nb_collections = await parseCsv(filename, indexes, keys);
  const delta = Date.now() - t2;
  sendDataToMaster({cmd: 'file_done', filename: filename, nb_inserts: nb_collections, time_ms: delta});
  total_collections += nb_collections;
}

const delta = Date.now() - t1;
const insert_per_sec = total_collections / delta;
sendDataToMaster({cmd: 'worker_done', nb_inserts: total_collections, time_ms: delta});
