import {parseCsv} from './parser.js';
import {MongoClient} from 'mongodb';

function sendDataToMaster(packet) {
  process.send({
    type: 'process:msg',
    data: packet,
  });
}

function sleep(timeout_ms) {
  return new Promise((resolve) => setTimeout(resolve, timeout_ms));
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

let pause = false;

process.on('message', function({data}) {
  const {cmd} = data;
  switch (cmd) {
    case 'pause':
      pause = true;
      break;

    case 'resume':
      pause = false;
      break;

    default:
      sendDataToMaster({cmd: 'error', reason: 'unknown command ' + cmd});
      sendDataToMaster({cmd: 'error', reason: packet});
      break;
  }
});

const client = new MongoClient('mongodb://127.0.0.1:27017/');
await client.connect();

let total_collections = 0;
for (let filename of filenames) {
  while (pause) {
    await sleep(500);
  }
  const t2 = Date.now()
  const nb_collections = await parseCsv(filename, indexes, keys, client);
  const delta = Date.now() - t2;
  sendDataToMaster({cmd: 'file_done', filename: filename, nb_inserts: nb_collections, time_ms: delta});
  total_collections += nb_collections;
}

await client.close();

const delta = Date.now() - t1;
sendDataToMaster({cmd: 'worker_done', nb_inserts: total_collections, time_ms: delta});
