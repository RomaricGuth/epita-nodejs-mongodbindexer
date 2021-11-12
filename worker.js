import {parseCsv} from './parser.js';

const args = process.argv.slice(2);
const filename = args[0];
const indexes = args[1];
const keys = args[2];

if (!filename || !indexes || !keys) {
  console.log('invalid args');
  process.exit(2);
}

await parseCsv(filename, indexes, keys);
