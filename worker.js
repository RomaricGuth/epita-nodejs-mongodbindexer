import {parseCsv} from './parser.js';

const args = process.argv.slice(2);
console.log(args);
const filename = args[0];
const indexes = JSON.parse(args[1]);
console.log(indexes);
const keys = JSON.parse(args[2]);
console.log(keys);

if (!filename || !indexes || !keys) {
  console.log('invalid args');
  process.exit(2);
}

await parseCsv(filename, indexes, keys);
console.log('end');
