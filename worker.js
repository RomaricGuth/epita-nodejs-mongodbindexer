import {parseCsv} from './parser.js';

const args = process.argv.slice(2);
const filenames = JSON.parse(args[0]);
const indexes = JSON.parse(args[1]);
const keys = JSON.parse(args[2]);

if (!filenames || !indexes || !keys) {
  console.log('invalid args');
  process.exit(2);
}

for (let filename of filenames) {
  console.log('parse ' + filename);
  await parseCsv(filename, indexes, keys);
}
