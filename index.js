import pm2 from 'pm2';

import {open, mkdir} from 'fs/promises';
import {createReadStream, createWriteStream} from 'fs';
import {splitCsvLine, splitCsvLineWithIndexes} from './csv_utils.js';
import {parseCsv} from './parser.js';

const filehandle = await open('./csv/header.csv');
const buf = await filehandle.readFile();
filehandle.close();

const fields =  splitCsvLine(buf);
const needed = [
  'siren',
  'nic',
  'siret',
  'dateCreationEtablissement',
  'dateDernierTraitementEtablissement',
  'typeVoieEtablissement',
  'libelleVoieEtablissement',
  'codePostalEtablissement',
  'libelleCommuneEtablissement',
  'codeCommuneEtablissement',
  'dateDebut',
  'etatAdministratifEtablissement'
];

const indexes = needed.map((key) => fields.indexOf(key));

pm2.connect((err) => {
  if (err) {
    console.error(err)
    process.exit(2)
  }

  const nbfiles = 84;
  const nbthread = 4;
  const files_per_thread = Math.floor(nbfiles / nbthread);
  const remain = nbfiles - nbthread * files_per_thread;

  const filenames = new Array(nbfiles);
  for (let i = 0; i < nbfiles; i++) {
    filenames[i] = `./csv/csv-${i.toString().padStart(4, '0')}.csv`;
  }

  for (let i = 0; i < nbthread; i++) {
    const add_one_file = i < remain;
    const first_file_index = i * files_per_thread + (add_one_file ? i : remain);
    const arg_filenames = filenames.slice(first_file_index, first_file_index + files_per_thread + add_one_file);
    const args = [JSON.stringify(arg_filenames), JSON.stringify(indexes), JSON.stringify(needed)];
    console.log(`worker ${i}: ${args}`);
    pm2.start({
      script: './worker.js',
      name: 'demo',
      autorestart: false,
      args: args,
    }, (err, apps) => {
      pm2.disconnect();
    });
  }
});
