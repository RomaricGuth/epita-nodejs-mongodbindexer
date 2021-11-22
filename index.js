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

  const nbfiles = 4;

  for (let i = 0; i < nbfiles; i += 1) {
    const filename = `./csv/csv-${i.toString().padStart(4, '0')}.csv`;
    pm2.start({
      script: './worker.js',
      name: 'demo',
      autorestart: false,
      args: [filename, JSON.stringify(indexes), JSON.stringify(needed)],
    }, (err, apps) => {
      pm2.disconnect();
    });
  }
});
