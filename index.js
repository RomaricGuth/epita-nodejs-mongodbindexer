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

parseCsv('./csv/csv-0000.csv', indexes, needed);

/*
pm2.connect((err) => {
  if (err) {
    console.error(err)
    process.exit(2)
  }

  const nbfiles = 84;

  for (let i = 0; i < nbfiles; i += 1) {
    pm2.start({
      script: './worker.js',
      name: 'demo',
      args: i
    }, (err, apps) => {
        console.log(apps);
    });
  }
});
*/
