import pm2 from 'pm2';

import {open, mkdir} from 'fs/promises';
import {createReadStream, createWriteStream} from 'fs';
import { stdin as input, stdout as output } from 'process';
import * as readline from 'readline';
import {splitCsvLineWithIndexes} from './csv_utils.js';
import {parseCsv} from './parser.js';

const nbfiles = 5334;
const nbthread = 4;

const t1 = Date.now();

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
    pm2.start({
      script: './worker.js',
      name: 'worker' + i,
      autorestart: false,
      args: args,
    }, (err, apps) => {
      //pm2.disconnect();
    });
  }
});

let total_inserts = 0;
let workers_done = 0;

// listen to workers messages
pm2.launchBus((err, pm2_bus) => {
  pm2_bus.on('process:msg', function(packet) {
    const {process, data} = packet;
    const {cmd, nb_inserts, time_ms, filename, reason} = data;
    const {name} = process;


    switch (cmd) {
      case 'file_done':
        console.log(`file ${filename}: ${nb_inserts} insertions in ${time_ms}ms`);
        break;

      case 'worker_done':
        console.log(`${name}: ${nb_inserts} insertions in ${time_ms}ms`);
        total_inserts += nb_inserts;
        workers_done++;

        if (workers_done == nbthread) {
          const total_time = Date.now() - t1;
          const insert_per_sec = total_inserts / (total_time / 1000);
          console.log(`Total: ${total_inserts} insertions in ${total_time}ms !`);
          console.log(`Average: ${insert_per_sec} insertions / seconds !`);
          pm2.disconnect()
        }
        break;

     case 'error':
        console.log(`${name} error: ${reason}`);
        break;

      default:
        console.log(`${name}: unknown command: ${packet}`);
        break;
    }
  });
});

function sendCommandToWorker(command, id) {
  pm2.sendDataToProcessId({
    id: id,
    type: 'process:msg',
    data : {
      cmd: command,
    },
    topic: true,
  }, () => {});
}

function sendCommandToWorkers(command) {
  pm2.list((err, apps) => {
    apps.forEach((app) => sendCommandToWorker(command, app.pm_id));
  });
}

function pauseWorkers() {
  sendCommandToWorkers('pause');
}

function resumeWorkers() {
  sendCommandToWorkers('resume');
}

let pause = false;
// prompt user for commands
const rl = readline.createInterface({ input, output });
rl.on('line', (input) => {
  switch (input) {
    case 'pause':
      if (pause) {
        console.log('insertion already paused');
      } else {
        pauseWorkers();
        pause = true;
        console.log('insertion paused - enter resume to resume the process');
      }
      break;

    case 'resume':
      if (pause) {
        resumeWorkers();
        pause = false;
        console.log('insertion resumed !');
      } else {
        console.log('insertion not paused');
      }
      break;

    default:
      console.log('unknown command');
      break;
  }
});
