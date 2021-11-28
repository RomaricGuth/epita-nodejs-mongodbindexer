import pm2 from 'pm2';

import {open, mkdir} from 'fs/promises';
import {createReadStream, createWriteStream} from 'fs';
import { stdin as input, stdout as output } from 'process';
import * as readline from 'readline';
import {splitCsvLine} from './csv_utils.js';
import {parseCsv} from './parser.js';

const nbfiles = 5334;
const nbthread = 4;

let t1 = Date.now();
let total_time = 0;

const filehandle = await open('./csv/header.csv');
const buf = await filehandle.readFile();
filehandle.close();

const fields =  splitCsvLine(buf); // split fields from header
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
]; // fields that must be inserted in db

const indexes = needed.map((key) => fields.indexOf(key)); // get their indexes in header

pm2.connect((err) => {
  if (err) {
    console.error(err)
    process.exit(2)
  }

  const files_per_thread = Math.floor(nbfiles / nbthread);
  const remain = nbfiles - nbthread * files_per_thread; // files to add to first threads when division

  const filenames = new Array(nbfiles);
  for (let i = 0; i < nbfiles; i++) {
    filenames[i] = `./csv/csv-${i.toString().padStart(4, '0')}.csv`; // build array of every filenames
  }

  for (let i = 0; i < nbthread; i++) { // start threads
    const add_one_file = i < remain; // add one file to first threads if we need to
    const first_file_index = i * files_per_thread + (add_one_file ? i : remain);
    const arg_filenames = filenames.slice(first_file_index, first_file_index + files_per_thread + add_one_file); // array of files to parse
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

const rl = readline.createInterface({ input, output }); // for user prompts
let total_inserts = 0; // total number of documents inserted
let workers_done = 0; // number of worker that are done

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

        if (workers_done == nbthread) { // stop when each worker is done
          const delta = Date.now() - t1;
          total_time += delta;
          const insert_per_sec = total_inserts / (total_time / 1000);
          console.log(`Total: ${total_inserts} insertions in ${total_time}ms !`);
          console.log(`Average: ${insert_per_sec} insertions / seconds !`);
          rl.close();
          pm2.disconnect();
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

function sendCommandToWorkers(command) { // get list of apps with pm2 and send command to each
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
rl.on('line', (input) => {
  switch (input) {
    case 'pause':
      if (pause) {
        console.log('insertion already paused');
      } else {
        const delta = Date.now() - t1;
        total_time += delta; // save time elapsed since last start and stop timer
        pauseWorkers();
        pause = true;
        console.log('insertion paused - enter resume to resume the process');
      }
      break;

    case 'resume':
      if (pause) {
        t1 = Date.now(); // restart timer
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
