import {open} from 'fs/promises';
import {Buffer} from 'buffer';

const endline = '\n'.charCodeAt(0);
const separator = ','.charCodeAt(0);
const bufferLen = 1024 * 64; // 64 KB

export const parseCsv = async (path, indexes, keys) => {
  const filehandle = await open(path);
  const buf = Buffer.allocUnsafe(bufferLen);
  let bytesRead;

  let res = [];
  let entry = {};

  let nb_keeped_fields = 0; // number of fields stored from the line
  let nb_read_fields = 0; // number of fields read from the line
  let field = []; // current field in array of charcode
  while ((bytesRead = (await filehandle.read(buf, 0, bufferLen)).bytesRead) !== 0) {
    for (let i = 0; i < bytesRead; ++i) {
      const keep_field = nb_read_fields === indexes[nb_keeped_fields];
      if (buf[i] === separator || buf[i] === endline) {
        if (keep_field) {
          // field must be indexed - add in js object
          if (field[0] !== undefined) {
            const key = keys[nb_keeped_fields];
            entry[key] = String.fromCharCode(...field);
          }
          field = [];
          nb_keeped_fields++;
        }

        if (buf[i] === endline) {
          res.push(entry);
          entry = {};

          nb_read_fields = 0;
          nb_keeped_fields = 0;
        } else {
          nb_read_fields++;
        }
      } else if (keep_field) {
        field.push(buf[i]);
      }
    }
  }

  filehandle.close();
  console.log(res);
}
