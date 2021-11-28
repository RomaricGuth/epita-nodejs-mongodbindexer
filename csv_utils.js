const separator = ','.charCodeAt(0);
const endline = '\n'.charCodeAt(0);

// split fields to array
export const splitCsvLine = (buffer) => {
  let fields = [];
  let field = [];
  for (let i = 0; buffer[i] !== endline; ++i) {
    if (buffer[i] === separator) {
      fields.push(String.fromCharCode(...field));
      field = [];
    } else {
      field.push(buffer[i]);
    }
  }

  fields.push(String.fromCharCode(...field));

  return fields;
}
