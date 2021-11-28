const separator = ','.charCodeAt(0);
const endline = '\n'.charCodeAt(0);

export const splitCsvLineWithIndexes = (buffer, indexes) => {
  let field_number = 0;
  let next_index = indexes[0];
  let current_index = 0;
  let fields = [];
  let field = [];
  for (let i = 0; buffer[i] !== endline; ++i) {
    if (buffer[i] === separator) {
      if (current_index === next_index) {
        fields.push(String.fromCharCode(...field));
        field = [];
        field_number++;
        next_index = indexes[field_number];
        if (next_index === undefined) {
          return fields;
        }
      }
      current_index++;
    } else if (current_index === next_index) {
      field.push(buffer[i]);
    }
  }

  fields.push(String.fromCharCode(...field));

  return fields;
}
