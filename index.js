import pm2 from 'pm2';

pm2.connect((err) => {
  if (err) {
    console.error(err)
    process.exit(2)
  }

  const arr = ['csv-1'];

  for (let i = 0; i < arr.length; i += 1) {
    pm2.start({
      script: './worker.js',
      name: 'demo',
      args: [arr[i]]
    }, (err, apps) => {
        console.log(apps);
    });
  }
});
