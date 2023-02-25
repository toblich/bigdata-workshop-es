const fs = require('fs');
const path = require('path');
const express = require('express');

const filepath = path.join(__dirname, 'SMSSpamCollection'); // Downloaded from https://archive.ics.uci.edu/ml/datasets/sms+spam+collection
const pageSize = 500;
const dataset = fs.readFileSync(filepath, 'utf-8').split('\n');

app = express();

app.post('/reset', (req, res) => {
  console.log('Resetting offset');
  offset = 0;
  res.json({ offset });
});

app.get('/', (req, res) => {
  const response = smsPage();
  res.json(response);
  console.log(`${req.method} ${req.originalUrl} --> ${JSON.stringify(response)}`);
});

app.listen(3000, () => console.log('Listening on port 3000...'));

// ---

let offset = 0;

function smsPage() {
  const page = dataset.slice(offset, offset + pageSize);

  const data = page.filter((s) => s).map(asJson);
  const response = { offset, data };

  offset += data.length; //* Update global offset

  return response;
}

function asJson(line) {
  const [label, sms] = line.split('\t');
  return { label, sms };
}
