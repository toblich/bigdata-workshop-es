const fs = require('fs');
const path = require('path');
const express = require('express');

const filepath = path.join(__dirname, 'SMSSpamCollection');
const pageSize = 100;
const dataset = fs.readFileSync(filepath, 'utf-8').split('\n');

app = express();

app.get('*', (req, res) => res.json(smsPage()));

app.listen(3000, () => console.log('Listening on port 3000...'));

// ---

let offset = 0;

function smsPage() {
  const page = dataset.slice(offset, offset + pageSize);

  const data = page.filter(s => s).map(asJson);
  const response = { offset, data };

  offset += data.length; //* Update global offset

  return response;
}

function asJson(line) {
  const [label, sms] = line.split('\t');
  return { label, sms };
}
