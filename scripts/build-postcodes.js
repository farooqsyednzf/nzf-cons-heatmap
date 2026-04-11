#!/usr/bin/env node
/**
 * scripts/build-postcodes.js
 *
 * Downloads Australian postcode data and builds the compact JSON lookup file
 * used by netlify/functions/lib/postcodes.js.
 *
 * Run once (or when you want to refresh):
 *   node scripts/build-postcodes.js
 *
 * Output: netlify/functions/data/au-postcodes.json (~500KB, compressed ~150KB)
 *
 * Source: Matthew Proctor's AU postcode dataset (public domain)
 * https://www.matthewproctor.com/australian_postcodes
 */

'use strict';

const https  = require('https');
const path   = require('path');
const fs     = require('fs');

const DATA_URL  = 'https://www.matthewproctor.com/content/postcodes/australian_postcodes.csv';
const OUT_FILE  = path.join(__dirname, '../netlify/functions/data/au-postcodes.json');

// Ensure output directory exists
fs.mkdirSync(path.dirname(OUT_FILE), { recursive: true });

console.log('Downloading Australian postcode data...');
console.log(`Source: ${DATA_URL}`);

let csvData = '';

https.get(DATA_URL, (res) => {
  if (res.statusCode !== 200) {
    console.error(`HTTP ${res.statusCode} — download failed`);
    console.error('Alternative: manually download the CSV from the URL above');
    console.error('Save it as /tmp/au-postcodes.csv and re-run:');
    console.error('  node scripts/build-postcodes.js --local /tmp/au-postcodes.csv');
    process.exit(1);
  }

  res.on('data', chunk => { csvData += chunk; });
  res.on('end', () => {
    console.log(`Downloaded ${(csvData.length / 1024).toFixed(0)} KB`);
    buildJson(csvData);
  });
}).on('error', (err) => {
  console.error('Download error:', err.message);
  // Try local file argument
  const localArg = process.argv.indexOf('--local');
  if (localArg !== -1 && process.argv[localArg + 1]) {
    const localFile = process.argv[localArg + 1];
    console.log(`Falling back to local file: ${localFile}`);
    try {
      buildJson(fs.readFileSync(localFile, 'utf8'));
    } catch (e) {
      console.error('Could not read local file:', e.message);
      process.exit(1);
    }
  } else {
    process.exit(1);
  }
});

function buildJson(csv) {
  const lines = csv.trim().split('\n');
  const header = lines[0].split(',').map(h => h.replace(/"/g, '').trim().toLowerCase());

  // Find column indices
  const col = name => header.indexOf(name);
  const idxPc     = col('postcode');
  const idxLoc    = col('locality'); // suburb/locality name
  const idxState  = col('state');
  const idxLat    = col('lat');
  const idxLng    = col('long');   // 'long' in Matthew Proctor's dataset

  if ([idxPc, idxLoc, idxState, idxLat, idxLng].some(i => i === -1)) {
    console.error('Unexpected CSV headers:', header);
    console.error('Expected: postcode, locality, state, lat, long');
    process.exit(1);
  }

  // Parse rows into compact array: [postcode, suburb, state, lat, lng]
  const seenPc   = new Set();
  const entries  = [];
  let skipped    = 0;

  for (let i = 1; i < lines.length; i++) {
    const row = parsecsv(lines[i]);
    if (!row || row.length < Math.max(idxPc, idxLoc, idxState, idxLat, idxLng) + 1) continue;

    const pc    = (row[idxPc]  || '').replace(/"/g, '').trim().padStart(4, '0');
    const loc   = (row[idxLoc] || '').replace(/"/g, '').trim().toUpperCase();
    const state = (row[idxState] || '').replace(/"/g, '').trim().toUpperCase();
    const lat   = parseFloat(row[idxLat]);
    const lng   = parseFloat(row[idxLng]);

    if (!pc || !/^\d{4}$/.test(pc) || !loc || !state || isNaN(lat) || isNaN(lng)) {
      skipped++;
      continue;
    }

    // AU state whitelist
    if (!['NSW', 'VIC', 'QLD', 'WA', 'SA', 'TAS', 'ACT', 'NT'].includes(state)) {
      skipped++;
      continue;
    }

    entries.push([pc, loc, state, parseFloat(lat.toFixed(6)), parseFloat(lng.toFixed(6))]);
  }

  // Sort by postcode, primary suburb first (keep all entries for reverse suburb lookup)
  entries.sort((a, b) => a[0].localeCompare(b[0]) || a[1].localeCompare(b[1]));

  const json = JSON.stringify(entries);
  fs.writeFileSync(OUT_FILE, json, 'utf8');

  const stats = {
    total:   entries.length,
    skipped,
    unique:  new Set(entries.map(e => e[0])).size,
    size:    `${(json.length / 1024).toFixed(0)} KB`,
  };

  console.log(`\n✅ Built postcode data:`);
  console.log(`   Total entries:    ${stats.total.toLocaleString()}`);
  console.log(`   Unique postcodes: ${stats.unique.toLocaleString()}`);
  console.log(`   Skipped rows:     ${stats.skipped.toLocaleString()}`);
  console.log(`   Output size:      ${stats.size}`);
  console.log(`   Saved to: ${OUT_FILE}`);
}

// Simple CSV row parser (handles quoted fields with commas)
function parsecsv(line) {
  const result = [];
  let cur = '';
  let inQuotes = false;
  for (const ch of line) {
    if (ch === '"') {
      inQuotes = !inQuotes;
    } else if (ch === ',' && !inQuotes) {
      result.push(cur);
      cur = '';
    } else {
      cur += ch;
    }
  }
  result.push(cur);
  return result;
}
