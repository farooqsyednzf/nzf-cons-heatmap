'use strict';

/**
 * scripts/refresh-map-data.js
 *
 * NZF Community Map — daily data pipeline.
 * Runs via GitHub Actions at 7:00 AM AEST (21:00 UTC) every day.
 *
 * Fetches from Zoho Analytics + CRM, aggregates, and writes to Netlify Blobs.
 * Designed to run as a plain Node.js script — no Netlify function context needed.
 *
 * Required environment variables (set as GitHub Secrets):
 *   ZOHO_CLIENT_ID, ZOHO_CLIENT_SECRET, ZOHO_REFRESH_TOKEN
 *   ZOHO_ORG_ID, ZOHO_WS_ID
 *   NETLIFY_SITE_ID, NETLIFY_BLOBS_TOKEN
 *
 * Optional:
 *   ANTHROPIC_API_KEY       — enables AI summaries (omit to use fallback summaries)
 *   MAX_SUMMARIES_PER_RUN   — max new AI summaries to generate per run (default: 50)
 */

const path = require('path');

// ─── ENVIRONMENT VALIDATION ───────────────────────────────────────────────────
const required = [
  'ZOHO_CLIENT_ID', 'ZOHO_CLIENT_SECRET', 'ZOHO_REFRESH_TOKEN',
  'ZOHO_ORG_ID', 'ZOHO_WS_ID',
  'NETLIFY_SITE_ID', 'NETLIFY_BLOBS_TOKEN',
];
const missing = required.filter(k => !process.env[k]);
if (missing.length) {
  console.error(`[refresh] Missing required environment variables: ${missing.join(', ')}`);
  process.exit(1);
}

const ENV = {
  ZOHO_CLIENT_ID:     process.env.ZOHO_CLIENT_ID,
  ZOHO_CLIENT_SECRET: process.env.ZOHO_CLIENT_SECRET,
  ZOHO_REFRESH_TOKEN: process.env.ZOHO_REFRESH_TOKEN,
  ZOHO_ORG_ID:        process.env.ZOHO_ORG_ID,
  ZOHO_WS_ID:         process.env.ZOHO_WS_ID,
  NETLIFY_SITE_ID:    process.env.NETLIFY_SITE_ID,
  NETLIFY_BLOBS_TOKEN: process.env.NETLIFY_BLOBS_TOKEN,
  ANTHROPIC_API_KEY:  process.env.ANTHROPIC_API_KEY || null,
  MAX_SUMMARIES:      parseInt(process.env.MAX_SUMMARIES_PER_RUN || '50', 10),
};

// ─── CONSTANTS ────────────────────────────────────────────────────────────────
const ZOHO_TOKEN_URL      = 'https://accounts.zoho.com/oauth/v2/token';
const ZOHO_ANALYTICS_BASE = 'https://analyticsapi.zoho.com/restapi/v2';
const ZOHO_CRM_BASE       = 'https://www.zohoapis.com/crm/v2';

const VIEW_CASES         = '1715382000001002494';
const VIEW_CLIENTS       = '1715382000001002492';
const VIEW_DONATIONS     = '1715382000006560082';
const VIEW_DISTRIBUTIONS = '1715382000001002628';

const BLOB_API    = 'https://api.netlify.com/api/v1/blobs';
const STORE_PATH  = 'site:nzf-map';
const BLOB_OUTPUT = 'aggregated-v2';
const BLOB_STATE  = 'cases-state-v1';

// ─── IMPORT LOCAL LIB ─────────────────────────────────────────────────────────
// Reuse the same transform and postcode helpers as the Netlify function
const {
  mapStageToStatus, hasDvContent,
  extractTags, hashText,
  generateSummary, buildFallbackSummary,
  sanitiseSummary,
} = require(path.join(__dirname, '../netlify/functions/lib/transforms'));

const { resolveLocation, lookupPostcode } = require(
  path.join(__dirname, '../netlify/functions/lib/postcodes')
);

// ─── MAIN ─────────────────────────────────────────────────────────────────────
let _zohoToken = null;

async function main() {
  const start = Date.now();
  console.log('[refresh] Starting daily refresh');
  console.log(`[refresh] Time: ${new Date().toISOString()}`);

  // 1. Zoho access token
  _zohoToken = await refreshZohoToken();
  console.log('[refresh] Zoho access token obtained');

  // 2a. Cases + Clients + DV flags + cached state — all in parallel
  console.log('[refresh] Fetching cases, clients, DV notes, cached state...');
  const [[caseRows, clientRows], dvNoteIds, casesState] = await Promise.all([
    Promise.all([fetchViewRows(VIEW_CASES), fetchViewRows(VIEW_CLIENTS)]),
    fetchDvCaseIdsFromCrm(),
    loadBlobJson(BLOB_STATE).then(v => v || {}),
  ]);
  console.log(`[refresh] ${caseRows.length} case rows, ${clientRows.length} client rows | DV flags: ${dvNoteIds.size} | cached: ${Object.keys(casesState).length}`);

  const clientMap    = buildClientMap(clientRows);
  const caseToClient = buildCaseToClientMap(caseRows);
  const rawCases     = buildRawCases(caseRows, clientMap);
  caseRows.length    = 0;
  clientRows.length  = 0;
  console.log(`[refresh] ${rawCases.length} cases after date filter`);

  // 2b. Donations + Distributions in parallel
  console.log('[refresh] Fetching donations + distributions...');
  const [donCsv, disCsv] = await Promise.all([
    fetchViewCsv(VIEW_DONATIONS),
    fetchViewCsv(VIEW_DISTRIBUTIONS),
  ]);
  const donations     = aggregateDonationsFromCsv(donCsv);
  const distributions = aggregateDistributionsFromCsv(disCsv, caseToClient, clientMap);
  console.log(`[refresh] ${Object.keys(donations).length} donation postcodes, ${Object.keys(distributions).length} distribution postcodes`);

  // 3. Process cases
  console.log('[refresh] Processing cases...');
  const { postcodes, updatedState, stats } = await processCases(rawCases, dvNoteIds, casesState);
  console.log(`[refresh] ${postcodes.length} postcodes | new: ${stats.newSummaries} | reused: ${stats.reused} | DV removed: ${stats.dvFiltered} | no location: ${stats.noLocation}`);

  // 4. Write to Netlify Blobs
  await saveBlobJson(BLOB_STATE, updatedState);

  const output = {
    generatedAt:   new Date().toISOString(),
    postcodes,
    donations,
    distributions,
  };

  await saveBlobJson(BLOB_OUTPUT, output);

  const elapsed = ((Date.now() - start) / 1000).toFixed(1);
  console.log(`[refresh] Done in ${elapsed}s`);
}

main().catch(err => {
  console.error('[refresh] Fatal error:', err.message);
  console.error(err.stack);
  process.exit(1);  // Non-zero exit causes GitHub Actions to mark the run as failed
});

// ─── ZOHO TOKEN ───────────────────────────────────────────────────────────────
async function refreshZohoToken() {
  const params = new URLSearchParams({
    grant_type:    'refresh_token',
    client_id:     ENV.ZOHO_CLIENT_ID,
    client_secret: ENV.ZOHO_CLIENT_SECRET,
    refresh_token: ENV.ZOHO_REFRESH_TOKEN,
  });

  const resp = await fetch(ZOHO_TOKEN_URL, {
    method:  'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body:    params.toString(),
  });

  if (!resp.ok) throw new Error(`Zoho token endpoint returned HTTP ${resp.status}`);
  const data = await resp.json();
  if (!data.access_token) throw new Error('Zoho token response missing access_token');
  return data.access_token;
}

// ─── ANALYTICS FETCH ─────────────────────────────────────────────────────────
async function fetchViewRows(viewId) {
  const cfg  = encodeURIComponent(JSON.stringify({ responseFormat: 'csv' }));
  const url  = `${ZOHO_ANALYTICS_BASE}/workspaces/${ENV.ZOHO_WS_ID}/views/${viewId}/data?CONFIG=${cfg}`;
  const resp = await fetch(url, {
    headers: {
      'Authorization':    `Zoho-oauthtoken ${_zohoToken}`,
      'ZANALYTICS-ORGID': ENV.ZOHO_ORG_ID,
    },
  });
  if (!resp.ok) {
    const body = await resp.text();
    throw new Error(`Analytics view ${viewId} HTTP ${resp.status}: ${body.slice(0, 200)}`);
  }
  return parseCsv(await resp.text());
}

async function fetchViewCsv(viewId) {
  const cfg  = encodeURIComponent(JSON.stringify({ responseFormat: 'csv' }));
  const url  = `${ZOHO_ANALYTICS_BASE}/workspaces/${ENV.ZOHO_WS_ID}/views/${viewId}/data?CONFIG=${cfg}`;
  const resp = await fetch(url, {
    headers: {
      'Authorization':    `Zoho-oauthtoken ${_zohoToken}`,
      'ZANALYTICS-ORGID': ENV.ZOHO_ORG_ID,
    },
  });
  if (!resp.ok) {
    const body = await resp.text();
    throw new Error(`Analytics view ${viewId} HTTP ${resp.status}: ${body.slice(0, 200)}`);
  }
  return resp.text();
}

// ─── CRM: DV CASE IDS ─────────────────────────────────────────────────────────
async function fetchDvCaseIdsFromCrm() {
  const dvIds = new Set();
  let page = 1;
  while (true) {
    const url  = `${ZOHO_CRM_BASE}/Notes?fields=Parent_Id&criteria=(Note_Content:contains:DV)&page=${page}&per_page=200`;
    const resp = await fetch(url, {
      headers: { 'Authorization': `Zoho-oauthtoken ${_zohoToken}` },
    });
    if (!resp.ok) break;
    const data = await resp.json();
    const records = data.data || [];
    records.forEach(r => { if (r.Parent_Id?.id) dvIds.add(r.Parent_Id.id); });
    if (!data.info?.more_records) break;
    page++;
  }
  return dvIds;
}

// ─── CRM: CLIENT + CASE MAPS ─────────────────────────────────────────────────
function buildClientMap(clientRows) {
  const map = {};
  for (const r of clientRows) {
    if (!r.id) continue;
    map[r.id] = {
      postcode: r.mailing_zip || r.postcode || r.post_code || '',
      suburb:   r.mailing_city || r.suburb || '',
      state:    r.mailing_state || r.state || '',
    };
  }
  return map;
}

function buildCaseToClientMap(caseRows) {
  const map = {};
  for (const r of caseRows) {
    if (r.id && r.contact_name) map[r.id] = r.contact_name;
  }
  return map;
}

function buildRawCases(caseRows, clientMap) {
  const CUTOFF = new Date('2025-01-01').getTime();
  return caseRows.filter(r => {
    if (!r.created_date) return false;
    const d = new Date(r.created_date);
    return !isNaN(d) && d.getTime() >= CUTOFF;
  }).map(r => {
    const client = clientMap[r.contact_name] || {};
    return {
      id:           r.id,
      stage:        r.stage || r.case_stage || '',
      description:  r.description || '',
      dv_flag:      r.dv_flag || 'false',
      postcode:     r.postcode || r.post_code || client.postcode || '',
      suburb:       r.suburb   || client.suburb || '',
      state:        r.state    || client.state  || '',
      created_date: r.created_date,
    };
  });
}

// ─── AGGREGATE: DONATIONS ─────────────────────────────────────────────────────
function aggregateDonationsFromCsv(csv) {
  const lines = (csv || '').replace(/^\uFEFF/, '').replace(/\r\n/g, '\n').replace(/\r/g, '\n').trim().split('\n');
  if (lines.length < 2) return {};

  const headers = splitCsvLine(lines[0]).map(h => h.replace(/"/g, '').trim().toLowerCase().replace(/[\s.()-]+/g, '_'));
  const pcIdx   = headers.indexOf('post_code');
  const amtIdx  = headers.indexOf('amount');
  const statIdx = headers.indexOf('status');
  const dateIdx = headers.indexOf('donation_timestamp');

  if (pcIdx === -1 || amtIdx === -1 || statIdx === -1) {
    console.warn('[refresh] Donations CSV missing expected columns:', headers.slice(0, 15).join(', '));
    return {};
  }

  const CUTOFF = new Date('2025-01-01').getTime();
  const out = {};
  for (let i = 1; i < lines.length; i++) {
    const vals   = splitCsvLine(lines[i]);
    const status = (vals[statIdx] || '').replace(/"/g, '').trim();
    if (status !== 'Completed') continue;

    let donDateMs = null;
    if (dateIdx !== -1) {
      const raw   = (vals[dateIdx] || '').replace(/"/g, '').trim();
      const parts = raw.split(/[\/\s:]/);
      if (parts.length >= 3) {
        const d = new Date(parts[2], parts[1] - 1, parts[0]);
        if (!isNaN(d)) {
          if (d.getTime() < CUTOFF) continue;
          donDateMs = d.getTime();
        }
      }
    }

    const pc    = (vals[pcIdx]  || '').replace(/"/g, '').trim();
    const total = parseFloat((vals[amtIdx] || '').replace(/[^0-9.]/g, '')) || 0;
    if (!pc || !/^\d{4}$/.test(pc) || total <= 0) continue;

    if (!out[pc]) {
      const geo = lookupPostcode(pc);
      out[pc] = { count: 0, total: 0, items: [], suburb: geo?.suburb || null, state: geo?.state || null, lat: geo?.lat || null, lng: geo?.lng || null };
    }
    out[pc].count++;
    out[pc].total = Math.round((out[pc].total + total) * 100) / 100;
    out[pc].items.push({ amount: Math.round(total * 100) / 100, dateMs: donDateMs });
  }
  Object.values(out).forEach(e => e.items.sort((a, b) => (b.dateMs || 0) - (a.dateMs || 0)));
  return out;
}

// ─── AGGREGATE: DISTRIBUTIONS ─────────────────────────────────────────────────
function aggregateDistributionsFromCsv(csv, caseToClient, clientMap) {
  const lines = (csv || '').replace(/^\uFEFF/, '').replace(/\r\n/g, '\n').replace(/\r/g, '\n').trim().split('\n');
  if (lines.length < 2) return {};

  const headers = splitCsvLine(lines[0]).map(h => h.replace(/"/g, '').trim().toLowerCase().replace(/[\s.()-]+/g, '_'));
  const statIdx = headers.indexOf('status');
  const caseIdx = headers.indexOf('case_name');
  const amtIdx  = headers.indexOf('grand_total');
  const dateIdx = headers.indexOf('created_time');
  const subjIdx = headers.indexOf('subject');

  if (statIdx === -1 || caseIdx === -1 || amtIdx === -1) {
    console.warn('[refresh] Distributions CSV missing expected columns:', headers.slice(0, 15).join(', '));
    return {};
  }

  const CUTOFF = new Date('2025-01-01').getTime();
  const out = {};
  for (let i = 1; i < lines.length; i++) {
    const vals   = splitCsvLine(lines[i]);
    const status = (vals[statIdx] || '').replace(/"/g, '').trim();
    if (status !== 'Paid' && status !== 'Extracted') continue;

    let disDateMs = null;
    if (dateIdx !== -1) {
      const raw = (vals[dateIdx] || '').replace(/"/g, '').trim();
      const d   = new Date(raw);
      if (!isNaN(d)) {
        if (d.getTime() < CUTOFF) continue;
        disDateMs = d.getTime();
      }
    }

    const caseId  = (vals[caseIdx] || '').replace(/"/g, '').trim();
    const amount  = parseFloat((vals[amtIdx] || '').replace(/[^0-9.]/g, '')) || 0;
    if (!caseId || amount <= 0) continue;

    const clientId = caseToClient[caseId];
    const client   = clientId ? clientMap[clientId] : null;
    if (!client?.postcode) continue;

    const pc = client.postcode;
    if (!/^\d{4}$/.test(pc)) continue;

    const subject = subjIdx !== -1 ? (vals[subjIdx] || '').replace(/"/g, '').trim() : '';

    if (!out[pc]) {
      const geo = lookupPostcode(pc);
      out[pc] = { count: 0, total: 0, items: [], suburb: client.suburb || geo?.suburb || null, state: client.state || geo?.state || null, lat: geo?.lat || null, lng: geo?.lng || null };
    }
    out[pc].count++;
    out[pc].total = Math.round((out[pc].total + amount) * 100) / 100;
    out[pc].items.push({ amount: Math.round(amount * 100) / 100, subject, dateMs: disDateMs });
  }
  Object.values(out).forEach(e => e.items.sort((a, b) => (b.dateMs || 0) - (a.dateMs || 0)));
  return out;
}

// ─── CASES PROCESSING ─────────────────────────────────────────────────────────
async function processCases(rawCases, dvNoteIds, casesState) {
  const stats          = { newSummaries: 0, reused: 0, dvFiltered: 0, noLocation: 0 };
  const postcodeMap    = new Map();
  const updatedState   = {};
  let   summariesThisRun = 0;

  for (let ci = 0; ci < rawCases.length; ci++) {
    if (ci > 0 && ci % 500 === 0) {
      console.log(`[refresh] Processing cases... ${ci} / ${rawCases.length}`);
    }

    const row         = rawCases[ci];
    const caseId      = row.id;
    const description = row.description;

    const isDv = row.dv_flag === 'true' || hasDvContent(description) || dvNoteIds.has(caseId);
    if (isDv) { stats.dvFiltered++; continue; }

    const location = resolveLocation({ postcode: row.postcode, suburb: row.suburb, state: row.state });
    if (!location) { stats.noLocation++; continue; }

    const status  = mapStageToStatus(row.stage);
    let   dateMs  = null;
    if (row.created_date) {
      const d = new Date(row.created_date);
      if (!isNaN(d)) dateMs = d.getTime();
    }

    const srcHash  = hashText(description);
    const cached   = casesState[caseId];
    const needsNew = !cached || cached.srcHash !== srcHash;
    let   summary, tags;

    if (needsNew && summariesThisRun < ENV.MAX_SUMMARIES) {
      tags    = extractTags(description);
      summary = await generateSummary(description);
      if (!summary) summary = buildFallbackSummary(description, tags);
      summary = sanitiseSummary(summary);
      updatedState[caseId] = { summary, tags, srcHash };
      stats.newSummaries++;
      summariesThisRun++;
    } else if (cached) {
      summary = sanitiseSummary(cached.summary);
      tags    = cached.tags || [];
      updatedState[caseId] = { summary, tags, srcHash };
      if (!needsNew) stats.reused++;
      else stats.newSummaries++;
    } else {
      tags    = extractTags(description);
      summary = sanitiseSummary(buildFallbackSummary(null, tags));
      updatedState[caseId] = { summary, tags, srcHash };
      stats.newSummaries++;
    }

    const pc = location.postcode;
    if (!postcodeMap.has(pc)) {
      postcodeMap.set(pc, { pc, city: location.suburb, state: location.state, lat: location.lat, lng: location.lng, cases: [] });
    }
    postcodeMap.get(pc).cases.push({ s: summary, t: tags, st: status, dateMs });
  }

  postcodeMap.forEach(entry => entry.cases.sort((a, b) => (b.dateMs || 0) - (a.dateMs || 0)));
  return { postcodes: Array.from(postcodeMap.values()), updatedState, stats };
}

// ─── CSV HELPERS ──────────────────────────────────────────────────────────────
function parseCsv(csv) {
  const lines = (csv || '').replace(/^\uFEFF/, '').replace(/\r\n/g, '\n').replace(/\r/g, '\n').trim().split('\n');
  if (lines.length < 2) return [];
  const headers = lines[0].split(',').map(h => h.replace(/"/g, '').trim().toLowerCase().replace(/[\s.()-]+/g, '_'));
  return lines.slice(1).map(line => {
    const vals = splitCsvLine(line);
    const obj  = {};
    headers.forEach((h, i) => { obj[h] = (vals[i] || '').replace(/"/g, '').trim(); });
    return obj;
  });
}

function splitCsvLine(line) {
  const result = [];
  let cur = '', inQ = false;
  for (const ch of line) {
    if (ch === '"')          { inQ = !inQ; continue; }
    if (ch === ',' && !inQ) { result.push(cur); cur = ''; continue; }
    cur += ch;
  }
  result.push(cur);
  return result;
}

// ─── NETLIFY BLOBS (raw HTTP — no SDK needed) ─────────────────────────────────
async function loadBlobJson(key) {
  try {
    const url  = `${BLOB_API}/${ENV.NETLIFY_SITE_ID}/${STORE_PATH}/${key}`;
    const resp = await fetch(url, { headers: { 'Authorization': `Bearer ${ENV.NETLIFY_BLOBS_TOKEN}` } });
    if (!resp.ok) return null;
    const text = await resp.text();
    return text ? JSON.parse(text) : null;
  } catch (e) {
    console.warn(`[refresh] loadBlobJson(${key}): ${e.message}`);
    return null;
  }
}

async function saveBlobJson(key, data) {
  const json = JSON.stringify(data);
  const kb   = (Buffer.byteLength(json) / 1024).toFixed(0);
  console.log(`[refresh] Writing blob: ${key} (${kb} KB)`);

  const url  = `${BLOB_API}/${ENV.NETLIFY_SITE_ID}/${STORE_PATH}/${key}`;
  const resp = await fetch(url, {
    method:  'PUT',
    headers: {
      'Authorization': `Bearer ${ENV.NETLIFY_BLOBS_TOKEN}`,
      'Content-Type':  'application/json',
    },
    body: json,
  });

  if (!resp.ok) {
    const body = await resp.text();
    throw new Error(`Blob write failed for ${key}: HTTP ${resp.status} — ${body.slice(0, 200)}`);
  }
  console.log(`[refresh] Saved blob: ${key}`);
}
