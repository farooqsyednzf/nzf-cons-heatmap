'use strict';

/**
 * refresh-data.js - NZF Community Map daily data pipeline
 *
 * Runs at 4:00 AM AEST (18:00 UTC) every day.
 * Uses the Zoho Analytics non-bulk view data API (no bulk scopes needed).
 * Cases and Clients are fetched from separate tables and joined in JS.
 */

const { schedule }  = require('@netlify/functions');
const { getStore }  = require('@netlify/blobs');

// Helper: create Blobs store with explicit site config when available
function getBlobStore(name) {
  const siteID = process.env.NETLIFY_SITE_ID;
  const token  = process.env.NETLIFY_BLOBS_TOKEN || process.env.NETLIFY_TOKEN;
  if (siteID && token) {
    return getStore({ name, siteID, token });
  }
  return getStore(name); // auto-config from Netlify context
}
const {
  mapStageToStatus, hasDvContent,
  extractTags, hashText,
  generateSummary, buildFallbackSummary,
} = require('./lib/transforms');
const { resolveLocation, lookupPostcode } = require('./lib/postcodes');

// Environment
const ZOHO_CLIENT_ID      = process.env.ZOHO_CLIENT_ID;
const ZOHO_CLIENT_SECRET  = process.env.ZOHO_CLIENT_SECRET;
const ZOHO_REFRESH_TOKEN  = process.env.ZOHO_REFRESH_TOKEN;
const ZOHO_TOKEN_URL      = 'https://accounts.zoho.com/oauth/v2/token';
const ZOHO_ORG_ID         = process.env.ZOHO_ORG_ID;
const ZOHO_WS_ID          = process.env.ZOHO_WS_ID;
const ZOHO_ANALYTICS_BASE = 'https://analyticsapi.zoho.com/restapi/v2';
const ZOHO_CRM_BASE       = 'https://www.zohoapis.com/crm/v2';

// Zoho Analytics view IDs (base tables - confirmed working with non-bulk API)
const VIEW_CASES         = '1715382000001002494';
const VIEW_CLIENTS       = '1715382000001002492';
const VIEW_DONATIONS     = '1715382000006560082';
const VIEW_DISTRIBUTIONS = '1715382000001002628';

const BLOB_STORE  = 'nzf-map';
const BLOB_OUTPUT = 'aggregated-v2';
const BLOB_STATE  = 'cases-state-v1';
const MAX_SUMMARIES = parseInt(process.env.MAX_SUMMARIES_PER_RUN || '0');

let _zohoToken = null;

// ── HANDLER ──────────────────────────────────────────────────────────────────
exports.handler = schedule('0 18 * * *', async () => {
  const start = Date.now();
  console.log('[refresh] Starting daily refresh');

  try {
    _zohoToken = await refreshZohoToken();
    console.log('[refresh] Zoho access token obtained');

    console.log('[refresh] Fetching cases from Analytics...');
    const rawCases = await fetchCasesFromAnalytics();
    console.log(`[refresh] ${rawCases.length} cases fetched`);

    console.log('[refresh] Scanning CRM notes for DV keywords...');
    const dvNoteIds = await fetchDvCaseIdsFromCrm();
    console.log(`[refresh] ${dvNoteIds.size} cases flagged via notes DV scan`);

    const casesState = await loadBlobJson(BLOB_STATE) || {};
    console.log(`[refresh] ${Object.keys(casesState).length} cached case states loaded`);

    console.log('[refresh] Fetching donations + distributions...');
    const [donations, distributions] = await Promise.all([
      fetchDonationsFromAnalytics().catch(e => { console.warn('[refresh] Donations failed:', e.message); return {}; }),
      fetchDistributionsFromAnalytics().catch(e => { console.warn('[refresh] Distributions failed:', e.message); return {}; }),
    ]);
    console.log(`[refresh] ${Object.keys(donations).length} donation postcodes, ${Object.keys(distributions).length} distribution postcodes`);

    console.log('[refresh] Processing cases...');
    const { postcodes, updatedState, stats } = await processCases(rawCases, dvNoteIds, casesState);
    console.log(`[refresh] ${postcodes.length} postcodes | new: ${stats.newSummaries} | reused: ${stats.reused} | DV: ${stats.dvFiltered} | no location: ${stats.noLocation}`);

    await saveBlobJson(BLOB_STATE, updatedState);

    const output = { generatedAt: new Date().toISOString(), postcodes, donations, distributions };
    await saveBlobJson(BLOB_OUTPUT, output);

    console.log(`[refresh] Done in ${((Date.now() - start) / 1000).toFixed(1)}s`);
    return { statusCode: 200 };
  } catch (err) {
    console.error('[refresh] Fatal error:', err.message, '\n', err.stack);
    return { statusCode: 500 };
  }
});

// ── TOKEN REFRESH ─────────────────────────────────────────────────────────────
async function refreshZohoToken() {
  if (!ZOHO_CLIENT_ID || !ZOHO_CLIENT_SECRET || !ZOHO_REFRESH_TOKEN) {
    throw new Error('ZOHO_CLIENT_ID, ZOHO_CLIENT_SECRET and ZOHO_REFRESH_TOKEN must all be set');
  }
  const params = new URLSearchParams({
    grant_type:    'refresh_token',
    client_id:     ZOHO_CLIENT_ID,
    client_secret: ZOHO_CLIENT_SECRET,
    refresh_token: ZOHO_REFRESH_TOKEN,
  });
  const resp = await fetch(ZOHO_TOKEN_URL, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: params.toString(),
  });
  if (!resp.ok) throw new Error(`HTTP ${resp.status} from Zoho token endpoint`);
  const data = await resp.json();
  if (!data.access_token) throw new Error('No access_token returned: ' + JSON.stringify(data));
  return data.access_token;
}

// ── ANALYTICS VIEW DATA FETCH ─────────────────────────────────────────────────
// Non-bulk API: GET /workspaces/{wsId}/views/{viewId}/data
// Paginates automatically. Works with ZohoAnalytics.data.read scope.
async function fetchAllViewRows(viewId) {
  // The Zoho Analytics view data endpoint returns all rows in a single call.
  // Only responseFormat is accepted in CONFIG — no pagination parameters.
  const cfg  = encodeURIComponent(JSON.stringify({ responseFormat: 'csv' }));
  const url  = `${ZOHO_ANALYTICS_BASE}/workspaces/${ZOHO_WS_ID}/views/${viewId}/data?CONFIG=${cfg}`;
  const resp = await fetch(url, {
    headers: {
      'Authorization':    `Zoho-oauthtoken ${_zohoToken}`,
      'ZANALYTICS-ORGID': ZOHO_ORG_ID,
    },
  });

  if (!resp.ok) {
    const err = await resp.text();
    throw new Error(`Analytics view ${viewId} HTTP ${resp.status}: ${err.slice(0, 200)}`);
  }

  return parseCsv(await resp.text());
}

// ── CASES + CLIENTS ───────────────────────────────────────────────────────────
async function fetchCasesFromAnalytics() {
  const [caseRows, clientRows] = await Promise.all([
    fetchAllViewRows(VIEW_CASES),
    fetchAllViewRows(VIEW_CLIENTS),
  ]);

  // Build client lookup
  const clientMap = {};
  clientRows.forEach(c => {
    const id = (c.id || c['id'] || '').trim();
    if (!id) return;
    clientMap[id] = {
      postcode: (c.mailing_zip   || c['mailing_zip']   || '').trim(),
      suburb:   (c.mailing_city  || c['mailing_city']  || '').trim(),
      state:    (c.mailing_state || c['mailing_state'] || '').trim(),
      dv_flag:  (c.domestic_violence || c['domestic_violence'] || '').trim().toLowerCase(),
    };
  });

  const cutoff = new Date('2025-01-01').getTime();
  const merged = [];

  caseRows.forEach(row => {
    // Type filter
    const type = (row.internal_case_type || row.type || '').trim();
    if (type && type !== 'Zakat Receiver') return;

    // Date filter
    const rawDate = row.created_time || row['created_time'] || '';
    if (rawDate) {
      const d = new Date(rawDate);
      if (!isNaN(d) && d.getTime() < cutoff) return;
    }

    const clientId = (row.client_name || row['client_name'] || '').trim();
    const client   = clientMap[clientId] || {};

    merged.push({
      id:           (row.id || '').trim(),
      case_id:      (row.id || '').trim(),
      stage:        (row.stage || '').trim(),
      description:  (row.description || '').trim(),
      created_date: rawDate,
      postcode:     client.postcode || '',
      suburb:       client.suburb   || '',
      state:        client.state    || '',
      dv_flag:      client.dv_flag  || '',
    });
  });

  return merged;
}

// ── DONATIONS ─────────────────────────────────────────────────────────────────
async function fetchDonationsFromAnalytics() {
  const rows = await fetchAllViewRows(VIEW_DONATIONS);

  const out = {};
  rows.forEach(r => {
    if ((r.status || '').trim() !== 'Completed') return;
    const pc    = (r.post_code || '').trim();
    const total = parseFloat((r.amount || '').replace(/[^0-9.]/g, '')) || 0;
    if (!pc || !/^\d{4}$/.test(pc) || total <= 0) return;
    if (!out[pc]) {
      const geo = lookupPostcode(pc);
      out[pc] = { count: 0, total: 0, suburb: geo ? geo.suburb : null, state: geo ? geo.state : null };
    }
    out[pc].count++;
    out[pc].total += total;
  });

  return out;
}

// ── DISTRIBUTIONS ─────────────────────────────────────────────────────────────
// Distributions have no postcode directly. We chain: Distribution.Case Name ->
// Cases.Client Name -> Clients.Mailing Zip to resolve the postcode.
async function fetchDistributionsFromAnalytics() {
  const [caseRows, clientRows, distRows] = await Promise.all([
    fetchAllViewRows(VIEW_CASES),
    fetchAllViewRows(VIEW_CLIENTS),
    fetchAllViewRows(VIEW_DISTRIBUTIONS),
  ]);

  // Build lookups
  const clientMap = {};
  clientRows.forEach(c => {
    const id = (c.id || '').trim();
    if (id) clientMap[id] = {
      postcode: (c.mailing_zip   || '').trim(),
      suburb:   (c.mailing_city  || '').trim(),
      state:    (c.mailing_state || '').trim(),
    };
  });

  const caseToClient = {};
  caseRows.forEach(row => {
    const cid      = (row.id || '').trim();
    const clientId = (row.client_name || '').trim();
    if (cid && clientId) caseToClient[cid] = clientId;
  });

  const out = {};
  distRows.forEach(r => {
    const distStatus = (r.status || '').trim();
    if (distStatus !== 'Paid' && distStatus !== 'Extracted') return;
    const caseId = (r.case_name || '').trim();
    const amount = parseFloat((r.grand_total || '').replace(/[^0-9.]/g, '')) || 0;
    if (!caseId || amount <= 0) return;

    const clientId = caseToClient[caseId];
    if (!clientId) return;

    const client = clientMap[clientId];
    if (!client || !client.postcode) return;

    const pc = client.postcode;
    if (!/^\d{4}$/.test(pc)) return;

    if (!out[pc]) {
      const geo = lookupPostcode(pc);
      out[pc] = { count: 0, total: 0, suburb: client.suburb || (geo ? geo.suburb : null), state: client.state || (geo ? geo.state : null) };
    }
    out[pc].count++;
    out[pc].total += amount;
  });

  return out;
}

// ── DV NOTES SCAN ─────────────────────────────────────────────────────────────
const DV_NOTE_PHRASES = [
  'domestic violence', 'family violence', 'physical abuse', 'sexual abuse',
  'abusive partner', 'abusive husband', 'violent partner', 'violent husband',
  'restraining order', 'intervention order', 'apprehended violence',
  'safe house', 'fleeing violence', 'fled violence',
];

async function fetchDvCaseIdsFromCrm() {
  const dvIds = new Set();
  if (!_zohoToken) { console.error('[refresh] No token - Notes DV scan skipped'); return dvIds; }

  const coqlCriteria = DV_NOTE_PHRASES
    .map(kw => `Note_Content like '%${kw}%'`)
    .join(' OR ');
  const baseQuery = `SELECT Parent_Id FROM Notes WHERE ($se_module = 'Deals') AND (${coqlCriteria})`;

  let offset = 0, more = true;
  while (more) {
    try {
      const resp = await fetch(`${ZOHO_CRM_BASE}/coql`, {
        method:  'POST',
        headers: { 'Authorization': `Zoho-oauthtoken ${_zohoToken}`, 'Content-Type': 'application/json' },
        body:    JSON.stringify({ select_query: `${baseQuery} LIMIT 200 OFFSET ${offset}` }),
      });
      if (resp.status === 204 || resp.status === 404) break;
      if (!resp.ok) { console.warn('[refresh] Notes COQL HTTP', resp.status); break; }
      const data = await resp.json();
      (data.data || []).forEach(row => {
        const pid = (row.Parent_Id && row.Parent_Id.id) ? row.Parent_Id.id : row.Parent_Id;
        if (pid) dvIds.add(String(pid));
      });
      more    = data.info && data.info.more_records === true;
      offset += 200;
    } catch (e) { console.warn('[refresh] Notes page failed:', e.message); break; }
  }
  return dvIds;
}

// ── CASES PROCESSING ──────────────────────────────────────────────────────────
async function processCases(rawCases, dvNoteIds, casesState) {
  const stats = { newSummaries: 0, reused: 0, dvFiltered: 0, noLocation: 0 };
  const postcodeMap  = new Map();
  const updatedState = {};
  let summariesThisRun = 0;

  for (const row of rawCases) {
    const caseId      = String(row.case_id || row.id || '').trim();
    const description = String(row.description || '').trim();
    const stage       = String(row.stage || '').trim();
    const rawDate     = row.created_date || '';

    // DV filter
    const isDv = row.dv_flag === 'true'
      || hasDvContent(description)
      || dvNoteIds.has(String(row.id || ''));
    if (isDv) { stats.dvFiltered++; continue; }

    // Location
    const location = resolveLocation({ postcode: row.postcode, suburb: row.suburb, state: row.state });
    if (!location) { stats.noLocation++; continue; }

    const status = mapStageToStatus(stage);
    let dateMs = null;
    if (rawDate) { const d = new Date(rawDate); if (!isNaN(d)) dateMs = d.getTime(); }

    // Summary + tags
    const srcHash = hashText(description);
    const cached  = casesState[caseId];
    const needsNew = !cached || cached.srcHash !== srcHash;

    let summary, tags;
    if (needsNew && summariesThisRun < MAX_SUMMARIES) {
      tags    = extractTags(description);
      summary = await generateSummary(description);
      if (!summary) summary = buildFallbackSummary(description, tags);
      updatedState[caseId] = { summary, tags, srcHash };
      stats.newSummaries++;
      summariesThisRun++;
    } else if (cached) {
      summary = cached.summary;
      tags    = cached.tags || [];
      updatedState[caseId] = { ...cached, srcHash };
      if (!needsNew) stats.reused++;
      else stats.newSummaries++;
    } else {
      tags    = extractTags(description);
      summary = buildFallbackSummary(description, tags);
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

// ── CSV PARSER ────────────────────────────────────────────────────────────────
function parseCsv(csv) {
  const lines = (csv || '').replace(/^\uFEFF/, '').trim().split('\n');
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
    if (ch === '"') { inQ = !inQ; continue; }
    if (ch === ',' && !inQ) { result.push(cur); cur = ''; continue; }
    cur += ch;
  }
  result.push(cur);
  return result;
}

// ── BLOBS ─────────────────────────────────────────────────────────────────────
async function loadBlobJson(key) {
  try { const r = await getBlobStore(BLOB_STORE).get(key); return r ? JSON.parse(r) : null; }
  catch (e) { console.warn(`[refresh] loadBlobJson(${key}):`, e.message); return null; }
}
async function saveBlobJson(key, data) {
  try { await getBlobStore(BLOB_STORE).set(key, JSON.stringify(data)); console.log(`[refresh] Saved: ${key}`); }
  catch (e) { console.warn(`[refresh] saveBlobJson(${key}):`, e.message); }
}
