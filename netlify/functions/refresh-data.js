'use strict';

/**
 * refresh-data.js — NZF Community Map daily data pipeline
 *
 * Runs at 4:00 AM AEST (18:00 UTC) every day.
 * Uses the Zoho Analytics non-bulk view data API (no bulk scopes needed).
 * Cases and Clients are fetched once and reused across all three data types.
 */

const { schedule }   = require('@netlify/functions');
const { getStore }   = require('@netlify/blobs');
const {
  mapStageToStatus, hasDvContent,
  extractTags, hashText,
  generateSummary, buildFallbackSummary,
  sanitiseSummary,
} = require('./lib/transforms');
const { resolveLocation, lookupPostcode } = require('./lib/postcodes');

// ─── ENVIRONMENT VALIDATION ───────────────────────────────────────────────────
// Validate all required env vars at startup. Fail fast with a clear message
// rather than surfacing a cryptic error mid-pipeline.
const ENV = (function () {
  const required = {
    ZOHO_CLIENT_ID:     process.env.ZOHO_CLIENT_ID,
    ZOHO_CLIENT_SECRET: process.env.ZOHO_CLIENT_SECRET,
    ZOHO_REFRESH_TOKEN: process.env.ZOHO_REFRESH_TOKEN,
    ZOHO_ORG_ID:        process.env.ZOHO_ORG_ID,
    ZOHO_WS_ID:         process.env.ZOHO_WS_ID,
  };
  const missing = Object.entries(required).filter(([, v]) => !v).map(([k]) => k);
  if (missing.length) {
    throw new Error(`Missing required environment variables: ${missing.join(', ')}`);
  }
  return {
    ...required,
    ANTHROPIC_API_KEY:    process.env.ANTHROPIC_API_KEY    || null,
    NETLIFY_SITE_ID:      process.env.NETLIFY_SITE_ID      || null,
    NETLIFY_BLOBS_TOKEN:  process.env.NETLIFY_BLOBS_TOKEN  || process.env.NETLIFY_TOKEN || null,
    MAX_SUMMARIES:        parseInt(process.env.MAX_SUMMARIES_PER_RUN || '0', 10),
  };
}());

const ZOHO_TOKEN_URL      = 'https://accounts.zoho.com/oauth/v2/token';
const ZOHO_ANALYTICS_BASE = 'https://analyticsapi.zoho.com/restapi/v2';
const ZOHO_CRM_BASE       = 'https://www.zohoapis.com/crm/v2';

// Zoho Analytics view IDs — base tables, confirmed working with non-bulk API
const VIEW_CASES         = '1715382000001002494';
const VIEW_CLIENTS       = '1715382000001002492';
const VIEW_DONATIONS     = '1715382000006560082';
const VIEW_DISTRIBUTIONS = '1715382000001002628';

const BLOB_STORE  = 'nzf-map';
const BLOB_OUTPUT = 'aggregated-v2';
const BLOB_STATE  = 'cases-state-v1';

// Access token for current run — refreshed at the top of every handler invocation
let _zohoToken = null;

// ─── BLOBS HELPER ─────────────────────────────────────────────────────────────
// Uses explicit siteID + token when available (required for "Run now" invocations
// that don't have Netlify's context auto-injected). Falls back to auto-config
// for properly-scheduled runs where the context is always present.
function getBlobStore() {
  if (ENV.NETLIFY_SITE_ID && ENV.NETLIFY_BLOBS_TOKEN) {
    return getStore({ name: BLOB_STORE, siteID: ENV.NETLIFY_SITE_ID, token: ENV.NETLIFY_BLOBS_TOKEN });
  }
  return getStore(BLOB_STORE);
}

// ─── HANDLER ─────────────────────────────────────────────────────────────────
exports.handler = schedule('0 18 * * *', async () => {
  const start = Date.now();
  console.log('[refresh] Starting daily refresh');

  try {
    // 1. Refresh Zoho access token (covers both Analytics and CRM)
    _zohoToken = await refreshZohoToken();
    console.log('[refresh] Zoho access token obtained');

    // 2. Fetch all four Analytics tables, DV notes, and cached state in parallel
    console.log('[refresh] Fetching all data sources in parallel...');
    const [tableResults, dvNoteIds, casesState] = await Promise.all([
      Promise.all([
        fetchViewRows(VIEW_CASES),
        fetchViewRows(VIEW_CLIENTS),
        fetchViewRows(VIEW_DONATIONS).catch(e  => { console.warn('[refresh] Donations table failed:', e.message);      return []; }),
        fetchViewRows(VIEW_DISTRIBUTIONS).catch(e => { console.warn('[refresh] Distributions table failed:', e.message); return []; }),
      ]),
      fetchDvCaseIdsFromCrm(),
      loadBlobJson(BLOB_STATE).then(v => v || {}),
    ]);

    const [caseRows, clientRows, donationRows, distRows] = tableResults;
    console.log(`[refresh] Fetched: ${caseRows.length} cases, ${clientRows.length} clients, ${donationRows.length} donation rows, ${distRows.length} distribution rows`);
    console.log(`[refresh] ${dvNoteIds.size} cases flagged via notes DV scan`);
    console.log(`[refresh] ${Object.keys(casesState).length} cached case states loaded`);

    // 3. Build shared lookups once — reused by cases and distributions
    const clientMap    = buildClientMap(clientRows);
    const caseToClient = buildCaseToClientMap(caseRows);

    // 4. Build output for each data type
    const rawCases     = buildRawCases(caseRows, clientMap);
    const donations    = buildDonations(donationRows);
    const distributions = buildDistributions(distRows, caseToClient, clientMap);
    console.log(`[refresh] ${rawCases.length} cases after date filter | ${Object.keys(donations).length} donation postcodes | ${Object.keys(distributions).length} distribution postcodes`);

    // 5. Process cases — DV filter, location resolve, generate summaries
    console.log('[refresh] Processing cases...');
    const { postcodes, updatedState, stats } = await processCases(rawCases, dvNoteIds, casesState);
    console.log(`[refresh] ${postcodes.length} postcodes | new: ${stats.newSummaries} | reused: ${stats.reused} | DV removed: ${stats.dvFiltered} | no location: ${stats.noLocation}`);

    // 6. Persist results to Blobs
    await saveBlobJson(BLOB_STATE, updatedState);

    const output = {
      generatedAt:   new Date().toISOString(),
      postcodes,
      donations,
      distributions,
    };
    await saveBlobJson(BLOB_OUTPUT, output);

    console.log(`[refresh] Done in ${((Date.now() - start) / 1000).toFixed(1)}s`);
    return { statusCode: 200 };

  } catch (err) {
    console.error('[refresh] Fatal error:', err.message);
    return { statusCode: 500 };
  }
});

// ─── ZOHO TOKEN REFRESH ───────────────────────────────────────────────────────
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
  // Do not log data — it may contain partial credentials
  if (!data.access_token) throw new Error('Zoho token response did not include access_token');

  return data.access_token;
}

// ─── ANALYTICS VIEW DATA FETCH ────────────────────────────────────────────────
// Non-bulk API: GET /workspaces/{wsId}/views/{viewId}/data
// Only responseFormat is accepted in CONFIG. Returns all rows in one call.
// Works with ZohoAnalytics.data.read scope.
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
    throw new Error(`Analytics view ${viewId} returned HTTP ${resp.status}: ${body.slice(0, 200)}`);
  }

  return parseCsv(await resp.text());
}

// ─── SHARED LOOKUP BUILDERS ───────────────────────────────────────────────────
function buildClientMap(clientRows) {
  const map = {};
  clientRows.forEach(c => {
    const id = (c.id || '').trim();
    if (!id) return;
    map[id] = {
      postcode: (c.mailing_zip       || '').trim(),
      suburb:   (c.mailing_city      || '').trim(),
      state:    (c.mailing_state     || '').trim(),
      dv_flag:  (c.domestic_violence || '').trim().toLowerCase(),
    };
  });
  return map;
}

function buildCaseToClientMap(caseRows) {
  const map = {};
  caseRows.forEach(row => {
    const cid = (row.id          || '').trim();
    const clt = (row.client_name || '').trim();
    if (cid && clt) map[cid] = clt;
  });
  return map;
}

// ─── CASES ────────────────────────────────────────────────────────────────────
function buildRawCases(caseRows, clientMap) {
  const CUTOFF = new Date('2025-01-01').getTime();
  const merged = [];

  caseRows.forEach(row => {
    // Filter to Zakat Receiver type only
    const type = (row.internal_case_type || row.type || '').trim();
    if (type && type !== 'Zakat Receiver') return;

    // Date filter: 2025-01-01 onwards
    const rawDate = (row.created_time || '').trim();
    if (rawDate) {
      const d = new Date(rawDate);
      if (!isNaN(d) && d.getTime() < CUTOFF) return;
    }

    const client = clientMap[(row.client_name || '').trim()] || {};
    merged.push({
      id:          (row.id          || '').trim(),
      stage:       (row.stage       || '').trim(),
      description: (row.description || '').trim(),
      created_date: rawDate,
      postcode:    client.postcode || '',
      suburb:      client.suburb   || '',
      state:       client.state    || '',
      dv_flag:     client.dv_flag  || '',
    });
  });

  return merged;
}

// ─── DONATIONS ────────────────────────────────────────────────────────────────
function buildDonations(rows) {
  const out = {};
  rows.forEach(r => {
    if ((r.status || '').trim() !== 'Completed') return;

    const pc    = (r.post_code || '').trim();
    const total = parseFloat((r.amount || '').replace(/[^0-9.]/g, '')) || 0;
    if (!pc || !/^\d{4}$/.test(pc) || total <= 0) return;

    if (!out[pc]) {
      const geo  = lookupPostcode(pc);
      out[pc] = {
        count:  0,
        total:  0,
        suburb: geo ? geo.suburb : null,
        state:  geo ? geo.state  : null,
        lat:    geo ? geo.lat    : null,
        lng:    geo ? geo.lng    : null,
      };
    }
    out[pc].count++;
    out[pc].total += total;
  });
  return out;
}

// ─── DISTRIBUTIONS ────────────────────────────────────────────────────────────
// Chain: Distribution.case_name → Cases.client_name → Clients.mailing_zip
function buildDistributions(distRows, caseToClient, clientMap) {
  const out = {};
  distRows.forEach(r => {
    const status = (r.status || '').trim();
    if (status !== 'Paid' && status !== 'Extracted') return;

    const caseId = (r.case_name   || '').trim();
    const amount = parseFloat((r.grand_total || '').replace(/[^0-9.]/g, '')) || 0;
    if (!caseId || amount <= 0) return;

    const clientId = caseToClient[caseId];
    if (!clientId) return;

    const client = clientMap[clientId];
    if (!client || !client.postcode) return;

    const pc = client.postcode;
    if (!/^\d{4}$/.test(pc)) return;

    if (!out[pc]) {
      const geo  = lookupPostcode(pc);
      out[pc] = {
        count:  0,
        total:  0,
        suburb: client.suburb || (geo ? geo.suburb : null),
        state:  client.state  || (geo ? geo.state  : null),
        lat:    geo ? geo.lat : null,
        lng:    geo ? geo.lng : null,
      };
    }
    out[pc].count++;
    out[pc].total += amount;
  });
  return out;
}

// ─── DV NOTES SCAN (Zoho CRM COQL) ───────────────────────────────────────────
// Fetches Deals note IDs that contain DV keywords server-side.
// Returns a Set of case (Deals) IDs whose notes flagged DV content.
const DV_NOTE_PHRASES = [
  'domestic violence', 'family violence', 'physical abuse', 'sexual abuse',
  'abusive partner', 'abusive husband', 'violent partner', 'violent husband',
  'restraining order', 'intervention order', 'apprehended violence',
  'safe house', 'fleeing violence', 'fled violence',
];

async function fetchDvCaseIdsFromCrm() {
  const dvIds = new Set();

  const criteria  = DV_NOTE_PHRASES.map(kw => `Note_Content like '%${kw}%'`).join(' OR ');
  const baseQuery = `SELECT Parent_Id FROM Notes WHERE ($se_module = 'Deals') AND (${criteria})`;

  let offset = 0;
  let more   = true;

  while (more) {
    try {
      const resp = await fetch(`${ZOHO_CRM_BASE}/coql`, {
        method:  'POST',
        headers: {
          'Authorization': `Zoho-oauthtoken ${_zohoToken}`,
          'Content-Type':  'application/json',
        },
        body: JSON.stringify({ select_query: `${baseQuery} LIMIT 200 OFFSET ${offset}` }),
      });

      if (resp.status === 204 || resp.status === 404) break;
      if (!resp.ok) {
        console.warn(`[refresh] Notes COQL HTTP ${resp.status} — DV notes scan incomplete`);
        break;
      }

      const data = await resp.json();
      (data.data || []).forEach(row => {
        const pid = (row.Parent_Id && row.Parent_Id.id) ? row.Parent_Id.id : row.Parent_Id;
        if (pid) dvIds.add(String(pid));
      });

      more    = !!(data.info && data.info.more_records);
      offset += 200;
    } catch (e) {
      console.warn('[refresh] Notes COQL page failed:', e.message);
      break;
    }
  }

  return dvIds;
}

// ─── CASES PROCESSING ─────────────────────────────────────────────────────────
async function processCases(rawCases, dvNoteIds, casesState) {
  const stats = { newSummaries: 0, reused: 0, dvFiltered: 0, noLocation: 0 };
  const postcodeMap    = new Map();
  const updatedState   = {};
  let   summariesThisRun = 0;

  for (const row of rawCases) {
    const caseId      = row.id;
    const description = row.description;
    const stage       = row.stage;

    // ── DV filter: three independent layers ──────────────────────────────────
    // 1. Dedicated boolean field on the Contact record
    // 2. Keyword scan of the case Description text
    // 3. CRM Notes COQL scan (dvNoteIds set)
    const isDv = row.dv_flag === 'true'
      || hasDvContent(description)
      || dvNoteIds.has(caseId);
    if (isDv) { stats.dvFiltered++; continue; }

    // ── Location ──────────────────────────────────────────────────────────────
    const location = resolveLocation({ postcode: row.postcode, suburb: row.suburb, state: row.state });
    if (!location) { stats.noLocation++; continue; }

    // ── Status mapping ────────────────────────────────────────────────────────
    const status = mapStageToStatus(stage);

    // ── Date ──────────────────────────────────────────────────────────────────
    let dateMs = null;
    if (row.created_date) {
      const d = new Date(row.created_date);
      if (!isNaN(d)) dateMs = d.getTime();
    }

    // ── Summary + tags (cached by caseId + description hash) ─────────────────
    const srcHash  = hashText(description);
    const cached   = casesState[caseId];
    const needsNew = !cached || cached.srcHash !== srcHash;

    let summary, tags;

    if (needsNew && summariesThisRun < ENV.MAX_SUMMARIES) {
      // Generate via Claude API
      tags    = extractTags(description);
      summary = await generateSummary(description);
      if (!summary) summary = buildFallbackSummary(description, tags);
      summary = sanitiseSummary(summary);
      updatedState[caseId] = { summary, tags, srcHash };
      stats.newSummaries++;
      summariesThisRun++;
    } else if (cached) {
      // Reuse cached result
      summary = cached.summary;
      tags    = cached.tags || [];
      summary = sanitiseSummary(summary);
      updatedState[caseId] = { summary, tags, srcHash };
      if (!needsNew) stats.reused++;
      else           stats.newSummaries++; // over per-run limit — used cached or fallback
    } else {
      // No cache, over limit — use safe tag-based fallback (no raw text exposed)
      tags    = extractTags(description);
      summary = buildFallbackSummary(null, tags); // null prevents raw text exposure
      summary = sanitiseSummary(summary);
      updatedState[caseId] = { summary, tags, srcHash };
      stats.newSummaries++;
    }

    // ── Group by postcode ─────────────────────────────────────────────────────
    const pc = location.postcode;
    if (!postcodeMap.has(pc)) {
      postcodeMap.set(pc, {
        pc,
        city:  location.suburb,
        state: location.state,
        lat:   location.lat,
        lng:   location.lng,
        cases: [],
      });
    }
    postcodeMap.get(pc).cases.push({ s: summary, t: tags, st: status, dateMs });
  }

  // Sort cases within each postcode newest-first
  postcodeMap.forEach(entry => entry.cases.sort((a, b) => (b.dateMs || 0) - (a.dateMs || 0)));

  return {
    postcodes:    Array.from(postcodeMap.values()),
    updatedState,
    stats,
  };
}

// ─── CSV PARSER ───────────────────────────────────────────────────────────────
function parseCsv(csv) {
  // Normalise line endings before splitting — Zoho CSVs use \r\n
  const lines = (csv || '')
    .replace(/^\uFEFF/, '')       // strip BOM
    .replace(/\r\n/g, '\n')       // normalise CRLF
    .replace(/\r/g, '\n')         // normalise bare CR
    .trim()
    .split('\n');

  if (lines.length < 2) return [];

  const headers = lines[0]
    .split(',')
    .map(h => h.replace(/"/g, '').trim().toLowerCase().replace(/[\s.()-]+/g, '_'));

  return lines.slice(1).map(line => {
    const vals = splitCsvLine(line);
    const obj  = {};
    headers.forEach((h, i) => {
      obj[h] = (vals[i] || '').replace(/"/g, '').trim();
    });
    return obj;
  });
}

function splitCsvLine(line) {
  const result = [];
  let cur = '', inQ = false;
  for (const ch of line) {
    if (ch === '"')              { inQ = !inQ;             continue; }
    if (ch === ',' && !inQ)     { result.push(cur); cur = ''; continue; }
    cur += ch;
  }
  result.push(cur);
  return result;
}

// ─── BLOBS ────────────────────────────────────────────────────────────────────
async function loadBlobJson(key) {
  try {
    const raw = await getBlobStore().get(key);
    return raw ? JSON.parse(raw) : null;
  } catch (e) {
    console.warn(`[refresh] loadBlobJson(${key}): ${e.message}`);
    return null;
  }
}

async function saveBlobJson(key, data) {
  try {
    await getBlobStore().set(key, JSON.stringify(data));
    console.log(`[refresh] Saved Blob: ${key}`);
  } catch (e) {
    console.warn(`[refresh] saveBlobJson(${key}): ${e.message}`);
  }
}
