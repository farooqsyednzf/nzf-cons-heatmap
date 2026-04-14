'use strict';

/**
 * refresh-data.js — NZF Community Map daily data pipeline
 *
 * Runs at 7:00 AM AEST (21:00 UTC) every day.
 * Zoho Analytics syncs around 5 AM AEST, so 7 AM ensures the latest data.
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
exports.handler = schedule('0 21 * * *', async () => {
  const start = Date.now();
  console.log('[refresh] Starting daily refresh');

  try {
    // 1. Refresh Zoho access token (covers both Analytics and CRM)
    _zohoToken = await refreshZohoToken();
    console.log('[refresh] Zoho access token obtained');

    // 2a. Cases + Clients (manageable size) — load together with DV scan + cache
    // Donations (230k rows) and Distributions (62k rows) are loaded separately
    // AFTER cases are processed, so only one large dataset is in memory at a time.
    console.log('[refresh] Fetching cases, clients, DV notes, cached state...');
    const [[caseRows, clientRows], dvNoteIds, casesState] = await Promise.all([
      Promise.all([fetchViewRows(VIEW_CASES), fetchViewRows(VIEW_CLIENTS)]),
      fetchDvCaseIdsFromCrm(),
      loadBlobJson(BLOB_STATE).then(v => v || {}),
    ]);
    console.log(`[refresh] ${caseRows.length} case rows, ${clientRows.length} client rows | DV flags: ${dvNoteIds.size} | cached: ${Object.keys(casesState).length}`);

    // Build shared lookup maps, then free the raw arrays immediately
    const clientMap    = buildClientMap(clientRows);
    const caseToClient = buildCaseToClientMap(caseRows);
    const rawCases     = buildRawCases(caseRows, clientMap);
    caseRows.length    = 0;
    clientRows.length  = 0;
    console.log(`[refresh] ${rawCases.length} cases after date filter`);

    // 2b. Donations + Distributions — fetched in parallel to save ~13 seconds.
    //     Both are stream-aggregated from raw CSV, never parsed into JS object arrays.
    //     Peak memory: donations CSV (~50 MB) + distributions CSV (~20 MB) = ~70 MB.
    console.log('[refresh] Fetching donations + distributions in parallel (stream aggregating)...');
    const [donCsv, disCsv] = await Promise.all([
      fetchViewCsv(VIEW_DONATIONS),
      fetchViewCsv(VIEW_DISTRIBUTIONS),
    ]);
    const donations     = aggregateDonationsFromCsv(donCsv);
    const distributions = aggregateDistributionsFromCsv(disCsv, caseToClient, clientMap);
    console.log(`[refresh] ${Object.keys(donations).length} donation postcodes, ${Object.keys(distributions).length} distribution postcodes`);

    // 5. Process cases — DV filter, location resolve, generate summaries
    console.log('[refresh] Processing cases...');
    const { postcodes, updatedState, stats } = await processCases(rawCases, dvNoteIds, casesState);
    console.log(`[refresh] ${postcodes.length} postcodes | new: ${stats.newSummaries} | reused: ${stats.reused} | DV removed: ${stats.dvFiltered} | no location: ${stats.noLocation}`);

    // 6. Persist results to Blobs
    // Save cases state first (smaller), then free processed data before building output
    await saveBlobJson(BLOB_STATE, updatedState);

    // Build final output and save — free large intermediate objects first
    const output = {
      generatedAt:   new Date().toISOString(),
      postcodes,
      donations,
      distributions,
    };

    // Free intermediates before JSON serialisation of the full output
    updatedState && Object.keys(updatedState).length > 0 && (Object.keys(updatedState).forEach(k => delete updatedState[k]));

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

// ─── ANALYTICS VIEW — RAW CSV ─────────────────────────────────────────────────
// Returns the raw CSV text without parsing into JS objects.
// Used for large tables (donations 230k rows, distributions 62k rows) so we can
// stream-aggregate without ever holding a full JS object array in memory.
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

// ─── STREAM-AGGREGATE: DONATIONS ─────────────────────────────────────────────
// Processes 230,301 donation rows directly from CSV text.
// Never builds a JS object array — aggregates on the fly by postcode.
// Memory usage: ~50 MB for the CSV string, ~0.1 MB for the output map.
function aggregateDonationsFromCsv(csv) {
  const lines = (csv || '').replace(/^\uFEFF/, '').replace(/\r\n/g, '\n').replace(/\r/g, '\n').trim().split('\n');
  if (lines.length < 2) return {};

  const headers  = splitCsvLine(lines[0]).map(h => h.replace(/"/g, '').trim().toLowerCase().replace(/[\s.()-]+/g, '_'));
  const pcIdx    = headers.indexOf('post_code');
  const amtIdx   = headers.indexOf('amount');
  const statIdx  = headers.indexOf('status');
  const dateIdx  = headers.indexOf('donation_timestamp');

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

    // Date filter + dateMs extraction (format: DD/MM/YYYY HH:MM:SS)
    let donDateMs = null;
    if (dateIdx !== -1) {
      const raw = (vals[dateIdx] || '').replace(/"/g, '').trim();
      if (raw) {
        const parts = raw.split(/[\/\s:]/);
        if (parts.length >= 3) {
          const d = new Date(parts[2], parts[1] - 1, parts[0]);
          if (!isNaN(d)) {
            if (d.getTime() < CUTOFF) continue;
            donDateMs = d.getTime();
          }
        }
      }
    }

    const pc    = (vals[pcIdx]  || '').replace(/"/g, '').trim();
    const total = parseFloat((vals[amtIdx] || '').replace(/[^0-9.]/g, '')) || 0;
    if (!pc || !/^\d{4}$/.test(pc) || total <= 0) continue;

    if (!out[pc]) {
      const geo = lookupPostcode(pc);
      out[pc] = {
        count: 0, total: 0, items: [],
        suburb: geo ? geo.suburb : null, state: geo ? geo.state : null,
        lat: geo ? geo.lat : null, lng: geo ? geo.lng : null,
      };
    }
    out[pc].count++;
    out[pc].total = Math.round((out[pc].total + total) * 100) / 100;
    // Store ALL items with dateMs for exact client-side time filtering
    out[pc].items.push({ amount: Math.round(total * 100) / 100, dateMs: donDateMs });
  }
  // Sort items newest-first (no truncation — all items needed for exact filtering)
  Object.values(out).forEach(entry => {
    entry.items.sort((a, b) => (b.dateMs || 0) - (a.dateMs || 0));
  });
  return out;
}

// ─── STREAM-AGGREGATE: DISTRIBUTIONS ─────────────────────────────────────────
// Processes 62,488 distribution rows directly from CSV text.
// Chains: Distribution.case_name → caseToClient → clientMap → postcode
function aggregateDistributionsFromCsv(csv, caseToClient, clientMap) {
  const lines = (csv || '').replace(/^\uFEFF/, '').replace(/\r\n/g, '\n').replace(/\r/g, '\n').trim().split('\n');
  if (lines.length < 2) return {};

  const headers  = splitCsvLine(lines[0]).map(h => h.replace(/"/g, '').trim().toLowerCase().replace(/[\s.()-]+/g, '_'));
  const statIdx  = headers.indexOf('status');
  const caseIdx  = headers.indexOf('case_name');
  const amtIdx   = headers.indexOf('grand_total');
  const dateIdx  = headers.indexOf('created_time');
  const subjIdx  = headers.indexOf('subject');

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

    // Date filter + dateMs extraction
    let disDateMs = null;
    if (dateIdx !== -1) {
      const raw = (vals[dateIdx] || '').replace(/"/g, '').trim();
      if (raw) {
        const d = new Date(raw);
        if (!isNaN(d)) {
          if (d.getTime() < CUTOFF) continue;
          disDateMs = d.getTime();
        }
      }
    }

    const caseId  = (vals[caseIdx] || '').replace(/"/g, '').trim();
    const amount  = parseFloat((vals[amtIdx] || '').replace(/[^0-9.]/g, '')) || 0;
    if (!caseId || amount <= 0) continue;

    const clientId = caseToClient[caseId];
    if (!clientId) continue;

    const client = clientMap[clientId];
    if (!client || !client.postcode) continue;

    const pc = client.postcode;
    if (!/^\d{4}$/.test(pc)) continue;

    const subject = subjIdx !== -1 ? (vals[subjIdx] || '').replace(/"/g, '').trim() : '';

    if (!out[pc]) {
      const geo = lookupPostcode(pc);
      out[pc] = {
        count: 0, total: 0, items: [],
        suburb: client.suburb || (geo ? geo.suburb : null),
        state:  client.state  || (geo ? geo.state  : null),
        lat: geo ? geo.lat : null, lng: geo ? geo.lng : null,
      };
    }
    out[pc].count++;
    out[pc].total = Math.round((out[pc].total + amount) * 100) / 100;
    // Store ALL items with dateMs + subject for exact client-side filtering
    out[pc].items.push({ amount: Math.round(amount * 100) / 100, subject, dateMs: disDateMs });
  }
  // Sort items newest-first
  Object.values(out).forEach(entry => {
    entry.items.sort((a, b) => (b.dateMs || 0) - (a.dateMs || 0));
  });
  return out;
}


// ─── CASES PROCESSING ─────────────────────────────────────────────────────────
async function processCases(rawCases, dvNoteIds, casesState) {
  const stats = { newSummaries: 0, reused: 0, dvFiltered: 0, noLocation: 0 };
  const postcodeMap    = new Map();
  const updatedState   = {};
  let   summariesThisRun = 0;

  const CHUNK = 500; // yield to GC every 500 cases
  for (let ci = 0; ci < rawCases.length; ci++) {
    // Yield to event loop every CHUNK cases — allows GC to run and prevents starvation
    if (ci > 0 && ci % CHUNK === 0) {
      await new Promise(resolve => setImmediate(resolve));
      console.log(`[refresh] Processing cases... ${ci} / ${rawCases.length}`);
    }

    const row         = rawCases[ci];
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
  const json = JSON.stringify(data);
  const kb   = (Buffer.byteLength(json) / 1024).toFixed(0);
  console.log(`[refresh] Writing Blob: ${key} (${kb} KB)`);
  try {
    await getBlobStore().set(key, json);
    console.log(`[refresh] Saved Blob: ${key}`);
  } catch (e) {
    // Throw so the error appears in logs — silent failures make debugging impossible
    console.error(`[refresh] BLOB WRITE FAILED (${key}): ${e.message}`);
    throw e;
  }
}
