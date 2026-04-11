'use strict';

/**
 * refresh-data.js — NZF Community Map daily data pipeline
 *
 * Runs at 4:00 AM AEST (18:00 UTC) every day.
 * Timeout: 15 minutes (Netlify scheduled background function).
 *
 * What it does:
 *   1. Fetches all Cases + Client addresses from Zoho Analytics (SQL JOIN)
 *   2. Scans Case Notes in Zoho CRM for domestic violence keywords (COQL)
 *   3. Fetches Donations and Distributions from Zoho Analytics
 *   4. Filters DV cases, maps stages to statuses, extracts tags
 *   5. Generates anonymised summaries via Claude API (cached — only new/changed cases)
 *   6. Stores everything in Netlify Blobs for instant serving by map-data.js
 */

const { schedule }  = require('@netlify/functions');
const { getStore }  = require('@netlify/blobs');
const {
  mapStageToStatus, hasDvContent,
  extractTags, hashText,
  generateSummary, buildFallbackSummary,
} = require('./lib/transforms');
const { resolveLocation, lookupPostcode } = require('./lib/postcodes');

// ─── ENVIRONMENT ──────────────────────────────────────────────────────────────
const ZOHO_TOKEN    = process.env.ZOHO_TOKEN;
const ZOHO_ORG_ID   = process.env.ZOHO_ORG_ID   || '668395719';
const ZOHO_WS_ID    = process.env.ZOHO_WS_ID    || '1715382000001002475';
const ZOHO_API_BASE = 'https://analyticsapi.zoho.com/restapi/v2';

// Zoho CRM uses OAuth with short-lived access tokens (1 hour).
// We store a long-lived refresh token and exchange it for a fresh access token
// at the start of each daily run. The refresh token does not expire.
const ZOHO_CRM_CLIENT_ID     = process.env.ZOHO_CRM_CLIENT_ID;
const ZOHO_CRM_CLIENT_SECRET = process.env.ZOHO_CRM_CLIENT_SECRET;
const ZOHO_CRM_REFRESH_TOKEN = process.env.ZOHO_CRM_REFRESH_TOKEN;
const ZOHO_CRM_BASE          = 'https://www.zohoapis.com/crm/v2';
const ZOHO_CRM_TOKEN_URL     = 'https://accounts.zoho.com/oauth/v2/token';

const BLOB_STORE  = 'nzf-map';
const BLOB_OUTPUT = 'aggregated-v2';
const BLOB_STATE  = 'cases-state-v1';

const MAX_SUMMARIES = parseInt(process.env.MAX_SUMMARIES_PER_RUN || '500');

// ─── MAIN HANDLER ─────────────────────────────────────────────────────────────
exports.handler = schedule('0 18 * * *', async () => {
  const start = Date.now();
  console.log('[refresh] ── Starting daily refresh ──────────────────────────');

  try {
    // ── Step 1: Get a fresh Zoho CRM access token ────────────────────────────
    let crmToken = null;
    try {
      crmToken = await refreshZohoCrmToken();
      console.log('[refresh] CRM access token obtained');
    } catch (e) {
      console.error('[refresh] ⚠️  CRM token refresh failed:', e.message);
      console.error('[refresh] ⚠️  Notes DV scan will be skipped this run');
    }

    // ── Step 2: Fetch cases from Analytics (bulk SQL JOIN) ──────────────────
    console.log('[refresh] Fetching cases + clients from Analytics...');
    const rawCases = await fetchCasesFromAnalytics();
    console.log(`[refresh] ${rawCases.length} cases fetched`);

    // ── Step 3: Fetch DV-flagged case IDs from CRM Notes ────────────────────
    console.log('[refresh] Scanning CRM notes for DV keywords...');
    const dvNoteIds = await fetchDvCaseIdsFromCrm(crmToken);
    console.log(`[refresh] ${dvNoteIds.size} cases flagged via notes DV scan`);

    // ── Step 4: Load cached summaries/tags from Blobs ───────────────────────
    const casesState = await loadBlobJson(BLOB_STATE) || {};
    console.log(`[refresh] ${Object.keys(casesState).length} cached case states loaded`);

    // ── Step 5: Fetch Donations + Distributions from Analytics ──────────────
    console.log('[refresh] Fetching donations + distributions...');
    const [donations, distributions] = await Promise.all([
      fetchDonationsFromAnalytics().catch(e => {
        console.warn('[refresh] Donations failed:', e.message); return {};
      }),
      fetchDistributionsFromAnalytics().catch(e => {
        console.warn('[refresh] Distributions failed:', e.message); return {};
      }),
    ]);
    console.log(`[refresh] ${Object.keys(donations).length} donation postcodes, ${Object.keys(distributions).length} distribution postcodes`);

    // ── Step 6: Process cases (DV filter, transform, generate summaries) ────
    console.log('[refresh] Processing cases...');
    const { postcodes, updatedState, stats } = await processCases(rawCases, dvNoteIds, casesState);
    console.log(`[refresh] Results: ${postcodes.length} postcodes | new summaries: ${stats.newSummaries} | reused: ${stats.reused} | DV removed: ${stats.dvFiltered} | no location: ${stats.noLocation}`);

    // ── Step 7: Save updated cases state ────────────────────────────────────
    await saveBlobJson(BLOB_STATE, updatedState);

    // ── Step 8: Build and store final map output ─────────────────────────────
    const output = {
      generatedAt: new Date().toISOString(),
      postcodes,
      donations,
      distributions,
    };
    await saveBlobJson(BLOB_OUTPUT, output);

    const elapsed = ((Date.now() - start) / 1000).toFixed(1);
    console.log(`[refresh] ── Done in ${elapsed}s ──────────────────────────────`);
    return { statusCode: 200 };

  } catch (err) {
    console.error('[refresh] Fatal error:', err.message, '\n', err.stack);
    return { statusCode: 500 };
  }
});

// ─── ZOHO CRM: TOKEN REFRESH ──────────────────────────────────────────────────
// Exchanges the long-lived refresh token for a fresh 1-hour access token.
// Called at the start of every daily run.
async function refreshZohoCrmToken() {
  if (!ZOHO_CRM_CLIENT_ID || !ZOHO_CRM_CLIENT_SECRET || !ZOHO_CRM_REFRESH_TOKEN) {
    throw new Error('ZOHO_CRM_CLIENT_ID, ZOHO_CRM_CLIENT_SECRET and ZOHO_CRM_REFRESH_TOKEN must all be set');
  }

  const body = new URLSearchParams({
    grant_type:    'refresh_token',
    client_id:     ZOHO_CRM_CLIENT_ID,
    client_secret: ZOHO_CRM_CLIENT_SECRET,
    refresh_token: ZOHO_CRM_REFRESH_TOKEN,
  });

  const resp = await fetch(ZOHO_CRM_TOKEN_URL, {
    method:  'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body:    body.toString(),
  });

  if (!resp.ok) throw new Error(`HTTP ${resp.status} from Zoho token endpoint`);

  const data = await resp.json();
  if (!data.access_token) throw new Error(`No access_token in response: ${JSON.stringify(data)}`);

  return data.access_token;
}

// ─── ZOHO CRM: NOTES DV SCAN (COQL) ──────────────────────────────────────────
// Queries CRM Notes directly for domestic violence keywords.
// Returns a Set of Deals (case) IDs whose notes contain DV content.
// Server-side COQL filtering means only matched notes are transferred.
const DV_NOTE_PHRASES = [
  'domestic violence', 'family violence', 'dv situation', 'dv case', 'dv history',
  'physical abuse', 'sexual abuse', 'emotional abuse', 'financial abuse', 'verbal abuse',
  'abusive partner', 'abusive husband', 'abusive spouse', 'abusive relationship',
  'violent partner', 'violent husband', 'violent ex',
  'fleeing violence', 'fled violence', 'escaping abuse', 'escaped abuse',
  'left abusive', 'left violent', 'safe house', 'refuge placement',
  'apprehended violence', 'restraining order', 'intervention order', 'protection order',
];

async function fetchDvCaseIdsFromCrm(crmToken) {
  const dvIds = new Set();

  if (!crmToken) {
    console.error('[refresh] ⚠️  No CRM token — Notes DV scan SKIPPED');
    console.error('[refresh] ⚠️  Set ZOHO_CRM_CLIENT_ID, ZOHO_CRM_CLIENT_SECRET and ZOHO_CRM_REFRESH_TOKEN');
    console.error('[refresh] ⚠️  DV detection running on Description text + Domestic_Violence field only');
    return dvIds;
  }

  // Use a focused subset of phrases for server-side COQL filtering.
  // The full list in transforms.js is applied client-side to description text.
  const coqlCriteria = [
    'domestic violence', 'family violence', 'physical abuse', 'sexual abuse',
    'abusive partner', 'abusive husband', 'violent partner', 'violent husband',
    'restraining order', 'intervention order', 'apprehended violence',
    'safe house', 'fleeing violence', 'fled violence',
  ].map(kw => `Note_Content like '%${kw}%'`).join(' OR ');

  const baseQuery = `SELECT Parent_Id FROM Notes WHERE ($se_module = 'Deals') AND (${coqlCriteria})`;

  let offset = 0;
  let more   = true;
  let pages  = 0;

  while (more) {
    const query = `${baseQuery} LIMIT 200 OFFSET ${offset}`;
    try {
      const resp = await fetch(`${ZOHO_CRM_BASE}/coql`, {
        method:  'POST',
        headers: {
          'Authorization': `Zoho-oauthtoken ${crmToken}`,
          'Content-Type':  'application/json',
        },
        body: JSON.stringify({ select_query: query }),
      });

      if (resp.status === 204 || resp.status === 404) break;

      if (!resp.ok) {
        const body = await resp.text();
        throw new Error(`COQL HTTP ${resp.status}: ${body.slice(0, 200)}`);
      }

      const data = await resp.json();
      const rows = data.data || [];

      for (const row of rows) {
        // Parent_Id is an object: { name, id, module: { api_name } }
        const parentId = row.Parent_Id?.id || row.Parent_Id;
        if (parentId) dvIds.add(String(parentId));
      }

      more   = data.info?.more_records === true;
      offset += 200;
      pages++;
    } catch (e) {
      console.warn(`[refresh] CRM notes page ${pages + 1} failed:`, e.message);
      break;
    }
  }

  console.log(`[refresh] CRM notes scan: ${pages} pages queried, ${dvIds.size} cases flagged`);
  return dvIds;
}

// ─── ZOHO ANALYTICS: CASES + CLIENTS ─────────────────────────────────────────
async function fetchCasesFromAnalytics() {
  const sql = `
    SELECT
      ca.Id                        AS id,
      ca.\`CASE-ID\`               AS case_id,
      ca.Stage,
      ca.Description,
      ca.\`Created Time\`          AS created_date,
      cl.\`Mailing Zip\`           AS postcode,
      cl.\`Mailing City\`          AS suburb,
      cl.\`Mailing State\`         AS state,
      cl.\`Domestic Violence\`     AS dv_flag
    FROM Cases ca
    JOIN Clients cl ON ca.\`Client Name\` = cl.Id
    WHERE ca.Stage IS NOT NULL
      AND (ca.Type = 'Zakat Receiver' OR ca.\`Internal Case Type\` = 'Zakat Receiver')
  `.trim();

  const csv = await zohoAnalyticsSql(sql);
  return parseCsv(csv);
}

// ─── ZOHO ANALYTICS: DONATIONS ────────────────────────────────────────────────
async function fetchDonationsFromAnalytics() {
  const sql = `
    SELECT post_code, COUNT(*) AS cnt, SUM(amount) AS total
    FROM donations
    WHERE post_code IS NOT NULL AND post_code != '' AND status = 'Completed'
    GROUP BY post_code
  `.trim();

  const csv  = await zohoAnalyticsSql(sql);
  const rows = parseCsv(csv);
  const out  = {};

  for (const r of rows) {
    const pc    = (r.post_code || '').trim();
    const total = parseFloat(r.total) || 0;
    if (!pc || !/^\d{4}$/.test(pc) || total <= 0) continue;
    const geo = lookupPostcode(pc);
    out[pc] = { count: parseInt(r.cnt) || 0, total, suburb: geo?.suburb || null, state: geo?.state || null };
  }
  return out;
}

// ─── ZOHO ANALYTICS: DISTRIBUTIONS ───────────────────────────────────────────
async function fetchDistributionsFromAnalytics() {
  const sql = `
    SELECT cl.\`Mailing Zip\` AS postcode, COUNT(d.Id) AS cnt, SUM(d.\`Grand Total\`) AS total
    FROM Distributions d
    JOIN Cases ca ON d.\`Case Name\` = ca.Id
    JOIN Clients cl ON ca.\`Client Name\` = cl.Id
    WHERE d.Status = 'Paid'
      AND cl.\`Mailing Zip\` IS NOT NULL AND cl.\`Mailing Zip\` != ''
    GROUP BY cl.\`Mailing Zip\`
  `.trim();

  const csv  = await zohoAnalyticsSql(sql);
  const rows = parseCsv(csv);
  const out  = {};

  for (const r of rows) {
    const pc    = (r.postcode || '').trim();
    const total = parseFloat(r.total) || 0;
    if (!pc || !/^\d{4}$/.test(pc) || total <= 0) continue;
    const geo = lookupPostcode(pc);
    out[pc] = { count: parseInt(r.cnt) || 0, total, suburb: geo?.suburb || null, state: geo?.state || null };
  }
  return out;
}

// ─── CASES PROCESSING PIPELINE ────────────────────────────────────────────────
async function processCases(rawCases, dvNoteIds, casesState) {
  const stats = { newSummaries: 0, reused: 0, dvFiltered: 0, noLocation: 0 };
  const postcodeMap  = new Map();
  const updatedState = {};
  let summariesThisRun = 0;

  for (const row of rawCases) {
    const caseId     = String(row.case_id || row.id || '').trim();
    const description = String(row.description || row.Description || '').trim();
    const stage      = String(row.stage || row.Stage || '').trim();
    const rawDate    = row.created_date || row['Created Time'];
    const rawDvFlag  = String(row.dv_flag || row['Domestic Violence'] || '').toLowerCase();

    // ── DV Filter (three independent layers) ────────────────────────────────
    // Layer 1: Dedicated Domestic_Violence boolean on the Contact record
    // Layer 2: Keyword scan of the case Description text
    // Layer 3: CRM Notes COQL scan (dvNoteIds set, populated above)
    // Any one layer matching is enough to exclude this case.
    const isDv = rawDvFlag === 'true'
      || hasDvContent(description)
      || dvNoteIds.has(String(row.id || ''));

    if (isDv) {
      stats.dvFiltered++;
      continue;
    }

    // ── Location resolution (forward lookup, then suburb fallback) ───────────
    const location = resolveLocation({
      postcode: String(row.postcode || row['Mailing Zip']   || '').trim(),
      suburb:   String(row.suburb   || row['Mailing City']  || '').trim(),
      state:    String(row.state    || row['Mailing State'] || '').trim(),
    });

    if (!location) { stats.noLocation++; continue; }

    // ── Stage → Status mapping ───────────────────────────────────────────────
    const status = mapStageToStatus(stage);

    // ── Date parsing ─────────────────────────────────────────────────────────
    let dateMs = null;
    if (rawDate) {
      const d = new Date(rawDate);
      if (!isNaN(d)) dateMs = d.getTime();
    }

    // ── Summary + Tags (cached by case_id + description hash) ────────────────
    const srcHash = hashText(description);
    const cached  = casesState[caseId];
    const needsNewSummary = !cached || cached.srcHash !== srcHash;

    let summary, tags;

    if (needsNewSummary && summariesThisRun < MAX_SUMMARIES) {
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
      if (!needsNewSummary) stats.reused++;
      else stats.newSummaries++; // Over limit — used fallback
    } else {
      tags    = extractTags(description);
      summary = buildFallbackSummary(description, tags);
      updatedState[caseId] = { summary, tags, srcHash };
      stats.newSummaries++;
    }

    // ── Group by postcode ────────────────────────────────────────────────────
    const pc = location.postcode;
    if (!postcodeMap.has(pc)) {
      postcodeMap.set(pc, { pc, city: location.suburb, state: location.state, lat: location.lat, lng: location.lng, cases: [] });
    }
    postcodeMap.get(pc).cases.push({ s: summary, t: tags, st: status, dateMs });
  }

  // Sort cases within each postcode by date, newest first
  for (const entry of postcodeMap.values()) {
    entry.cases.sort((a, b) => (b.dateMs || 0) - (a.dateMs || 0));
  }

  return { postcodes: Array.from(postcodeMap.values()), updatedState, stats };
}

// ─── ZOHO ANALYTICS: SQL EXPORT (async job pattern) ──────────────────────────
async function zohoAnalyticsSql(sql) {
  if (!ZOHO_TOKEN) throw new Error('ZOHO_TOKEN environment variable not set');

  const cfg  = encodeURIComponent(JSON.stringify({ sqlQuery: sql, responseFormat: 'csv' }));
  const url  = `${ZOHO_API_BASE}/bulk/workspaces/${ZOHO_WS_ID}/exportjobs?CONFIG=${cfg}`;
  const hdrs = { 'Authorization': `Zoho-oauthtoken ${ZOHO_TOKEN}`, 'ZANALYTICS-ORGID': ZOHO_ORG_ID };

  const r = await fetch(url, { method: 'POST', headers: hdrs });
  if (!r.ok) throw new Error(`Analytics create job: HTTP ${r.status}: ${(await r.text()).slice(0, 200)}`);

  const d = await r.json();
  if (!d.data?.jobId) throw new Error('Analytics: no jobId returned');

  return pollAnalyticsJob(d.data.jobId, hdrs);
}

async function pollAnalyticsJob(jobId, hdrs, attempts = 0) {
  if (attempts > 60) throw new Error('Analytics export job timed out');

  const url  = `${ZOHO_API_BASE}/bulk/workspaces/${ZOHO_WS_ID}/exportjobs/${jobId}`;
  const resp = await fetch(url, { headers: hdrs });
  const d    = await resp.json();
  const code = String(d.data?.jobCode || '');

  if (code === '1004') {
    const dl = await fetch(d.data.downloadUrl, { headers: hdrs });
    if (!dl.ok) throw new Error(`Analytics download: HTTP ${dl.status}`);
    return dl.text();
  }
  if (code === '1003' || code === '1005') throw new Error(`Analytics job failed: code ${code}`);

  await sleep(attempts < 5 ? 1000 : 2000);
  return pollAnalyticsJob(jobId, hdrs, attempts + 1);
}

// ─── CSV PARSER ───────────────────────────────────────────────────────────────
function parseCsv(csv) {
  const lines = (csv || '').trim().split('\n');
  if (lines.length < 2) return [];

  const headers = lines[0].split(',').map(h => h.replace(/"/g, '').trim().toLowerCase().replace(/[\s-]+/g, '_'));

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

// ─── NETLIFY BLOBS ────────────────────────────────────────────────────────────
async function loadBlobJson(key) {
  try {
    const raw = await getStore(BLOB_STORE).get(key);
    return raw ? JSON.parse(raw) : null;
  } catch (e) {
    console.warn(`[refresh] loadBlobJson(${key}):`, e.message);
    return null;
  }
}

async function saveBlobJson(key, data) {
  try {
    await getStore(BLOB_STORE).set(key, JSON.stringify(data));
    console.log(`[refresh] Saved: ${key}`);
  } catch (e) {
    console.warn(`[refresh] saveBlobJson(${key}):`, e.message);
  }
}

function sleep(ms) { return new Promise(res => setTimeout(res, ms)); }
