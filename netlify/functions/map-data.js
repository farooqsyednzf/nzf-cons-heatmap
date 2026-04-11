'use strict';

/**
 * map-data.js — NZF Community Map data endpoint
 *
 * Serves pre-processed map data from Netlify Blobs.
 * Data is populated daily by refresh-data.js (scheduled function).
 *
 * Cache hierarchy (fastest to slowest):
 *   1. In-memory (warm Lambda reuse, ~0ms)
 *   2. Netlify Blobs (~100ms)
 *   3. Empty scaffold (first deploy, before any refresh has run)
 */

const BLOB_STORE  = 'nzf-map';
const BLOB_OUTPUT = 'aggregated-v2';

// In-memory cache: survives across warm Lambda reuses within the same invocation hour.
// We store generatedAt alongside the data so we can invalidate when fresh data arrives.
let _memCache       = null;
let _memGeneratedAt = null;

exports.handler = async (event) => {
  const method = event.httpMethod;

  // Handle CORS preflight
  if (method === 'OPTIONS') {
    return {
      statusCode: 204,
      headers: {
        ...buildCorsHeaders(event),
        'Access-Control-Max-Age': '86400',
      },
      body: '',
    };
  }

  if (!['GET', 'HEAD'].includes(method)) {
    return { statusCode: 405, headers: { Allow: 'GET, HEAD, OPTIONS' }, body: 'Method Not Allowed' };
  }

  const headers = buildCorsHeaders(event);

  if (method === 'HEAD') {
    return { statusCode: 200, headers, body: '' };
  }

  // ── 1. In-memory cache ─────────────────────────────────────────────────────
  if (_memCache) {
    return ok(_memCache, headers, 'MEM');
  }

  // ── 2. Netlify Blobs ───────────────────────────────────────────────────────
  try {
    const { getStore } = require('@netlify/blobs');

    // Use explicit credentials when available (for "Run now" invocations that
    // don't have the Netlify context auto-injected)
    const siteID = process.env.NETLIFY_SITE_ID;
    const token  = process.env.NETLIFY_BLOBS_TOKEN || process.env.NETLIFY_TOKEN;
    const store  = (siteID && token)
      ? getStore({ name: BLOB_STORE, siteID, token })
      : getStore(BLOB_STORE);

    const raw = await store.get(BLOB_OUTPUT);

    if (raw) {
      const parsed = JSON.parse(raw);

      // Accept data up to 26 hours old. The 4am refresh + 15-minute runtime means
      // freshest data is ~4:15am. A user at 3:59am the next day is 23h44m later —
      // well within 26h. This prevents a 1-hour daily window of empty responses.
      const ageMs = Date.now() - new Date(parsed.generatedAt || 0).getTime();
      if (ageMs < 26 * 60 * 60 * 1000) {
        // Only cache in memory if it's the same data we already have, or we have none.
        // This prevents serving stale in-memory data after the daily refresh runs.
        if (!_memGeneratedAt || parsed.generatedAt !== _memGeneratedAt) {
          _memCache       = parsed;
          _memGeneratedAt = parsed.generatedAt;
        }
        return ok(parsed, headers, 'BLOB');
      }
    }
  } catch (e) {
    console.log('[map-data] Blobs unavailable:', e.message);
  }

  // ── 3. Empty scaffold ──────────────────────────────────────────────────────
  // Returned on first deploy before any refresh has run.
  // The client checks for _status: 'initialising' and shows a loading message.
  return ok({
    generatedAt:   new Date().toISOString(),
    postcodes:     [],
    donations:     {},
    distributions: {},
    _status:       'initialising',
  }, headers, 'EMPTY');
};

// ─── HELPERS ─────────────────────────────────────────────────────────────────
function ok(data, headers, cacheHit) {
  return {
    statusCode: 200,
    headers: {
      ...headers,
      'Content-Type':  'application/json; charset=utf-8',
      'Cache-Control': 'public, max-age=3600, stale-while-revalidate=86400',
      'X-Cache':       cacheHit,
    },
    body: JSON.stringify(data),
  };
}

function buildCorsHeaders(event) {
  const origin  = (event.headers && (event.headers.origin || event.headers.referer)) || '';
  const allowed = (process.env.ALLOWED_ORIGINS || 'https://nzf.org.au,https://www.nzf.org.au')
    .split(',')
    .map(s => s.trim());

  if (process.env.NODE_ENV === 'development') {
    allowed.push('http://localhost:8888', 'http://localhost:3000');
  }

  const corsOrigin = allowed.find(o => origin.startsWith(o)) || allowed[0];

  return {
    'Access-Control-Allow-Origin':  corsOrigin,
    'Access-Control-Allow-Methods': 'GET, HEAD, OPTIONS',
    'Vary':                         'Origin',
  };
}
