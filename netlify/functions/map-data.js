'use strict';

/**
 * map-data.js — NZF Community Map data endpoint
 *
 * Serves pre-processed map data from Netlify Blobs.
 * Data is populated daily by refresh-data.js (scheduled function).
 *
 * Cache hierarchy:
 *   1. In-memory (warm Lambda reuse, ~0ms)
 *   2. Netlify Blobs (~100ms)
 *   3. Empty scaffold (Blobs not yet populated — first deploy)
 */

const BLOB_STORE  = 'nzf-map';
const BLOB_OUTPUT = 'aggregated-v2';

const MEM_TTL = 23 * 60 * 60 * 1000; // 23 hours
let _memCache   = null;
let _memCacheTs = 0;

exports.handler = async (event) => {
  if (!['GET', 'HEAD'].includes(event.httpMethod)) {
    return { statusCode: 405, body: 'Method Not Allowed' };
  }

  const headers = buildCorsHeaders(event);

  if (event.httpMethod === 'HEAD') {
    return { statusCode: 200, headers, body: '' };
  }

  // ── 1. In-memory cache ─────────────────────────────────────────────────────
  if (_memCache && Date.now() - _memCacheTs < MEM_TTL) {
    return ok(_memCache, headers, 'MEM');
  }

  // ── 2. Netlify Blobs ───────────────────────────────────────────────────────
  try {
    const { getStore } = require('@netlify/blobs');
    const store        = getStore(BLOB_STORE);
    const raw          = await store.get(BLOB_OUTPUT);

    if (raw) {
      const parsed = JSON.parse(raw);
      const age    = Date.now() - new Date(parsed.generatedAt || 0).getTime();
      if (age < MEM_TTL) {
        _memCache   = parsed;
        _memCacheTs = Date.now();
        return ok(parsed, headers, 'BLOB');
      }
    }
  } catch (e) {
    console.log('[map-data] Blobs unavailable:', e.message);
  }

  // ── 3. Empty scaffold (initial deploy — data not yet generated) ────────────
  // Returning an empty but valid response so the map renders while refresh runs.
  const scaffold = {
    generatedAt:   new Date().toISOString(),
    postcodes:     [],
    donations:     {},
    distributions: {},
    _status:       'initialising',
    _message:      'Daily data refresh is running. Data will appear within a few minutes.',
  };
  return ok(scaffold, headers, 'EMPTY');
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
  const origin  = (event.headers?.origin || event.headers?.referer || '');
  const allowed = (process.env.ALLOWED_ORIGINS || 'https://nzf.org.au,https://www.nzf.org.au')
    .split(',').map(s => s.trim());

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
