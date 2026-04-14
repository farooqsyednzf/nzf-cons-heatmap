'use strict';

/**
 * map-data.js — NZF Community Map data endpoint
 *
 * Serves pre-processed map data from Netlify Blobs.
 * Data is populated daily by refresh-data-background.js (scheduled function).
 *
 * Uses raw HTTP fetch against the Netlify Blobs REST API directly,
 * bypassing the @netlify/blobs SDK to avoid context/namespace issues.
 * Store path: site:nzf-map/aggregated-v2
 */

const BLOB_API    = 'https://api.netlify.com/api/v1/blobs';
const SITE_ID     = process.env.NETLIFY_SITE_ID;
const BLOB_TOKEN  = process.env.NETLIFY_BLOBS_TOKEN || process.env.NETLIFY_TOKEN;
const STORE_PATH  = 'site:nzf-map';  // SDK adds "site:" prefix to store names
const BLOB_OUTPUT = 'aggregated-v2';

// In-memory cache: survives across warm Lambda reuses
let _memCache       = null;
let _memGeneratedAt = null;

exports.handler = async (event) => {
  const method = event.httpMethod;

  if (method === 'OPTIONS') {
    return { statusCode: 204, headers: { ...buildCorsHeaders(event), 'Access-Control-Max-Age': '86400' }, body: '' };
  }

  if (!['GET', 'HEAD'].includes(method)) {
    return { statusCode: 405, headers: { Allow: 'GET, HEAD, OPTIONS' }, body: 'Method Not Allowed' };
  }

  const headers = buildCorsHeaders(event);
  if (method === 'HEAD') return { statusCode: 200, headers, body: '' };

  // ── 1. In-memory cache ───────────────────────────────────────────────────────
  if (_memCache) {
    return ok(_memCache, headers, 'MEM');
  }

  // ── 2. Netlify Blobs (raw HTTP — no SDK) ─────────────────────────────────────
  if (BLOB_TOKEN && SITE_ID) {
    try {
      const url  = `${BLOB_API}/${SITE_ID}/${STORE_PATH}/${BLOB_OUTPUT}`;
      const resp = await fetch(url, {
        headers: { 'Authorization': `Bearer ${BLOB_TOKEN}` },
      });

      if (resp.ok) {
        const parsed = await resp.json();

        // Serve whatever is in the blob — age does not matter.
        // The scheduled function keeps it fresh; serving old data
        // is always better than serving nothing.
        if (!_memGeneratedAt || parsed.generatedAt !== _memGeneratedAt) {
          _memCache       = parsed;
          _memGeneratedAt = parsed.generatedAt;
        }
        return ok(parsed, headers, 'BLOB');
      } else {
        console.log('[map-data] Blob fetch HTTP', resp.status);
      }
    } catch (e) {
      console.log('[map-data] Blob fetch error:', e.message);
    }
  } else {
    console.log('[map-data] NETLIFY_BLOBS_TOKEN not set — serving scaffold');
  }

  // ── 3. Empty scaffold ────────────────────────────────────────────────────────
  return ok({
    generatedAt:   new Date().toISOString(),
    postcodes:     [],
    donations:     {},
    distributions: {},
    _status:       'initialising',
  }, headers, 'EMPTY');
};

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
