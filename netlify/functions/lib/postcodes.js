'use strict';

/**
 * lib/postcodes.js — Australian postcode lookup utility
 *
 * Provides fast in-memory lookups from a bundled JSON dataset.
 * Dataset format: [ [postcode, suburb, state, lat, lng], ... ]
 *
 * Generate the dataset: node scripts/build-postcodes.js
 * Source: https://github.com/matthewproctor/australianpostcodes
 */

let _byPostcode = null; // Map<postcode, { suburb, state, lat, lng }>
let _bySuburb   = null; // Map<"SUBURB|STATE", postcode>

function load() {
  if (_byPostcode) return; // already loaded
  try {
    const raw   = require('../data/au-postcodes.json');
    _byPostcode = new Map();
    _bySuburb   = new Map();

    for (const [pc, suburb, state, lat, lng] of raw) {
      const key = String(pc).padStart(4, '0');

      if (!_byPostcode.has(key)) {
        _byPostcode.set(key, { suburb: String(suburb), state: String(state), lat, lng });
      }

      const suburbKey = `${String(suburb).toUpperCase()}|${String(state).toUpperCase()}`;
      if (!_bySuburb.has(suburbKey)) {
        _bySuburb.set(suburbKey, key);
      }
    }
    console.log(`[postcodes] Loaded ${_byPostcode.size} postcodes, ${_bySuburb.size} suburb entries`);
  } catch (e) {
    console.warn('[postcodes] Could not load au-postcodes.json:', e.message);
    console.warn('[postcodes] Run: node scripts/build-postcodes.js');
    _byPostcode = new Map();
    _bySuburb   = new Map();
  }
}

/**
 * Look up a postcode to get suburb, state, lat, lng.
 * @param {string} postcode
 * @returns {{ suburb: string, state: string, lat: number, lng: number } | null}
 */
function lookupPostcode(postcode) {
  load();
  if (!postcode) return null;
  return _byPostcode.get(String(postcode).trim().padStart(4, '0')) || null;
}

/**
 * Find a postcode from suburb + optional state.
 * Used as fallback when a client record has suburb but no postcode.
 * @param {string} suburb
 * @param {string} [state]
 * @returns {string | null}
 */
function findPostcodeBySuburb(suburb, state) {
  load();
  if (!suburb) return null;

  const suburbUp = String(suburb).trim().toUpperCase();
  const stateUp  = state ? String(state).trim().toUpperCase() : null;

  // Exact suburb + state match
  if (stateUp) {
    const match = _bySuburb.get(`${suburbUp}|${stateUp}`);
    if (match) return match;
  }

  // Suburb-only fallback across all states
  for (const [key, pc] of _bySuburb) {
    if (key.startsWith(`${suburbUp}|`)) return pc;
  }
  return null;
}

/**
 * Get the primary suburb name for a postcode.
 * @param {string} postcode
 * @returns {string | null}
 */
function getSuburb(postcode) {
  const info = lookupPostcode(postcode);
  return info ? info.suburb : null;
}

/**
 * Resolve full location from partial data.
 * Priority: postcode forward lookup → suburb+state reverse lookup.
 * @param {{ postcode?: string, suburb?: string, state?: string }} input
 * @returns {{ postcode: string, suburb: string, state: string, lat: number, lng: number } | null}
 */
function resolveLocation({ postcode, suburb, state } = {}) {
  load();

  // 1. Forward lookup from postcode
  if (postcode && postcode.trim()) {
    const info = lookupPostcode(postcode.trim());
    if (info) {
      return {
        postcode: String(postcode).trim().padStart(4, '0'),
        suburb:   suburb || info.suburb,
        state:    state  || info.state,
        lat:      info.lat,
        lng:      info.lng,
      };
    }
  }

  // 2. Reverse lookup from suburb + state
  if (suburb && suburb.trim()) {
    const pc   = findPostcodeBySuburb(suburb.trim(), state);
    const info = pc ? lookupPostcode(pc) : null;
    if (info) {
      return {
        postcode: pc,
        suburb:   suburb.trim(),
        state:    state || info.state,
        lat:      info.lat,
        lng:      info.lng,
      };
    }
  }

  return null;
}

module.exports = { lookupPostcode, findPostcodeBySuburb, getSuburb, resolveLocation };
