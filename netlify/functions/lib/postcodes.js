'use strict';

/**
 * lib/postcodes.js — Australian postcode lookup utility
 *
 * Provides fast in-memory lookups from a bundled JSON dataset.
 * Format: [ [postcode, suburb, state, lat, lng], ... ]
 *
 * To generate data/au-postcodes.json, run: node scripts/build-postcodes.js
 * Source: https://www.matthewproctor.com/australian_postcodes
 */

const path = require('path');

let _byPostcode = null; // Map<postcode_string, { suburb, state, lat, lng }>
let _bySuburb   = null; // Map<"suburb|state", postcode_string>

function load() {
  if (_byPostcode) return;
  try {
    const raw  = require('../data/au-postcodes.json');
    _byPostcode = new Map();
    _bySuburb   = new Map();

    for (const [pc, suburb, state, lat, lng] of raw) {
      const key = String(pc).padStart(4, '0');
      // Keep only first entry per postcode (primary suburb)
      if (!_byPostcode.has(key)) {
        _byPostcode.set(key, { suburb: String(suburb), state: String(state), lat, lng });
      }
      // Reverse lookup: suburb+state → first postcode
      const suburbKey = `${String(suburb).toUpperCase()}|${String(state).toUpperCase()}`;
      if (!_bySuburb.has(suburbKey)) {
        _bySuburb.set(suburbKey, key);
      }
    }
    console.log(`[postcodes] Loaded ${_byPostcode.size} postcodes, ${_bySuburb.size} suburb entries`);
  } catch (e) {
    console.warn('[postcodes] Could not load au-postcodes.json:', e.message);
    console.warn('[postcodes] Run `node scripts/build-postcodes.js` to generate the data file.');
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
  const key = String(postcode).trim().padStart(4, '0');
  return _byPostcode.get(key) || null;
}

/**
 * Find a postcode from suburb + optional state.
 * Used when a client record has suburb but no postcode.
 * @param {string} suburb
 * @param {string} [state]
 * @returns {string | null} 4-digit postcode string or null
 */
function findPostcodeBySuburb(suburb, state) {
  load();
  if (!suburb) return null;
  const suburbUp = String(suburb).trim().toUpperCase();
  const stateUp  = state ? String(state).trim().toUpperCase() : null;

  // Try exact suburb + state match first
  if (stateUp) {
    const match = _bySuburb.get(`${suburbUp}|${stateUp}`);
    if (match) return match;
  }

  // Fall back to suburb-only search across all states
  for (const [key, pc] of _bySuburb.entries()) {
    if (key.startsWith(`${suburbUp}|`)) return pc;
  }
  return null;
}

/**
 * Get the primary suburb name for a postcode.
 * Useful for enriching donation records that only have a postcode.
 * @param {string} postcode
 * @returns {string | null}
 */
function getSuburb(postcode) {
  const info = lookupPostcode(postcode);
  return info ? info.suburb : null;
}

/**
 * Resolve full location info for a case that may have partial data.
 * Priority: postcode (forward lookup) → suburb+state (reverse lookup)
 * @param {{ postcode?: string, suburb?: string, state?: string }} input
 * @returns {{ postcode: string, suburb: string, state: string, lat: number, lng: number } | null}
 */
function resolveLocation(input) {
  load();
  const { postcode, suburb, state } = input || {};

  // 1. Forward lookup from postcode
  if (postcode && postcode.trim()) {
    const info = lookupPostcode(postcode.trim());
    if (info) {
      return {
        postcode: String(postcode).trim().padStart(4, '0'),
        suburb:   suburb || info.suburb,  // prefer CRM suburb if available
        state:    state  || info.state,
        lat:      info.lat,
        lng:      info.lng,
      };
    }
  }

  // 2. Reverse lookup from suburb + state
  if (suburb && suburb.trim()) {
    const pc = findPostcodeBySuburb(suburb.trim(), state);
    if (pc) {
      const info = lookupPostcode(pc);
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
  }

  return null; // Cannot determine location
}

module.exports = { lookupPostcode, findPostcodeBySuburb, getSuburb, resolveLocation };
