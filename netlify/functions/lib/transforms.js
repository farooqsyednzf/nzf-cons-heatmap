'use strict';

/**
 * lib/transforms.js — NZF data transformation rules
 *
 * Contains all business logic for processing CRM case data:
 * - Domestic violence detection
 * - Stage → Status mapping
 * - Tag extraction (keyword-based)
 * - Summary generation (Claude Sonnet API)
 *
 * ─── PII POLICY ──────────────────────────────────────────────────────────────
 * This pipeline handles sensitive personal data sourced from Zoho CRM and
 * Zoho Analytics. The following rules are absolute and apply to ALL output
 * that leaves this function and is stored in Netlify Blobs or served publicly:
 *
 *   1. RAW TEXT RULE — Raw CRM/Analytics field values (Description, Note_Content,
 *      client names, addresses, phone numbers, email addresses) MUST NEVER be
 *      stored in Blobs or included in any public API response. Only
 *      Claude-reviewed summaries or tag-based fallbacks are permitted.
 *
 *   2. MINIMUM FIELDS RULE — Only the minimum fields needed for map display are
 *      fetched from Zoho. Fields not listed in fetchViewRows column selections
 *      must not be added without a documented public-safety review.
 *
 *   3. SANITISE BEFORE STORE RULE — sanitiseSummary() MUST be called on every
 *      summary string before it is written to the cases state or output Blob.
 *      This is a last-resort safety net for any PII that Claude or the fallback
 *      may have inadvertently included.
 *
 *   4. NO DONOR/CLIENT IDENTITY RULE — Donation and distribution data is
 *      aggregated to postcode level only. No donor names, client names, donor
 *      emails, or individual transaction details are stored or served.
 *
 *   5. NO INTERNAL IDs IN OUTPUT RULE — Internal Zoho record IDs (case ID,
 *      client ID, donor ID) must not appear in any public API response. These
 *      IDs could be used to correlate records across data sources.
 * ─────────────────────────────────────────────────────────────────────────────
 */

const crypto = require('crypto');

// ─── STAGE → STATUS MAPPING ───────────────────────────────────────────────────
const STAGE_TO_STATUS = {
  // Being Assisted
  'Funding':                       'Being Assisted',
  'Ongoing Funding':               'Being Assisted',
  'Post-Follow-Up':                'Being Assisted',
  'Phase 4: Monitoring & Impact':  'Being Assisted',
  // Closed - Not Assisted
  'Closed - Not Funded':           'Closed - Not Assisted',
  'Closed - NO Response':          'Closed - Not Assisted',
  'Gaza-2023':                     'Closed - Not Assisted',
  // Successfully Assisted
  'Closed - Funded':               'Successfully Assisted',
  // Under Assessment
  'Ready for Allocation':          'Under Assessment',
  'Ready For Allocation P2':       'Under Assessment',
  'Allocated':                     'Under Assessment',
  'NM Approval':                   'Under Assessment',
  'Interview':                     'Under Assessment',
  'Waiting On Client':             'Under Assessment',
  'Follow Up':                     'Under Assessment',
  // Pending Review
  'Intake':                        'Pending Review',
};

function mapStageToStatus(stage) {
  if (!stage) return 'Pending Review';
  return STAGE_TO_STATUS[String(stage).trim()] || 'Pending Review';
}

// ─── DOMESTIC VIOLENCE DETECTION ─────────────────────────────────────────────
// Single source of truth for keyword-based DV detection.
// Applied to case Description text in-process.
// A representative subset is also used in the CRM COQL server-side filter
// (see refresh-data.js fetchDvCaseIdsFromCrm).
const DV_PHRASES = [
  'domestic violence',   'family violence',     'dv situation',
  'dv case',             'dv history',           'dv perpetrator',
  'dv order',            'physical abuse',       'sexual abuse',
  'emotional abuse',     'financial abuse',      'verbal abuse',
  'abusive partner',     'abusive husband',      'abusive spouse',
  'abusive relationship','violent partner',      'violent husband',
  'violent ex',          'he was abusive',       'she was abusive',
  'being abused',        'was abused',           'apprehended violence',
  'restraining order',   'intervention order',   'protection order',
  'avo order',           'fleeing violence',      'fled violence',
  'escaping abuse',      'escaped abuse',        'left abusive',
  'left violent',        'safe house',           'refuge placement',
  'crisis accommodation',
];

function hasDvContent(text) {
  if (!text) return false;
  const lower = String(text).toLowerCase();
  return DV_PHRASES.some(phrase => lower.includes(phrase));
}

// ─── TAG EXTRACTION ───────────────────────────────────────────────────────────
// Ordered: more specific tags before generic. Max 3 tags returned.
const TAG_RULES = [
  { tag: 'rent assistance',      kws: ['rent', 'rental', 'landlord', 'lease', 'eviction', 'evict', 'housing payment'] },
  { tag: 'food/groceries',       kws: ['food', 'groceries', 'grocery', 'hungry', 'starving', 'eat', 'meals', 'feed children'] },
  { tag: 'utilities',            kws: ['electricity', 'gas bill', 'water bill', 'utility', 'utilities', 'power bill', 'energy bill', 'disconnected'] },
  { tag: 'school fees',          kws: ['school fees', 'school fee', 'education fees', 'tuition', 'school uniform', 'school supplies', 'stationery'] },
  { tag: 'medical',              kws: ['medical', 'hospital', 'medicine', 'medication', 'surgery', 'health', 'illness', 'sick', 'diagnosis', 'treatment', 'prescription', 'dental'] },
  { tag: 'funeral costs',        kws: ['funeral', 'burial', 'bury', 'deceased', 'passed away', 'death in family', 'coffin', 'janaza'] },
  { tag: 'debt',                 kws: ['debt', 'loan', 'repayment', 'credit card', 'overdue', 'owing', 'centrelink debt', 'owe'] },
  { tag: 'job loss',             kws: ['unemployed', 'lost job', 'job loss', 'redundant', 'laid off', 'retrenchment', 'no income', 'lost employment', "can't work", 'cannot work'] },
  { tag: 'refugee/migrant',      kws: ['refugee', 'migrant', 'asylum seeker', 'visa', 'recently arrived', 'new to australia', 'settled', 'humanitarian'] },
  { tag: 'mental health',        kws: ['mental health', 'depression', 'anxiety', 'ptsd', 'trauma', 'psychological', 'psychiatric', 'suicidal', 'breakdown'] },
  { tag: 'single parent',        kws: ['single mother', 'single mum', 'single father', 'single dad', 'single parent', 'sole parent', 'sole carer', 'no partner'] },
  { tag: 'disability',           kws: ['disability', 'disabled', 'wheelchair', 'impairment', 'cannot work due to', 'chronic illness', 'ndis'] },
  { tag: 'appliances/household', kws: ['fridge', 'washing machine', 'washer', 'appliance', 'furniture', 'heater', 'household item', 'broken appliance'] },
  { tag: 'fuel/transport',       kws: ['fuel', 'petrol', 'transport', 'car registration', 'public transport', 'bus fare', 'travel costs'] },
  { tag: 'housing',              kws: ['homeless', 'homelessness', 'no home', 'sleeping rough', 'accommodation', 'shelter', 'boarding', 'couch surfing'] },
  { tag: 'emergency relief',     kws: ['emergency', 'urgent', 'immediate', 'crisis situation', 'desperate'] },
  { tag: 'family hardship',      kws: ['family hardship', 'family struggling', 'hardship', 'financial difficulty', 'financial crisis', 'financial stress'] },
];

function extractTags(description) {
  if (!description) return [];
  const text  = String(description).toLowerCase();
  const found = [];
  for (const rule of TAG_RULES) {
    if (rule.kws.some(kw => text.includes(kw))) {
      found.push(rule.tag);
    }
    if (found.length >= 3) break;
  }
  return found;
}

// ─── DESCRIPTION HASHING (change detection) ──────────────────────────────────
function hashText(text) {
  return crypto.createHash('md5').update(String(text || '')).digest('hex').slice(0, 12);
}

// ─── SUMMARY GENERATION (Claude Sonnet) ──────────────────────────────────────
const SUMMARY_SYSTEM_PROMPT = `You write short anonymised descriptions of Zakat applications for a public map in Australia. Each description is one sentence about one person or family asking for help.

CRITICAL PRIVACY RULES — these override everything else:
- NEVER include any name (first name, last name, or nickname)
- NEVER include any location more specific than the type of area (e.g. never a suburb, street, or city name)
- NEVER include phone numbers, email addresses, or any contact detail
- NEVER include dollar amounts, specific debt figures, or financial account details
- NEVER include ages, dates of birth, or any identifier tied to a specific individual
- NEVER include employer names, school names, mosque names, or community organisation names
- NEVER include medical diagnoses, case numbers, reference numbers, or government ID numbers
- If the description contains any of the above, describe only the general situation — not the specifics

Writing rules:
- One sentence only, under 22 words
- Start with a role: Brother, Sister, Mother, Father, Student, Single mother, Single father, Refugee family, Elderly person, Family, Couple. Use "Applicant" only if nothing else fits. Never use "Individual"
- Say what the problem is, then what could happen if they do not get help
- Use plain everyday words. Write like a person talking, not a report
- No long dashes mid-sentence. Use a comma instead
- No jargon. Avoid: navigating, ensuring, fostering, vital, crucial, holistic, multifaceted, testament, leverage, seeking to, deterioration, procure, obtain

Good examples:
"Single mother fell behind on rent after losing her job and could lose her home."
"Father cannot afford to pay for his daughter's funeral without help."
"Elderly woman cannot buy her medication and her health is getting worse each week."
"Refugee family just arrived and cannot pay rent while they wait to be allowed to work."
"Brother lost his job and has no money left to feed his children."`;

async function generateSummary(description) {
  if (!process.env.ANTHROPIC_API_KEY) return null;
  if (!description || description.trim().length < 20) {
    return 'Person in financial difficulty and asking for help covering basic costs.';
  }

  try {
    const resp = await fetch('https://api.anthropic.com/v1/messages', {
      method:  'POST',
      headers: {
        'Content-Type':      'application/json',
        'x-api-key':         process.env.ANTHROPIC_API_KEY,
        'anthropic-version': '2023-06-01',
      },
      body: JSON.stringify({
        model:      'claude-sonnet-4-6',
        max_tokens: 150,
        system:     SUMMARY_SYSTEM_PROMPT,
        messages:   [{ role: 'user', content: 'Application description:\n' + description.slice(0, 1500) }],
      }),
    });

    if (!resp.ok) {
      throw new Error('Claude API HTTP ' + resp.status);
    }

    const data  = await resp.json();
    const block = data.content && data.content[0];
    const text  = (block && block.text) ? block.text : '';
    return text.trim().replace(/^"|"$/g, '') || null;
  } catch (e) {
    console.warn('[transforms] generateSummary failed:', e.message);
    return null;
  }
}

// ─── FALLBACK SUMMARY ─────────────────────────────────────────────────────────
// Used when Claude is unavailable or when a case hasn't been processed yet.
// IMPORTANT: when description is null, only tag-based text is used — raw CRM
// description text is NEVER exposed on the public map without Claude review.
function buildFallbackSummary(description, tags) {
  // If we have tags, build a natural-language sentence from them
  if (tags && tags.length > 0) {
    const needsMap = {
      'rent assistance':      'help paying rent',
      'food/groceries':       'help buying food',
      'utilities':            'help paying utility bills',
      'school fees':          'help with school fees',
      'medical':              'help with medical costs',
      'funeral costs':        'help with funeral costs',
      'debt':                 'help managing debt',
      'job loss':             'support after losing work',
      'refugee/migrant':      'support settling in Australia',
      'mental health':        'mental health support',
      'single parent':        'support as a single parent',
      'disability':           'support due to disability',
      'appliances/household': 'help replacing household items',
      'fuel/transport':       'help with transport costs',
      'housing':              'help finding stable housing',
      'emergency relief':     'emergency assistance',
      'family hardship':      'support through family hardship',
    };
    const needs = tags.map(t => needsMap[t] || t).slice(0, 2);
    return 'Family asking for ' + needs.join(' and ') + '.';
  }

  // No tags and no description — fully generic safe fallback
  return 'Family struggling to pay for basic needs and asking for help.';
}


// ─── PII SANITISER ────────────────────────────────────────────────────────────
// Last-resort safety net applied to every summary before public storage.
// Strips common PII patterns that may have slipped through Claude or the
// fallback. Called in refresh-data.js on every case summary before Blob write.
//
// This is a belt-and-braces check — the primary protection is:
//   (a) Claude's system prompt privacy rules
//   (b) buildFallbackSummary never using raw description text
// Do not rely on this sanitiser as the sole PII control.

const PII_PATTERNS = [
  // Email addresses
  { re: /[\w.+\-]+@[\w\-]+\.[\w.]+/gi,                      sub: '[contact]' },
  // Australian mobile numbers (04xx xxx xxx variants)
  { re: /\b04\d{2}[\s\-]?\d{3}[\s\-]?\d{3}\b/g,           sub: '[phone]'   },
  // Australian landlines (+61 / 0x variants)
  { re: /(\+?61\s?|0)[2-9][\s\-]?\d{4}[\s\-]?\d{4}/g,     sub: '[phone]'   },
  // Dollar amounts with figures (e.g. $1,200 or $450.00)
  { re: /\$[\d,]+(\.\d{1,2})?/g,                               sub: '[amount]'  },
  // Amounts written as words with figures (e.g. "1,200 dollars")
  { re: /\b\d[\d,]*\s*dollars?\b/gi,                           sub: '[amount]'  },
  // Australian postcodes appearing inline (4-digit, standalone)
  { re: /\b\d{4}\b/g,                                            sub: '[area]'    },
  // Medicare / Centrelink / CRN style numbers (10-digit blocks)
  { re: /\b\d{10}\b/g,                                           sub: '[ref]'     },
  // BSB format (000-000)
  { re: /\b\d{3}-\d{3}\b/g,                                     sub: '[bsb]'     },
  // Common name + surname pattern (Title-case word pairs)
  { re: /\b[A-Z][a-z]{1,20}\s[A-Z][a-z]{1,20}\b/g,             sub: '[name]'    },
];

function sanitiseSummary(text) {
  if (!text) return text;
  let out = String(text);
  for (const { re, sub } of PII_PATTERNS) {
    out = out.replace(re, sub);
  }
  return out;
}

module.exports = {
  mapStageToStatus,
  hasDvContent,
  extractTags,
  hashText,
  generateSummary,
  buildFallbackSummary,
  sanitiseSummary,
};
