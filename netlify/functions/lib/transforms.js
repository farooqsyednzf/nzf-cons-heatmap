'use strict';

/**
 * lib/transforms.js — NZF data transformation rules
 *
 * Contains all business logic for processing CRM case data:
 * - Domestic violence detection
 * - Stage → Status mapping
 * - Tag extraction (keyword-based)
 * - Summary generation (Claude Haiku API)
 */

const crypto = require('crypto');

// ─── STAGE → STATUS MAPPING ───────────────────────────────────────────────────
const STAGE_TO_STATUS = {
  // Being Assisted
  'Funding':                        'Being Assisted',
  'Ongoing Funding':                'Being Assisted',
  'Post-Follow-Up':                 'Being Assisted',
  'Phase 4: Monitoring & Impact':   'Being Assisted',
  // Closed - Not Assisted
  'Closed - Not Funded':            'Closed - Not Assisted',
  'Closed - NO Response':           'Closed - Not Assisted',
  'Gaza-2023':                      'Closed - Not Assisted',
  // Successfully Assisted
  'Closed - Funded':                'Successfully Assisted',
  // Under Assessment
  'Ready for Allocation':           'Under Assessment',
  'Ready For Allocation P2':        'Under Assessment',
  'Allocated':                      'Under Assessment',
  'NM Approval':                    'Under Assessment',
  'Interview':                      'Under Assessment',
  'Waiting On Client':              'Under Assessment',
  'Follow Up':                      'Under Assessment',
  // Pending Review
  'Intake':                         'Pending Review',
};

function mapStageToStatus(stage) {
  if (!stage) return 'Pending Review';
  const s = String(stage).trim();
  return STAGE_TO_STATUS[s] || 'Pending Review';
}

// ─── DOMESTIC VIOLENCE DETECTION ─────────────────────────────────────────────
// Applied to both description text and (via CRM COQL) case notes.
// This list is the single source of truth for keyword-based DV detection.
// The COQL query in refresh-data.js uses a representative subset for server-side
// filtering; this full list is applied in-process to description text.
const DV_PHRASES = [
  // Specific DV terms
  'domestic violence',
  'family violence',
  'dv situation',
  'dv case',
  'dv history',
  'dv perpetrator',
  'dv order',
  // Abuse types
  'physical abuse',
  'sexual abuse',
  'emotional abuse',
  'financial abuse',
  'verbal abuse',
  // Relationship violence
  'abusive partner',
  'abusive husband',
  'abusive spouse',
  'abusive relationship',
  'violent partner',
  'violent husband',
  'violent ex',
  'he was abusive',
  'she was abusive',
  'being abused',
  'was abused',
  // Orders / legal
  'apprehended violence',
  'restraining order',
  'intervention order',
  'protection order',
  'avo order',
  // Escape / fleeing
  'fleeing violence',
  'fled violence',
  'escaping abuse',
  'escaped abuse',
  'left abusive',
  'left violent',
  'safe house',
  'refuge placement',
  'crisis accommodation',
];

function hasDvContent(text) {
  if (!text) return false;
  const lower = String(text).toLowerCase();
  return DV_PHRASES.some(phrase => lower.includes(phrase));
}

// ─── TAG EXTRACTION ───────────────────────────────────────────────────────────
// Order matters — more specific tags before generic ones
// Max 3 tags returned
const TAG_RULES = [
  // Specific needs first
  { tag: 'rent assistance',       kws: ['rent', 'rental', 'landlord', 'lease', 'eviction', 'evict', 'housing payment'] },
  { tag: 'food/groceries',        kws: ['food', 'groceries', 'grocery', 'hungry', 'starving', 'eat', 'meals', 'feed children'] },
  { tag: 'utilities',             kws: ['electricity', 'gas bill', 'water bill', 'utility', 'utilities', 'power bill', 'energy bill', 'disconnected'] },
  { tag: 'school fees',           kws: ['school fees', 'school fee', 'education fees', 'tuition', 'school uniform', 'school supplies', 'stationery'] },
  { tag: 'medical',               kws: ['medical', 'hospital', 'medicine', 'medication', 'surgery', 'health', 'illness', 'sick', 'diagnosis', 'treatment', 'prescription', 'dental'] },
  { tag: 'funeral costs',         kws: ['funeral', 'burial', 'bury', 'deceased', 'passed away', 'death in family', 'coffin', 'janaza'] },
  { tag: 'debt',                  kws: ['debt', 'loan', 'repayment', 'credit card', 'overdue', 'owing', 'centrelink debt', 'owe'] },
  { tag: 'job loss',              kws: ['unemployed', 'lost job', 'job loss', 'redundant', 'laid off', 'retrenchment', 'no income', 'lost employment', 'can\'t work', 'cannot work'] },
  { tag: 'refugee/migrant',       kws: ['refugee', 'migrant', 'asylum seeker', 'visa', 'recently arrived', 'new to australia', 'settled', 'humanitarian'] },
  { tag: 'mental health',         kws: ['mental health', 'depression', 'anxiety', 'ptsd', 'trauma', 'psychological', 'psychiatric', 'suicidal', 'breakdown'] },
  { tag: 'single parent',         kws: ['single mother', 'single mum', 'single father', 'single dad', 'single parent', 'sole parent', 'sole carer', 'no partner'] },
  { tag: 'disability',            kws: ['disability', 'disabled', 'wheelchair', 'impairment', 'cannot work due to', 'chronic illness', 'ndis'] },
  { tag: 'appliances/household',  kws: ['fridge', 'washing machine', 'washer', 'appliance', 'furniture', 'heater', 'household item', 'broken appliance'] },
  { tag: 'fuel/transport',        kws: ['fuel', 'petrol', 'transport', 'car registration', 'public transport', 'bus fare', 'travel costs'] },
  // Broader tags last
  { tag: 'housing',               kws: ['homeless', 'homelessness', 'no home', 'sleeping rough', 'accommodation', 'shelter', 'boarding', 'couch surfing'] },
  { tag: 'emergency relief',      kws: ['emergency', 'urgent', 'immediate', 'crisis situation', 'desperate'] },
  { tag: 'family hardship',       kws: ['family hardship', 'family struggling', 'hardship', 'financial difficulty', 'financial crisis', 'financial stress'] },
];

function extractTags(description) {
  if (!description) return [];
  const text = String(description).toLowerCase();
  const found = [];
  for (const rule of TAG_RULES) {
    if (rule.kws.some(kw => text.includes(kw))) {
      found.push(rule.tag);
    }
    if (found.length >= 3) break;
  }
  return found;
}

// ─── DESCRIPTION HASHING (change detection) ───────────────────────────────────
function hashText(text) {
  return crypto.createHash('md5').update(String(text || '')).digest('hex').slice(0, 12);
}

// ─── SUMMARY GENERATION (Claude Haiku) ────────────────────────────────────────
const SUMMARY_SYSTEM_PROMPT = `You write short descriptions of Zakat applications for a public map in Australia. Each description is one sentence about one person or family asking for help.

Rules:
- One sentence only, under 22 words
- No names, ages, locations, dollar amounts or any detail that could identify the person
- Start with a role: Brother, Sister, Mother, Father, Student, Single mother, Single father, Refugee family, Elderly person, Family, Couple. Use "Applicant" only if nothing else fits. Never use "Individual"
- Say what the problem is, then what could happen if they do not get help
- Use plain everyday words. Write like a person talking, not a report
- No long dashes (the dash character used mid-sentence). Use a comma instead
- No jargon. Avoid words like: navigating, ensuring, fostering, vital, crucial, holistic, multifaceted, testament, leverage, seeking to, deterioration, procure, obtain

Good examples:
"Single mother fell behind on rent after losing her job and could lose her home."
"Father cannot afford to pay for his daughter's funeral without help."
"Elderly woman cannot buy her medication and her health is getting worse each week."
"Refugee family just arrived and cannot pay rent while they wait to be allowed to work."
"Brother lost his job and has no money left to feed his children."`;

async function generateSummary(description) {
  if (!process.env.ANTHROPIC_API_KEY) {
    return null;
  }
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
      const err = await resp.text();
      throw new Error('Claude API ' + resp.status + ': ' + err.slice(0, 200));
    }

    const data = await resp.json();
    var arr   = data.content;
    var block = arr && arr[0];
    var text  = (block && block.text) ? block.text : '';
    return text.trim().replace(/^"|"$/g, '') || null;
  } catch (e) {
    console.warn('[transforms] generateSummary failed:', e.message);
    return null;
  }
}

// Keyword-based fallback when Claude is unavailable or times out
function buildFallbackSummary(description, tags) {
  if (!description || description.trim().length < 10) {
    return 'Family struggling to pay for basic needs and asking for help.';
  }
  // Truncate and clean description to a safe public summary
  const clean = description
    .replace(/\b[A-Z][a-z]+ [A-Z][a-z]+\b/g, '[Name]') // remove full names
    .replace(/[\w.+-]+@[\w-]+\.[\w.]+/gi, '[email]')     // remove emails
    .replace(/(\+?61\s?|0)[2-9][\s-]?\d{4}[\s-]?\d{4}/g, '[phone]') // AU phones
    .replace(/\b04\d{2}[\s-]?\d{3}[\s-]?\d{3}\b/g, '[phone]')       // mobiles
    .slice(0, 120)
    .trim();
  return clean.endsWith('.') ? clean : clean + '.';
}

module.exports = {
  mapStageToStatus,
  hasDvContent,
  extractTags,
  hashText,
  generateSummary,
  buildFallbackSummary,
};
