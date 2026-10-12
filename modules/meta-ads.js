'use strict';

// Manual marketing records only. This module never calls Meta or changes an ad.
// It keeps its own file on the existing runtime volume and never writes accounts,
// inventory, or procurement files.
const express = require('express');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { rolesOf } = require('./auth-users');

const BASE = '/api/marketing/meta';
const VERSION = 1;
const PROCESS_INSTANCE = crypto.randomUUID();
const TYPES = { campaigns: 'campaigns', adsets: 'adSets', ads: 'ads', results: 'results' };
const ENTITY = { campaigns: 'campaign', adSets: 'adSet', ads: 'ad', results: 'result' };
const STATUSES = new Set(['draft', 'reported_running', 'delivery_unconfirmed', 'active', 'paused', 'completed', 'archived', 'unknown', 'pending']);
const METRICS = ['spend', 'impressions', 'reach', 'linkClicks', 'profileVisits', 'messagingStarts', 'qualifiedEnquiries', 'orders', 'revenue', 'storeVisits'];
const MONEY = new Set(['budgetDaily', 'spend', 'revenue']);
const SCHEMAS = {
  campaigns: {
    name: ['text', 200, true], objective: ['text', 120], businessGoal: ['text', 500],
    status: ['status'], adAccountId: ['text', 120], notes: ['text', 5000]
  },
  adSets: {
    campaignId: ['id', true], name: ['text', 200, true], collection: ['text', 80],
    budgetDaily: ['number'], conversionLocation: ['text', 160], performanceGoal: ['text', 250],
    destination: ['text', 160], destinationRef: ['reference'], facebookPage: ['text', 200],
    instagramProfile: ['text', 160], whatsappNumber: ['phone'], locations: ['locations'],
    ageMin: ['age'], ageMax: ['age'], gender: ['gender'], interests: ['list', 150],
    interestsConfirmed: ['booleanOrNull'], languages: ['list', 120], placements: ['list', 120],
    placementsConfirmed: ['booleanOrNull'], audienceMode: ['text', 160],
    frequencyCount: ['count'], frequencyDays: ['count'], frequencyMode: ['text', 100],
    status: ['status'], confirmedOn: ['date'], notes: ['text', 5000]
  },
  ads: {
    adSetId: ['id', true], name: ['text', 200, true], model: ['text', 160],
    format: ['text', 100], description: ['text', 5000], mediaUrls: ['urls'],
    mediaCount: ['count'], cta: ['text', 150], primaryText: ['text', 10000],
    status: ['status'], notes: ['text', 5000]
  },
  results: {
    adSetId: ['id', true], adId: ['id'], periodStart: ['date', true], periodEnd: ['date', true],
    ...Object.fromEntries(METRICS.map(key => [key, [MONEY.has(key) ? 'number' : 'count']])),
    source: ['text', 200], notes: ['text', 5000]
  }
};

class RecordError extends Error {
  constructor(message, status = 400) { super(message); this.status = status; }
}
function clone(value) { return JSON.parse(JSON.stringify(value)); }
function plain(value) { return value && typeof value === 'object' && !Array.isArray(value) && Object.getPrototypeOf(value) === Object.prototype; }
function fail(message, status) { throw new RecordError(message, status); }
function safeText(value, key, max, required = false) {
  if (value === null || value === undefined) {
    if (required) fail(`${key} is required.`);
    return null;
  }
  if (typeof value !== 'string') fail(`${key} must be text.`);
  const text = value.trim();
  if (text.length > max || /[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f]/.test(text)) fail(`${key} contains invalid or excessive text.`);
  if (required && !text) fail(`${key} is required.`);
  return text;
}
function date(value, key, required = false) {
  if (value === null || value === undefined || value === '') {
    if (required) fail(`${key} is required.`);
    return null;
  }
  if (typeof value !== 'string' || !/^\d{4}-\d{2}-\d{2}$/.test(value)) fail(`${key} must use YYYY-MM-DD.`);
  const parsed = new Date(`${value}T00:00:00.000Z`);
  if (!Number.isFinite(parsed.getTime()) || parsed.toISOString().slice(0, 10) !== value) fail(`${key} must be a real calendar date.`);
  return value;
}
function number(value, key, integer = false) {
  if (value === null || value === undefined || value === '') return null;
  if (typeof value !== 'number' || !Number.isFinite(value) || value < 0 || value > 1e12 || (integer && !Number.isSafeInteger(value))) fail(`${key} must be a nonnegative ${integer ? 'integer' : 'number'}.`);
  return value;
}
function id(value, key, required = false) {
  const text = safeText(value, key, 120, required);
  if (text !== null && !/^[A-Za-z0-9][A-Za-z0-9_.:-]*$/.test(text)) fail(`${key} is invalid.`);
  return text;
}
function url(value, key) {
  const text = safeText(value, key, 2048, true);
  let parsed;
  try { parsed = new URL(text); } catch { fail(`${key} must be an absolute http(s) URL.`); }
  if (!['https:', 'http:'].includes(parsed.protocol) || parsed.username || parsed.password) fail(`${key} must be an absolute http(s) URL without embedded credentials.`);
  return text;
}
function list(value, key, max = 150, urls = false) {
  if (value === null || value === undefined) return [];
  if (!Array.isArray(value) || value.length > 50) fail(`${key} must be an array with at most 50 items.`);
  return [...new Set(value.map(item => urls ? url(item, key) : safeText(item, key, max, true)))];
}
function fieldValue(value, key, spec) {
  const [type, option, required] = spec;
  if (type === 'text') return safeText(value, key, option, required);
  if (type === 'id') return id(value, key, option);
  if (type === 'date') return date(value, key, option);
  if (type === 'number' || type === 'count') return number(value, key, type === 'count');
  if (type === 'booleanOrNull') {
    if (value === null || value === undefined) return null;
    if (typeof value !== 'boolean') fail(`${key} must be true, false, or null.`);
    return value;
  }
  if (type === 'status') {
    const status = value == null ? 'draft' : value;
    if (!STATUSES.has(status)) fail(`${key} is not a recognised status.`);
    return status;
  }
  if (type === 'age') {
    const age = number(value, key, true);
    if (age !== null && (age < 13 || age > 100)) fail(`${key} must be between 13 and 100.`);
    return age;
  }
  if (type === 'gender') {
    if (value == null || value === '') return null;
    const gender = typeof value === 'string' ? value.toLowerCase().trim() : '';
    if (!['women', 'men', 'all', 'all genders'].includes(gender)) fail('gender must be women, men, or all.');
    return gender === 'all genders' ? 'all' : gender;
  }
  if (type === 'list') return list(value, key, option);
  if (type === 'urls') return list(value, key, 2048, true);
  if (type === 'reference') {
    const reference = safeText(value, key, 2048);
    if (reference && /^[A-Za-z][A-Za-z0-9+.-]*:/.test(reference)) return url(reference, key);
    return reference;
  }
  if (type === 'phone') {
    const phone = safeText(value, key, 40);
    if (phone && !/^\+?[\d ()-]{7,40}$/.test(phone)) fail('whatsappNumber is invalid.');
    if (phone && !/^\d{7,15}$/.test(phone.replace(/\D/g, ''))) fail('whatsappNumber must contain 7 to 15 digits.');
    return phone;
  }
  if (type === 'locations') {
    if (value == null) return [];
    if (!Array.isArray(value) || value.length > 50) fail('locations must be an array with at most 50 items.');
    return value.map(location => {
      if (!plain(location) || Object.keys(location).some(key => !['name', 'radiusKm'].includes(key))) fail('Each location must contain only name and radiusKm.');
      const radiusKm = number(location.radiusKm, 'radiusKm');
      if (radiusKm !== null && radiusKm > 1000) fail('radiusKm must be at most 1000.');
      return { name: safeText(location.name, 'location name', 200, true), radiusKm };
    });
  }
  fail(`Unsupported field ${key}.`);
}

function normalizeRecord(type, input, previous = null) {
  if (!plain(input)) fail('Record must be an object.');
  const schema = SCHEMAS[type];
  const allowed = new Set([...Object.keys(schema), 'id', 'archived']);
  for (const key of Object.keys(input)) if (!allowed.has(key)) fail(`Unknown field: ${key}.`);
  if (previous && input.id !== undefined && input.id !== previous.id) fail('Record ID cannot be changed.');
  const record = {};
  record.id = previous ? previous.id : (id(input.id, 'id') || `${ENTITY[type]}-${crypto.randomUUID()}`);
  for (const [key, spec] of Object.entries(schema)) {
    const value = input[key] !== undefined ? input[key] : previous ? previous[key] : undefined;
    record[key] = fieldValue(value, key, spec);
  }
  record.archived = input.archived === undefined ? !!(previous && previous.archived) : input.archived;
  if (typeof record.archived !== 'boolean') fail('archived must be true or false.');
  if (record.status === 'archived') record.archived = true;
  if (type === 'adSets') {
    if (record.ageMin !== null && record.ageMax !== null && record.ageMin > record.ageMax) fail('ageMin cannot be greater than ageMax.');
    if (record.frequencyCount !== null && record.frequencyDays !== null && record.frequencyCount > 0 && record.frequencyDays === 0) fail('frequencyDays must be positive when frequencyCount is set.');
  }
  if (type === 'results' && record.periodStart > record.periodEnd) fail('periodStart cannot be after periodEnd.');
  return record;
}
function validateRelationships(state) {
  for (const key of Object.values(TYPES)) {
    const ids = new Set();
    for (const record of state[key]) {
      if (ids.has(record.id)) fail(`Duplicate ${ENTITY[key]} ID.`, 409);
      ids.add(record.id);
    }
  }
  const campaigns = new Set(state.campaigns.map(record => record.id));
  const adSets = new Map(state.adSets.map(record => [record.id, record]));
  const ads = new Map(state.ads.map(record => [record.id, record]));
  for (const adSet of state.adSets) if (!campaigns.has(adSet.campaignId)) fail('campaignId must refer to an existing campaign.');
  for (const ad of state.ads) if (!adSets.has(ad.adSetId)) fail('adSetId must refer to an existing ad set.');
  const observations = new Set();
  for (const result of state.results) {
    if (!adSets.has(result.adSetId)) fail('Result adSetId must refer to an existing ad set.');
    if (result.adId !== null && (!ads.has(result.adId) || ads.get(result.adId).adSetId !== result.adSetId)) fail('Result adId must belong to its adSetId.');
    if (!result.archived) {
      const key = JSON.stringify([result.adSetId, result.adId, result.periodStart, result.periodEnd]);
      if (observations.has(key)) fail('A result already exists for this ad set, ad, and date range. Edit it instead.', 409);
      observations.add(key);
    }
  }
}
function configuredSummary(state) {
  const campaigns = state.campaigns.filter(record => !record.archived);
  const campaignIds = new Set(campaigns.map(record => record.id));
  const adSets = state.adSets.filter(record => !record.archived && campaignIds.has(record.campaignId));
  const adSetIds = new Set(adSets.map(record => record.id));
  return {
    campaignCount: campaigns.length, adSetCount: adSets.length,
    adCount: state.ads.filter(record => !record.archived && adSetIds.has(record.adSetId)).length,
    configuredDailyBudget: adSets.reduce((total, record) => total + (record.budgetDaily || 0), 0),
    pendingBudgetCount: adSets.filter(record => record.budgetDaily === null).length,
    resultCount: state.results.filter(record => !record.archived).length,
    budgetBasis: 'Configured ad-set budgets; includes paused and unverified records, not actual spend.'
  };
}

function createStore(options = {}) {
  const runtimeDir = process.env.DATA_PATH ? path.dirname(process.env.DATA_PATH) : path.join(__dirname, '..');
  const file = options.file || path.join(runtimeDir, 'meta-ads.json');
  const seedFile = options.seedFile || path.join(__dirname, 'meta-ads-seed.json');
  const clock = options.clock || (() => new Date().toISOString());
  const lockFile = `${file}.lock`;
  function locked(work) {
    fs.mkdirSync(path.dirname(file), { recursive: true });
    let lock;
    for (let attempt = 0; attempt < 2; attempt++) {
      try { lock = fs.openSync(lockFile, 'wx', 0o600); break; }
      catch (error) {
        if (error.code !== 'EEXIST') throw error;
        let stale = false;
        try {
          const prior = JSON.parse(fs.readFileSync(lockFile, 'utf8'));
          if (Number.isSafeInteger(prior.pid) && prior.pid > 0) {
            if (prior.pid === process.pid) stale = prior.instance !== PROCESS_INSTANCE;
            else {
              try { process.kill(prior.pid, 0); }
              catch (signalError) { stale = signalError.code === 'ESRCH'; }
            }
          }
        } catch { /* unknown lock ownership must fail closed */ }
        if (!stale || attempt > 0) fail('Another marketing write is in progress. Reload and try again.', 409);
        try { fs.unlinkSync(lockFile); }
        catch (unlinkError) { if (unlinkError.code !== 'ENOENT') throw unlinkError; }
      }
    }
    try {
      fs.writeFileSync(lock, JSON.stringify({ pid: process.pid, instance: PROCESS_INSTANCE }));
      return work();
    }
    finally { fs.closeSync(lock); fs.unlinkSync(lockFile); }
  }
  function save(state) {
    const temporary = `${file}.tmp-${process.pid}-${crypto.randomUUID()}`;
    try {
      const fd = fs.openSync(temporary, 'wx', 0o600);
      try { fs.writeFileSync(fd, JSON.stringify(state, null, 2)); fs.fsyncSync(fd); }
      finally { fs.closeSync(fd); }
      fs.renameSync(temporary, file);
    } finally { if (fs.existsSync(temporary)) fs.unlinkSync(temporary); }
  }
  function fromSeed() {
    let raw;
    if (options.seed !== undefined) raw = clone(options.seed);
    else {
      // Production campaign details can be injected privately on Railway. Never
      // log this value, and never consult it after a persisted store exists.
      const source = process.env.META_ADS_SEED_JSON === undefined
        ? fs.readFileSync(seedFile, 'utf8') : process.env.META_ADS_SEED_JSON;
      try { raw = JSON.parse(source); }
      catch { fail('Initial marketing seed is invalid JSON. No records were imported.', 500); }
    }
    if (!plain(raw)) fail('Initial marketing seed must be an object.', 500);
    const now = clock();
    const state = { version: VERSION, revision: 0, campaigns: [], adSets: [], ads: [], results: [], changes: [] };
    for (const key of Object.values(TYPES)) {
      if (!Array.isArray(raw[key]) || raw[key].length > 10000) fail(`Seed ${key} must be an array.`, 500);
      state[key] = raw[key].map(record => ({ ...normalizeRecord(key, record), createdAt: now, updatedAt: now, createdBy: 'seed', updatedBy: 'seed' }));
    }
    validateRelationships(state);
    return state;
  }
  function readExisting() {
    let state;
    try { state = JSON.parse(fs.readFileSync(file, 'utf8')); }
    catch (error) {
      if (error.code === 'ENOENT') return null;
      fail('Marketing data could not be read. Existing data has been preserved.', 500);
    }
    if (!plain(state) || state.version !== VERSION || !Number.isSafeInteger(state.revision) || state.revision < 0 || [...Object.values(TYPES), 'changes'].some(key => !Array.isArray(state[key]))) fail('Marketing data has an unsupported or invalid format. Existing data has been preserved.', 500);
    return state;
  }
  function loadUnlocked() {
    const existing = readExisting();
    if (existing) return existing;
    const seeded = fromSeed();
    save(seeded);
    return seeded;
  }
  function load() {
    const existing = readExisting();
    return existing || locked(loadUnlocked);
  }
  function view() {
    const state = load();
    return { ...clone(state), summary: configuredSummary(state) };
  }
  function mutate(type, method, recordId, body, actor) {
    if (!plain(body) || Buffer.byteLength(JSON.stringify(body), 'utf8') > 128 * 1024) fail('Record body must be an object below 128 KiB.');
    return locked(() => {
      const state = loadUnlocked();
      if (!Number.isSafeInteger(body.revision) || body.revision < 0) fail('revision is required. Reload before saving.', 428);
      if (body.revision !== state.revision) fail('Marketing records changed. Reload before saving your edits.', 409);
      const reason = safeText(body.reason, 'reason', 1000);
      const payload = { ...body };
      delete payload.revision;
      delete payload.reason;
      const index = recordId ? state[type].findIndex(record => record.id === recordId) : -1;
      if (method === 'PATCH' && index === -1) fail('Record not found.', 404);
      const previous = method === 'PATCH' ? state[type][index] : null;
      const record = normalizeRecord(type, payload, previous);
      if (!previous && state[type].some(existing => existing.id === record.id)) fail('Record ID already exists.', 409);
      const changedFields = ['id', ...Object.keys(SCHEMAS[type]), 'archived'].filter(key => JSON.stringify(previous ? previous[key] : null) !== JSON.stringify(record[key]));
      if (previous && !changedFields.length) return { record: clone(previous), revision: state.revision };
      const now = clock();
      const actorName = safeText(actor, 'actor', 160, true);
      record.createdAt = previous ? previous.createdAt : now;
      record.createdBy = previous ? previous.createdBy : actorName;
      record.updatedAt = now;
      record.updatedBy = actorName;
      if (previous) state[type][index] = record;
      else state[type].push(record);
      validateRelationships(state);
      state.revision++;
      const action = !previous ? 'create' : record.archived !== previous.archived ? record.archived ? 'archive' : 'restore' : 'update';
      for (const field of changedFields) state.changes.unshift({
        id: crypto.randomUUID(), at: now, actor: actorName, entityType: ENTITY[type],
        entityId: record.id, field, before: previous ? previous[field] : null,
        after: record[field], action, reason, revision: state.revision
      });
      save(state);
      return { record: clone(record), revision: state.revision };
    });
  }
  return { file, load, view, mutate };
}

function createRouter(store = createStore()) {
  const router = express.Router();
  router.use(BASE, (req, res, next) => {
    if (!req.user) return res.status(401).json({ success: false, error: 'Unauthorized' });
    if (!rolesOf(req.user).includes('owner')) return res.status(403).json({ success: false, error: 'Owner only.' });
    res.setHeader('Cache-Control', 'no-store');
    next();
  });
  function sendError(error, res) {
    const status = error instanceof RecordError ? error.status : 500;
    const response = { success: false, error: error instanceof RecordError ? error.message : 'Marketing records could not be saved. Please try again.' };
    if (status === 409 || status === 428) {
      try { response.revision = store.load().revision; } catch { /* do not replace the original failure */ }
    }
    return res.status(status).json(response);
  }
  router.get(BASE, (req, res) => {
    try { res.json({ success: true, ...store.view() }); }
    catch (error) { sendError(error, res); }
  });
  router.use(BASE, express.json({ limit: '128kb' }));
  for (const [route, type] of Object.entries(TYPES)) {
    router.post(`${BASE}/${route}`, (req, res) => {
      try { res.status(201).json({ success: true, ...store.mutate(type, 'POST', null, req.body, req.user.username) }); }
      catch (error) { sendError(error, res); }
    });
    router.patch(`${BASE}/${route}/:id`, (req, res) => {
      try { res.json({ success: true, ...store.mutate(type, 'PATCH', req.params.id, req.body, req.user.username) }); }
      catch (error) { sendError(error, res); }
    });
  }
  router.all(BASE, (req, res) => res.status(405).json({ success: false, error: 'Bulk replacement is not supported. Create or update individual records.' }));
  router.all(`${BASE}/*`, (req, res) => res.status(405).json({ success: false, error: 'Unsupported marketing operation. Use record create/update; history is immutable.' }));
  return router;
}

module.exports = { router: createRouter(), createRouter, createStore, configuredSummary };
