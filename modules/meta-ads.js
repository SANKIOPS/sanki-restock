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
const METRICS = ['spend', 'impressions', 'reach', 'linkClicks', 'profileVisits', 'messagingStarts', 'qualifiedEnquiries', 'orders', 'revenue', 'storeVisits','metaPurchases','metaPurchaseValue','videoPlays3s','thruPlays','reportedResults'];
const MONEY = new Set(['budgetDaily', 'spend', 'revenue','metaPurchaseValue']);
const SCHEMAS = {
  campaigns: {
    metaSourceId: ['text',120], name: ['text', 200, true], objective: ['text', 120], businessGoal: ['text', 500],
    status: ['status'], adAccountId: ['text', 120], notes: ['text', 5000]
  },
  adSets: {
    setupDetails: ['text',60000], metaSourceId: ['text',120], campaignId: ['id', true], name: ['text', 200, true], collection: ['text', 80],
    budgetDaily: ['number'], conversionLocation: ['text', 160], performanceGoal: ['text', 250],
    destination: ['text', 160], destinationRef: ['reference'], facebookPage: ['text', 200],
    instagramProfile: ['text', 160], whatsappNumber: ['phone'], locations: ['locations'],
    ageMin: ['age'], ageMax: ['age'], gender: ['gender'], interests: ['list', 150],
    behaviors:['list',150],behaviorsConfirmed:['booleanOrNull'], interestsConfirmed: ['booleanOrNull'], languages: ['list', 120], placements: ['list', 120],
    placementsConfirmed: ['booleanOrNull'], audienceMode: ['text', 160],
    frequencyCount: ['count'], frequencyDays: ['count'], frequencyMode: ['text', 100],
    status: ['status'], confirmedOn: ['date'], notes: ['text', 5000]
  },
  ads: {
    collection:['text',80],creativeTypes:['list',100],
    metaSourceId: ['text',120], adSetId: ['id', true], name: ['text', 200, true], model: ['text', 160],
    format: ['text', 100], description: ['text', 5000], mediaUrls: ['urls'],
    setupSource:['text',60000],whatsappNumber:['phone'],mediaCount: ['count'], cta: ['text', 150], primaryText: ['text', 10000],
    status: ['status'], notes: ['text', 5000]
  },
  results: {
    reportGoal:['text',250],reportDestination:['text',160],resultIndicator:['text',200], attributionSetting:['text',200], reportedDelivery:['text',100],
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
  function view(period) {
    const state = load();
    const output=clone(state);
    for(const set of output.adSets){
      try{const raw=JSON.parse(set.setupDetails||'{}'),groups=JSON.parse(raw['Flexible Inclusions']||'[]');
        if(set.behaviorsConfirmed==null&&Array.isArray(groups)&&raw['Flexible Inclusions']){set.interests=groups.flatMap(g=>(g.interests||[]).map(x=>x.name));set.interestsConfirmed=true;set.behaviors=groups.flatMap(g=>(g.behaviors||[]).map(x=>x.name));set.behaviorsConfirmed=true;}
      }catch{/* Preserve existing records when the source audience is unavailable. */}
    }
    return { ...output, summary: configuredSummary(state), insights:require('./meta-ads-insights').insights(state,period) };
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
  function importSetup(records, revision, actor, preview = false, options = {}) {
    return locked(() => {
      const state=loadUnlocked();
      if(revision!==state.revision)fail('Marketing records changed. Preview the upload again.',409);
      const now=clock(), counts={created:0,updated:0,unchanged:0};
      const remap=new Map(),review=[],original=clone(state);
      for(const type of ['campaigns','adSets','ads']) for(const input of records[type]) {
        const payload={...input,metaSourceId:input.id,archived:input.status==='archived'};
        for(const key of ['campaignId','adSetId'])if(payload[key])payload[key]=remap.get(payload[key])||payload[key];
        let index=state[type].findIndex(x=>x.id===input.id||x.metaSourceId===input.id);
        if(index<0){const matches=state[type].map((x,i)=>({x,i})).filter(({x})=>!x.metaSourceId&&x.name===payload.name&&(!payload.campaignId||x.campaignId===payload.campaignId)&&(!payload.adSetId||x.adSetId===payload.adSetId));if(matches.length>1)fail(`Multiple manual records match ${payload.name}. Resolve them before importing.`,409);if(matches.length===1)index=matches[0].i;}
        const previous=index<0?null:state[type][index];
        if(previous)payload.id=previous.id;
        remap.set(input.id,payload.id);
        const record=normalizeRecord(type,payload,previous);
        const changed=Object.keys(payload).filter(k=>JSON.stringify(previous?.[k])!==JSON.stringify(record[k]));
        if(!changed.length){counts.unchanged++;continue;}
        counts[previous?'updated':'created']++;
        review.push({type,id:record.id,name:record.name,action:previous?'update':'create',fields:changed.map(field=>({field,before:previous?.[field]??null,after:record[field]}))});
        Object.assign(record,{createdAt:previous?.createdAt||now,createdBy:previous?.createdBy||actor,updatedAt:now,updatedBy:actor});
        if(previous)state[type][index]=record;else state[type].push(record);
        for(const field of changed)state.changes.unshift({id:crypto.randomUUID(),at:now,actor,entityType:ENTITY[type],entityId:record.id,field,before:previous?.[field]??null,after:record[field],action:previous?'update':'create',reason:'Meta configuration upload',revision:state.revision+1});
      }
      const includedCampaigns=new Set(records.campaigns.map(r=>remap.get(r.id)||r.id)),includedSets=new Set(records.adSets.map(r=>remap.get(r.id)||r.id)),includedAds=new Set(records.ads.map(r=>remap.get(r.id)||r.id));
      const missingSets=options.level==='campaigns'?[]:original.adSets.filter(r=>!r.archived&&includedCampaigns.has(r.campaignId)&&!includedSets.has(r.id));
      const missingSetIds=new Set(missingSets.map(r=>r.id));
      const missing=missingSets.map(r=>({type:'adSets',id:r.id,name:r.name})).concat(original.ads.filter(r=>!r.archived&&includedSets.has(r.adSetId)&&!missingSetIds.has(r.adSetId)&&!includedAds.has(r.id)).map(r=>({type:'ads',id:r.id,name:r.name})));
      const decisions=options.decisions||[];
      if(!Array.isArray(decisions))fail('Missing-record decisions must be a list.');
      if(!preview&&missing.some(r=>!decisions.some(d=>d.type===r.type&&d.id===r.id&&['keep','remove'].includes(d.action))))fail('Review each missing record: keep existing or mark removed.');
      counts.removed=0;
      for(const candidate of missing){if(!decisions.some(d=>d.type===candidate.type&&d.id===candidate.id&&d.action==='remove'))continue;
        const removals=[{type:candidate.type,id:candidate.id}];if(candidate.type==='adSets')removals.push(...state.ads.filter(a=>a.adSetId===candidate.id&&!a.archived).map(a=>({type:'ads',id:a.id})));
        for(const entry of removals){const record=state[entry.type].find(r=>r.id===entry.id);if(record.archived)continue;record.archived=true;record.updatedAt=now;record.updatedBy=actor;counts.removed++;state.changes.unshift({id:crypto.randomUUID(),at:now,actor,entityType:ENTITY[entry.type],entityId:record.id,field:'archived',before:false,after:true,action:'archive',reason:'Owner confirmed removed in Meta after setup upload',revision:state.revision+1});}
      }
      validateRelationships(state);
      if(!preview && (counts.created||counts.updated||counts.removed)){state.revision++;save(state);}
      return {counts,revision:state.revision,review,missing};
    });
  }
  function importPerformance(rows,revision,actor,preview=false) {
    return locked(()=>{
      const state=loadUnlocked();
      if(!Number.isSafeInteger(revision)||revision!==state.revision)fail('Records changed. Preview the performance file again.',409);
      const counts={created:0,updated:0,unchanged:0}, now=clock(), report=[];
      const match=(type,meta)=>state[type].find(x=>x.id===meta||x.metaSourceId===meta);
      for(const row of rows){
        const campaign=match('campaigns',row.campaignMetaId),set=match('adSets',row.setMetaId),ad=match('ads',row.adMetaId);
        if(!campaign||!set||!ad)fail(`Upload campaign setup first: ${row.adName||row.adMetaId} is not linked to an imported ad.`,400);
        if(set.campaignId!==campaign.id||ad.adSetId!==set.id)fail('Report IDs do not match the saved campaign hierarchy. Upload the latest setup first.');
        const index=state.results.findIndex(x=>x.adId===ad.id&&x.periodStart===row.record.periodStart&&x.periodEnd===row.record.periodEnd),previous=index<0?null:state.results[index];
        const payload={...row.record,archived:false,reportGoal:previous?.reportGoal||set.performanceGoal,reportDestination:previous?.reportDestination||set.destination,id:previous?.id||'period-'+row.adMetaId+'-'+row.record.periodStart+'-'+row.record.periodEnd,adId:ad.id,adSetId:set.id};
        const record=normalizeRecord('results',payload,previous), changed=Object.keys(payload).filter(k=>JSON.stringify(previous?.[k])!==JSON.stringify(record[k]));
        report.push({ad:ad.name,adSet:set.name,date:record.periodStart,periodStart:record.periodStart,periodEnd:record.periodEnd,spend:record.spend,action:!changed.length?'unchanged':previous?'updated':'new'});
        if(!changed.length){counts.unchanged++;continue;}counts[previous?'updated':'created']++;
        Object.assign(record,{createdAt:previous?.createdAt||now,createdBy:previous?.createdBy||actor,updatedAt:now,updatedBy:actor});
        if(previous)state.results[index]=record;else state.results.push(record);
        for(const field of changed)state.changes.unshift({id:crypto.randomUUID(),at:now,actor,entityType:'result',entityId:record.id,field,before:previous?.[field]??null,after:record[field],action:previous?'update':'create',reason:'Meta performance upload',revision:state.revision+1});
      }
      validateRelationships(state);
      if(!preview&&(counts.created||counts.updated)){state.revision++;save(state);}
      return {counts,revision:state.revision,report};
    });
  }
  function bulk(body,actor){
    if(!plain(body)||!['campaigns','adSets','ads','results'].includes(body.type)||!Array.isArray(body.ids)||!body.ids.length||body.ids.length>2000||new Set(body.ids).size!==body.ids.length)fail('Select 1–2000 distinct records.');
    if(!['edit','remove'].includes(body.action))fail('Unsupported bulk action.');
    const fields=body.fields||{};
    const allowed=body.type==='adSets'?['status','budgetDaily','notes']:body.type==='results'?['notes']:['status','notes'];
    if(!plain(fields)||Object.keys(fields).some(k=>!allowed.includes(k))||fields.status==='archived')fail('Unsupported shared setting. Use Remove selected for removal.');
    if(body.action==='edit'&&!Object.keys(fields).length)fail('Choose at least one shared setting to change.');
    return locked(()=>{
      const state=loadUnlocked();if(!Number.isSafeInteger(body.revision)||body.revision!==state.revision)fail('Records changed. Reload before trying again.',409);
      const selected=new Set(body.ids);if(body.ids.some(id=>!state[body.type].some(r=>r.id===id)))fail('One or more selected records no longer exist.',404);
      const targets={campaigns:new Set(),adSets:new Set(),ads:new Set(),results:new Set()};targets[body.type]=selected;
      if(body.action==='remove'){
        for(const r of state.adSets)if(targets.campaigns.has(r.campaignId))targets.adSets.add(r.id);
        for(const r of state.ads)if(targets.adSets.has(r.adSetId))targets.ads.add(r.id);
        for(const r of state.results)if(targets.adSets.has(r.adSetId)||targets.ads.has(r.adId))targets.results.add(r.id);
      }
      let changedRecords=0;const now=clock(),reason=safeText(body.reason,'reason',1000)|| (body.action==='remove'?'Remove selected records':'Bulk edit');
      for(const [type,ids] of Object.entries(targets))for(const recordId of ids){
        const index=state[type].findIndex(r=>r.id===recordId),previous=state[type][index];
        const patch=body.action==='remove'?{archived:true}:fields;
        const record=normalizeRecord(type,patch,previous),changed=Object.keys(patch).filter(k=>JSON.stringify(previous[k])!==JSON.stringify(record[k]));if(!changed.length)continue;
        Object.assign(record,{createdAt:previous.createdAt,createdBy:previous.createdBy,updatedAt:now,updatedBy:actor});state[type][index]=record;changedRecords++;
        for(const field of changed)state.changes.unshift({id:crypto.randomUUID(),at:now,actor,entityType:ENTITY[type],entityId:record.id,field,before:previous[field]??null,after:record[field],action:body.action==='remove'?'archive':'update',reason,revision:state.revision+1});
      }
      validateRelationships(state);if(changedRecords){state.revision++;save(state);}return {revision:state.revision,changedRecords};
    });
  }
  return { file, load, view, mutate, importSetup, importPerformance,bulk };
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
    try { res.json({ success: true, ...store.view(typeof req.query?.period==='string'?req.query.period:undefined) }); }
    catch (error) { sendError(error, res); }
  });
  const upload=require('multer')({storage:require('multer').memoryStorage(),limits:{fileSize:5*1024*1024,files:1}}).single('file');
  router.post(`${BASE}/import-performance`,(req,res)=>upload(req,res,error=>{
    if(error)return res.status(400).json({success:false,error:'Upload one performance CSV or XLSX below 5 MB.'});
    try{
      if(!req.file||! /\.(csv|xlsx)$/i.test(req.file.originalname))fail('Choose an ad-level performance CSV.');
      let rows;try{rows=require('./meta-ads-performance').parsePerformance(req.file.buffer);}catch(e){fail(e.message);}
      res.json({success:true,...store.importPerformance(rows,Number(req.body.revision),req.user.username,req.body.preview==='true')});
    }catch(e){sendError(e,res);}
  }));
  router.post(`${BASE}/import-setup`, (req,res) => upload(req,res,error => {
    if(error)return res.status(400).json({success:false,error:'Upload one CSV or XLSX file below 5 MB.'});
    try {
      if(!req.file || !/\.(csv|xlsx)$/i.test(req.file.originalname))fail('Choose a Meta configuration CSV or XLSX export.');
      let parsed;try{parsed=require('./meta-ads-import').parseSetup(req.file.buffer);}catch(e){fail(e.message);}
      const campaignChoices=parsed.level==='campaigns'?parsed.records.campaigns.map(r=>({id:r.id,name:r.name,status:r.status})):[];
      if(parsed.level==='campaigns'&&req.body.campaignIds){let ids;try{ids=JSON.parse(req.body.campaignIds);}catch{fail('Invalid campaign selection.');}if(!Array.isArray(ids)||!ids.length||ids.some(id=>!campaignChoices.some(c=>c.id===id)))fail('Select at least one campaign from this file.');parsed.records.campaigns=parsed.records.campaigns.filter(r=>ids.includes(r.id));}
      const preview=req.body.preview==='true';
      if(!preview&&parsed.level==='campaigns'&&!req.body.campaignIds)fail('Choose campaigns and preview the selected campaigns before saving.');
      if(!preview&&req.body.confirmed!=='true')fail('Confirm the reviewed setup changes before saving.');
      let decisions=[];try{decisions=JSON.parse(req.body.decisions||'[]');}catch{fail('Invalid missing-record decisions.');}
      const result=store.importSetup(parsed.records,Number(req.body.revision),req.user.username,preview,{completeCampaigns:req.body.completeCampaigns==='true',decisions,level:parsed.level});
      res.json({success:true,...result,...(preview?{...parsed,campaignChoices}:{})});
    }catch(e){sendError(e,res);}
  }));
  router.use(BASE, express.json({ limit: '128kb' }));
  router.post(`${BASE}/bulk`,(req,res)=>{try{res.json({success:true,...store.bulk(req.body,req.user.username)});}catch(e){sendError(e,res);}});
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
