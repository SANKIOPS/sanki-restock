'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { createStore, createRouter } = require('../modules/meta-ads');
const { apiAllowedForUser } = require('../auth');
const { visibleFor } = require('../modules/module-registry');

const seed = {
  campaigns: [{ id: 'C1', name: 'Recorded campaign', objective: 'Traffic', status: 'reported_running' }],
  adSets: [{ id: 'S1', campaignId: 'C1', name: 'Femme', budgetDaily: 500, ageMin: 19, ageMax: 45, gender: 'Women', status: 'reported_running' }],
  ads: [{ id: 'A1', adSetId: 'S1', name: 'Model video', mediaCount: 1, status: 'reported_running' }],
  results: []
};

async function fixture(t, options = {}) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'sanki-meta-ads-'));
  const file = path.join(dir, 'meta-ads.json');
  const store = createStore({ file, seed, clock: () => '2026-10-12T12:00:00.000Z', ...options });
  const router = createRouter(store);
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  async function call(method, suffix = '', body, role = 'owner') {
    // Exercise the actual Express middleware and routes without opening sockets.
    // server.js parses JSON before mounting this router; _body models that input.
    return new Promise((resolve, reject) => {
      const req = { method, url: `/api/marketing/meta${suffix}`, headers: { 'content-type': 'application/json' }, body, _body: true };
      if (role !== 'anonymous') req.user = { username: 'marketing-test', roles: [role], role };
      const res = {
        statusCode: 200,
        setHeader() {},
        status(code) { this.statusCode = code; return this; },
        json(payload) { resolve({ statusCode: this.statusCode, ...payload }); }
      };
      router.handle(req, res, error => error ? reject(error) : resolve({ statusCode: 404, success: false, error: 'Not found' }));
    });
  }
  async function write(method, suffix, fields) {
    const { revision } = store.load();
    return call(method, suffix, { revision, ...fields });
  }
  return { dir, file, store, call, write };
}

test('marketing API and launcher are owner-only, including ordinary admin', async t => {
  const { call, file } = await fixture(t);
  for (const role of ['admin', 'accounting', 'sales', 'inventory', 'model_calendar']) {
    assert.equal(apiAllowedForUser({ roles: [role] }, '/api/marketing/meta'), false, role);
    assert.equal(apiAllowedForUser({ roles: [role] }, '/api/marketing/meta/ads', 'POST'), false, role);
    assert.equal((await call('GET', '', undefined, role)).statusCode, 403, role);
    assert.equal((await call('POST', '/campaigns', { revision: 0, name: 'Forbidden' }, role)).statusCode, 403, role);
    assert.equal(visibleFor({ roles: [role] }).some(module => module.key === 'meta-ads'), false, role);
  }
  assert.equal((await call('GET', '', undefined, 'anonymous')).statusCode, 401);
  assert.equal(fs.existsSync(file), false, 'unauthorized reads must not initialize storage');
  assert.equal(apiAllowedForUser({ roles: ['owner'] }, '/api/marketing/meta'), true);
  assert.equal(visibleFor({ roles: ['owner'] }).some(module => module.key === 'meta-ads'), true);
  assert.equal((await call('GET')).statusCode, 200);
});

test('synthetic seed imports once and preserves unknown metadata and observations', async t => {
  const { dir } = await fixture(t);
  const file = path.join(dir, 'production-seed.json');
  const store = createStore({ file, seed });
  const first = store.view();
  assert.deepEqual([first.campaigns.length, first.adSets.length, first.ads.length, first.results.length], [1, 1, 1, 0]);
  assert.equal(first.summary.configuredDailyBudget, 500);
  assert.equal(first.adSets[0].status, 'reported_running');
  assert.equal(first.adSets[0].performanceGoal, null);
  const diskBefore = fs.readFileSync(file, 'utf8');
  const replacement = createStore({ file, seed: { campaigns: [], adSets: [], ads: [], results: [] } });
  assert.deepEqual(replacement.load(), store.load());
  assert.equal(fs.readFileSync(file, 'utf8'), diskBefore);
});

test('private environment seed validates and imports once without replacing later edits', async t => {
  const previous = process.env.META_ADS_SEED_JSON;
  t.after(() => {
    if (previous === undefined) delete process.env.META_ADS_SEED_JSON;
    else process.env.META_ADS_SEED_JSON = previous;
  });
  process.env.META_ADS_SEED_JSON = JSON.stringify(seed);
  const { store, file } = await fixture(t, { seed: undefined });
  assert.equal(store.load().campaigns[0].id, 'C1');
  const changed = store.mutate('adSets', 'PATCH', 'S1', { revision: 0, budgetDaily: 275, reason: 'Synthetic test edit' }, 'test-owner');
  assert.equal(changed.revision, 1);
  process.env.META_ADS_SEED_JSON = '{invalid-replacement';
  const restarted = createStore({ file });
  assert.equal(restarted.load().adSets[0].budgetDaily, 275);
  assert.equal(restarted.load().revision, 1);
  assert.ok(restarted.load().changes.some(change => change.reason === 'Synthetic test edit'));

  const invalidFile = path.join(path.dirname(file), 'invalid-env.json');
  assert.throws(() => createStore({ file: invalidFile }).load(), /invalid JSON/);
  assert.equal(fs.existsSync(invalidFile), false);
  process.env.META_ADS_SEED_JSON = JSON.stringify({ ...seed, adSets: [{ ...seed.adSets[0], campaignId: 'missing' }] });
  assert.throws(() => createStore({ file: invalidFile }).load(), /existing campaign/);
  assert.equal(fs.existsSync(invalidFile), false);

  // Explicit synthetic seed remains the highest-priority test configuration.
  assert.equal(createStore({ file: invalidFile, seed }).load().campaigns[0].id, 'C1');
});

test('record CRUD persists isolated changes and audits actor, reason and every changed field', async t => {
  const { store, dir, file, write } = await fixture(t);
  const accounting = path.join(dir, 'expenses.json');
  fs.writeFileSync(accounting, '{"doNotTouch":true}');
  const createCampaign = await write('POST', '/campaigns', { name: 'Homme test', objective: 'Traffic', reason: 'Test a new collection' });
  assert.equal(createCampaign.statusCode, 201, createCampaign.error);
  assert.match(createCampaign.record.id, /^campaign-/);
  const createSet = await write('POST', '/adsets', { campaignId: createCampaign.record.id, name: 'Homme', budgetDaily: 250, gender: 'Men' });
  assert.equal(createSet.statusCode, 201, createSet.error);
  const createAd = await write('POST', '/ads', { adSetId: createSet.record.id, name: 'Video 1', mediaUrls: ['https://example.com/video.mp4'], mediaCount: 1 });
  assert.equal(createAd.statusCode, 201, createAd.error);
  const edited = await write('PATCH', `/adsets/${createSet.record.id}`, { budgetDaily: 300, name: 'Homme video test', reason: 'Owner revised the test allocation' });
  assert.equal(edited.statusCode, 200, edited.error);
  const state = store.load();
  const changes = state.changes.filter(change => change.revision === edited.revision);
  assert.deepEqual(changes.map(change => change.field).sort(), ['budgetDaily', 'name']);
  assert.ok(changes.every(change => change.actor === 'marketing-test' && change.at === '2026-10-12T12:00:00.000Z' && change.reason === 'Owner revised the test allocation'));
  const budgetChange = changes.find(change => change.field === 'budgetDaily');
  assert.equal(budgetChange.before, 250);
  assert.equal(budgetChange.after, 300);
  assert.equal(budgetChange.entityType, 'adSet');
  assert.equal(JSON.parse(fs.readFileSync(file, 'utf8')).revision, edited.revision);
  assert.equal(createStore({ file }).load().adSets.find(record => record.id === createSet.record.id).budgetDaily, 300);
  assert.equal(fs.readFileSync(accounting, 'utf8'), '{"doNotTouch":true}');
  assert.ok(!fs.readdirSync(dir).some(name => name.endsWith('.lock') || name.includes('.tmp-')));
});

test('stale or absent revisions reject writes without losing a successful edit', async t => {
  const { store, call, write } = await fixture(t);
  const initialRevision = store.load().revision;
  const edited = await write('PATCH', '/adsets/S1', { budgetDaily: 600 });
  assert.equal(edited.statusCode, 200);
  const diskBefore = JSON.stringify(store.load());
  const conflict = await call('PATCH', '/adsets/S1', { revision: initialRevision, budgetDaily: 200 });
  assert.equal(conflict.statusCode, 409);
  assert.equal(conflict.revision, edited.revision);
  assert.equal((await call('POST', '/campaigns', { name: 'No revision' })).statusCode, 428);
  assert.equal(JSON.stringify(store.load()), diskBefore);
  const secondClient = createStore({ file: store.file });
  assert.throws(() => secondClient.mutate('adSets', 'PATCH', 'S1', { revision: initialRevision, budgetDaily: 100 }, 'second-user'), /Reload/);
});

test('required parent relationships and result consistency are enforced', async t => {
  const { write } = await fixture(t);
  assert.equal((await write('POST', '/adsets', { name: 'Orphan', campaignId: 'missing' })).statusCode, 400);
  assert.equal((await write('POST', '/ads', { name: 'Orphan', adSetId: 'missing' })).statusCode, 400);
  assert.equal((await write('POST', '/results', { adSetId: 'missing', periodStart: '2026-10-10', periodEnd: '2026-10-11' })).statusCode, 400);
  const second = await write('POST', '/adsets', { id: 'S2', name: 'Second set', campaignId: 'C1' });
  assert.equal(second.statusCode, 201);
  assert.equal((await write('POST', '/results', { adSetId: 'S2', adId: 'A1', periodStart: '2026-10-10', periodEnd: '2026-10-11' })).statusCode, 400);
  const recorded = await write('POST', '/results', { adSetId: 'S1', adId: 'A1', periodStart: '2026-10-10', periodEnd: '2026-10-11', orders: 0 });
  assert.equal(recorded.statusCode, 201);
  assert.equal((await write('PATCH', '/ads/A1', { adSetId: 'S2' })).statusCode, 400, 'reparenting cannot silently invalidate results');
});

test('date, age, budget, count and record-size validation reject invalid data', async t => {
  const { write } = await fixture(t);
  for (const patch of [{ budgetDaily: -1 }, { budgetDaily: '500' }, { ageMin: 46, ageMax: 45 }, { ageMin: 12 }, { ageMax: 101 }, { frequencyDays: 0, frequencyCount: 5 }, { gender: 'not-a-gender' }, { interestsConfirmed: 'yes' }, { confirmedOn: '2026-02-30' }]) {
    assert.equal((await write('PATCH', '/adsets/S1', patch)).statusCode, 400, JSON.stringify(patch));
  }
  for (const fields of [{ periodStart: '2026-02-29', periodEnd: '2026-03-01' }, { periodStart: '2026-10-11', periodEnd: '2026-10-10' }, { periodStart: '2026-10-10' }, { periodStart: '2026-10-10', periodEnd: '2026-10-11', orders: 1.5 }, { periodStart: '2026-10-10', periodEnd: '2026-10-11', spend: -5 }]) {
    assert.equal((await write('POST', '/results', { adSetId: 'S1', ...fields })).statusCode, 400, JSON.stringify(fields));
  }
  assert.equal((await write('POST', '/campaigns', { name: '' })).statusCode, 400);
  assert.equal((await write('PATCH', '/ads/A1', { primaryText: 'x'.repeat(10001) })).statusCode, 400);
  assert.equal((await write('PATCH', '/ads/A1', { mediaCount: -1 })).statusCode, 400);
  assert.equal((await write('PATCH', '/campaigns/C1', { status: 'made-up' })).statusCode, 400);
  assert.equal((await write('POST', '/results', { adSetId: 'S1', periodStart: '2028-02-29', periodEnd: '2028-02-29' })).statusCode, 201);
});

test('safe URLs, immutable IDs and server audit fields cannot be overridden', async t => {
  const { write, store } = await fixture(t);
  for (const mediaUrl of ['javascript:alert(1)', 'data:text/html,x', '/relative.mp4', 'https://user:secret@example.com/video']) {
    assert.equal((await write('PATCH', '/ads/A1', { mediaUrls: [mediaUrl] })).statusCode, 400, mediaUrl);
  }
  assert.equal((await write('PATCH', '/adsets/S1', { destinationRef: 'javascript:alert(1)' })).statusCode, 400);
  assert.equal((await write('PATCH', '/campaigns/C1', { id: 'changed' })).statusCode, 400);
  assert.equal((await write('PATCH', '/ads/A1', { updatedBy: 'someone-else' })).statusCode, 400);
  assert.equal((await write('PATCH', '/ads/A1', { actor: 'someone-else' })).statusCode, 400);
  assert.equal((await write('PATCH', '/adsets/S1', { whatsappNumber: '+91 wrong' })).statusCode, 400);
  assert.equal((await write('PATCH', '/ads/A1', { mediaUrls: ['https://example.com/video.mp4'] })).statusCode, 200);
  assert.equal(store.load().ads[0].updatedBy, 'marketing-test');
});

test('observations preserve null versus zero and duplicate date-range records are rejected', async t => {
  const { write, store } = await fixture(t);
  const fields = { adSetId: 'S1', adId: null, periodStart: '2026-10-10', periodEnd: '2026-10-11', spend: 385.19, profileVisits: 183, orders: 0, qualifiedEnquiries: 0, storeVisits: null, source: 'Owner reported screenshot' };
  const created = await write('POST', '/results', fields);
  assert.equal(created.statusCode, 201, created.error);
  assert.equal(created.record.orders, 0);
  assert.equal(created.record.storeVisits, null);
  assert.equal(created.record.revenue, null);
  assert.equal(created.record.impressions, null);
  assert.equal((await write('POST', '/results', fields)).statusCode, 409);
  assert.equal((await write('PATCH', `/results/${created.record.id}`, { revenue: 0 })).statusCode, 200);
  const saved = store.load().results[0];
  assert.equal(saved.revenue, 0);
  assert.equal(saved.storeVisits, null);
  assert.equal((await write('POST', '/results', { ...fields, adId: 'A1' })).statusCode, 201, 'ad-level observations remain separate from ad-set observations');
  assert.equal((await write('POST', '/results', { ...fields, periodStart: '2026-10-11', periodEnd: '2026-10-12' })).statusCode, 201, 'overlapping snapshots are stored without an unsafe summed summary');
  assert.equal(store.view().summary.spend, undefined, 'summary must not double-count performance');
});

test('archive keeps records and relationships while immutable history and bulk state are protected', async t => {
  const { write, call, store } = await fixture(t);
  const archived = await write('PATCH', '/campaigns/C1', { archived: true, reason: 'Stopped test' });
  assert.equal(archived.statusCode, 200);
  assert.equal(store.load().campaigns.length, 1);
  assert.equal(store.load().ads.length, 1);
  assert.equal(store.view().summary.configuredDailyBudget, 0);
  assert.ok(store.load().changes.some(change => change.action === 'archive' && change.field === 'archived'));
  const changes = JSON.stringify(store.load().changes);
  assert.equal((await call('DELETE', '/campaigns/C1')).statusCode, 405);
  assert.equal((await call('POST', '/changes', { revision: store.load().revision })).statusCode, 405);
  assert.equal((await call('PATCH', '/changes/one', { revision: store.load().revision })).statusCode, 405);
  assert.equal((await call('POST', '', { revision: store.load().revision, campaigns: [] })).statusCode, 405);
  assert.equal(JSON.stringify(store.load().changes), changes);
});

test('corrupted persisted data fails closed and is never replaced by seed', async t => {
  const { file, call } = await fixture(t);
  fs.writeFileSync(file, '{broken');
  const before = fs.readFileSync(file, 'utf8');
  const response = await call('GET');
  assert.equal(response.statusCode, 500);
  assert.match(response.error, /preserved/);
  assert.equal(fs.readFileSync(file, 'utf8'), before);
});

test('no-op updates retain revision and history; unknown fields and missing records fail', async t => {
  const { write, store } = await fixture(t);
  const state = store.load();
  const noop = await write('PATCH', '/adsets/S1', { budgetDaily: 500 });
  assert.equal(noop.statusCode, 200);
  assert.equal(noop.revision, state.revision);
  assert.deepEqual(store.load().changes, state.changes);
  assert.equal((await write('PATCH', '/adsets/missing', { budgetDaily: 100 })).statusCode, 404);
  assert.equal((await write('PATCH', '/adsets/S1', { inventedSetting: true })).statusCode, 400);
});

test('another process holding the write lock prevents concurrent state replacement', async t => {
  const { file, store, write } = await fixture(t);
  store.load();
  fs.writeFileSync(`${file}.lock`, '');
  const blocked = await write('PATCH', '/adsets/S1', { budgetDaily: 200 });
  assert.equal(blocked.statusCode, 409);
  assert.equal(store.load().adSets[0].budgetDaily, 500);
  fs.unlinkSync(`${file}.lock`);
  assert.equal((await write('PATCH', '/adsets/S1', { budgetDaily: 200 })).statusCode, 200);
});

test('a lock left by an earlier process instance can recover without replacing records', async t => {
  const { file, store, write } = await fixture(t);
  store.load();
  fs.writeFileSync(`${file}.lock`, JSON.stringify({ pid: process.pid, instance: 'previous-process-instance' }));
  const recovered = await write('PATCH', '/adsets/S1', { budgetDaily: 250 });
  assert.equal(recovered.statusCode, 200, recovered.error);
  assert.equal(store.load().campaigns[0].id, 'C1');
  assert.equal(store.load().adSets[0].budgetDaily, 250);
  assert.equal(fs.existsSync(`${file}.lock`), false);
});

test('copy settings stays within campaign, preserves identity and metrics, and records history atomically',async t=>{
  const {store,write}=await fixture(t);
  await write('POST','/adsets',{id:'S2',campaignId:'C1',name:'Second',status:'paused',budgetDaily:100});
  await write('POST','/ads',{id:'A2',adSetId:'S2',name:'Second creative',mediaUrls:['https://example.com/own']});
  await write('PATCH','/adsets/S1',{destination:'Instagram profile',locations:[{name:'India',radiusKm:null}]});
  await write('PATCH','/ads/A1',{collection:'Femme',model:'Julia'});
  const before=store.load();const result=store.copySettings({revision:before.revision,source:'S1',targets:['S2'],groups:['destination','locations','collection','model']},'tester');
  const after=store.load(),set=after.adSets.find(s=>s.id==='S2'),ad=after.ads.find(a=>a.id==='A2');
  assert.equal(set.destination,'Instagram profile');assert.equal(set.name,'Second');assert.equal(set.status,'paused');assert.equal(set.budgetDaily,100);assert.equal(ad.model,'Julia');assert.equal(ad.collection,'Femme');assert.deepEqual(ad.mediaUrls,['https://example.com/own']);assert.equal(result.changedRecords,2);assert.ok(after.changes.some(c=>c.reason.startsWith('Copy settings from ad set')));
  assert.throws(()=>store.copySettings({revision:before.revision,source:'S1',targets:['S2'],groups:['age']},'tester'),/Records changed/);
  assert.throws(()=>store.copySettings({revision:after.revision,source:'S1',targets:['S1'],groups:['age']},'tester'),/same campaign/);
  assert.throws(()=>store.copySettings({revision:after.revision,source:'S1',targets:['S2'],groups:['status']},'tester'),/Choose target/);
});

test('ad product and line-separated SKU list persist with per-field history and survive setup imports',async t=>{
  const {store,write}=await fixture(t);
  const saved=await write('PATCH','/ads/A1',{product:'T-shirts',skus:['SKU-001','SKU-002']});assert.equal(saved.success,true);
  const current=store.load();assert.deepEqual(current.ads[0].skus,['SKU-001','SKU-002']);assert.equal(current.ads[0].product,'T-shirts');
  store.importSetup({campaigns:[],adSets:[],ads:[{id:'A1',adSetId:'S1',name:'Updated video'}]},current.revision,'tester');
  const after=store.load();assert.equal(after.ads[0].product,'T-shirts');assert.deepEqual(after.ads[0].skus,['SKU-001','SKU-002']);assert.ok(after.changes.some(c=>c.field==='skus'));
});
