const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const { router, buildPlan, settingsWithDefaults, validSplitBoxes, splitDetectionNeedsDetail, separateHorizontalSplitBoxes } = require('../modules/casuals');

test('all Casual categories support a design-first target while only uppers use fit', () => {
  const settings = settingsWithDefaults({ settings: {} });
  for (const key of ['Trouser', 'Shirt', 'T-shirt']) {
    settings.categories[key].sizeMode = 'designs';
    settings.categories[key].designs = 10;
  }
  const plan = buildPlan([], settings);
  for (const key of ['Trouser', 'Shirt', 'T-shirt']) {
    const category = plan.categories.find(c => c.category === key);
    assert.equal(category.sizeMode, 'designs');
    assert.equal(category.designsTarget, 10);
    assert.equal(category.estUnits, 10 * category.set);
    assert.ok(category.calc, `${key} exposes transparent target calculations`);
    assert.ok(category.calc.colours.length, `${key} exposes colour coverage`);
    if (key === 'Trouser') assert.equal(category.calc.fits.length, 0, 'Trouser has no fit allocation');
    else assert.ok(category.calc.fits.length, `${key} exposes fit coverage`);
  }
  for (const key of ['Shirt', 'T-shirt']) {
    const category = plan.categories.find(c => c.category === key);
    assert.equal(category.hasPrint, true);
    assert.ok(category.calc.prints.length, `${key} retains its print dimension`);
  }
});

test('detailed Casual UI presents the design-first workflow', () => {
  const html = fs.readFileSync(path.join(__dirname, '..', 'public', 'fresh-procurement.html'), 'utf8');
  assert.match(html, /Available for Trousers, Shirts and T-shirts/);
  assert.match(html, /upload <b>all of its colour photos together<\/b>/);
  assert.match(html, /SHIRT \/ T-SHIRT OPERATIONAL PARITY/);
  assert.match(html, /<b>Coverage table<\/b>/);
  assert.match(html, /<b>Design order sheet<\/b>/);
  assert.match(html, /id="czDesignFolder"[^>]*webkitdirectory/);
  assert.match(html, /function czDImportFolder\(fileList\)/);
  assert.match(html, /Each subfolder becomes a design automatically/);
  assert.match(html, /id="czModeSplitter"/);
  assert.match(html, /id="czSplitBatch"/);
  assert.match(html, /\/api\/casuals\/photo-split/);
  assert.doesNotMatch(html, /Plan this category by/);
});

test('Casual batch rows use the explicit activation endpoint and expose failures', () => {
  const html = fs.readFileSync(path.join(__dirname, '..', 'public', 'fresh-procurement.html'), 'utf8');
  assert.match(html, /czActivateRequest\('\/api\/casuals\/batches\/'\+encoded\+'\/active',\{method:'PUT'/);
  assert.match(html, /czActivateRequest\('\/api\/casuals\/batches\/active',\{method:'POST'/);
  assert.match(html, /czBatchError/);
  assert.match(html, /setAttribute\('role', 'button'\)/);

  const routes = router.stack
    .filter(layer => layer.route)
    .map(layer => ({ path: layer.route.path, methods: layer.route.methods }));
  assert.ok(routes.some(route => route.path === '/api/casuals/batches/:id/active' && route.methods.put));
  assert.ok(routes.some(route => route.path === '/api/casuals/batches/active' && route.methods.post));
  const source = fs.readFileSync(path.join(__dirname, '..', 'modules', 'casuals.js'), 'utf8');
  assert.match(source, /activeBatchOverride = id/);
  assert.match(source, /catch \(err\)[\s\S]*active batch is using runtime fallback/);
});

test('Fresh Procurement organises pile uploads without a paid AI provider', () => {
  const html = fs.readFileSync(path.join(__dirname, '..', 'public', 'fresh-procurement.html'), 'utf8');
  const source = fs.readFileSync(path.join(__dirname, '..', 'modules', 'casuals.js'), 'utf8');

  assert.match(html, />▶ Organise photos</);
  assert.match(html, /Pile &amp; organise/);
  assert.match(html, /no paid AI/);
  assert.match(html, /czsegcatpick/);
  assert.match(html, /Nothing is guessed from the image/);

  assert.match(source, /mode: 'local'/);
  assert.match(source, /organisedBy = 'batch-category'/);
  assert.match(source, /typeof b\.category === 'string'/);
  assert.doesNotMatch(source, /AI segregation is not enabled/);
  assert.doesNotMatch(html, /On <b>Segregate<\/b>/);
});

test('Trouser planning selects named colourways by colour only', () => {
  const settings = settingsWithDefaults({ settings: {} });
  settings.categories.Trouser.enabled = true;
  settings.categories.Shirt.enabled = false;
  settings.categories['T-shirt'].enabled = false;
  settings.categories.Trouser.sizeMode = 'designs';
  settings.categories.Trouser.designs = 4;
  settings.categories.Trouser.sizeSystem = 'numeric';
  settings.categories.Trouser.sizes = { 26:1, 28:1, 30:1, 32:1, 34:1, 36:1, 38:0, 40:0 };
  settings.categories.Trouser.colours = { Black:50, Beige:50 };

  const candidates = [
    { id:'a', category:'Trouser', colour:'Black', designName:'Design A', uploadedAt:'2026-01-01', dupeOf:null },
    { id:'b', category:'Trouser', colour:'Black', designName:'Design B', uploadedAt:'2026-01-02', dupeOf:null },
    { id:'c', category:'Trouser', colour:'Black', designName:'Design C', uploadedAt:'2026-01-03', dupeOf:null },
    { id:'d', category:'Trouser', colour:'Black', designName:'Design D', uploadedAt:'2026-01-04', dupeOf:null },
    { id:'e', category:'Trouser', colour:'Beige', designName:'Design E', uploadedAt:'2026-01-05', dupeOf:null }
  ];
  const trouser = buildPlan(candidates, settings).categories.find(c => c.category === 'Trouser');
  assert.equal(trouser.colourOnly, true);
  assert.equal(trouser.set, 6);
  assert.equal(trouser.estUnits, 24);
  assert.deepEqual(trouser.colourPlan.map(r => [r.label,r.target,r.selected,r.shortage,r.held]), [
    ['Black',2,2,0,2], ['Beige',2,1,1,0]
  ]);
  assert.deepEqual(trouser.fits[0].designs.filter(d => d.included).map(d => d.designName), ['Design A','Design B','Design E']);
  assert.ok(trouser.fits[0].designs.every(d => d.fit == null));
});

test('Trouser UI omits fit from setup, review and exported rows', () => {
  const html = fs.readFileSync(path.join(__dirname, '..', 'public', 'fresh-procurement.html'), 'utf8');
  assert.match(html, /isTrouser \? \['sizes','colours'\]/);
  assert.match(html, /cat\.colourOnly \? '' : '<label class="czsegl">Fit/);
  assert.match(html, /fit:cat\.colourOnly\?'':f\.label/);
  assert.match(html, /Trouser plan uses <b>colour only<\/b>/);
});

test('Casual planning settings are snapshotted per batch', () => {
  const source = fs.readFileSync(path.join(__dirname, '..', 'modules', 'casuals.js'), 'utf8');
  const html = fs.readFileSync(path.join(__dirname, '..', 'public', 'fresh-procurement.html'), 'utf8');
  assert.match(source, /planSettings: JSON\.parse\(JSON\.stringify\(settingsWithDefaults\(s\)\)\)/);
  assert.match(source, /function settingsForActiveBatch\(s, persistLegacy\)/);
  assert.match(source, /activeBatch\.planSettings = JSON\.parse\(JSON\.stringify\(next\)\)/);
  assert.match(source, /router\.get\('\/api\/casuals\/settings'[\s\S]*?settingsForActiveBatch\(s\);[\s\S]*?res\.json\(\{ success: true, settings \}\);/);
  assert.match(html, /if\(d\.success && d\.settings\) czSettings = d\.settings/);
});

test('photo splitter sanitizes crop boxes and removes near duplicates', () => {
  assert.deepEqual(validSplitBoxes([
    [0.05, 0.10, 0.45, 0.90],
    [0.051, 0.101, 0.451, 0.901],
    [0.55, 0.10, 0.95, 0.90],
    [0, 0, 0.02, 0.02],
    ['bad', 0, 1, 1]
  ]), [
    [0.05, 0.10, 0.45, 0.90],
    [0.55, 0.10, 0.95, 0.90]
  ]);
});

test('photo splitter retries a suspicious whole-image crop at garment level', () => {
  assert.equal(splitDetectionNeedsDetail([[0.02, 0.03, 0.98, 0.97]]), true);
  assert.equal(splitDetectionNeedsDetail([[0.20, 0.10, 0.70, 0.80]]), false);
  assert.equal(splitDetectionNeedsDetail([
    [0.05, 0.10, 0.45, 0.90],
    [0.55, 0.10, 0.95, 0.90]
  ]), false);
});

test('photo splitter converts overlapping rack garments into clean non-overlapping columns', () => {
  const separated = separateHorizontalSplitBoxes([
    [0.08, 0.10, 0.48, 0.92],
    [0.26, 0.11, 0.68, 0.91],
    [0.49, 0.09, 0.91, 0.93]
  ]);
  assert.equal(separated.length, 3);
  assert.ok(separated[0][2] <= separated[1][0]);
  assert.ok(separated[1][2] <= separated[2][0]);
  assert.ok(separated.every(b => b[2] - b[0] >= 0.06));
  assert.ok(separated.every(b => b[1] <= 0.09 && b[3] >= 0.93));
});
