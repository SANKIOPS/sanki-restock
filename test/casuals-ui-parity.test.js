const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const { router, buildPlan, settingsWithDefaults, validSplitBoxes, splitDetectionNeedsDetail, separateHorizontalSplitBoxes } = require('../modules/casuals');

test('Shirts and T-shirts support the same design-first target as Trousers', () => {
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
    assert.ok(category.calc.fits.length, `${key} exposes fit coverage`);
  }
  for (const key of ['Shirt', 'T-shirt']) {
    const category = plan.categories.find(c => c.category === key);
    assert.equal(category.hasPrint, true);
    assert.ok(category.calc.prints.length, `${key} retains its print dimension`);
  }
});

test('legacy Casual UI presents shared upper and trouser workflow', () => {
  const html = fs.readFileSync(path.join(__dirname, '..', 'public', 'fresh-procurement.html'), 'utf8');
  assert.match(html, /Available for Trousers, Shirts and T-shirts/);
  assert.match(html, /same design-first setup/);
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
