const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const { buildPlan, settingsWithDefaults, validSplitBoxes } = require('../modules/casuals');

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
