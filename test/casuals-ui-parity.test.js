const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const { buildPlan, settingsWithDefaults } = require('../modules/casuals');

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
  assert.doesNotMatch(html, /Plan this category by/);
});
