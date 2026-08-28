const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const { buildPlan, settingsWithDefaults, validSplitBoxes, splitDetectionNeedsDetail, splitBoxesNeedFocusCards } = require('../modules/casuals');

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
  assert.match(html, /AI reconstructed/);
  assert.match(html, /View original/);
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

test('photo splitter retries a suspicious whole-image crop at garment level', () => {
  assert.equal(splitDetectionNeedsDetail([[0.02, 0.03, 0.98, 0.97]]), true);
  assert.equal(splitDetectionNeedsDetail([[0.20, 0.10, 0.70, 0.80]]), false);
  assert.equal(splitDetectionNeedsDetail([
    [0.05, 0.10, 0.45, 0.90],
    [0.55, 0.10, 0.95, 0.90]
  ]), false);
});

test('photo splitter preserves the full source with focus cards for overlapping garments', () => {
  assert.equal(splitBoxesNeedFocusCards([
    [0.08, 0.10, 0.48, 0.92],
    [0.26, 0.11, 0.68, 0.91],
    [0.49, 0.09, 0.91, 0.93]
  ]), true);
  assert.equal(splitBoxesNeedFocusCards([
    [0.02, 0.05, 0.45, 0.95],
    [0.55, 0.05, 0.98, 0.95]
  ]), false);
});

test('photo splitter reconstruction uses OpenAI exclusively', () => {
  const source = fs.readFileSync(path.join(__dirname, '..', 'modules', 'casuals.js'), 'utf8');
  assert.match(source, /OPENAI_API_KEY/);
  assert.match(source, /api\.openai\.com\/v1\/images\/edits/);
  assert.match(source, /gpt-image-1\.5/);
  const splitterVision = source.slice(source.indexOf('async function requestPhotoBoxes'), source.indexOf('async function detectPhotoBoxes'));
  assert.match(splitterVision, /api\.openai\.com\/v1\/chat\/completions/);
  assert.doesNotMatch(splitterVision, /api\.anthropic\.com|ANTHROPIC_API_KEY/);
  assert.doesNotMatch(source, /GEMINI_API_KEY|GOOGLE_API_KEY|generativelanguage\.googleapis\.com/);
});

test('photo splitter requires a destination batch before upload', () => {
  const html = fs.readFileSync(path.join(__dirname, '..', 'public', 'fresh-procurement.html'), 'utf8');
  assert.match(html, /id="czSplitChoose"/);
  assert.match(html, /Create a destination batch first, then choose the collage photos/);
  assert.match(html, /Photo splitter server error/);
});
