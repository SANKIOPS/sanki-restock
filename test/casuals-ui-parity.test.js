const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const { buildPlan, settingsWithDefaults, validSplitBoxes, splitDetectionNeedsDetail, splitBoxesNeedFocusCards, safeZipImages, unsafeImportIp } = require('../modules/casuals');
const AdmZip = require('adm-zip');

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

test('ZIP import keeps supported images and ignores metadata and non-images', () => {
  const zip = new AdmZip();
  zip.addFile('vendor/look-1.JPG', Buffer.from([1, 2, 3]));
  zip.addFile('__MACOSX/._look-1.JPG', Buffer.from([4]));
  zip.addFile('notes.txt', Buffer.from('no'));
  const images = safeZipImages(zip.toBuffer());
  assert.equal(images.length, 1);
  assert.equal(images[0].name, 'look-1.JPG');
});

test('ZIP link guard identifies local and private network addresses', () => {
  ['127.0.0.1', '10.1.2.3', '172.16.0.1', '192.168.1.2', '169.254.1.1', '::1', 'fd00::1'].forEach(ip => assert.equal(unsafeImportIp(ip), true));
  assert.equal(unsafeImportIp('8.8.8.8'), false);
});

test('fresh procurement UI offers ZIP file and ZIP link import', () => {
  const html = fs.readFileSync(path.join(__dirname, '..', 'public', 'fresh-procurement.html'), 'utf8');
  assert.match(html, /id="czZipFile"/);
  assert.match(html, /id="czZipUrl"/);
  assert.match(html, /\/api\/casuals\/candidates\/import-zip-url/);
});

test('Procurement V2 duplicates the proven workflow and adds a controlling requirement table', () => {
  const loader = fs.readFileSync(path.join(__dirname, '..', 'public', 'fresh-procurement-v2.html'), 'utf8');
  const base = fs.readFileSync(path.join(__dirname, '..', 'public', 'fresh-procurement-simple.html'), 'utf8');
  const addon = fs.readFileSync(path.join(__dirname, '..', 'public', 'fresh-procurement-v2-addon.js'), 'utf8');
  assert.match(loader, /fresh-procurement-simple\.html/);
  assert.match(loader, /fresh-procurement-v2-addon\.js/);
  assert.match(addon, /Detailed sourcing requirement/);
  assert.match(addon, /v2-plan\/import/);
  assert.match(addon, /Approve requirement table & add vendor photos/);
  assert.match(addon, /base\.addVendor\(\)/);
  assert.match(addon, /base\.go\(2\)/);
  assert.match(base, /fresh-procurement-v2.*SankiProcV2Base/);
  assert.match(base, /fresh-procurement-v2.*sanki_v2_current/);
});
