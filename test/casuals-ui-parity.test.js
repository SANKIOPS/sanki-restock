const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const { buildPlan, settingsWithDefaults, validSplitBoxes, splitDetectionNeedsDetail, splitBoxesNeedFocusCards, safeZipImages, safeZipPackage, buildManifestRecommendation, unsafeImportIp } = require('../modules/casuals');
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

test('Procurement V2 ZIP maps Photo Splitter manifest metadata to images', () => {
  const zip = new AdmZip();
  zip.addFile('photos/brown.jpg', Buffer.from([1, 2, 3]));
  zip.addFile('manifest.json', Buffer.from(JSON.stringify({ items: [{ filename: 'brown.jpg', slot: 'R2', colour: 'Chocolate' }] })));
  const pkg = safeZipPackage(zip.toBuffer());
  assert.equal(pkg.manifestName, 'manifest.json');
  assert.equal(pkg.images[0].meta.slot, 'R2');
  assert.equal(pkg.images[0].meta.colour, 'Chocolate');
});

test('Procurement V2 re-scores manifest slots without a paid AI API', () => {
  const result = buildManifestRecommendation([
    { id: 'brown', suggestedSlot: 'R2', colour: 'Chocolate', vendor: 'MAG' },
    { id: 'manual', colour: 'Cream', vendor: 'MAG' }
  ], { mix: [{ name: 'R2 · Wide-leg · Single pleat', styles: 1 }], requirements: [{ type: 'Wide-leg', design: 'Single pleat', colours: ['Chocolate'] }] });
  assert.equal(result.selected[0].id, 'brown');
  assert.equal(result.selected[0].score, 25);
  assert.equal(result.basis.paidApi, false);
  assert.equal(result.excluded[0].excludeReason, 'Colour is outside the approved requirement');
});

test('Procurement V2 does not hard-lock every automated candidate to the same manifest slot', () => {
  const result = buildManifestRecommendation([
    { id:'black', suggestedSlot:'R14', slotSource:'manifest', colour:'Black', surface:'Solid' },
    { id:'beige', suggestedSlot:'R14', slotSource:'visual-auto', colour:'Beige', surface:'Solid' }
  ], { mix:[
    { name:'R1 · Office black', styles:1 },
    { name:'R2 · Casual beige', styles:1 },
    { name:'R14 · Pull-on summer', styles:1 }
  ], requirements:[
    { design:'Office', surface:'Solid', colours:['Black'] },
    { design:'Casual', surface:'Solid', colours:['Beige'] },
    { type:'Pull-on summer', design:'Linen-look', colours:['Olive'] }
  ] });
  assert.deepEqual(result.selected.map(x => x.slot).sort(), ['R1 · Office black', 'R2 · Casual beige']);
  assert.ok(result.selected.every(x => !/Exact slot supplied/.test(x.reason)));
});

test('Procurement V2 preserves an explicit manual slot assignment', () => {
  const result = buildManifestRecommendation([
    { id:'manual', suggestedSlot:'R14', slotSource:'manual', colour:'Black' }
  ], { mix:[{ name:'R14 · Pull-on summer', styles:1 }], requirements:[{}] });
  assert.equal(result.selected[0].score, 100);
  assert.equal(result.selected[0].reason, 'Manually assigned; hard requirements still require confirmation');
});

test('Procurement V2 uses the complete visual scorecard to fill different open slots', () => {
  const result = buildManifestRecommendation([
    { id:'a', slotSource:'visual-auto', visualMatches:{ 'R1 · Office':0.9, 'R2 · Casual':0.4 } },
    { id:'b', slotSource:'visual-auto', visualMatches:{ 'R1 · Office':0.5, 'R2 · Casual':0.88 } }
  ], { mix:[{ name:'R1 · Office', styles:1 },{ name:'R2 · Casual', styles:1 }], requirements:[{},{}] });
  assert.deepEqual(result.selected.map(x => x.slot).sort(), ['R1 · Office','R2 · Casual']);
  assert.equal(result.basis.priorities.visualScorecard,60);
});

test('Procurement V2 treats trouser type as a soft guide instead of an exclusion filter', () => {
  const result = buildManifestRecommendation([
    { id:'pull-on', garmentType:'Pull-on summer', designName:'Everyday casual', surface:'Solid', colour:'Black', vendor:'MAG' }
  ], { mix:[{ name:'R1 · Wide-leg · Office casual', styles:1 }], requirements:[{
    type:'Wide-leg', design:'Office casual', surface:'Solid', colours:['Black']
  }] });
  assert.equal(result.selected[0].id,'pull-on');
  assert.match(result.selected[0].reason,/Hard-eligible metadata match/);
  assert.equal(result.basis.typeMismatchExcludes,false);
  assert.equal(result.basis.priorities.silhouetteTypeSoftGuide,5);
  assert.equal(result.basis.priorities.colour,25);
});

test('Procurement V2 enforces hard colour requirements before soft visual ranking', () => {
  const result = buildManifestRecommendation([
    { id:'wrong', colour:'Red', visualMatches:{ 'R1 · Black office':0.99 } },
    { id:'right', colour:'Black', visualMatches:{ 'R1 · Black office':0.70 } }
  ], { category:'Trouser', mix:[{ name:'R1 · Black office', styles:1 }], requirements:[{ colours:['Black'], sizes:{28:1,30:2,32:2,34:1} }] });
  assert.equal(result.selected[0].id,'right');
  assert.match(result.excluded[0].excludeReason,/Colour is outside/);
  assert.deepEqual(result.basis.stages,['hard-limits','hard-requirements','soft-targets','soft-preferences']);
  assert.deepEqual(result.selected[0].hardRequirements.requiredSizes,{28:1,30:2,32:2,34:1});
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
  assert.match(addon, /\/api\/casuals\/v2-recommend/);
  assert.match(addon, /ordinary vendor photos/);
  assert.match(addon, /zero-shot-image-classification/);
  assert.match(addon, /Xenova\/clip-vit-base-patch32/);
  assert.match(addon, /Auto-detecting colour/);
  assert.doesNotMatch(addon, /\/api\/casuals\/analyze|\/api\/casuals\/simple-recommend/);
  assert.match(base, /fresh-procurement-v2.*SankiProcV2Base/);
  assert.match(base, /fresh-procurement-v2.*sanki_v2_current/);
});

test('Procurement V2 expands each design into no more than two hard colourway slots', () => {
  const v2AddonHtml = fs.readFileSync(path.join(__dirname, '..', 'public', 'fresh-procurement-v2-addon.js'), 'utf8');
  assert.match(v2AddonHtml, /function expandColourwayPlan/);
  assert.match(v2AddonHtml, /filter\(Boolean\)\.slice\(0,2\)/);
  assert.match(v2AddonHtml, /colourwayKey/);
  assert.match(v2AddonHtml, /addvariant\{display:none!important\}/);
});

test('Procurement V2 auto-detects and displays catalogue colours', () => {
  const v2AddonHtml = fs.readFileSync(path.join(__dirname, '..', 'public', 'fresh-procurement-v2-addon.js'), 'utf8');
  assert.match(v2AddonHtml, /Auto-detected:/);
  assert.match(v2AddonHtml, /colourSource:'auto'/);
  assert.match(v2AddonHtml, /colourConfidence/);
  assert.match(v2AddonHtml, /main fabric colour/);
});
