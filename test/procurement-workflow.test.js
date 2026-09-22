const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const { parseSerial, nextSerial, buildSku, rebuildLineSku, canManagePurchases, canStartPaidPilot, parseLocalInvoiceText, genSeo, retireAudienceModelImages } = require('../modules/procurement');

test('listing copy does not repeat the product type and includes a display name', () => {
  const seo = genSeo({ designName: 'Casuals T-shirt', productType: 'T-Shirt', colour: 'Pink', fit: 'Oversized', audience: 'Unisex', sizeLabels: ['FS'] });
  assert.equal(seo.displayName, 'Casuals');
  assert.equal(seo.title, 'Casuals T-Shirt — Oversized Fit, Pink');
  assert.doesNotMatch(seo.metaTitle, /T-Shirt\s+T-Shirt/i);
});

test('basic women’s listing copy says Top, while winter keeps the bill product type', () => {
  const base = { designName: 'Casuals T-shirt', productType: 'T-Shirt', colour: 'White', fit: 'Muscle Fit', audience: 'Women', sizeLabels: ['FS'] };
  const summer = genSeo(base);
  assert.match(summer.title, /Casuals Top/);
  assert.doesNotMatch([summer.title, summer.metaTitle, summer.metaDescription, summer.imageAlt].join(' '), /\b(?:t[ -]?shirt|muscle\s*fit)\b/i);
  const winter = genSeo({ ...base, season: 'Winter' });
  assert.match(winter.title, /T-Shirt/);
});

test('original photo remains a private reference and posting requires approved views', () => {
  const html = fs.readFileSync(path.join(__dirname, '..', 'public', 'procurement.html'), 'utf8');
  const js = fs.readFileSync(path.join(__dirname, '..', 'modules', 'procurement.js'), 'utf8');
  assert.match(html, /Original references · never posted/);
  assert.doesNotMatch(html, /data-useoriginal/);
  assert.match(js, /\/use-original-photo'/);
  assert.match(js, /x\.type !== 'original'/);
  assert.match(js, /po\.status = 'posting_partial'/);
  assert.match(js, /Approve all required product and matching model views/);
  assert.match(js, /const variantConflicts = \[\.\.\.bySize\]/);
  assert.match(js, /Different SKUs have the same product, colour and size/);
});

test('purchase SKU serials roll from Z999 to AA1 without punctuation', () => {
  assert.deepEqual(nextSerial({ alpha: 'Z', num: 999 }), { alpha: 'AA', num: 1 });
  assert.deepEqual(nextSerial({ alpha: 'AA', num: 999 }), { alpha: 'AB', num: 1 });
  assert.deepEqual(parseSerial('SA111AA134'), { alpha: 'AA', num: 1 });
  assert.equal(
    buildSku({ brand: 'SA', products: { Trouser: 11 }, colours: { Black: 1 }, sizes: {} }, 'Trouser', 'Black', '34', { alpha: 'AA', num: 1 }).sku,
    'SA111AA134'
  );
});

test('trouser waist sizes 24 and 26 produce valid SKUs and preserve serials on edits', () => {
  const store = { brand: 'SA', products: { Trouser: 11 }, colours: { Black: 1, Blue: 2 }, sizes: {} };
  assert.equal(buildSku(store, 'Trouser', 'Black', '24', { alpha: 'AA', num: 1 }).sku, 'SA111AA124');
  assert.deepEqual(parseSerial('SA111AA124'), { alpha: 'AA', num: 1 });
  assert.equal(rebuildLineSku(store, { productType: 'Trouser', colour: 'Blue', sizeLabel: '26' }, 'SA111AA124').sku, 'SA112AA126');
});

test('invoice OCR can fill bill headers before vendor, bill number and date are entered', () => {
  const html = fs.readFileSync(path.join(__dirname, '..', 'public', 'procurement.html'), 'utf8');
  assert.match(html, /if\(d\.vendor\)/);
  assert.match(html, /vendorSel\.value=vendorName/);
  assert.doesNotMatch(html, /Select the vendor first \(required\)/);
  assert.doesNotMatch(html, /Enter the bill number first \(required\)/);
  assert.match(html, /var TROUSER_WAIST_SIZES=\['24','26'/);
  assert.match(html, /function sizesFor\(p\)\{ return isTrouser\(p\)\?TROUSER_WAIST_SIZES/);
});

test('China invoice reading uses local Chinese OCR and parses reviewable garment lines without paid AI credits', () => {
  const source = fs.readFileSync(path.join(__dirname, '..', 'modules', 'procurement.js'), 'utf8');
  const route = source.match(/router\.post\('\/api\/procurement\/parse-invoice'[\s\S]*?\n\}\);/)[0];
  assert.match(source, /@tesseract\.js-data\/chi_sim/);
  assert.match(route, /localInvoiceOcr/);
  assert.doesNotMatch(route, /api\.anthropic\.com/);
  assert.doesNotMatch(route, /ANTHROPIC_API_KEY/);

  const parsed = parseLocalInvoiceText([
    '广州衣尚服饰有限公司',
    '订单号: CN-7788',
    '日期: 2026年09月14日',
    '1 6921 阔腿裤 黑色 L 6 80 480',
    '2 A611 衬衫 白色 XL 10 55 550',
    '合计 1030'
  ].join('\n'), {
    products: { Trouser: 11, Shirt: 1, 'T-Shirt': 2 },
    colours: { Black: 1, White: 12 },
    vendors: []
  });
  assert.equal(parsed.vendor, '广州衣尚服饰有限公司');
  assert.equal(parsed.billNo, 'CN-7788');
  assert.equal(parsed.datePurchase, '2026-09-14');
  assert.deepEqual(parsed.lines.map(line => ({ code: line.designCode, type: line.productType, colour: line.colour, size: line.sizeLabel, qty: line.qty, price: line.perPcsYuan })), [
    { code: '6921', type: 'Trouser', colour: 'Black', size: 'L', qty: 6, price: 80 },
    { code: 'A611', type: 'Shirt', colour: 'White', size: 'XL', qty: 10, price: 55 }
  ]);
});

test('owner and procurement roles receive the full Purchases workflow in the UI', () => {
  const html = fs.readFileSync(path.join(__dirname, '..', 'public', 'procurement.html'), 'utf8');
  assert.match(html, /userRoles\.indexOf\('owner'\)>=0/);
  assert.match(html, /userRoles\.indexOf\('procurement'\)>=0/);
  assert.match(html, /userRoles\.indexOf\('inventory'\)>=0/);
  assert.match(html, /if\(me\.canManage && !posted\)/);
  assert.match(html, /Final preview/);
  assert.match(html, /Confirm &amp; post to Shopify/);
});

test('Nida-style Inventory users can call the complete Purchases workflow', () => {
  assert.equal(canManagePurchases({ user: { role: 'inventory', roles: ['inventory'] } }), true);
  assert.equal(canManagePurchases({ user: { role: 'inventory', roles: ['inventory', 'procurement'] } }), true);
  assert.equal(canManagePurchases({ user: { role: 'owner', roles: ['owner'] } }), true);
  assert.equal(canManagePurchases({ user: { role: 'sales', roles: ['sales'] } }), false);
});

test('inventory can start paid image generation, without granting it to other staff roles', () => {
  const html = fs.readFileSync(path.join(__dirname, '..', 'public', 'procurement.html'), 'utf8');
  assert.equal(canStartPaidPilot({ user: { role: 'inventory', roles: ['inventory'] } }), true);
  assert.equal(canStartPaidPilot({ user: { role: 'owner', roles: ['owner'] } }), true);
  assert.equal(canStartPaidPilot({ user: { role: 'admin' } }), true);
  assert.equal(canStartPaidPilot({ user: { role: 'procurement', roles: ['procurement'] } }), false);
  assert.equal(canStartPaidPilot({ user: { role: 'sales', roles: ['sales'] } }), false);
  assert.match(html, /function canStartPaidImages\(\)/);
  assert.match(html, /role==='owner'\|\|role==='admin'\|\|role==='inventory'/);
  assert.match(html, /canStartPaidImages\(\)\?'<button type="button" class="btn sm" data-openai-pilot=/);
  assert.match(html, /canStartPaidImages\(\)\?'<button class="btn ghost sm" data-paid-regen=/);
});

test('Purchases Summary has a category-first PO explorer plus the complete history', () => {
  const html = fs.readFileSync(path.join(__dirname, '..', 'public', 'procurement.html'), 'utf8');
  const js = fs.readFileSync(path.join(__dirname, '..', 'modules', 'procurement.js'), 'utf8');
  assert.match(html, /Purchase history/);
  assert.match(html, /fetch\('\/api\/procurement\/history'\)/);
  assert.match(html, /data-history-po/);
  assert.match(html, /historyBody/);
  assert.match(html, /Ordered<\/span>/);
  assert.match(html, /Received<\/span>/);
  assert.match(html, /Posted to Shopify/);
  assert.match(html, /Recovered from Shopify/);
  assert.match(html, /Historical purchase/);
  assert.match(html, /Explore purchases by category/);
  assert.match(html, /id="historyCategory"/);
  assert.match(html, /data-history-scope="all"/);
  assert.match(html, /data-history-scope="choose"/);
  assert.match(html, /data-history-pick/);
  assert.match(html, /All matching POs/);
  assert.match(html, /historyCategoryRows/);
  assert.match(html, /data-history-category-row/);
  assert.match(html, /historyDrillCategory/);
  assert.match(html, /historyCategoryLabel/);
  assert.match(html, /'T-Shirt':'T-Shirts'/);
  assert.match(html, /'Trouser':'Trousers'/);
  assert.match(html, /'Shirt':'Shirts'/);
  assert.doesNotMatch(html, /Matching purchase details/);
  assert.doesNotMatch(html, /id="historyExplorerResults"/);
  assert.match(html, /Filters above apply to this one list/);
  assert.match(html, /data-hx-po/);
  assert.match(html, /data-hx-design/);
  assert.doesNotMatch(js, /p\.id !== 'PO-0001'/);
  assert.doesNotMatch(js, /p\.id !== 'PO-0002'/);
  assert.doesNotMatch(js, /po\.id === 'PO-0001'/);
  assert.doesNotMatch(js, /po\.id === 'PO-0002'/);
  assert.match(html, /Click to enlarge/);
  assert.match(html, /Category<\/th><th>Colour<\/th><th>Size/);
  assert.match(html, /historyStatus\(po\)/);
  assert.match(html, /po\.dateReceive\)return 'received'/);
  assert.doesNotMatch(html, /data-history-select/);
  assert.match(html, /historyDesignKey\(po,line,category\)/);
  assert.doesNotMatch(html.match(/function historyDesignKey[\s\S]*?\n    \}/)[0], /colour/);
});

test('purchase history recovers unmatched Shopify products from August 2026 without inventing bill costs', () => {
  const html = fs.readFileSync(path.join(__dirname, '..', 'public', 'procurement.html'), 'utf8');
  const js = fs.readFileSync(path.join(__dirname, '..', 'modules', 'procurement.js'), 'utf8');
  assert.match(js, /created_at_min=2026-08-01T00:00:00/);
  assert.match(js, /fields=id,title,created_at,vendor,product_type,status,variants,images,image/);
  assert.match(js, /imageUrl: String\(\(p\.image && p\.image\.src\)/);
  assert.match(js, /await loadShopifyPurchaseHistory\(req\.query\.refresh === '1'\)/);
  assert.match(js, /linkedProducts\.get\(String\(product\.productId\)\) !== batch\.datePurchase/);
  assert.match(js, /historyWarning: 'Shopify recovery is temporarily unavailable:/);
  assert.match(html, /Shopify recovery/);
  assert.match(js, /inventory_items\.json\?ids=/);
  assert.match(js, /inventoryHistory\(first: 20, inventoryItemId:/);
  assert.match(js, /receivedByInventoryId\[id\]/);
  assert.match(js, /variant\.recordedCost =/);
  assert.match(js, /const byDateAndVendor = \{\}/);
  assert.match(js, /vendorNames: \[group\.vendor\]/);
  assert.match(js, /historicalPiecesKnown:/);
  assert.match(html, /pcs came/);
  assert.match(html, /Pieces came<\/th><th>Recorded cost\/pc/);
  assert.match(js, /b\.date\.localeCompare\(a\.date\) \|\| a\.vendor\.localeCompare\(b\.vendor\)/);
  assert.match(html, /Cost\/pc is Shopify\\'s saved inventory-item cost/);
  assert.match(html, /Recorded cost\/pc<\/th><th>Selling price<\/th><th>Weight\/pc/);
  assert.match(html, /unique\('recordedCost'\)/);
  assert.match(html, /p\.imageUrl\?'<img class="thumb" data-zoom/);
  assert.match(html, /<th>Photo<\/th><th>Product<\/th>/);
  assert.doesNotMatch(html, /purchaseHistoryHead\(po,'Not recorded','Not recorded','Not recorded','Not recorded','Not recorded','Not recorded'\)/);
});

test('Audit Purchases has a strict on-the-way category and vendor explorer', () => {
  const html = fs.readFileSync(path.join(__dirname, '..', 'public', 'procurement.html'), 'utf8');
  assert.match(html, /What is on the way\?/);
  assert.match(html, /data-owview="category"/);
  assert.match(html, /data-owview="vendor"/);
  assert.match(html, /if\(po\.status!==['"]advance['"]\) return/);
  assert.match(html, /All categories/);
  assert.match(html, /All fits/);
  assert.match(html, /All vendors/);
  assert.match(html, /All colours/);
  assert.match(html, /All sizes/);
  assert.match(html, /Arriving by/);
  assert.match(html, /data-owpo/);
  assert.match(html, /title="Click to enlarge"/);
  // Colourways and size rows are not separate designs. The identity stays
  // vendor + design code/name + category, while colour remains filterable.
  assert.match(html, /designKey:\[vendor,code\|\|name,category\]/);
  assert.doesNotMatch(html, /designKey:\[vendor,code\|\|name,category,owText\(l\.colour\)\]/);
});

test('audience and fit can be corrected during purchase audit', () => {
  const html = fs.readFileSync(path.join(__dirname, '..', 'public', 'procurement.html'), 'utf8');
  const js = fs.readFileSync(path.join(__dirname, '..', 'modules', 'procurement.js'), 'utf8');
  assert.match(html, /edSelect\(l,'audience'/);
  assert.match(html, /edSelect\(l,'fit'/);
  assert.match(js, /ORDERED_FIELDS = \[[^\]]*'audience'/);
  assert.match(js, /LINE_EDIT_FIELDS = \[[^\]]*'audience'/);
});

test('product audience control retires old model views but preserves product photos',()=>{
  const html=fs.readFileSync(path.join(__dirname,'..','public','procurement.html'),'utf8');
  const js=fs.readFileSync(path.join(__dirname,'..','modules','procurement.js'),'utf8');
  const po={aiImages:{'shirt|white':[
    {type:'front',url:'product.png',approved:true},
    {type:'model-front',url:'wrong-model.png',approved:true},
    {type:'model-side',url:'wrong-side.png',approved:true}
  ]},qaRejected:{'shirt|white':[{type:'model-front',url:'held.png'}]}};
  retireAudienceModelImages(po,'shirt|white','Men','Women');
  assert.deepEqual(po.aiImages['shirt|white'].map(image=>image.type),['front']);
  assert.deepEqual(po.qaRejected['shirt|white'],[]);
  assert.deepEqual(po.audienceImageHistory[0].images.map(image=>image.type),['model-front','model-side']);
  assert.match(html,/data-audience=/);
  assert.match(html,/data-reset-models=/);
  assert.match(html,/Only the selected model audience is generated/);
  assert.match(js,/router\.post\('\/api\/procurement\/pos\/:id\/group-audience'/);
  assert.match(js,/const audience = audiences\.length === 1/);
  assert.match(js,/retireAudienceModelImages\(po,key,oldAudiences\.join/);
  assert.match(js,/expireStalePaidAttempts\(po\);\s*if \(\(\(po\.openaiPilot/);
  assert.match(html,/required\.indexOf\(x\.type\)>=0/);
  assert.match(html,/id="f_audience"><option value="">Select audience/);
  assert.match(js,/audience:\s*\(raw\.audience \|\| ''\)\.trim\(\)/);
});

test('image-studio product variants include saved weight for product-scoped generation checks',()=>{
  const js=fs.readFileSync(path.join(__dirname,'..','modules','procurement.js'),'utf8');
  assert.match(js,/qty: l\.qty, weightGrams:l\.weightGrams, landed:/);
});

test('receipt can record missing and extra products without deleting the billed line', () => {
  const html = fs.readFileSync(path.join(__dirname, '..', 'public', 'procurement.html'), 'utf8');
  const js = fs.readFileSync(path.join(__dirname, '..', 'modules', 'procurement.js'), 'utf8');
  assert.match(html, /Did not arrive/);
  assert.match(html, /Add product received but not on bill/);
  assert.match(html, /Billed '\+esc\(l\.ordered/);
  assert.match(js, /router\.post\('\/api\/procurement\/pos\/:id\/receipt-missing'/);
  assert.match(js, /line\.qty = 0/);
  assert.match(js, /router\.post\('\/api\/procurement\/pos\/:id\/receipt-add'/);
  assert.match(js, /line\.ordered = \{ \.\.\.orderedSnapshot\(line\), qty: 0 \}/);
  assert.match(js, /const receivedLines = \(po\.lines \|\| \[\]\)\.filter\(line => num\(line\.qty\) > 0\)/);
});
