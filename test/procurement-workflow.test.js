const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const { parseSerial, nextSerial, buildSku, rebuildLineSku, canManagePurchases, canStartPaidPilot, canReviewPaidImage, parseLocalInvoiceText, genSeo, canonicalSeoNaming, retireAudienceModelImages, reconcileStudioKeysAfterLineEdit } = require('../modules/procurement');

test('purchase product corrections move one-to-one studio work but preserve ambiguous splits for recovery', () => {
  const bundle = [{ type: 'front', url: '/paid-a.png', approved: true }];
  const renamed = { aiImages: { '87375|white': bundle }, imageStyling: { '87375|white': { pair: 'Jeans' } }, seoDraft: [{ key: '87375|white', seoApproved: true }] };
  reconcileStudioKeysAfterLineEdit(renamed,
    [{ sku: 'M', designCode: '87375', colour: 'White', photoUrl: '/raw-a.png' }],
    [{ sku: 'M', designCode: '87375 A', colour: 'White', photoUrl: '/raw-a.png' }]);
  assert.strictEqual(renamed.aiImages['87375 a|white'], bundle);
  assert.equal(renamed.aiImages['87375|white'], undefined);
  assert.equal(renamed.seoDraft[0].key, '87375 a|white');

  const split = { aiImages: { '87375|white': bundle }, seoDraft: [{ key: '87375|white', seoApproved: true }] };
  reconcileStudioKeysAfterLineEdit(split,
    [{ sku: 'M', designCode: '87375', colour: 'White', photoUrl: '/raw-a.png' }, { sku: 'L', designCode: '87375', colour: 'White', photoUrl: '/raw-b.png' }],
    [{ sku: 'M', designCode: '87375 A', colour: 'White', photoUrl: '/raw-a.png' }, { sku: 'L', designCode: '87375 B', colour: 'White', photoUrl: '/raw-b.png' }]);
  assert.strictEqual(split.aiImages['87375|white'], bundle);
  assert.equal(split.aiImages['87375 a|white'], undefined);
  assert.deepEqual(split.orphanedStudioDrafts[0].targetKeys.sort(), ['87375 a|white', '87375 b|white']);
  assert.deepEqual(split.orphanedStudioDrafts[0].sourcePhotos, ['/raw-a.png', '/raw-b.png']);
});

test('listing copy does not repeat the product type and includes a display name', () => {
  const seo = genSeo({ designName: 'Casuals T-shirt', productType: 'T-Shirt', colour: 'Pink', fit: 'Oversized', audience: 'Unisex', sizeLabels: ['FS'] });
  assert.equal(seo.displayName, 'Casuals');
  assert.equal(seo.title, 'SANKI Pink Oversized Fit Casuals T-Shirt Unisex');
  assert.doesNotMatch(seo.metaTitle, /T-Shirt\s+T-Shirt/i);
});

test('purchase SEO titles use one catalogue pattern and normalize equivalent button wording', () => {
  const group = { designCode: 'H26353', colour: 'Olive', fit: 'Slim Fit', productType: 'T-Shirt', audience: 'Women' };
  const first = canonicalSeoNaming({ displayName: 'Olive Slim Fit V-Neck Top with Button Placket for Women' }, group);
  assert.equal(first.title, 'SANKI Olive Slim Fit V-Neck Button-Detail Top for Women');
  assert.equal(first.displayName, 'V-Neck Button-Detail Top');
  assert.ok(first.metaTitle.length <= 60);
  const white = canonicalSeoNaming({ displayName: 'SANKI White Slim Fit V-Neck Button-Trim Top for Women' }, { ...group, colour: 'White' }, first.styleDescriptor);
  assert.equal(white.title, 'SANKI White Slim Fit V-Neck Button-Detail Top for Women');
  const purple = canonicalSeoNaming({ displayName: 'SANKI Purple V-Neck Knit Top Slim Fit FS' }, { ...group, colour: 'Purple' });
  assert.equal(purple.title, 'SANKI Purple Slim Fit V-Neck Knit Top for Women');
  assert.doesNotMatch(purple.title, /\bFS\b/);
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
  assert.match(html, /Posting was started, but did not finish/);
  assert.match(html, /Where posting stopped/);
  assert.match(html, /Do not post this PO again/);
  assert.match(html, /Check Shopify &amp; post only missing products/);
  assert.match(js, /resume-posting/);
  assert.match(js, /loadCatalogue\(true\)/);
  assert.match(js, /A Shopify product is only partly present/);
  assert.match(js, /mergedSameArticle/);
  assert.match(js, /Missing readable approved image\(s\)/);
  assert.match(js, /allowLostImages/);
  assert.match(html, /allowLostImages:true/);
  assert.match(js, /missing readable approved view/);
  assert.match(js, /recoverableLostFlatFront/);
  assert.match(js, /imageRecoveryHistory/);
  assert.match(js, /np\.images = readableApproved/);
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

test('invoice OCR rejoins split rows and extracts vendor code, Top, size, quantity and unit price', () => {
  const parsed = parseLocalInvoiceText([
    '供应商: NTVG',
    '单号: TOP-991',
    '货号: WZ882',
    '女装上衣 米白色',
    '尺码: XL 数量: 12 单价: 48 金额: 576'
  ].join('\n'), {
    products: { Top: 20, Shirt: 1, 'T-Shirt': 2 },
    colours: { Cream: 4, White: 12 },
    vendors: ['NTVG']
  });
  assert.equal(parsed.vendor, 'NTVG');
  assert.equal(parsed.billNo, 'TOP-991');
  assert.deepEqual(parsed.lines.map(line => ({ code: line.designCode, type: line.productType, size: line.sizeLabel, qty: line.qty, price: line.perPcsYuan })), [
    { code: 'WZ882', type: 'Top', size: 'XL', qty: 12, price: 48 }
  ]);
});

test('invoice OCR expands Chinese size-grid tables and carries design codes across colour rows', () => {
  const parsed = parseLocalInvoiceText([
    '名称 颜色 M L XL 2XL 数量 单价 金额',
    '6801#西装面料阔腿裤秋款',
    '黑色 1 1 1 2 5 51 255',
    '灰色 1 1 1 2 5 51 255',
    '合计 数量:10 金额:510'
  ].join('\n'), {
    products: { Trouser: 11, Shirt: 1 }, colours: { Black: 1, Grey: 6 }, vendors: []
  });
  assert.equal(parsed.lines.length, 8);
  assert.deepEqual(parsed.lines.map(line => line.designCode), Array(8).fill('6801'));
  assert.deepEqual(parsed.lines.slice(0, 4).map(line => [line.sizeLabel, line.qty, line.perPcsYuan]),
    [['M', 1, 51], ['L', 1, 51], ['XL', 1, 51], ['XXL', 2, 51]]);
  assert.equal(parsed.totals.extractedQty, 10);
  assert.equal(parsed.totals.extractedAmount, 510);
  assert.deepEqual(parsed.warnings, []);
});

test('invoice OCR carries product codes through compact colour-list invoices and validates totals', () => {
  const parsed = parseLocalInvoiceText([
    '商品 颜色 数量 单价 小计',
    '971 黑色 5 22 110',
    '白杏 5 22 110',
    '栗灰 5 22 110',
    '星灰 5 22 110',
    '958 黑色 5 21 105',
    '粉色 5 21 105',
    '杏灰 5 21 105',
    '合计 数量:35 金额:755'
  ].join('\n'), {
    products: { Top: 20 }, colours: { Black: 1, White: 12, Beige: 14, Grey: 6, Pink: 9 }, vendors: []
  });
  assert.deepEqual(parsed.lines.map(line => line.designCode), ['971', '971', '971', '971', '958', '958', '958']);
  assert.equal(parsed.totals.extractedQty, 35);
  assert.equal(parsed.totals.extractedAmount, 755);
  assert.doesNotMatch(parsed.warnings.join(' '), /Invoice says|Invoice total/);
});

test('Top is a permanent intake lookup and added-line quantity is retained', () => {
  const source = fs.readFileSync(path.join(__dirname, '..', 'modules', 'procurement.js'), 'utf8');
  const html = fs.readFileSync(path.join(__dirname, '..', 'public', 'procurement.html'), 'utf8');
  assert.match(source, /'Top': 20/);
  assert.match(source, /s\.products = \{ \.\.\.SEED\.products, \.\.\.s\.products \}/);
  assert.doesNotMatch(html, /function resetLineForm\(\)\{\s*el\('f_qty'\)\.value='1'/);
});

test('duplicate vendor bill numbers are rejected consistently', () => {
  const { duplicateBillPo } = require('../modules/procurement');
  const store = { pos: {
    'PO-0001': { id: 'PO-0001', billNo: ' INV  /  77 ' },
    'PO-0002': { id: 'PO-0002', billNo: 'OTHER-1' }
  } };
  assert.equal(duplicateBillPo(store, 'inv/77').id, 'PO-0001');
  assert.equal(duplicateBillPo(store, 'INV / 77', 'PO-0001'), null);
  assert.equal(duplicateBillPo(store, 'new-1'), null);
});

test('Audit Purchase saves in place and propagates weight across an article', () => {
  const html = fs.readFileSync(path.join(__dirname, '..', 'public', 'procurement.html'), 'utf8');
  assert.match(html, /function auditArticleKey\(index\)/);
  assert.match(html, /auditArticleKey\(other\.getAttribute\('data-w'\)\)===key/);
  const saveBlock = html.slice(html.indexOf('function saveCorrections(id)'), html.indexOf('function computeReceive(id)'));
  assert.doesNotMatch(saveBlock, /loadPos\(\).*openPo/);
  assert.match(saveBlock, /Corrections saved/);
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
  assert.equal(canReviewPaidImage({ user: { role: 'inventory', roles: ['inventory'] } }), true);
  assert.equal(canReviewPaidImage({ user: { role: 'procurement', roles: ['procurement'] } }), false);
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
