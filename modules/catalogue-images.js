'use strict';

const express = require('express');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const fetch = require('node-fetch');
const { shopifyClient } = require('./shopify-client');
const pilot = require('./procurement-openai-pilot');
const profileFor = require('../public/purchase-product-profile');
const inventoryData = require('../public/inventory-data.json');

const router = express.Router();
const STORE = process.env.SHOPIFY_STORE || '';
const API = '2024-07';
const ROOT = process.env.DATA_PATH ? path.dirname(process.env.DATA_PATH) : path.join(__dirname, '..');
const STATE_FILE = path.join(ROOT, 'catalogue-image-studio.json');
const IMAGE_DIR = path.join(ROOT, 'catalogue-images');
const KNOWN_AUDIENCES = ['Women', 'Men', 'Unisex'];
const byHandle = new Map(inventoryData.map(item => [String(item.handle || ''), item]));
let runner = null;

function now() { return new Date().toISOString(); }
function atomicWrite(file, data) {
  const tmp = file + '.tmp-' + process.pid + '-' + Date.now();
  fs.writeFileSync(tmp, data);
  fs.renameSync(tmp, file);
}
function blankState() { return { version: 1, revision: 0, pilot: null, history: [] }; }
function load() {
  let state;
  try { state = JSON.parse(fs.readFileSync(STATE_FILE, 'utf8')); } catch { state = blankState(); }
  if (!state || !Array.isArray(state.history)) state = blankState();
  if (state.pilot && state.pilot.running) {
    state.pilot.running = false;
    state.pilot.status = 'interrupted';
    for (const item of state.pilot.items || []) if (['queued','generating'].includes(item.status)) item.status = 'interrupted';
    state.pilot.message = 'The server restarted during generation. Existing saved drafts were preserved; start generation again to continue only unfinished products.';
    save(state);
  }
  return state;
}
function save(state) {
  fs.mkdirSync(path.dirname(STATE_FILE), { recursive: true });
  state.revision = Number(state.revision || 0) + 1;
  atomicWrite(STATE_FILE, JSON.stringify(state, null, 2));
}
function owner(req) { return ((req.user && req.user.roles) || []).includes('owner'); }
function cleanTags(tags) { return Array.isArray(tags) ? tags : String(tags || '').split(',').map(x => x.trim()).filter(Boolean); }
function tagged(tags, prefix) {
  const value = cleanTags(tags).find(tag => tag.toLowerCase().startsWith(prefix.toLowerCase()));
  return value ? value.slice(prefix.length).trim() : '';
}
function normalizeAudience(value) {
  const text = String(value || '').toLowerCase();
  if (/women|female/.test(text)) return 'Women';
  if (/unisex/.test(text)) return 'Unisex';
  if (/men|male/.test(text)) return 'Men';
  return '';
}
function audienceOf(product) {
  const inventory = byHandle.get(String(product.handle || ''));
  return normalizeAudience(tagged(product.tags, 'SANKI Gender:')) || normalizeAudience(inventory && inventory.gender) ||
    normalizeAudience((String(product.title || '').match(/(?:for\s+women|women(?:'s)?|for\s+men|men(?:'s)?|unisex)/i) || [])[0]);
}
function colourOf(product) {
  const inventory = byHandle.get(String(product.handle || ''));
  const colorOption = (product.options || []).find(option => /^colou?r$/i.test(option.name || ''));
  const optionValue = colorOption && colorOption.values && colorOption.values[0];
  const inventoryColour = inventory && inventory.variants && inventory.variants[0] && inventory.variants[0].colour;
  const titleColour = String(product.title || '').match(/\b(black|white|blue|navy|brown|cream|green|grey|gray|maroon|orange|pink|purple|red|yellow|beige|olive|khaki|golden|silver)\b/i);
  return String(optionValue || inventoryColour || (titleColour && titleColour[1]) || 'Unknown');
}
function productTypeOf(product) {
  const inventory = byHandle.get(String(product.handle || ''));
  return String((inventory && (inventory.productType || inventory.category)) || tagged(product.tags, 'SANKI Category:') || product.product_type || 'Product');
}
function fitOf(product) {
  const inventory = byHandle.get(String(product.handle || ''));
  return String(tagged(product.tags, 'SANKI Fit:') || (inventory && inventory.fit) || '');
}
function lineOf(product) {
  const inventory = byHandle.get(String(product.handle || ''));
  return String(tagged(product.tags, 'SANKI Collection:') || (inventory && inventory.collection) || 'Casuals');
}
function skuOf(product) {
  const positive = (product.variants || []).find(variant => Number(variant.inventory_quantity || 0) > 0);
  return String((positive || product.variants?.[0] || {}).sku || '').trim().toUpperCase();
}
function imageName(src) {
  try { return decodeURIComponent(new URL(src).pathname.split('/').pop() || ''); } catch { return ''; }
}
function publicImages(product) {
  return (product.images || []).map((image, index) => ({ id: String(image.id || ''), src: image.src, alt: image.alt || '', position: Number(image.position || index + 1), name: imageName(image.src) })).filter(image => image.src);
}
function itemFromProduct(product) {
  const audience = audienceOf(product), images = publicImages(product), group = {
    key: String(product.id), designCode: skuOf(product), designName: product.title, colour: colourOf(product),
    productType: productTypeOf(product), audience, fit: fitOf(product), line: lineOf(product), season: ''
  };
  const views = audience ? profileFor(group, false).views : [];
  return { productId: String(product.id), handle: product.handle, title: product.title, sku: skuOf(product), audience,
    productType: group.productType, colour: group.colour, fit: group.fit, line: group.line, currentImages: images,
    sourceUrl: images[0] && images[0].src || '', requiredViews: views, status: 'ready', drafts: [], attempts: [],
    message: images.length ? 'Ready to generate a staged three-image set.' : 'No Shopify image is available as a product reference.' };
}
function candidateScore(item) {
  const count = item.currentImages.length;
  let score = count < 3 ? 1000 - count * 100 : count === 3 ? 450 : 100;
  if (item.currentImages.some(image => /whatsapp|screen\s*shot|screenshot/i.test(image.name))) score += 300;
  if (count > 10) score += 80;
  return score;
}
function choosePilot(products, perAudience = 4) {
  const items = products.map(itemFromProduct).filter(item => item.sourceUrl && KNOWN_AUDIENCES.includes(item.audience) && !profileFor(item, false).productOnly);
  return KNOWN_AUDIENCES.flatMap(audience => items.filter(item => item.audience === audience)
    .sort((a,b) => candidateScore(b) - candidateScore(a) || a.sku.localeCompare(b.sku)).slice(0, perAudience));
}
async function shopifyJson(url, options = {}) {
  const response = await shopifyClient.request(url, options);
  const body = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error('Shopify ' + response.status + ': ' + JSON.stringify(body.errors || body).slice(0, 240));
  return { body, response };
}
async function fetchProducts() {
  let url = `https://${STORE}/admin/api/${API}/products.json?limit=250&fields=id,title,handle,status,published_at,product_type,tags,images,variants,options`;
  const products = [];
  while (url) {
    const { body, response } = await shopifyJson(url);
    products.push(...(body.products || []).filter(product => product.status === 'active' && (product.variants || []).some(variant => Number(variant.inventory_quantity || 0) > 0)));
    const match = (response.headers.get('Link') || '').match(/<([^>]+)>;\s*rel="next"/);
    url = match ? match[1] : null;
  }
  return products;
}
async function downloadSource(url) {
  const parsed = new URL(url);
  if (parsed.protocol !== 'https:' || parsed.hostname !== 'cdn.shopify.com') throw new Error('Only a Shopify CDN image can be used as the source.');
  const response = await fetch(url, { signal: AbortSignal.timeout(60000), size: 20 * 1024 * 1024 });
  if (!response.ok) throw new Error('Could not download the selected Shopify source image.');
  const finalUrl = new URL(response.url || url);
  if (finalUrl.protocol !== 'https:' || finalUrl.hostname !== 'cdn.shopify.com') throw new Error('The source image redirected outside Shopify CDN.');
  const buf = await response.buffer();
  if (!buf.length || buf.length > 20 * 1024 * 1024) throw new Error('The selected source image is empty or too large.');
  return { buf, mime: response.headers.get('content-type') || 'image/jpeg' };
}
function groupFor(item) {
  return { key: item.productId, designCode: item.sku, designName: item.title, colour: item.colour,
    productType: item.productType, audience: item.audience, fit: item.fit, line: item.line, season: '', sizeLabels: [] };
}
function defaultStyling(item) {
  return pilot.normalizeStyling({ aesthetic: 'Limestone estate', modelOrigin: 'Indian', femaleComplexion: 'Fair', maleComplexion: 'Fair',
    pair: 'Auto', tuck: 'Auto', shoes: 'Auto', chain: 'None', capStyle: 'None', bagStyle: 'None', bagColour: 'Auto', sunglasses: false, watch: false }, groupFor(item));
}
function safeFile(name) { return /^[a-f0-9-]+-[a-z-]+-\d+\.png$/.test(name || '') ? name : ''; }
function filePath(name) { const clean = safeFile(name); return clean ? path.join(IMAGE_DIR, clean) : ''; }
function draftBuffer(draft) { const file = filePath(draft && draft.file); return file && fs.existsSync(file) ? fs.readFileSync(file) : null; }
function accepted(qa) { return qa && (qa.status === 'pass' || pilot.canAutoAcceptAdvisoryCheck(qa)); }

async function generateView(item, type, source, group, styling) {
  const continuityType = type === 'model-side' ? 'model-front' : type === 'model-side-female' ? 'female' : type === 'model-side-male' ? 'male' : '';
  const continuityDraft = continuityType && item.drafts.find(draft => draft.type === continuityType && draft.accepted);
  const continuityBuffer = continuityDraft && draftBuffer(continuityDraft);
  if (continuityType && !continuityBuffer) throw new Error('The matching model front must pass before the three-quarter view is generated.');
  let repair = [];
  for (let attempt = 1; attempt <= 2; attempt++) {
    const made = await pilot.generateImage({ key: process.env.OPENAI_API_KEY, group, source, continuitySource: continuityBuffer ? { buf: continuityBuffer } : null,
      type, styling, repairFields: repair });
    const qa = await pilot.verifyImage({ key: process.env.OPENAI_API_KEY, group, source, generated: made.buffer,
      continuitySource: continuityBuffer ? { buf: continuityBuffer } : null, type, styling });
    fs.mkdirSync(IMAGE_DIR, { recursive: true });
    const filename = crypto.randomUUID() + '-' + type + '-' + attempt + '.png';
    fs.writeFileSync(path.join(IMAGE_DIR, filename), made.buffer);
    const record = { type, attempt, file: filename, qa, accepted: accepted(qa), createdAt: now(), usage: made.usage || null };
    item.attempts.push(record);
    if (record.accepted) { item.drafts = item.drafts.filter(draft => draft.type !== type); item.drafts.push(record); return record; }
    if (!pilot.shouldRetryImageCheck(qa, attempt, 2)) return record;
    repair = qa.failed || [];
  }
}
async function generateItem(state, item) {
  item.status = 'generating'; item.message = 'Downloading the selected source image…'; item.startedAt = now(); save(state);
  try {
    if (!process.env.OPENAI_API_KEY) throw new Error('OPENAI_API_KEY is not configured. No paid generation was started.');
    if (!KNOWN_AUDIENCES.includes(item.audience)) throw new Error('Select Women, Men or Unisex before generation.');
    if (!item.sourceUrl) throw new Error('Select a real product reference before generation.');
    const source = await downloadSource(item.sourceUrl), group = groupFor(item), styling = defaultStyling(item);
    const required = profileFor(group, false).views;
    item.requiredViews = required;
    for (const type of required) {
      if (item.drafts.some(draft => draft.type === type && draft.accepted && draftBuffer(draft))) continue;
      item.message = 'Generating and checking ' + type.replace(/-/g, ' ') + '…'; save(state);
      const result = await generateView(item, type, source, group, styling);
      if (!result || !result.accepted) break;
    }
    const missing = required.filter(type => !item.drafts.some(draft => draft.type === type && draft.accepted && draftBuffer(draft)));
    item.status = missing.length ? 'review' : 'passed';
    item.message = missing.length ? 'Held by visual QA: ' + missing.join(', ') + '. Existing Shopify gallery was not changed.' : 'All three staged images passed visual QA. Review them before publishing.';
  } catch (error) {
    item.status = 'failed'; item.message = String(error.message || error).slice(0, 500);
  }
  item.finishedAt = now(); save(state);
}
async function runQueue(state) {
  try {
    for (const item of state.pilot.items) if (['queued','interrupted','ready','failed','review'].includes(item.status) && !item.publishedAt) await generateItem(state, item);
    state.pilot.status = state.pilot.items.every(item => ['passed','published'].includes(item.status)) ? 'passed' : 'review';
    state.pilot.message = state.pilot.status === 'passed' ? 'Pilot generation finished. Every product has a complete QA-passed trio.' : 'Pilot generation finished. Passed products are ready; held products need review or regeneration.';
  } catch (error) {
    state.pilot.status = 'failed'; state.pilot.message = String(error.message || error).slice(0, 500);
  } finally {
    state.pilot.running = false; state.pilot.finishedAt = now(); save(state); runner = null;
  }
}
function publicState(state) {
  const clone = JSON.parse(JSON.stringify(state));
  for (const item of clone.pilot?.items || []) for (const draft of item.drafts || []) draft.url = '/api/catalogue-images/files/' + draft.file;
  return clone;
}
async function uploadImage(productId, buffer, alt) {
  const { body } = await shopifyJson(`https://${STORE}/admin/api/${API}/products/${productId}/images.json`, {
    method: 'POST', body: JSON.stringify({ image: { attachment: buffer.toString('base64'), alt: String(alt || '').slice(0, 512) } })
  });
  if (!body.image || !body.image.id) throw new Error('Shopify did not return the new image ID.');
  return String(body.image.id);
}
async function setPosition(productId, imageId, position) {
  await shopifyJson(`https://${STORE}/admin/api/${API}/products/${productId}/images/${imageId}.json`, {
    method: 'PUT', body: JSON.stringify({ image: { id: Number(imageId), position } })
  });
}

router.use('/api/catalogue-images', (req,res,next) => owner(req) ? next() : res.status(403).json({ success:false, error:'Owner only.' }));
router.get('/api/catalogue-images', (req,res) => res.json({ success:true, ...publicState(load()), generating:!!runner }));
router.post('/api/catalogue-images/pilot/prepare', async (req,res) => {
  try {
    if (runner) return res.status(409).json({ success:false, error:'Generation is already running.' });
    const products = await fetchProducts(), items = choosePilot(products, 4);
    const counts = Object.fromEntries(KNOWN_AUDIENCES.map(audience => [audience, items.filter(item => item.audience === audience).length]));
    if (KNOWN_AUDIENCES.some(audience => counts[audience] < 4)) return res.status(409).json({ success:false, error:'Could not find four source-ready clothing products for every audience.', counts });
    const state = load();
    if (state.pilot) state.history.unshift({ createdAt:state.pilot.createdAt, finishedAt:state.pilot.finishedAt || null, status:state.pilot.status, items:state.pilot.items.map(item => ({productId:item.productId,sku:item.sku,status:item.status})) });
    state.history = state.history.slice(0, 10);
    state.pilot = { id:crypto.randomUUID(), createdAt:now(), status:'ready', running:false, counts, items,
      message:'Mixed pilot prepared: 4 Women, 4 Men and 4 Unisex products. Confirm each source and audience before paid generation.' };
    save(state); res.json({ success:true, ...publicState(state) });
  } catch (error) { res.status(502).json({ success:false, error:String(error.message || error) }); }
});
router.patch('/api/catalogue-images/pilot/items/:productId', (req,res) => {
  try {
    if (runner) return res.status(409).json({ success:false, error:'Wait for generation to finish before changing the setup.' });
    const state=load(), item=state.pilot?.items.find(value => value.productId === req.params.productId);
    if (!item) return res.status(404).json({ success:false, error:'Pilot product not found.' });
    const audience=String(req.body?.audience || item.audience), sourceUrl=String(req.body?.sourceUrl || item.sourceUrl);
    if (!KNOWN_AUDIENCES.includes(audience)) return res.status(400).json({ success:false, error:'Select Women, Men or Unisex.' });
    if (!item.currentImages.some(image => image.src === sourceUrl)) return res.status(400).json({ success:false, error:'Select one of this product’s current Shopify images.' });
    item.audience=audience;item.sourceUrl=sourceUrl;item.requiredViews=profileFor(groupFor(item),false).views;item.drafts=[];item.attempts=[];item.status='ready';item.message='Setup saved. Ready for paid generation.';save(state);
    res.json({ success:true, ...publicState(state) });
  } catch (error) { res.status(500).json({ success:false, error:String(error.message || error) }); }
});
router.post('/api/catalogue-images/pilot/generate', (req,res) => {
  try {
    if (req.body?.confirmPaid !== true) return res.status(400).json({ success:false, error:'Paid generation confirmation is required.' });
    if (runner) return res.status(409).json({ success:false, error:'Generation is already running.' });
    const state=load();
    if (!state.pilot?.items?.length) return res.status(409).json({ success:false, error:'Prepare the mixed pilot first.' });
    if (!process.env.OPENAI_API_KEY) return res.status(503).json({ success:false, error:'OPENAI_API_KEY is not configured. No paid generation was started.' });
    state.pilot.running=true;state.pilot.status='running';state.pilot.startedAt=now();state.pilot.message='Generating one product at a time. Shopify galleries remain unchanged.';
    for (const item of state.pilot.items) if (!['passed','published'].includes(item.status)) item.status='queued';
    save(state); runner=runQueue(state); res.status(202).json({ success:true, message:state.pilot.message });
  } catch (error) { res.status(500).json({ success:false, error:String(error.message || error) }); }
});
router.post('/api/catalogue-images/pilot/items/:productId/publish', async (req,res) => {
  try {
    if (req.body?.confirm !== true) return res.status(400).json({ success:false, error:'Publishing confirmation is required.' });
    if (runner) return res.status(409).json({ success:false, error:'Wait for paid generation to finish before publishing.' });
    const state=load(), item=state.pilot?.items.find(value => value.productId === req.params.productId);
    if (!item) return res.status(404).json({ success:false, error:'Pilot product not found.' });
    const ordered=item.requiredViews.map(type => item.drafts.find(draft => draft.type===type&&draft.accepted&&draftBuffer(draft)));
    if (ordered.some(draft => !draft) || ordered.length !== 3) return res.status(409).json({ success:false, error:'A complete QA-passed three-image set is required.' });
    item.status='publishing';item.message='Uploading the staged trio. Existing images are being retained.';item.uploadedImageIds=[];save(state);
    for (const draft of ordered) { const id=await uploadImage(item.productId,draftBuffer(draft),item.title+' · '+draft.type.replace(/-/g,' ')+' · '+item.sku);item.uploadedImageIds.push(id);save(state); }
    for (let index=ordered.length-1;index>=0;index--) await setPosition(item.productId,item.uploadedImageIds[index],index+1);
    const {body}=await shopifyJson(`https://${STORE}/admin/api/${API}/products/${item.productId}.json?fields=id,images`), first=(body.product?.images||[]).slice(0,3).map(image=>String(image.id));
    if (first.join(',')!==item.uploadedImageIds.join(',')) throw new Error('Shopify uploaded the trio but did not confirm positions 1–3. Existing images were retained; inspect this product before retrying.');
    item.status='published';item.publishedAt=now();item.message='The verified trio is live in positions 1–3. Earlier gallery images remain after it for rollback.';save(state);
    res.json({ success:true, ...publicState(state) });
  } catch (error) { const state=load(),item=state.pilot?.items.find(value=>value.productId===req.params.productId);if(item){item.status='publish-review';item.message=String(error.message||error).slice(0,500);save(state);}res.status(502).json({success:false,error:String(error.message||error)}); }
});
router.get('/api/catalogue-images/files/:name', (req,res) => {
  const file=filePath(req.params.name);if(!file||!fs.existsSync(file))return res.status(404).end();res.setHeader('Cache-Control','private, max-age=300');res.type('png').sendFile(file);
});

module.exports = { router, audienceOf, itemFromProduct, candidateScore, choosePilot, groupFor, defaultStyling };
