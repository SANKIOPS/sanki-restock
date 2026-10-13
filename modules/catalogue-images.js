'use strict';

const express = require('express');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const multer = require('multer');
const sharp = require('sharp');
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
const codexUpload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 15 * 1024 * 1024, files: 1 },
  fileFilter: (req, file, cb) => cb(null, /^image\/(?:jpeg|png|webp)$/i.test(file.mimetype || ''))
});

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
function groupFor(item) {
  return { key: item.productId, designCode: item.sku, designName: item.title, colour: item.colour,
    productType: item.productType, audience: item.audience, fit: item.fit, line: item.line, season: '', sizeLabels: [] };
}
function defaultStyling(item) {
  const group = groupFor(item);
  const tuck = String(item.audience).toLowerCase() === 'women' && pilot.garmentCategory(group) === 'upper'
    ? 'Tucked in'
    : 'Auto';
  return pilot.normalizeStyling({ aesthetic: 'Limestone estate', modelOrigin: 'Indian', femaleComplexion: 'Fair', maleComplexion: 'Fair',
    pair: 'Auto', tuck, shoes: 'Auto', chain: 'None', capStyle: 'None', bagStyle: 'None', bagColour: 'Auto', sunglasses: false, watch: false }, group);
}
function safeFile(name) { return /^[a-f0-9-]+-[a-z-]+-\d+\.png$/.test(name || '') ? name : ''; }
function filePath(name) { const clean = safeFile(name); return clean ? path.join(IMAGE_DIR, clean) : ''; }
function draftBuffer(draft) { const file = filePath(draft && draft.file); return file && fs.existsSync(file) ? fs.readFileSync(file) : null; }

function codexFingerprint(item) {
  return crypto.createHash('sha256').update(JSON.stringify({
    productId:item.productId, sku:item.sku, audience:item.audience, sourceUrl:item.sourceUrl,
    requiredViews:item.requiredViews
  })).digest('hex');
}
function codexBatchManifest(pilotState) {
  const batchId = crypto.randomUUID();
  return {
    id: batchId,
    version: 1,
    pilotId: pilotState.id,
    createdAt: now(),
    status: 'prepared',
    mode: 'codex-built-in-image-generation',
    billing: 'Do not call the Railway OpenAI or Gemini APIs. Generate in the user-started Codex session with the built-in image tool.',
    instructions: [
      'Use the selected Shopify image only as the garment reference.',
      'Preserve the exact garment colour, print, weave, collar, buttons, pockets, sleeves, hem, silhouette and length.',
      'Return one image file per requested view. Never return a collage, split frame or contact sheet.',
      'Model views must match the saved audience exactly. Women use an adult woman; Men use an adult man.',
      'Every women\'s upper must be tucked into its supporting trousers in all model views.',
      'Use understated old-money styling, a limestone-estate setting and natural editorial lighting.',
      'Keep the same trousers, shoes, accessories and model identity between matching front and three-quarter views.',
      'Do not add logos, text or watermarks. Do not invent unseen product details.',
      'Upload each result to Catalogue Images, inspect it, and approve it there before publishing to Shopify.'
    ],
    products: pilotState.items.map(item => {
      const group=groupFor(item), styling=defaultStyling(item);
      return {
        productId:item.productId, handle:item.handle, sku:item.sku, title:item.title, audience:item.audience,
        productType:item.productType, colour:item.colour, fit:item.fit, line:item.line,
        sourceUrl:item.sourceUrl, sourceName:imageName(item.sourceUrl), fingerprint:codexFingerprint(item), styling,
        views:item.requiredViews.map(type => ({
          type,
          label:type.replace(/-/g,' '),
          continuityView:type === 'model-side' ? 'model-front' : null,
          prompt:pilot.imagePrompt(group,type,styling,type === 'model-side')
        }))
      };
    })
  };
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
router.get('/api/catalogue-images', (req,res) => res.json({ success:true, ...publicState(load()), generating:false }));
router.post('/api/catalogue-images/pilot/prepare', async (req,res) => {
  try {
    const products = await fetchProducts(), items = choosePilot(products, 4);
    const counts = Object.fromEntries(KNOWN_AUDIENCES.map(audience => [audience, items.filter(item => item.audience === audience).length]));
    if (KNOWN_AUDIENCES.some(audience => counts[audience] < 4)) return res.status(409).json({ success:false, error:'Could not find four source-ready clothing products for every audience.', counts });
    const state = load();
    if (state.pilot) state.history.unshift({ createdAt:state.pilot.createdAt, finishedAt:state.pilot.finishedAt || null, status:state.pilot.status, items:state.pilot.items.map(item => ({productId:item.productId,sku:item.sku,status:item.status})) });
    state.history = state.history.slice(0, 10);
    state.pilot = { id:crypto.randomUUID(), createdAt:now(), status:'ready', running:false, counts, items,
      codexBatches:{}, message:'Mixed pilot prepared: 4 Women, 4 Men and 4 Unisex products. Confirm each source and audience, then prepare the Codex batch.' };
    save(state); res.json({ success:true, ...publicState(state) });
  } catch (error) { res.status(502).json({ success:false, error:String(error.message || error) }); }
});
router.patch('/api/catalogue-images/pilot/items/:productId', (req,res) => {
  try {
    const state=load(), item=state.pilot?.items.find(value => value.productId === req.params.productId);
    if (!item) return res.status(404).json({ success:false, error:'Pilot product not found.' });
    const audience=String(req.body?.audience || item.audience), sourceUrl=String(req.body?.sourceUrl || item.sourceUrl);
    if (!KNOWN_AUDIENCES.includes(audience)) return res.status(400).json({ success:false, error:'Select Women, Men or Unisex.' });
    if (!item.currentImages.some(image => image.src === sourceUrl)) return res.status(400).json({ success:false, error:'Select one of this product’s current Shopify images.' });
    item.audience=audience;item.sourceUrl=sourceUrl;item.requiredViews=profileFor(groupFor(item),false).views;item.drafts=[];item.attempts=[];item.status='ready';item.message='Setup saved. Prepare a fresh Codex batch before returning images.';
    state.pilot.activeCodexBatchId='';state.pilot.status='ready';state.pilot.message='A product setup changed. Prepare a fresh Codex batch before returning images.';save(state);
    res.json({ success:true, ...publicState(state) });
  } catch (error) { res.status(500).json({ success:false, error:String(error.message || error) }); }
});
router.post('/api/catalogue-images/pilot/generate', (req,res) => {
  res.status(410).json({ success:false, error:'Paid Railway generation is disabled for Catalogue Images. Prepare a Codex batch and generate it in Codex instead.' });
});
router.post('/api/catalogue-images/pilot/codex-batch', (req,res) => {
  try {
    const state=load();
    if(!state.pilot?.items?.length)return res.status(409).json({success:false,error:'Prepare the mixed pilot first.'});
    if(state.pilot.items.some(item=>!KNOWN_AUDIENCES.includes(item.audience)||!item.sourceUrl||item.requiredViews.length!==3))
      return res.status(409).json({success:false,error:'Every pilot product needs a saved audience, source and three requested views.'});
    const batch=codexBatchManifest(state.pilot);
    state.pilot.codexBatches=state.pilot.codexBatches||{};
    state.pilot.codexBatches[batch.id]=batch;
    state.pilot.activeCodexBatchId=batch.id;
    state.pilot.status='codex-prepared';
    state.pilot.running=false;
    state.pilot.message='Codex batch prepared. Generate the files in Codex, then return each view here for review. No paid Railway AI call was made.';
    for(const item of state.pilot.items)if(!['passed','published'].includes(item.status)){item.status='codex-prepared';item.message='Waiting for Codex images from batch '+batch.id+'.';}
    save(state);
    res.json({success:true,batch,...publicState(state)});
  }catch(error){res.status(400).json({success:false,error:String(error.message||error)});}
});
router.get('/api/catalogue-images/pilot/codex-batch/:batchId', (req,res) => {
  const state=load(),batch=state.pilot?.codexBatches?.[req.params.batchId];
  if(!batch)return res.status(404).json({success:false,error:'Codex batch not found.'});
  res.json({success:true,batch});
});
router.post('/api/catalogue-images/pilot/codex-batch/:batchId/items/:productId/results', codexUpload.single('image'), async (req,res) => {
  try{
    if(!req.file)return res.status(400).json({success:false,error:'Choose a JPG, PNG or WebP image.'});
    const state=load(),batch=state.pilot?.codexBatches?.[req.params.batchId],item=state.pilot?.items?.find(value=>value.productId===req.params.productId);
    if(!batch||!item)return res.status(404).json({success:false,error:'Codex batch or product not found.'});
    const batchItem=batch.products.find(value=>value.productId===item.productId),type=String(req.body?.type||'');
    if(!batchItem||batchItem.fingerprint!==codexFingerprint(item))return res.status(409).json({success:false,error:'The product setup changed. Prepare a fresh Codex batch.'});
    if(!item.requiredViews.includes(type))return res.status(400).json({success:false,error:'Select a requested view for this product.'});
    const metadata=await sharp(req.file.buffer,{failOn:'error'}).metadata();
    if(!metadata.width||!metadata.height||metadata.width<700||metadata.height<700)throw new Error('Return a clear image at least 700 × 700 pixels.');
    fs.mkdirSync(IMAGE_DIR,{recursive:true});
    const filename=crypto.randomUUID()+'-'+type+'-codex-1.png';
    await sharp(req.file.buffer,{failOn:'error'}).rotate().png({quality:96}).toFile(path.join(IMAGE_DIR,filename));
    const prior=item.drafts.find(draft=>draft.type===type);
    if(prior)item.attempts.push({...prior,replacedAt:now()});
    const record={type,attempt:1,file:filename,qa:{status:'manual-review',warnings:[],issues:[]},accepted:false,source:'codex-batch',codexBatchId:batch.id,createdAt:now()};
    item.drafts=item.drafts.filter(draft=>draft.type!==type);item.drafts.push(record);item.status='review';item.message='Codex image returned. Inspect it and approve the view before publishing.';
    batch.status='partially-returned';batch.returnedAt=now();state.pilot.status='review';state.pilot.message='Codex images are being returned for owner review. Shopify remains unchanged.';save(state);
    res.json({success:true,...publicState(state)});
  }catch(error){res.status(400).json({success:false,error:String(error.message||error)});}
});
router.post('/api/catalogue-images/pilot/items/:productId/drafts/:type/review', (req,res) => {
  try{
    const state=load(),item=state.pilot?.items?.find(value=>value.productId===req.params.productId),type=String(req.params.type||'');
    if(!item)return res.status(404).json({success:false,error:'Pilot product not found.'});
    const draft=item.drafts.find(value=>value.type===type&&draftBuffer(value));
    if(!draft)return res.status(404).json({success:false,error:'Returned Codex image not found.'});
    draft.accepted=req.body?.accepted===true;draft.qa={status:draft.accepted?'pass':'manual-review',warnings:[],issues:[]};draft.reviewedAt=now();draft.reviewedBy=req.user.username;
    const complete=item.requiredViews.every(view=>item.drafts.some(value=>value.type===view&&value.accepted&&draftBuffer(value)));
    item.status=complete?'passed':'review';item.message=complete?'All three Codex images are approved. The trio is ready to publish.':'View saved. Approve all three returned Codex images before publishing.';
    state.pilot.status=state.pilot.items.every(value=>['passed','published'].includes(value.status))?'passed':'review';
    state.pilot.message=state.pilot.status==='passed'?'Every pilot product has an approved Codex trio.':'Codex images are under review. Shopify remains unchanged.';save(state);
    res.json({success:true,...publicState(state)});
  }catch(error){res.status(400).json({success:false,error:String(error.message||error)});}
});
router.post('/api/catalogue-images/pilot/items/:productId/publish', async (req,res) => {
  try {
    if (req.body?.confirm !== true) return res.status(400).json({ success:false, error:'Publishing confirmation is required.' });
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

module.exports = { router, audienceOf, itemFromProduct, candidateScore, choosePilot, groupFor, defaultStyling, codexFingerprint, codexBatchManifest };
