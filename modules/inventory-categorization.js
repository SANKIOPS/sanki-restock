'use strict';

const express = require('express');
const path = require('path');
const { ShopifyClient, shopifyClient } = require('./shopify-client');

const router = express.Router();
const DATA = require(path.join(__dirname, '..', 'public', 'inventory-data.json'));
const STORE = process.env.SHOPIFY_STORE || '';
const API = '2024-07';
const CONTROLLED_TAGS = ['SANKI Category:', 'SANKI Fit:', 'SANKI Gender:', 'SANKI Collection:'];
let job = { status: 'idle', total: DATA.length, completed: 0, updated: 0, skipped: 0, failed: 0, errors: [], startedAt: null, finishedAt: null };
let catalogCache = { at: 0, products: null };
let catalogInflight = null;
const catalogClient = new ShopifyClient({ minIntervalMs: 250 });
const galleryCache = new Map();
let costCache = { at: 0, products: null };
let costInflight = null;

async function jsonRequest(url, options) {
  const response = await shopifyClient.request(url, options);
  const body = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(`Shopify ${response.status}: ${JSON.stringify(body).slice(0, 300)}`);
  return body;
}

async function fetchProducts(client = shopifyClient) {
  let url = `https://${STORE}/admin/api/${API}/products.json?limit=250&fields=id,handle,title,product_type,tags,image,images`;
  const output = [];
  while (url) {
    const response = await client.request(url);
    const body = await response.json().catch(() => ({}));
    if (!response.ok) throw new Error(`Shopify ${response.status}: ${JSON.stringify(body).slice(0, 300)}`);
    output.push(...(body.products || []));
    const link = response.headers.get('Link') || '';
    const next = link.match(/<([^>]+)>;\s*rel="next"/);
    url = next ? next[1] : null;
  }
  return output;
}

async function fetchCatalogImages() {
  let url = `https://${STORE}/admin/api/${API}/products.json?limit=250&fields=handle,image`;
  const output = [];
  while (url) {
    const response = await catalogClient.request(url);
    const body = await response.json().catch(() => ({}));
    if (!response.ok) throw new Error(`Shopify ${response.status}: ${JSON.stringify(body).slice(0, 300)}`);
    output.push(...(body.products || []));
    const next = (response.headers.get('Link') || '').match(/<([^>]+)>;\s*rel="next"/);
    url = next ? next[1] : null;
  }
  return output;
}

async function fetchCostAttention() {
  let url = `https://${STORE}/admin/api/${API}/products.json?limit=250&fields=handle,title,image,variants`;
  const shopifyProducts = [];
  while (url) {
    const response = await catalogClient.request(url);
    const body = await response.json().catch(() => ({}));
    if (!response.ok) throw new Error(`Shopify ${response.status}: ${JSON.stringify(body).slice(0, 300)}`);
    shopifyProducts.push(...(body.products || []));
    const next = (response.headers.get('Link') || '').match(/<([^>]+)>;\s*rel="next"/);
    url = next ? next[1] : null;
  }
  const sourceByHandle = new Map(DATA.map(product => [product.handle, product]));
  const relevant = shopifyProducts.filter(product => sourceByHandle.has(product.handle));
  const itemIds = [];
  for (const product of relevant) for (const variant of (product.variants || [])) if (variant.inventory_item_id) itemIds.push(String(variant.inventory_item_id));
  const costs = new Map();
  for (let index = 0; index < itemIds.length; index += 100) {
    const ids = itemIds.slice(index, index + 100);
    const response = await catalogClient.request(`https://${STORE}/admin/api/${API}/inventory_items.json?ids=${ids.join(',')}&limit=100`);
    const body = await response.json().catch(() => ({}));
    if (!response.ok) throw new Error(`Shopify ${response.status}: ${JSON.stringify(body).slice(0, 300)}`);
    for (const item of (body.inventory_items || [])) costs.set(String(item.id), Number(item.cost) || 0);
  }
  return relevant.map(product => {
    const source = sourceByHandle.get(product.handle);
    const physicalBySku = new Map((source.variants || []).map(variant => [String(variant.sku || '').trim(), variant]));
    const missing = (product.variants || []).map(variant => {
      const sku = String(variant.sku || '').trim();
      const physical = physicalBySku.get(sku);
      return { sku, inventoryItemId: String(variant.inventory_item_id || ''), cost: costs.get(String(variant.inventory_item_id)) || 0, physical };
    }).filter(item => item.physical && (Number(item.physical.displayQty) + Number(item.physical.warehouseQty)) > 0 && !item.cost && item.inventoryItemId);
    if (!missing.length) return null;
    return {
      handle: product.handle,
      title: source.title || product.title,
      image: product.image && product.image.src || null,
      collection: source.collection,
      category: source.category,
      productType: source.productType,
      gender: source.gender,
      missingSkus: missing.map(item => ({
        sku: item.sku,
        inventoryItemId: item.inventoryItemId,
        variant: item.physical.variant,
        size: item.physical.size,
        colour: item.physical.colour,
        displayQty: Number(item.physical.displayQty) || 0,
        warehouseQty: Number(item.physical.warehouseQty) || 0,
        totalQty: (Number(item.physical.displayQty) || 0) + (Number(item.physical.warehouseQty) || 0)
      }))
    };
  }).filter(Boolean);
}

function desiredTags(product, existing) {
  const keep = String(existing || '').split(',').map(x => x.trim()).filter(Boolean)
    .filter(tag => !CONTROLLED_TAGS.some(prefix => tag.toLowerCase().startsWith(prefix.toLowerCase())));
  return Array.from(new Set(keep.concat([
    `SANKI Category: ${product.category}`,
    `SANKI Fit: ${product.fit}`,
    `SANKI Gender: ${product.gender}`,
    `SANKI Collection: ${product.collection}`
  ])));
}

function metafields(product, ownerId) {
  const values = {
    inventory_category: product.category,
    product_type_detail: product.productType,
    fit: product.fit,
    gender: product.gender,
    collection_line: product.collection,
    design: product.design,
    fabric: product.fabric,
    season: product.season,
    sleeves: product.sleeves
  };
  return Object.entries(values).map(([key, value]) => ({ ownerId, namespace: 'custom', key, type: 'single_line_text_field', value: String(value || 'Unknown') }));
}

async function graphQL(query, variables) {
  const body = await jsonRequest(`https://${STORE}/admin/api/${API}/graphql.json`, {
    method: 'POST', body: JSON.stringify({ query, variables })
  });
  if (body.errors && body.errors.length) throw new Error(body.errors.map(x => x.message).join('; '));
  return body.data || {};
}

async function updateProduct(source, shopifyProduct) {
  const gid = `gid://shopify/Product/${shopifyProduct.id}`;
  const update = await graphQL(
    'mutation UpdateProduct($input: ProductInput!){productUpdate(input:$input){product{id} userErrors{field message}}}',
    { input: { id: gid, productType: source.productType, tags: desiredTags(source, shopifyProduct.tags) } }
  );
  const productErrors = (update.productUpdate && update.productUpdate.userErrors) || [];
  if (productErrors.length) throw new Error(productErrors.map(x => x.message).join('; '));
  const set = await graphQL(
    'mutation SetInventoryFields($metafields:[MetafieldsSetInput!]!){metafieldsSet(metafields:$metafields){metafields{key} userErrors{field message code}}}',
    { metafields: metafields(source, gid) }
  );
  const fieldErrors = (set.metafieldsSet && set.metafieldsSet.userErrors) || [];
  if (fieldErrors.length) throw new Error(fieldErrors.map(x => x.message).join('; '));
}

async function runApply() {
  try {
    const shopify = await fetchProducts();
    const byHandle = new Map(shopify.map(p => [p.handle, p]));
    for (const product of DATA) {
      try {
        const target = byHandle.get(product.handle);
        if (!target) { job.skipped++; job.errors.push({ handle: product.handle, error: 'Product handle not found in Shopify' }); }
        else { await updateProduct(product, target); job.updated++; }
      } catch (error) {
        job.failed++;
        job.errors.push({ handle: product.handle, error: String(error.message || error).slice(0, 240) });
      }
      job.completed++;
      if (job.errors.length > 50) job.errors = job.errors.slice(-50);
    }
    job.status = job.failed ? 'completed_with_errors' : 'succeeded';
  } catch (error) {
    job.status = 'failed';
    job.errors.push({ handle: null, error: String(error.message || error).slice(0, 300) });
  } finally { job.finishedAt = new Date().toISOString(); }
}

router.get('/api/inventory-categorization/status', (req, res) => res.json({ success: true, job }));

router.get('/api/inventory-costs/attention', async (req, res) => {
  try {
    if (!costCache.products || Date.now() - costCache.at > 15 * 60 * 1000) {
      if (!costInflight) costInflight = fetchCostAttention().then(products => { costCache = { at: Date.now(), products }; }).finally(() => { costInflight = null; });
      await costInflight;
    }
    const missingSkus = costCache.products.reduce((total, product) => total + product.missingSkus.length, 0);
    const missingPieces = costCache.products.reduce((total, product) => total + product.missingSkus.reduce((qty, sku) => qty + sku.totalQty, 0), 0);
    res.json({ success: true, products: costCache.products, productCount: costCache.products.length, missingSkus, missingPieces });
  } catch (error) { res.status(502).json({ success: false, error: String(error.message || error) }); }
});

router.post('/api/inventory-costs/set', async (req, res) => {
  try {
    const cost = Number(req.body && req.body.cost);
    const itemIds = Array.from(new Set((req.body && req.body.inventoryItemIds || []).map(String).filter(id => /^\d+$/.test(id))));
    if (!Number.isFinite(cost) || cost <= 0 || cost > 100000) return res.status(400).json({ success: false, error: 'Enter a valid landed cost between ₹0.01 and ₹1,00,000.' });
    if (!itemIds.length || itemIds.length > 500) return res.status(400).json({ success: false, error: 'No valid SKUs were selected.' });
    if (!costCache.products) return res.status(409).json({ success: false, error: 'Refresh Needs Attention before saving.' });
    const allowedIds = new Set(costCache.products.flatMap(product => product.missingSkus.map(sku => sku.inventoryItemId)));
    if (itemIds.some(id => !allowedIds.has(id))) return res.status(400).json({ success: false, error: 'One or more SKUs are not in the current missing-cost audit.' });
    for (const id of itemIds) {
      const response = await shopifyClient.request(`https://${STORE}/admin/api/${API}/inventory_items/${id}.json`, {
        method: 'PUT', body: JSON.stringify({ inventory_item: { id: Number(id), cost: cost.toFixed(2) } })
      });
      const body = await response.json().catch(() => ({}));
      if (!response.ok) throw new Error(`Shopify ${response.status}: ${JSON.stringify(body).slice(0, 300)}`);
    }
    const savedIds = new Set(itemIds);
    costCache.products = costCache.products.map(product => ({
      ...product,
      missingSkus: product.missingSkus.filter(sku => !savedIds.has(sku.inventoryItemId))
    })).filter(product => product.missingSkus.length);
    costCache.at = Date.now();
    res.json({ success: true, updated: itemIds.length, cost });
  } catch (error) { res.status(502).json({ success: false, error: String(error.message || error) }); }
});

router.get('/api/inventory-categorization/catalog', async (req, res) => {
  try {
    if (!catalogCache.products || Date.now() - catalogCache.at > 30 * 60 * 1000) {
      if (!catalogInflight) catalogInflight = (async () => {
        try {
          const shopify = await fetchCatalogImages();
          const byHandle = new Map(shopify.map(p => [p.handle, p]));
          catalogCache = {
            at: Date.now(),
            products: DATA.map(product => {
              const match = byHandle.get(product.handle);
              const images = match && match.image && match.image.src ? [match.image.src] : [];
              return { ...product, images, image: images[0] || null };
            })
          };
        } finally { catalogInflight = null; }
      })();
      await catalogInflight;
    }
    res.json({ success: true, products: catalogCache.products });
  } catch (error) { res.status(502).json({ success: false, error: String(error.message || error) }); }
});

router.get('/api/inventory-categorization/catalog/:handle', async (req, res) => {
  try {
    const handle = String(req.params.handle || '');
    const cached = galleryCache.get(handle);
    if (cached && Date.now() - cached.at < 30 * 60 * 1000) return res.json({ success: true, ...cached.data });
    const url = `https://${STORE}/admin/api/${API}/products.json?limit=1&handle=${encodeURIComponent(handle)}&fields=handle,images,image,variants`;
    const response = await catalogClient.request(url);
    const body = await response.json().catch(() => ({}));
    if (!response.ok) throw new Error(`Shopify ${response.status}: ${JSON.stringify(body).slice(0, 300)}`);
    const product = (body.products || []).find(item => item.handle === handle);
    if (!product) return res.status(404).json({ success: false, error: 'Product not found in Shopify' });
    const images = (product.images || []).map(image => ({ src: image.src, variantIds: (image.variant_ids || []).map(String) })).filter(image => image.src);
    if (!images.length && product.image && product.image.src) images.push({ src: product.image.src, variantIds: [] });
    const variantImages = {};
    for (const variant of (product.variants || [])) {
      const matched = images.filter(image => image.variantIds.includes(String(variant.id))).map(image => image.src);
      variantImages[String(variant.sku || '').trim()] = matched.length ? matched : (images[0] ? [images[0].src] : []);
    }
    const data = { handle, images: images.map(image => image.src), variantImages };
    galleryCache.set(handle, { at: Date.now(), data });
    res.json({ success: true, ...data });
  } catch (error) { res.status(502).json({ success: false, error: String(error.message || error) }); }
});

router.get('/api/inventory-categorization/preview', async (req, res) => {
  try {
    const shopify = await fetchProducts();
    const byHandle = new Map(shopify.map(p => [p.handle, p]));
    const missing = DATA.filter(p => !byHandle.has(p.handle)).map(p => p.handle);
    res.json({ success: true, dashboardProducts: DATA.length, matchedProducts: DATA.length - missing.length, missingProducts: missing.length, missingHandles: missing.slice(0, 50), fields: ['Product type', 'Category', 'Fit', 'Gender', 'Collection', 'Design', 'Fabric', 'Season', 'Sleeves'], quantitiesChanged: false, productsDeleted: false });
  } catch (error) { res.status(502).json({ success: false, error: String(error.message || error) }); }
});

router.post('/api/inventory-categorization/apply', (req, res) => {
  if (req.body && req.body.confirmation !== 'APPLY APPROVED CATEGORIZATION') return res.status(400).json({ success: false, error: 'Confirmation phrase is required.' });
  if (job.status === 'running') return res.status(409).json({ success: false, error: 'Categorization update is already running.', job });
  job = { status: 'running', total: DATA.length, completed: 0, updated: 0, skipped: 0, failed: 0, errors: [], startedAt: new Date().toISOString(), finishedAt: null };
  setImmediate(() => runApply());
  res.status(202).json({ success: true, job });
});

module.exports = { router };
