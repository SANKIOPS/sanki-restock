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

router.get('/api/inventory-categorization/catalog', async (req, res) => {
  try {
    if (!catalogCache.products || Date.now() - catalogCache.at > 30 * 60 * 1000) {
      if (!catalogInflight) catalogInflight = (async () => {
        try {
          const shopify = await fetchProducts(catalogClient);
          const byHandle = new Map(shopify.map(p => [p.handle, p]));
          catalogCache = {
            at: Date.now(),
            products: DATA.map(product => {
              const match = byHandle.get(product.handle);
              const images = match ? (match.images || []).map(image => image.src).filter(Boolean) : [];
              if (match && match.image && match.image.src && !images.includes(match.image.src)) images.unshift(match.image.src);
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
