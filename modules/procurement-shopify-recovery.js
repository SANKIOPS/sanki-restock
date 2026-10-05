'use strict';

const crypto = require('crypto');
const skuOf = value => String(value || '').trim().toUpperCase();
const fingerprint = value => crypto.createHash('sha256').update(JSON.stringify(value)).digest('hex');

// Recovery uses the posted receipt, never a newly classified purchase preview.
// Existing restock stock is preserved; explicitly selected deleted variants
// can be recreated with their saved receipt quantity.
function buildRecoveryPlan(po, catalogue, {groupKey, sizes, readPhoto, inventoryMode = 'zero', includeRestocks = false}) {
  if (po.status !== 'posted') throw new Error('Only a posted purchase can recover deleted Shopify drafts.');
  if (!['zero', 'received'].includes(inventoryMode)) throw new Error('Choose zero stock or saved received quantities.');
  const received = (po.lines || []).filter(line => Number(line.qty) > 0);
  const restocks = received.filter(line => line.classification === 'EXISTING');
  const skippedRestocks = restocks.filter(line => !includeRestocks || (catalogue[skuOf(line.sku)] || []).length === 1);
  const original = received.filter(line => line.classification === 'NEW' ||
    includeRestocks && line.classification === 'EXISTING' && !skippedRestocks.includes(line));
  const groups = new Map();
  for (const line of original) {
    const key = groupKey(line);
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(line);
  }
  const allSkus = original.map(line => skuOf(line.sku));
  if (allSkus.some(sku => !sku) || new Set(allSkus).size !== allSkus.length) throw new Error('The saved receipt has missing or duplicate SKUs. No recovery write is allowed.');
  const rawUrls = new Set((po.lines || []).flatMap(line => [line.photoUrl, line.rawPhotoUrl]).filter(Boolean));
  const rejectedUrls = new Set((po.imageRejectionHistory || []).map(image => image.url));
  const products = [];
  for (const [key, lines] of groups) {
    const skus = lines.map(line => skuOf(line.sku));
    const matches = skus.map(sku => catalogue[sku] || []);
    const present = skus.filter((sku, index) => matches[index].length);
    const productIds = [...new Set(matches.flat().map(match => String(match.productId)))];
    const snapshot = (po.newProducts || []).find(product => product.key === key);
    const approvedSeo = (po.seoDraft || []).find(draft => draft.key === key && draft.seoApproved);
    const seo = snapshot && snapshot.seo || approvedSeo && approvedSeo.seo;
    const row = {key, label: (lines[0].designCode || lines[0].designName || key) + ' · ' + (lines[0].colour || ''),
      skus, receivedQty: lines.reduce((sum, line) => sum + Number(line.qty), 0), present,
      productIds, status: 'ready', issues: [], images: [], variants: [], seo: seo || null,
      productType: lines[0].productType, colour: lines[0].colour, designCode: lines[0].designCode,
      vendor: po.vendor, designName: lines[0].designName};
    if (present.length) {
      row.status = present.length === skus.length && productIds.length === 1 && matches.every(list => list.length === 1) ? 'existing' : 'blocked';
      if (row.status === 'blocked') row.issues.push('Only some SKUs exist, or SKUs belong to multiple products. Reconcile these products before recovery.');
      products.push(row);
      continue;
    }
    if ((po.shopifyDraftRecoveryHistory || []).some(run => run.status === 'needs-reconciliation' && run.currentGroup === key))
      row.issues.push('An earlier creation outcome is uncertain. Reconcile that attempt in Shopify before retrying this product.');
    if (!seo || !String(seo.title || '').trim() || !String(seo.bodyHtml || '').trim()) row.issues.push('Saved posted listing text is missing.');
    const active = (po.aiImages || {})[key] || [];
    const postedImages = snapshot && snapshot.images;
    const images = Array.isArray(postedImages) && postedImages.length ? postedImages : active.filter(image => image.approved);
    const seen = new Set();
    for (const image of images) {
      if (!image || !image.url || seen.has(image.url) || rawUrls.has(image.url) || rejectedUrls.has(image.url) || image.type === 'original') continue;
      const current = active.find(candidate => candidate.url === image.url);
      if (current && (current.approved === false || current.qa && !['pass', 'manual-reviewed'].includes(current.qa.status))) continue;
      if (image.qa && !['pass', 'manual-reviewed'].includes(image.qa.status)) continue;
      const source = readPhoto(image.url);
      if (!source || !source.buf || !source.buf.length) continue;
      seen.add(image.url);
      row.images.push({url: image.url, alt: image.alt || seo && seo.imageAlt || '',
        sha256: crypto.createHash('sha256').update(source.buf).digest('hex')});
    }
    if (!row.images.length) row.issues.push('No readable previously approved listing photos remain. Original reference photos are not used as replacements.');
    const usedSizes = new Set();
    for (const line of lines) {
      const sku = skuOf(line.sku), savedVariant = ((snapshot || {}).variants || []).find(variant => skuOf(variant.sku) === sku);
      const sizeCode = String(savedVariant && savedVariant.sizeCode || sizes[line.sizeLabel] || line.sizeLabel || '').trim();
      const price = Number(savedVariant && savedVariant.price != null ? savedVariant.price : line.manualMrp || line.suggestedMrp);
      if (!sizeCode || usedSizes.has(sizeCode)) row.issues.push('Missing or duplicate size for ' + sku + '.');
      usedSizes.add(sizeCode);
      if (!Number.isFinite(price) || price <= 0) row.issues.push('Saved selling price is missing for ' + sku + '.');
      const qty = Number(line.qty);
      if (!Number.isSafeInteger(qty) || qty <= 0) row.issues.push('Invalid received quantity for ' + sku + '.');
      row.variants.push({sku, sizeCode, price, qty: inventoryMode === 'received' ? qty : 0});
    }
    if (row.issues.length) row.status = 'blocked';
    products.push(row);
  }
  const plan = {poId: po.id, inventoryMode, includeRestocks, products,
    excludedNotReceived: (po.lines || []).length - received.length,
    excludedRestocks: skippedRestocks.map(line => ({sku: skuOf(line.sku), qty: Number(line.qty)}))};
  plan.fingerprint = fingerprint(plan);
  return plan;
}

function publicRecoveryPlan(plan) {
  return {...plan, products: plan.products.map(({seo, images, variants, ...product}) => ({...product,
    title: seo && seo.title || '', photoCount: images.length,
    photos: images.map(image => ({url: image.url, alt: image.alt})),
    variants: variants.map(({sku, sizeCode, price, qty}) => ({sku, size: sizeCode, price, qty}))}))};
}

module.exports = {buildRecoveryPlan, publicRecoveryPlan};
