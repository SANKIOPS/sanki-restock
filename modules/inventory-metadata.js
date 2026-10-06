'use strict';
const fs = require('fs');
const path = require('path');
const keys = { category: 'inventory_category', productType: 'product_type_detail', fit: 'fit', gender: 'gender', collection: 'collection_line', design: 'design', fabric: 'fabric', season: 'season', sleeves: 'sleeves' };
const graphqlFields = Object.entries(keys).map(([field, key]) => `${field}Field:metafield(namespace:"custom",key:"${key}"){value}`).join(' ');
const tagNames = { category: 'Category', productType: 'Product type', fit: 'Fit', gender: 'Gender', collection: 'Collection', design: 'Design', fabric: 'Fabric', season: 'Season', sleeves: 'Sleeves' };
const categoryAliases = { tshirt: 'T-Shirts', tshirts: 'T-Shirts', shirt: 'Shirts', shirts: 'Shirts', jeans: 'Jeans', jean: 'Jeans', trouser: 'Trousers', trousers: 'Trousers', lower: 'Lowers', lowers: 'Lowers', short: 'Shorts', shorts: 'Shorts', winterwear: 'Winter Wear', kurtapajama: 'Kurta Pajama', coord: 'Co-ords', coords: 'Co-ords', sando: 'Sando', accessories: 'Accessories', accessory: 'Accessories', bag: 'Bags', bags: 'Bags', top: 'Tops', tops: 'Tops' };
function createMetadataReader({ physical, query, now = Date.now }) {
  const saved = new Map(physical.map(p => [p.handle,p])), cached = new Map();
  return async function enrich(items) {
    const products = new Map(items.map(i => [i.product.id,i.product]));
    const needed = [...products.values()].filter(p => p.id && !['category','collection','fit','gender'].every(k => clean(saved.get(p.handle)?.[k])) && (!cached.has(p.id) || now() - cached.get(p.id).at > 30*60*1000));
    for (let start = 0; start < needed.length; start += 50) {
      const batch = needed.slice(start,start+50);
      try {
        const result = await query(`query($ids:[ID!]!){nodes(ids:$ids){... on Product{id ${graphqlFields}}}}`, {ids:batch.map(p=>p.id)});
        if (!Array.isArray(result.nodes)) throw Error('Shopify classification response is incomplete.');
        for (const p of batch) cached.set(p.id,{at:now(),fields:result.nodes.find(n=>n?.id===p.id)||{}});
      } catch (e) {
        // Classification availability must not discard confirmed quantities.
        for (const p of batch) cached.set(p.id,{at:now()-29*60*1000,fields:{...(cached.get(p.id)?.fields||{}),metadataError:e.message}});
      }
    }
    return items.map(i=>({...i,product:{...i.product,...(cached.get(i.product.id)?.fields||{})}}));
  };
}
function clean(value) { const text = String(value || '').trim(); return /^(unknown|uncategorized|unclassified|n\/a|-)$/i.test(text) ? '' : text; }
function normalize(field, value) {
  const text = clean(value), compact = text.toLowerCase().replace(/[\s_-]/g, '');
  if (field === 'category') return categoryAliases[compact] || text;
  if (field === 'collection') return /^(sanki)?funky$/.test(compact) ? 'SANKI Funky' : /^(sanki)?casuals$/.test(compact) ? 'SANKI Casuals' : text;
  if (field === 'gender') return { male: 'Men', men: 'Men', mens: 'Men', female: 'Women', women: 'Women', womens: 'Women', unisex: 'Unisex' }[compact] || text;
  if (field === 'fit') return { oversized: 'Oversized', baggy: 'Baggy', relaxed: 'Relaxed', narrow: 'Narrow', straight: 'Straight', modernfit: 'Modern Fit', regular: 'Regular', slim: 'Slim' }[compact] || text;
  return text;
}
function loadPurchases(file = process.env.PROCUREMENT_PATH || path.join(process.env.DATA_PATH ? path.dirname(process.env.DATA_PATH) : path.join(__dirname, '..'), 'procurement.json')) {
  const bySku = new Map();
  try {
    const data = JSON.parse(fs.readFileSync(file, 'utf8'));
    for (const po of Object.values(data.pos || {})) {
      if (po.status !== 'posted') continue;
      for (const line of po.lines || []) {
        const sku = String(line.sku || '').trim().toUpperCase();
        if (!sku) continue;
        const values = { category: line.category || line.productType, productType: line.productType, fit: line.fit, gender: line.audience, collection: po.line, design: line.designName, fabric: line.fabric, season: line.season, sleeves: line.sleeves };
        bySku.set(sku, (bySku.get(sku) || []).concat(values));
      }
    }
  } catch (_) { /* Catalogue reads must still work without a purchase register. */ }
  return bySku;
}
function resolveMetadata(product, physical = {}, purchases = []) {
  const tags = Array.isArray(product.tags) ? product.tags : String(product.tags || '').split(',');
  const values = {}, issues = product.metadataError ? ['Shopify custom classification could not be refreshed: '+product.metadataError] : [];
  for (const field of Object.keys(keys)) {
    const prefix = 'sanki ' + tagNames[field].toLowerCase() + ':';
    const tagged = [...new Set(tags.filter(t => t.trim().toLowerCase().startsWith(prefix)).map(t => normalize(field, t.slice(t.indexOf(':') + 1))).filter(Boolean))];
    const purchased = [...new Set(purchases.map(p => normalize(field, p[field])).filter(Boolean))];
    const saved = normalize(field, physical[field]), metafield = normalize(field, product[field + 'Field']?.value);
    // Keep approved physical-count labels; fill missing fields from explicit
    // Shopify metadata and posted purchases. Never guess from product titles.
    const candidates = [saved, metafield, ...tagged, ...purchased].filter(Boolean);
    const distinct = [...new Map(candidates.map(v => [v.toLowerCase(), v])).values()];
    values[field] = saved || metafield || (tagged.length === 1 ? tagged[0] : '') || (purchased.length === 1 ? purchased[0] : '');
    if (distinct.length > 1) issues.push('Conflicting ' + tagNames[field].toLowerCase() + ': ' + distinct.join(' / '));
  }
  values.productType ||= clean(product.productType) || 'Uncategorized';
  values.category ||= normalize('category', product.productType) || 'Uncategorized';
  values.collection ||= 'Uncategorized';
  for (const field of ['category', 'collection', 'fit', 'gender']) if (!clean(values[field])) issues.push('Missing ' + tagNames[field].toLowerCase());
  return { ...values, classificationIssues: issues };
}
module.exports = { graphqlFields, normalize, loadPurchases, resolveMetadata, createMetadataReader };
