'use strict';

// A barcode is an identifier, never a number: leading zeroes are significant.
function createLookup(query) {
  return async function lookup(value) {
    const barcode = typeof value === 'string' ? value.trim() : '';
    function fail(message, status) { const e = new Error(message); e.status = status; throw e; }
    if (!barcode || barcode.length > 200 || /[\x00-\x1f\x7f]/.test(barcode)) fail('Enter a valid barcode.', 400);
    const matches = new Map();
    let after = null;
    do {
      const data = await query(`query($query:String!,$after:String){productVariants(first:50,query:$query,after:$after){nodes{id sku barcode product{handle title}} pageInfo{hasNextPage endCursor}}}`, { query: 'barcode:' + JSON.stringify(barcode), after });
      const page = data.productVariants;
      if (!Array.isArray(page?.nodes) || !page.pageInfo) fail('Shopify barcode results could not be confirmed. Try again.', 502);
      for (const v of page.nodes) {
        if (v.barcode === barcode) matches.set(v.id, { sku: String(v.sku || '').trim(), handle: v.product?.handle, title: v.product?.title });
      }
      if (page.pageInfo.hasNextPage && (!page.pageInfo.endCursor || page.pageInfo.endCursor === after)) fail('Shopify barcode results could not be confirmed. Try again.', 502);
      after = page.pageInfo.hasNextPage ? page.pageInfo.endCursor : null;
    } while (after);
    if (!matches.size) fail('No Shopify SKU has this barcode. Check the label or search by SKU.', 404);
    if (matches.size > 1) fail('This barcode belongs to more than one Shopify variant. Search by SKU and correct the duplicate barcodes in Shopify.', 409);
    const match = [...matches.values()][0];
    if (!match.sku || !match.handle) fail('This Shopify variant has no SKU. Add its SKU in Shopify before scanning it.', 422);
    return { barcode, ...match };
  };
}

function register(router) {
  const { ShopifyClient } = require('./shopify-client');
  const { graphql } = require('./inventory-state');
  const client = new ShopifyClient({ minIntervalMs: 250 });
  const lookup = createLookup((query, variables) => graphql(query, variables, client));
  router.get('/barcode', async (req, res) => {
    res.set('Cache-Control', 'no-store');
    try { res.json({ success: true, match: await lookup(req.query.barcode) }); }
    catch (e) { res.status(e.status || 502).json({ success: false, error: e.status ? e.message : 'Could not reach Shopify. Try scanning again.' }); }
  });
}
module.exports = { createLookup, register };
