// ═══════════════════════════════════════════════════════════════
// modules/orders.js — THE SALES SPINE
//
// A strictly READ-ONLY mirror of every Shopify order (POS + Website),
// from the very first order onward, into a permanent local ledger on the
// /data volume. This is the database every downstream calculation stands
// on — accounting, restocking, fresh procurement, unit-economics.
//
//   • Backfill: pull ALL history via Link-header pagination.
//   • Stay synced: a lightweight internal poller runs an incremental sync
//     (updated_at_min) every few minutes, plus a manual "Sync now".
//   • NEVER writes back to Shopify and NEVER touches inventory. Read-only.
//
// It also absorbs the old "Manual POS Dispatch" Google Sheet: each order
// carries an editable in-app DISPATCH layer (courier, tracking URL,
// packing status, order confirmation, COD paid/due, remarks) so every
// order is tracked end-to-end on ONE page. Order core = read-only from
// Shopify; dispatch layer = in-app CRUD (the app is the source of truth,
// retiring the dispatch sheet).
//
// Endpoints (all behind the auth gate). NOTE the /api/orders-ledger/* base:
// server.js already owns a legacy GET/POST /api/orders (the velocity/analytics
// COD cache, different shape) that is registered first and would otherwise
// shadow these. Keeping this module on its own namespace avoids the clash.
//   POST /api/orders-ledger/sync         → backfill (first run) or incremental
//   GET  /api/orders-ledger/sync/status  → last sync meta
//   GET  /api/orders-ledger              → filtered list + summary
//   GET  /api/orders-ledger/:id          → one order (with dispatch layer)
//   POST /api/orders-ledger/:id/dispatch → update the editable dispatch fields
// ═══════════════════════════════════════════════════════════════
const express = require('express');
const path    = require('path');
const fs      = require('fs');
const { shopifyClient } = require('./shopify-client');
const { validDate, day: accountingDay } = require('./pnl-report');

const router = express.Router();

const SHOPIFY_STORE = process.env.SHOPIFY_STORE;
const SHOPIFY_TOKEN = process.env.SHOPIFY_ACCESS_TOKEN;

const ORDERS_PATH = process.env.ORDERS_PATH ||
  path.join(process.env.DATA_PATH ? path.dirname(process.env.DATA_PATH) : path.join(__dirname, '..'),
            'orders.json');
const ACCOUNTING_BOUNDARY_PATH = path.join(path.dirname(ORDERS_PATH), 'accounting-boundary.json');
// Increment this whenever the accounting ledger needs a fresh historical order
// scan. Incremental Shopify syncs only see recently updated orders, so an order
// omitted by an older importer would otherwise remain missing forever.
const ACCOUNTING_ORDER_BACKFILL_VERSION = 4;
function accountingStartAt() {
  try { return String(JSON.parse(fs.readFileSync(ACCOUNTING_BOUNDARY_PATH, 'utf8')).startAt || ''); }
  catch { return ''; }
}

// Fields we pull from Shopify (keep the payload lean but complete).
const ORDER_FIELDS = [
  'id', 'name', 'order_number', 'created_at', 'updated_at', 'processed_at', 'cancelled_at',
  'source_name', 'financial_status', 'fulfillment_status', 'currency',
  'subtotal_price', 'total_discounts', 'total_tax', 'total_price', 'total_shipping_price_set',
  'discount_codes', 'payment_gateway_names', 'customer', 'email', 'contact_email', 'phone',
  'shipping_address', 'billing_address', 'note', 'line_items', 'refunds',
  'taxes_included', 'shipping_lines', 'fulfillments'
].join(',');

// ── JSON store (atomic write) ────────────────────────────────────
function atomicWrite(filePath, data) {
  const tmp = filePath + '.tmp-' + process.pid + '-' + Date.now();
  fs.writeFileSync(tmp, data);
  fs.renameSync(tmp, filePath);
}
function loadStore() {
  // { orders:   { [id]: normalizedOrder },
  //   dispatch: { [id]: { courier, trackingUrl, packingStatus, orderConfirmation,
  //                       paymentMode, paidAmount, dueAmount, remarks, updatedAt, updatedBy } },
  //   sync:     { lastSyncedAt, lastCursorUpdatedAt, count, oldestAt, running } }
  try {
    const s = JSON.parse(fs.readFileSync(ORDERS_PATH, 'utf8'));
    if (!s.orders) s.orders = {};
    if (!s.dispatch) s.dispatch = {};
    if (!s.sync) s.sync = {};
    return s;
  } catch { return { orders: {}, dispatch: {}, sync: {} }; }
}
function saveStore(store) { atomicWrite(ORDERS_PATH, JSON.stringify(store)); }

// ── Shopify paginated fetch (read-only) ──────────────────────────
async function shopifyFetchAll(startUrl) {
  let all = [], url = startUrl;
  while (url) {
    const r = await shopifyClient.request(url);
    if (!r.ok) {
      const body = await r.text().catch(() => '');
      const err = new Error('Shopify ' + r.status + ': ' + body.slice(0, 300));
      err.status = r.status;
      throw err;
    }
    const d = await r.json();
    const key = Object.keys(d).find(k => Array.isArray(d[k]));
    if (key) all = all.concat(d[key]);
    const link = r.headers.get('Link') || '';
    const next = link.match(/<([^>]+)>;\s*rel="next"/);
    url = next ? next[1] : null;
  }
  return all;
}

// ── Normalisation: raw Shopify order → lean ledger record ────────
function channelOf(sourceName) {
  const s = String(sourceName || '').toLowerCase();
  if (s === 'pos') return 'POS';
  return 'Website'; // web / gokwik / online-store / anything else
}
function num(v) { const n = parseFloat(v); return isNaN(n) ? 0 : n; }
function shippingTotal(o) {
  try { return num(((o.total_shipping_price_set || {}).shop_money || {}).amount); } catch { return 0; }
}
function refundTotal(o) {
  if (!Array.isArray(o.refunds)) return 0;
  let sum = 0;
  o.refunds.forEach(rf => (rf.transactions || []).forEach(t => { if (t.kind === 'refund') sum += num(t.amount); }));
  return sum;
}
function refundComponents(o) {
  const rows = [];
  (Array.isArray(o.refunds) ? o.refunds : []).forEach(refund => {
    (refund.transactions || []).forEach(tx => {
      if (String(tx.kind || '').toLowerCase() !== 'refund' || String(tx.status || 'success').toLowerCase() !== 'success') return;
      const amount = num(tx.amount), gateway = String(tx.gateway || tx.payment_gateway || '');
      if (!(amount > 0)) return;
      rows.push({ id:String(tx.id || ''), parentId:String(tx.parent_id || ''), refundId:String(refund.id || ''), amount, gateway, processedAt:tx.processed_at || refund.processed_at || refund.created_at || '' });
    });
  });
  const isStoreCredit = row => /store.?credit|gift.?card/i.test(row.gateway);
  return {
    transactions: rows,
    storeCreditIssued: rows.filter(isStoreCredit).reduce((sum, row) => sum + row.amount, 0),
    moneyRefunded: rows.filter(row => !isStoreCredit(row)).reduce((sum, row) => sum + row.amount, 0)
  };
}
function normalizePaymentTransactions(rows) {
  return (rows || []).map(tx => ({
    id:String(tx.id || ''), parentId:String(tx.parent_id || tx.parentId || ''), kind:String(tx.kind || ''),
    status:String(tx.status || ''), gateway:String(tx.gateway || tx.payment_gateway || ''),
    amount:num(tx.amount), processedAt:tx.processed_at || tx.processedAt || tx.created_at || '',
    manualPaymentGateway:tx.manual_payment_gateway===true||tx.manualPaymentGateway===true,
    message:String(tx.message || ''), authorization:String(tx.authorization || ''),
    userId:String(tx.user_id || tx.userId || ''), sourceName:String(tx.source_name || tx.sourceName || '')
  }));
}
function addressObj(a) {
  a = a || {};
  return {
    name: a.name || [a.first_name, a.last_name].filter(Boolean).join(' ').trim() || '',
    phone: a.phone || '',
    address1: a.address1 || '', address2: a.address2 || '',
    city: a.city || '', province: a.province || '', zip: a.zip || '',
    country: a.country || ''
  };
}
function normalizeOrder(o, previous = {}) {
  const cust = o.customer || {};
  const ship = o.shipping_address || {};
  const custName = [cust.first_name, cust.last_name].filter(Boolean).join(' ').trim()
    || ship.name || '';
  const email = cust.email || o.email || o.contact_email || '';
  const lineItems = (o.line_items || []).map(li => ({
    id: String(li.id || ''), taxable: li.taxable !== false,
    productId: String(li.product_id || ''), variantId: String(li.variant_id || ''), image: li.image && li.image.src || '', sku: li.sku || '', title: li.title || '', variantTitle: li.variant_title || '',
    qty: li.quantity || 0, price: num(li.price),
    discount: (li.discount_allocations || []).reduce((sum, d) => sum + num(d.amount), 0),
    taxLines: (li.tax_lines || []).map(t => ({ title: t.title, rate: num(t.rate), price: num(t.price) }))
  }));
  const refunds = refundComponents(o);
  const fulfillments = o.fulfillments || [];
  const delivered = fulfillments.filter(f => f.status !== 'cancelled' && f.shipment_status === 'delivered');
  const deliveredQty = new Map();
  delivered.forEach(f => (f.line_items || []).forEach(l => deliveredQty.set(String(l.id), (deliveredQty.get(String(l.id)) || 0) + num(l.quantity))));
  const deliveryComplete = lineItems.length > 0 && lineItems.every(l => (deliveredQty.get(l.id) || 0) >= l.qty);
  // updated_at can change for unrelated edits, so it is not evidence of
  // actual delivery. Otherwise the confirmed dispatch date is required.
  const deliveryDates = delivered.map(f => f.delivered_at || f.deliveredAt).filter(Boolean);
  const completedAt = previous.completedAt || (deliveryComplete && deliveryDates.length === delivered.length ? deliveryDates.sort().at(-1) : null);
  const creditNotes = (o.refunds || []).map(r => ({
    id: String(r.id), date: r.created_at,
    amount: (r.transactions || []).filter(t => t.kind === 'refund' && (!t.status || t.status === 'success')).reduce((sum, t) => sum + num(t.amount), 0),
    lineItems: (r.refund_line_items || []).map(l => ({ lineItemId: String(l.line_item_id || l.line_item?.id || ''), sku: l.line_item?.sku || '', qty: num(l.quantity), subtotal: num(l.subtotal), tax: l.total_tax != null ? num(l.total_tax) : null, restockType: l.restock_type || 'no_restock' })),
    adjustmentAmount: (r.order_adjustments || []).reduce((sum, a) => sum + num(a.amount) + num(a.tax_amount), 0)
  }));
  return {
    id: String(o.id),
    name: o.name || ('#' + (o.order_number || '')),
    number: o.order_number || null,
    createdAt: o.created_at || null,
    updatedAt: o.updated_at || null,
    processedAt: o.processed_at || null,
    completedAt, pnlSchemaVersion: 1,
    taxesIncluded: typeof o.taxes_included === 'boolean' ? o.taxes_included : null,
    shippingTax: Array.isArray(o.shipping_lines) ? o.shipping_lines.reduce((sum, l) => sum + (l.tax_lines || []).reduce((n, t) => n + num(t.price), 0), 0) : null,
    cancelledAt: o.cancelled_at || null,
    channel: channelOf(o.source_name),
    sourceName: o.source_name || '',
    customer: { id: String(cust.id || previous.customer&&previous.customer.id || ''), name: custName || cust.name || previous.customer&&previous.customer.name || '', phone: o.phone || cust.phone || ship.phone || '', email },
    city: ship.city || '',
    shipAddress: addressObj(ship),
    billAddress: addressObj(o.billing_address),
    note: o.note || '',
    lineItems,
    itemCount: lineItems.reduce((s, li) => s + (li.qty || 0), 0),
    currency: o.currency || 'INR',
    subtotal: num(o.subtotal_price),
    discount: num(o.total_discounts),
    tax: num(o.total_tax),
    shipping: shippingTotal(o),
    total: num(o.total_price),
    refundAmount: refundTotal(o),
    refunds: creditNotes,
    refundTransactions: refunds.transactions,
    storeCreditIssued: refunds.storeCreditIssued,
    moneyRefunded: refunds.moneyRefunded,
    financialStatus: o.financial_status || '',
    fulfillmentStatus: o.fulfillment_status || 'unfulfilled',
    deliveryComplete,
    shipments: fulfillments.filter(f => f.status !== 'cancelled').map(f => ({ id: String(f.id), awb: f.tracking_number || '', courier: f.tracking_company || '', trackingUrl: f.tracking_url || '', status: f.shipment_status || 'Tracking update pending', weight: null })),
    paymentGateways: o.payment_gateway_names || [],
    paymentTransactions: normalizePaymentTransactions(o._paymentTransactions),
    paymentTransactionSchemaVersion: 2,
    couponCodes: (o.discount_codes || []).map(d => d.code).filter(Boolean)
  };
}

// Derive a Prepaid/COD guess so the dispatch layer has a sensible default.
function paymentModeGuess(n) {
  const g = (n.paymentGateways || []).join(' ').toLowerCase();
  if (g.includes('cod') || g.includes('cash on delivery')) return 'COD';
  if (n.financialStatus === 'paid') return 'Prepaid';
  if (n.financialStatus === 'pending') return 'COD';
  return '';
}

// ── Sync (backfill or incremental) ───────────────────────────────
let _syncing = false;
async function runSync(opts = {}) {
  if (!SHOPIFY_STORE || !SHOPIFY_TOKEN) throw new Error('Shopify env not configured');
  if (_syncing) return { skipped: 'already running' };
  _syncing = true;
  try {
    const store = loadStore();
    // Also run a one-time accounting-boundary backfill after this importer is
    // deployed. This recovers older missing orders (for example a cash order
    // created on 23 August) that can never be found by the normal recent-update
    // poller. Manual full backfill still retrieves the entire Shopify history.
    const needsAccountingBackfill = store.sync.accountingOrderBackfillVersion !== ACCOUNTING_ORDER_BACKFILL_VERSION;
    const isBackfill = opts.full === true || Object.keys(store.orders).length === 0 || needsAccountingBackfill;
    // Incremental syncs re-pull anything updated since a small safety margin
    // before the last sync (catches edits/refunds/fulfilment changes).
    let url = `https://${SHOPIFY_STORE}/admin/api/2024-01/orders.json?status=any&limit=250&fields=${ORDER_FIELDS}`;
    if (needsAccountingBackfill && opts.full !== true && Object.keys(store.orders).length > 0) {
      const boundary = accountingStartAt();
      if (boundary) url += `&created_at_min=${encodeURIComponent(boundary)}`;
    } else if (!isBackfill && store.sync.lastSyncedAt) {
      const since = new Date(Date.now() - 10 * 60 * 1000).toISOString(); // last 10 min margin
      const min = store.sync.lastSyncedAt < since ? store.sync.lastSyncedAt : since;
      url += `&updated_at_min=${encodeURIComponent(min)}`;
    }
    const raw = await shopifyFetchAll(url);
    // Orders expose only gateway names. The transaction endpoint is the source
    // of truth for split tenders and gateway-specific refunds, so cache those
    // components with every imported/updated order.
    await Promise.all(raw.map(async order => {
      order._paymentTransactions = await shopifyFetchAll(`https://${SHOPIFY_STORE}/admin/api/2024-01/orders/${order.id}/transactions.json?limit=250`);
    }));
    let imported = 0;
    raw.forEach(o => { store.orders[String(o.id)] = normalizeOrder(o, store.orders[String(o.id)]); imported++; });
    const boundary=accountingStartAt(),missingPaymentDetails=Object.values(store.orders).filter(order=>String(order.createdAt||'')>=boundary&&order.paymentTransactionSchemaVersion!==2);
    await Promise.all(missingPaymentDetails.map(async order => {
      const transactions=await shopifyFetchAll(`https://${SHOPIFY_STORE}/admin/api/2024-01/orders/${order.id}/transactions.json?limit=250`);
      order.paymentTransactions=normalizePaymentTransactions(transactions);
      order.paymentTransactionSchemaVersion=2;
    }));

    // Recompute meta.
    const ids = Object.keys(store.orders);
    let oldest = null;
    ids.forEach(id => { const c = store.orders[id].createdAt; if (c && (!oldest || c < oldest)) oldest = c; });
    store.sync = {
      lastSyncedAt: new Date().toISOString(),
      count: ids.length,
      oldestAt: oldest,
      lastImported: imported,
      paymentDetailsBackfilled: missingPaymentDetails.length,
      accountingOrderBackfillVersion: ACCOUNTING_ORDER_BACKFILL_VERSION,
      mode: isBackfill ? 'backfill' : 'incremental'
    };
    saveStore(store);
    return { imported, total: ids.length, oldestAt: oldest, mode: store.sync.mode };
  } finally { _syncing = false; }
}

// ── Read + filter ────────────────────────────────────────────────
function mergedList(store) {
  return Object.values(store.orders).map(o => {
    const d = store.dispatch[o.id] || {};
    return Object.assign({}, o, {
      dispatch: {
        courier: d.courier || '',
        velocityAwb: d.velocityAwb || '',
        deliveredAt: d.deliveredAt || '',
        trackingUrl: d.trackingUrl || '',
        packingStatus: d.packingStatus || 'pending',
        orderConfirmation: d.orderConfirmation || 'pending',
        paymentMode: d.paymentMode || paymentModeGuess(o),
        paidAmount: d.paidAmount != null ? d.paidAmount : null,
        dueAmount: d.dueAmount != null ? d.dueAmount : null,
        remarks: d.remarks || '',
        updatedAt: d.updatedAt || null,
        updatedBy: d.updatedBy || null
      }
    });
  });
}

router.get('/api/orders-ledger/sync/status', (req, res) => {
  const s = loadStore();
  res.json({ success: true, sync: s.sync || {}, running: _syncing });
});

router.post('/api/orders-ledger/sync', async (req, res) => {
  try {
    const full = req.query.full === '1' || !!(req.body && req.body.full);
    const out = await runSync({ full });
    res.json({ success: true, ...out });
  } catch (e) {
    res.status(500).json({ success: false, error: e.message });
  }
});

router.get('/api/orders-ledger', (req, res) => {
  const store = loadStore();
  let list = mergedList(store);
  const resetBoundary = accountingStartAt();
  if (resetBoundary) list = list.filter(o => String(o.createdAt || '') >= resetBoundary);
  const q = (req.query.q || '').toString().trim().toLowerCase();
  const channel = (req.query.channel || '').toString();
  const from = (req.query.from || '').toString();
  const to = (req.query.to || '').toString();
  const packing = (req.query.packing || '').toString();
  const fulfillment = (req.query.fulfillment || '').toString();

  if (channel) list = list.filter(o => o.channel === channel);
  if (from) list = list.filter(o => o.createdAt && o.createdAt >= from);
  if (to) list = list.filter(o => o.createdAt && o.createdAt <= to + 'T23:59:59');
  if (packing) list = list.filter(o => o.dispatch.packingStatus === packing);
  if (fulfillment) list = list.filter(o => (o.fulfillmentStatus || 'unfulfilled') === fulfillment);
  if (q) list = list.filter(o =>
    (o.name || '').toLowerCase().includes(q) ||
    (o.customer.name || '').toLowerCase().includes(q) ||
    (o.customer.phone || '').toLowerCase().includes(q) ||
    (o.customer.email || '').toLowerCase().includes(q) ||
    (o.city || '').toLowerCase().includes(q) ||
    o.lineItems.some(li => (li.sku || '').toLowerCase().includes(q) || (li.title || '').toLowerCase().includes(q))
  );

  list.sort((a, b) => (b.createdAt || '').localeCompare(a.createdAt || ''));

  // Summary over the filtered set (before paging).
  const summary = { count: list.length, revenue: 0, byChannel: {}, units: 0 };
  list.forEach(o => {
    if (o.cancelledAt) return;
    summary.revenue += o.total;
    summary.units += o.itemCount;
    summary.byChannel[o.channel] = (summary.byChannel[o.channel] || 0) + o.total;
  });

  const limit = Math.min(parseInt(req.query.limit, 10) || 200, 1000);
  const offset = parseInt(req.query.offset, 10) || 0;
  res.json({ success: true, total: list.length, summary, orders: list.slice(offset, offset + limit) });
});

const financeUpload = require('multer')({storage: require('multer').memoryStorage(), limits: {fileSize: 10 * 1024 * 1024}});
router.post('/api/orders-ledger/gokwik-report', financeUpload.single('file'), (req,res) => {
  try {
    if(!req.file)return res.status(400).json({success:false,error:'Choose a GoKwik settlement report'});
    const xlsx=require('xlsx'),book=xlsx.read(req.file.buffer,{type:'buffer',raw:true});
    const rows=require('./gokwik-settlements').normalizeRows(xlsx.utils.sheet_to_json(book.Sheets[book.SheetNames[0]],{defval:''}));
    const file=path.join(path.dirname(ORDERS_PATH),'gokwik_settlement_evidence.json');
    let prior={rows:[]};try{prior=JSON.parse(fs.readFileSync(file,'utf8'));}catch(e){if(e.code!=='ENOENT')throw e;}
    const combined=new Map((prior.rows||[]).map(r=>[r.paymentId+'|'+r.type+'|'+r.utr,r]));
    rows.forEach(r=>combined.set(r.paymentId+'|'+r.type+'|'+r.utr,r));
    atomicWrite(file,JSON.stringify({rows:[...combined.values()],importedAt:new Date().toISOString(),importedBy:req.user&&req.user.username||'admin'}));
    res.json({success:true,imported:rows.length,total:combined.size,bankVerified:false});
  }catch(e){res.status(400).json({success:false,error:e.message});}
});
function gokwikFile(){return path.join(path.dirname(ORDERS_PATH),'gokwik_api_evidence.json');}
function syncGoKwik(force=false){
 let report={rows:[]};try{report=JSON.parse(fs.readFileSync(path.join(path.dirname(ORDERS_PATH),'gokwik_settlement_evidence.json'),'utf8'));}catch(e){if(e.code!=='ENOENT')throw e;}
 return require('./gokwik-auto-sync').sync({file:gokwikFile(),orders:loadStore().orders,preferredDates:(report.rows||[]).map(r=>r.date),write:atomicWrite,force});
}
function startGoKwikSync(){
 const tick=()=>{if(!process.env.GOKWIK_APP_ID||!process.env.GOKWIK_APP_SECRET)return;syncGoKwik().then(r=>{if(!r.skipped)console.log('[gokwik-api] Synced '+r.rows+' settlement records across '+r.days+' days');}).catch(e=>console.error('[gokwik-api] '+e.message));};
 setTimeout(tick,45000);setInterval(tick,30*60*1000);
}
router.post('/api/orders-ledger/gokwik-sync',(req,res)=>{
 if(!process.env.GOKWIK_APP_ID||!process.env.GOKWIK_APP_SECRET)return res.status(503).json({success:false,error:'GoKwik API credentials are not configured'});
 syncGoKwik(true).catch(e=>console.error('[gokwik-api] '+e.message));
 res.status(202).json({success:true,status:require('./gokwik-auto-sync').status(gokwikFile())});
});
router.get('/api/orders-ledger/gokwik-status',(req,res)=>res.json({success:true,...require('./gokwik-auto-sync').status(gokwikFile())}));
let logisticsPromise=null;
function syncLogistics(){
  if(logisticsPromise)return logisticsPromise;
  logisticsPromise=(async()=>{
    const file=path.join(path.dirname(ORDERS_PATH),'velocity_order_index.json');
    let previous={byOrder:{},shipments:{}};
    try{previous=JSON.parse(fs.readFileSync(file,'utf8'));}catch(e){if(e.code!=='ENOENT')throw e;}
    try{
      if(!process.env.VELOCITY_API_KEY)throw new Error('Velocity API key is not configured');
      const adapter=require('./velocity-order-sync');
      const index=await adapter.fetchShipmentIndex({token:process.env.VELOCITY_API_KEY});
      // Load after network requests so orders arriving during discovery are included.
      const store=loadStore(),matched=adapter.matchShipments(store.orders,index.rows);
      const awbs=[...new Set([...index.rows.map(s=>s.awb),...Object.values(store.orders).flatMap(o=>(o.shipments||[]).map(s=>s.awb)),...Object.values(store.dispatch).map(d=>d.velocityAwb)].filter(Boolean))];
      let tracking={},trackingError=null;
      try{tracking=await require('./velocity-tracking').fetchTracking({awbs,token:process.env.VELOCITY_API_KEY});}catch(e){trackingError=e.message;}
      const now=new Date().toISOString();
      const shipments={...(previous.shipments||{})};
      for(const row of index.rows.filter(s=>s.awb))shipments[row.awb]={...shipments[row.awb],...row,...tracking[row.awb],courierName:(tracking[row.awb]||{}).courierName||row.courier,lastUpdated:now};
      for(const [awb,t] of Object.entries(tracking))shipments[awb]={...shipments[awb],...t,courierName:t.courierName||(shipments[awb]||{}).courierName||''};
      const result={byOrder:matched.byOrder,shipments,lastSync:now,lastAttempt:now,error:trackingError,total:index.total,matchedOrders:Object.keys(matched.byOrder).length,unmatched:matched.unmatched};
      atomicWrite(file,JSON.stringify(result));
      return {success:true,total:result.total,matchedOrders:result.matchedOrders};
    }catch(e){atomicWrite(file,JSON.stringify({...previous,lastAttempt:new Date().toISOString(),error:e.message}));throw e;}
  })().finally(()=>{logisticsPromise=null;});
  return logisticsPromise;
}
router.post('/api/orders-ledger/velocity-sync',async(req,res)=>{
 try{res.json(await syncLogistics());}catch(e){res.status(502).json({success:false,error:e.message});}
});
let financeRefreshing=false, financeLastAttempt=0;
async function syncFinance() {
  if(financeRefreshing)throw new Error('Velocity finance sync already running');
  if(!process.env.VELOCITY_API_KEY)throw new Error('A named Velocity API key must be configured on the server');
  if(Date.now()-financeLastAttempt<60000)throw new Error('Please wait one minute before another finance sync');
  financeRefreshing=true;financeLastAttempt=Date.now();
  try {
    const store=loadStore();let legacy={orders:[]};
    try{legacy=JSON.parse(fs.readFileSync(path.join(path.dirname(ORDERS_PATH),'sanki_cache.json'),'utf8'));}catch(e){if(e.code!=='ENOENT')throw e;}
    let index={byOrder:{}};try{index=JSON.parse(fs.readFileSync(path.join(path.dirname(ORDERS_PATH),'velocity_order_index.json'),'utf8'));}catch(e){if(e.code!=='ENOENT')throw e;}
    const linked=new Set(Object.keys(store.orders));
    const awbs=[...new Set([...Object.values(index.byOrder||{}).flatMap(rows=>rows.map(s=>s.awb)),...Object.values(store.dispatch).map(d=>d.velocityAwb),...Object.values(store.orders).flatMap(o=>(o.shipments||[]).map(s=>s.awb)),...(legacy.orders||[]).filter(o=>linked.has(String(o.id))).flatMap(o=>(o.fulfillments||[]).filter(f=>f.status!=='cancelled').map(f=>f.tracking_number))].map(v=>String(v||'').trim()).filter(v=>v&&!/^(n\/a|na|none)$/i.test(v)))];
    if(!awbs.length)throw new Error('No AWBs available. Sync Shopify orders first.');
    const tracking=await require('./velocity-tracking').fetchTracking({awbs,token:process.env.VELOCITY_API_KEY});
    atomicWrite(path.join(path.dirname(ORDERS_PATH),'velocity_tracking_evidence.json'),JSON.stringify({shipments:tracking,lastSync:new Date().toISOString()}));
    const rows=await require('./velocity-finance').fetchRemittances({awbs,token:process.env.VELOCITY_API_KEY});
    const file=path.join(path.dirname(ORDERS_PATH),'velocity_finance_evidence.json');
    atomicWrite(file,JSON.stringify({rows,updatedAt:new Date().toISOString()}));
    return {success:true,checked:rows.length,reportedSettled:rows.filter(r=>r.utr).length,bankVerified:false};
  }finally{financeRefreshing=false;}
}
router.post('/api/orders-ledger/velocity-finance/sync', async(req,res)=>{
  try{const result=await syncFinance();if(process.env.VELOCITY_MCP_ACCESS_TOKEN&&process.env.VELOCITY_MCP_UID)result.payouts=await syncVelocityPayouts();res.json(result);}catch(e){res.status(502).json({success:false,error:e.message});}
});

function syncVelocityPayouts(){return require('./velocity-payout-sync').sync({file:path.join(path.dirname(ORDERS_PATH),'velocity_payout_evidence.json'),write:atomicWrite});}
router.get('/api/orders-ledger/integrated', (req, res) => {
  try {
    const store = loadStore();
    const read = (name, fallback) => { try { return JSON.parse(fs.readFileSync(path.join(path.dirname(ORDERS_PATH), name), 'utf8')); } catch (e) { if (e.code === 'ENOENT') return fallback; throw e; } };
    const velocity = read('velocity_cache.json', { shipments: {} });
    const tracking = read('velocity_tracking_evidence.json',{shipments:{}});
    const index = read('velocity_order_index.json',{byOrder:{},shipments:{}});
    velocity.shipments=Object.assign({},velocity.shipments,tracking.shipments);
    for(const [awb,row] of Object.entries(index.shipments||{})){const old=velocity.shipments[awb]||{};velocity.shipments[awb]=String(row.lastUpdated||'')>=String(old.lastUpdated||'')?{...old,...row}:{...row,...old};}
    velocity.lastSync=index.lastSync||tracking.lastSync||velocity.lastSync;
    const remittances = read('velocity_remittance.json', { entries: [] });
    const legacy = read('sanki_cache.json', { orders: [] });
    const finance = read('velocity_finance_evidence.json', {rows:[]});
    const online = read('gokwik_settlement_evidence.json', {rows:[]});
    const apiState=read('gokwik_api_evidence.json',{days:{}});
    online.rows=require('./gokwik-api').mergedEvidence(online.rows||[],Object.values(apiState.days||{}).flat());
    const gokwikApiStatus=require('./gokwik-auto-sync').status(gokwikFile());
    const { integrateOrder } = require('./sales-order-view');
    const boundary = accountingStartAt();
    const orders = mergedList(store)
      .map(o => integrateOrder({...o,velocityShipments:(index.byOrder||{})[o.id]||[]}, velocity.shipments || {}, remittances.entries || [], legacy.orders || [], finance.rows || [], online.rows || []))
      .sort((a,b) => String(b.createdAt || '').localeCompare(String(a.createdAt || '')));
    const accounting=read('expenses.json',{});
    require('./order-settlement-links').attachSettlementLinks(orders,accounting,online.rows||[],finance.rows||[]);
    const payoutFile=path.join(path.dirname(ORDERS_PATH),'velocity_payout_evidence.json');
    const payoutState=require('./velocity-payout-sync').read(payoutFile);
    require('./order-money-journey').attach(orders,accounting,online.rows||[],payoutState.batches||[]);
    const paytmPayouts=require('./expenses').paytmOrderSettlementEvidence(accounting);
    require('./order-pos-settlements').attach(orders,accounting,paytmPayouts);
    const paymentBatches=require('./order-payment-batches').paytmBatches(orders,accounting,paytmPayouts);
    const paytmDates=Object.values(accounting.paytmReportTransactions||{}).filter(t=>t.isCustomerPayment!==false).map(t=>t.date).filter(Boolean).sort();
    const paytmCoverage={transactionThrough:paytmDates.at(-1)||null,payoutThrough:paytmPayouts.map(p=>p.settledDate).filter(Boolean).sort().at(-1)||null};
    const velocityPayouts=require('./velocity-payout-sync').status(payoutFile);
    res.json({ success: true, orders, paymentBatches, sources: { paytmCoverage, velocityPayouts, gokwikApi:gokwikApiStatus, velocityDiscovery: {lastSync:index.lastSync||null,lastAttempt:index.lastAttempt||null,error:index.error||null,total:index.total||0,matchedOrders:index.matchedOrders||0}, shopify: store.sync.lastSyncedAt || null, velocity: velocity.lastSync || null, remittances: remittances.lastImport || null, onlineSettlements: online.importedAt || 'Connection required', velocityFinance: finance.updatedAt || null, posClearance: 'Order-level clearance linking required' } });
  } catch (e) { res.status(500).json({ success: false, error: 'Unable to read integrated order sources: ' + e.message }); }
});

router.get('/api/orders-ledger/:id', (req, res) => {
  const store = loadStore();
  const o = store.orders[String(req.params.id)];
  if (!o) return res.status(404).json({ success: false, error: 'Order not found' });
  if (accountingStartAt() && String(o.createdAt || '') < accountingStartAt()) return res.status(404).json({ success: false, error: 'Order predates the accounting reset' });
  const merged = mergedList({ orders: { [o.id]: o }, dispatch: store.dispatch })[0];
  res.json({ success: true, order: merged });
});

// Verified provider references are kept separate from the Shopify source mirror.
router.post('/api/orders-ledger/:id/velocity-link', (req,res)=>{
  const store=loadStore(),id=String(req.params.id),awb=String(req.body.awb||'').trim();
  if(!store.orders[id])return res.status(404).json({success:false,error:'Order not found'});
  if(!/^[A-Za-z0-9-]{6,50}$/.test(awb))return res.status(400).json({success:false,error:'A valid verified AWB is required'});
  store.dispatch[id]={...(store.dispatch[id]||{}),velocityAwb:awb,courier:String(req.body.courier||'').trim(),updatedAt:new Date().toISOString(),updatedBy:(req.user&&req.user.username)||'system'};
  saveStore(store);res.json({success:true,awb});
});

const PACKING_STATES = ['pending', 'packed', 'booked', 'dispatched', 'delivered', 'rto', 'cancelled'];
const CONFIRM_STATES = ['pending', 'confirmed', 'unreachable', 'cancelled'];

router.post('/api/orders-ledger/:id/dispatch', (req, res) => {
  const store = loadStore();
  const id = String(req.params.id);
  if (!store.orders[id]) return res.status(404).json({ success: false, error: 'Order not found' });
  const b = req.body || {};
  const cur = store.dispatch[id] || {};
  const deliveredAt = b.deliveredAt || cur.deliveredAt || null;
  if (b.deliveredAt && (!validDate(String(b.deliveredAt)) || b.deliveredAt > accountingDay(new Date().toISOString()) || b.deliveredAt < accountingDay(store.orders[id].createdAt))) return res.status(400).json({ success: false, error: 'Use a valid actual delivery date between the order date and today.' });
  if (b.packingStatus === 'delivered' && !deliveredAt && !store.orders[id].completedAt) return res.status(400).json({ success: false, error: 'Enter the actual delivery date. P&L must not use the date this form was edited.' });
  const next = {
    velocityAwb:       cur.velocityAwb || '',
    courier:           b.courier != null ? String(b.courier).trim() : (cur.courier || ''),
    trackingUrl:       b.trackingUrl != null ? String(b.trackingUrl).trim() : (cur.trackingUrl || ''),
    packingStatus:     PACKING_STATES.includes(b.packingStatus) ? b.packingStatus : (cur.packingStatus || 'pending'),
    deliveredAt,
    orderConfirmation: CONFIRM_STATES.includes(b.orderConfirmation) ? b.orderConfirmation : (cur.orderConfirmation || 'pending'),
    paymentMode:       b.paymentMode != null ? String(b.paymentMode).trim() : (cur.paymentMode || ''),
    paidAmount:        b.paidAmount != null && b.paidAmount !== '' ? num(b.paidAmount) : (cur.paidAmount != null ? cur.paidAmount : null),
    dueAmount:         b.dueAmount != null && b.dueAmount !== '' ? num(b.dueAmount) : (cur.dueAmount != null ? cur.dueAmount : null),
    remarks:           b.remarks != null ? String(b.remarks).trim() : (cur.remarks || ''),
    updatedAt:         new Date().toISOString(),
    updatedBy:         (req.user && req.user.username) || 'system'
  };
  store.dispatch[id] = next;
  saveStore(store);
  res.json({ success: true, dispatch: next });
});

// ── Auto-sync poller (kept lightweight; incremental only) ────────
function startAutoSync() {
  if (!SHOPIFY_STORE || !SHOPIFY_TOKEN) { console.warn('[orders] Shopify env unset — auto-sync disabled'); return; }
  const every = parseInt(process.env.ORDERS_SYNC_INTERVAL_MS, 10) || 10 * 60 * 1000; // 10 min
  const tick = () => runSync().then(r => {
    if (r && r.imported) console.log(`[orders] sync: +${r.imported} (${r.mode}), total ${r.total}`);
  }).catch(e => console.error('[orders] sync error:', e.message));
  setTimeout(tick, 15 * 1000);        // first run shortly after boot
  setInterval(tick, every);
}

module.exports = { router, startAutoSync, runSync, normalizeOrder, syncFinance, syncVelocityPayouts, syncLogistics, syncGoKwik, startGoKwikSync };
