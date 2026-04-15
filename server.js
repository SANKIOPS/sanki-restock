// ═══════════════════════════════════════════════════════════════
// SANKI BUSINESS OS — server.js  v4.0
// Velocity API Direct + Shopify webhooks + full automation
// ═══════════════════════════════════════════════════════════════
const express = require('express');
const path    = require('path');
const fs      = require('fs');
const multer  = require('multer');
require('dotenv').config();

// Multer — memory storage for remittance Excel upload (no disk needed)
const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 10 * 1024 * 1024 } });

const app  = express();
const PORT = process.env.PORT || 3000;

// Webhook endpoints need raw body — must come BEFORE express.json()
app.use('/api/webhooks/shopify',  express.raw({ type: 'application/json' }));
app.use('/api/webhooks/velocity', express.raw({ type: 'application/json' }));
app.use(express.json({ limit: '10mb' }));
app.use(express.static(path.join(__dirname, 'public'), {
  etag: false,
  setHeaders: (res, fp) => {
    if (fp.endsWith('.html')) {
      res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate');
      res.setHeader('Pragma', 'no-cache');
      res.setHeader('Expires', '0');
    }
  }
}));

// ── Environment ───────────────────────────────────────────────
const SHOPIFY_STORE          = process.env.SHOPIFY_STORE;
const SHOPIFY_TOKEN          = process.env.SHOPIFY_ACCESS_TOKEN;
const SHOPIFY_WEBHOOK_SECRET = process.env.SHOPIFY_WEBHOOK_SECRET || '';
const BITESPEED_API_KEY      = process.env.BITESPEED_API_KEY;
const BITESPEED_APP_ID       = process.env.BITESPEED_APP_ID;
const VELOCITY_API_KEY       = process.env.VELOCITY_API_KEY;    // direct token (long-lived)
const VELOCITY_USERNAME      = process.env.VELOCITY_USERNAME;   // mobile with country code e.g. +91XXXXXXXXXX
const VELOCITY_PASSWORD      = process.env.VELOCITY_PASSWORD;   // Velocity portal password
const VELOCITY_BASE          = 'https://shazam.velocity.in/custom/api/v1';
const SELF_URL               = process.env.RENDER_EXTERNAL_URL || `http://localhost:${PORT}`;

// ════════════════════════════════════════════════════════════════
//  VELOCITY TOKEN MANAGEMENT  (24-hour tokens, auto-refresh)
// ════════════════════════════════════════════════════════════════
let velToken = null;
let velTokenExpiry = 0;

async function getVelToken() {
  // Still valid (with 5-min buffer)?
  if (velToken && Date.now() < velTokenExpiry - 5 * 60 * 1000) return velToken;

  // Try VELOCITY_API_KEY as direct long-lived token first
  if (VELOCITY_API_KEY && !VELOCITY_USERNAME) {
    velToken = VELOCITY_API_KEY;
    velTokenExpiry = Date.now() + 23 * 60 * 60 * 1000; // assume 23 h
    return velToken;
  }

  // Username + password → get fresh token
  if (!VELOCITY_USERNAME || !VELOCITY_PASSWORD) {
    throw new Error('Set VELOCITY_USERNAME + VELOCITY_PASSWORD (or VELOCITY_API_KEY) in Render Environment');
  }
  const fetch = require('node-fetch');

  // Velocity Express uses 'mobile' + 'password' — try both field name variants
  const bodies = [
    { mobile:   VELOCITY_USERNAME, password: VELOCITY_PASSWORD },
    { username: VELOCITY_USERNAME, password: VELOCITY_PASSWORD },
    { email:    VELOCITY_USERNAME, password: VELOCITY_PASSWORD },
    { phone:    VELOCITY_USERNAME, password: VELOCITY_PASSWORD },
  ];

  let lastResp = null;
  for (const body of bodies) {
    const r = await fetch(`${VELOCITY_BASE}/auth-token`, {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify(body)
    });
    const d = await r.json();
    lastResp = d;
    const tok = d.token || d.access_token || d.data?.token || d.result?.token;
    if (tok) {
      velToken = tok;
      velTokenExpiry = d.expires_at
        ? new Date(d.expires_at).getTime()
        : Date.now() + 23 * 60 * 60 * 1000;
      console.log(`[velocity] ✅ Token refreshed using fields: ${Object.keys(body).join('+')}, expires: ${d.expires_at}`);
      return velToken;
    }
  }
  throw new Error('Velocity auth failed — tried mobile/username/email/phone. Response: ' + JSON.stringify(lastResp));
}

// ════════════════════════════════════════════════════════════════
//  VELOCITY STATUS MAP  (all 20+ Velocity statuses → internal)
// ════════════════════════════════════════════════════════════════
const VEL_STATUS_MAP = {
  pending:                  'not_shipped',
  rejected:                 'cancelled',
  processing:               'not_shipped',
  ready_for_pickup:         'ready_for_pickup',
  pickup_scheduled:         'ready_for_pickup',
  not_picked:               'ready_for_pickup',
  in_transit:               'in_transit',
  out_for_delivery:         'out_for_delivery',
  delivered:                'delivered',
  ndr_raised:               'ndr',
  need_attention:           'ndr',
  reattempt_delivery:       'ndr',
  cancelled:                'cancelled',
  lost:                     'rto',
  rto_initiated:            'rto',
  rto_cancelled:            'in_transit',
  rto_in_transit:           'rto',
  rto_need_attention:       'rto',
  rto_delivered:            'rto',
  externally_fulfilled:     'in_transit',
  return_pickup_scheduled:  'rto',
  return_not_picked:        'rto',
  return_qc_failed:         'rto',
  return_in_transit:        'rto',
  return_delivered:         'rto',
  return_cancelled:         'rto',
  return_ndr_raised:        'rto'
};

function normalizeVelStatus(raw) {
  const s = (raw || '').toLowerCase().trim();
  if (VEL_STATUS_MAP[s]) return VEL_STATUS_MAP[s];
  // Fuzzy fallback
  if (s.includes('deliver') && !s.includes('rto') && !s.includes('return')) return 'delivered';
  if (s.includes('out_for') || s.includes('out for'))                        return 'out_for_delivery';
  if (s.includes('rto') || s.includes('return'))                             return 'rto';
  if (s.includes('ndr') || s.includes('attempt') || s.includes('undeliver')) return 'ndr';
  if (s.includes('transit') || s.includes('pickup'))                         return 'in_transit';
  return 'in_transit';
}

// ════════════════════════════════════════════════════════════════
//  VELOCITY CACHE  (AWB → shipment data, persisted to disk)
// ════════════════════════════════════════════════════════════════
const VEL_CACHE_PATH = path.join(__dirname, 'velocity_cache.json');
// shipments: { [awb]: { awb, status, internalStatus, currentLocation, deliveredDate,
//              pickupDate, isCod, codAmount, ndrReason, rtoReason, activities[], lastUpdated } }
let velCache = { shipments: {}, lastSync: null, syncing: false };

function loadVelCache() {
  try {
    if (fs.existsSync(VEL_CACHE_PATH)) {
      const c = JSON.parse(fs.readFileSync(VEL_CACHE_PATH, 'utf8'));
      velCache.shipments = c.shipments || {};
      velCache.lastSync  = c.lastSync  || null;
      console.log(`[velocity] Loaded ${Object.keys(velCache.shipments).length} cached shipments`);
    }
  } catch(e) { console.error('[velocity] Cache load failed:', e.message); }
}

function saveVelCache() {
  try {
    fs.writeFileSync(VEL_CACHE_PATH, JSON.stringify(
      { shipments: velCache.shipments, lastSync: velCache.lastSync }, null, 2
    ));
  } catch(e) { console.error('[velocity] Cache save failed:', e.message); }
}

// Parse Velocity tracking response into clean object
function parseVelTracking(awb, rawData) {
  // Velocity Express API response — handle both nested and flat shapes
  const rd = rawData || {};
  // Velocity shazam API returns data directly on the object
  // Fields: current_status, status_description, is_cod, cod_amount, ndr_reason, rto_reason,
  //         delivery_date, pickup_date, location, activities[]
  // Also handle legacy Shiprocket-style: tracking_data.shipment_track[0]
  const td    = rd.tracking_data || rd;
  const track = (td.shipment_track && td.shipment_track[0]) || td || {};

  // Status — try all known field names
  const rawStatus = rd.current_status || rd.status || rd.shipment_status ||
                    td.shipment_status || track.current_status || '';

  // COD
  const isCod     = rd.is_cod !== undefined ? !!rd.is_cod : (td.is_cod !== undefined ? !!td.is_cod : false);
  const codAmount = parseFloat(rd.cod_amount || td.cod_amount || track.cod_amount || 0);

  // Activities — try multiple field names (needed before reason extraction)
  const acts = (rd.activities || rd.tracking_activities || td.shipment_track_activities ||
                track.activities || []).slice(0, 20);

  // NDR reason — check all known locations in priority order
  // 1. Direct fields on root/td/track
  // 2. track.remark / track.reason (Velocity often puts reason here)
  // 3. Scan activities for the most recent NDR activity description
  function extractReason(keywords) {
    // From track object
    const fromTrack = track.remark || track.reason || track.ndr_reason || track.ndr_remark ||
                      track.rto_reason || track.rto_remark || track.remarks || '';
    if (fromTrack && keywords.some(k => rawStatus.toLowerCase().includes(k))) return fromTrack;
    // From activities — find latest activity matching keyword
    for (const act of acts) {
      const actText = (act.activity || act.status || act.description || act.remark || '').toLowerCase();
      if (keywords.some(k => actText.includes(k))) {
        return act.description || act.remark || act.activity || act.status || '';
      }
    }
    return null;
  }

  const ndrReason = rd.ndr_reason || rd.ndr_remarks || rd.ndr_remark ||
                    td.ndr_reason || td.ndr_remarks ||
                    track.ndr_reason || track.ndr_remark ||
                    track.remark || track.reason ||
                    extractReason(['ndr','undeliver','attempt','no attempt','not available','absent','refused','rescheduled','fake attempt']) ||
                    null;

  const rtoReason = rd.rto_reason || rd.rto_remarks || rd.rto_remark ||
                    td.rto_reason || td.rto_remarks ||
                    track.rto_reason || track.rto_remark ||
                    extractReason(['rto','return','returned','return to origin','pickup return']) ||
                    null;

  // Location / dates
  const location     = rd.location || rd.current_location || track.destination || track.origin ||
                       track.current_location || td.current_location || '';
  const deliveredDate= rd.delivery_date || rd.delivered_date || track.delivered_date ||
                       track.delivery_date || td.delivered_date || null;
  const pickupDate   = rd.pickup_date   || track.pickup_date   || td.pickup_date || null;

  return {
    awb,
    rawStatus,
    status:          normalizeVelStatus(rawStatus),
    currentLocation: location,
    deliveredDate,
    pickupDate,
    isCod,
    codAmount,
    ndrReason,
    rtoReason,
    courierName:     rd.courier_name || track.courier_name || 'Velocity',
    activities:      acts,
    lastUpdated:     new Date().toISOString(),
    _raw:            rd  // keep raw so we can inspect in debug endpoint
  };
}

// ════════════════════════════════════════════════════════════════
//  VELOCITY BACKGROUND SYNC  (runs every 30 min)
//  Collects AWBs from Shopify cache → batch-tracks via Velocity API
// ════════════════════════════════════════════════════════════════
async function syncVelocityShipments() {
  const hasAuth = VELOCITY_API_KEY || (VELOCITY_USERNAME && VELOCITY_PASSWORD);
  if (!hasAuth) return;
  if (velCache.syncing) return;
  velCache.syncing = true;

  try {
    const fetch = require('node-fetch');
    const token = await getVelToken();

    // Collect all unique AWBs from Shopify order cache
    const awbSet = new Set();
    ordersCache.orders.forEach(o => {
      (o.fulfillments || []).forEach(f => {
        if (f.tracking_number) awbSet.add(f.tracking_number.trim());
      });
    });

    const allAWBs = Array.from(awbSet);
    if (!allAWBs.length) {
      console.log('[velocity] No AWBs found in order cache — skipping sync');
      velCache.syncing = false;
      return;
    }

    // ── STEP 1: Batch order-tracking (50 per call) ──
    const BATCH = 50;
    let updated = 0;

    for (let i = 0; i < allAWBs.length; i += BATCH) {
      const batch = allAWBs.slice(i, i + BATCH);
      try {
        const r = await fetch(`${VELOCITY_BASE}/order-tracking`, {
          method:  'POST',
          headers: { 'Content-Type': 'application/json', 'Authorization': token },
          body:    JSON.stringify({ awbs: batch })
        });

        if (r.status === 401) { velToken = null; break; }

        const d = await r.json();
        const resultMap = d.result || d.data || {};
        Object.keys(resultMap).forEach(awb => {
          velCache.shipments[awb] = parseVelTracking(awb, resultMap[awb]);
          updated++;
        });
      } catch(batchErr) {
        console.error(`[velocity] Batch error:`, batchErr.message);
      }
    }

    // ── STEP 2: /shipments API — get actual cod_amount + attempt_count per AWB ──
    // Must include DELIVERED orders without codAmount — COD data only comes from /shipments, not tracking
    // Skip only: delivered/rto/cancelled that already have a confirmed codAmount
    const activeAWBs = allAWBs.filter(awb => {
      const s = velCache.shipments[awb];
      if (!s) return true; // never fetched yet
      if (s.status === 'delivered' && !s.codAmount) return true; // delivered but no COD data yet
      if (s.status === 'rto'       && !s.codAmount) return true; // RTO but no COD data yet
      return !['delivered','rto','cancelled'].includes(s.status); // active shipments always
    });

    for (const awb of activeAWBs) {
      try {
        await new Promise(r => setTimeout(r, 300)); // 300ms delay to avoid rate limit
        const r = await fetch(`${VELOCITY_BASE}/shipments`, {
          method:  'POST',
          headers: { 'Content-Type': 'application/json', 'Authorization': token },
          body:    JSON.stringify({ search: awb, per_page: 1 })
        });
        const contentType = r.headers.get('content-type') || '';
        if (!contentType.includes('json')) continue; // skip if rate-limited (returns HTML)
        const d = await r.json();
        const attrs = (d.data && d.data[0] && d.data[0].attributes) || {};
        if (Object.keys(attrs).length && velCache.shipments[awb]) {
          if (attrs.cod_amount)    velCache.shipments[awb].codAmount     = parseFloat(attrs.cod_amount);
          if (attrs.attempt_count !== undefined) velCache.shipments[awb].attemptCount = attrs.attempt_count;
          velCache.shipments[awb].isCod = !!(parseFloat(attrs.cod_amount) > 0);

          // ── Shipping charge extraction (per Velocity API docs) ──────────────
          // /shipments API returns: attrs.fare_breakup (object with shipping_charges, cod_charges, rto_charges etc)
          // Forward-order creation returns: payload.charges.frwd_charges.shipping_charges
          // We check fare_breakup first (most accurate from /shipments), then fall back to known fields
          // ── Velocity fare_breakup structure (confirmed from live API):
          // fare_breakup = { charges: 104, rto_charges: 0, cod_charges: 0, multiplier: 1.408, platform_fee: 0 }
          // "charges" = base shipping cost, "cod_charges" = COD handling fee
          const fb = attrs.fare_breakup || {};
          const shippingCharge = parseFloat(fb.charges || 0);
          const codChargeVel   = parseFloat(fb.cod_charges || 0);
          const rtoCharge      = parseFloat(fb.rto_charges || 0);
          const platformFee    = parseFloat(fb.platform_fee || 0);
          // Total you pay Velocity = shipping + COD fee + platform fee
          const totalVelCharge = shippingCharge + codChargeVel + platformFee;

          if (shippingCharge > 0 || codChargeVel > 0) {
            velCache.shipments[awb].shippingCharge      = shippingCharge;
            velCache.shipments[awb].codChargeVelocity   = codChargeVel;
            velCache.shipments[awb].rtoCharge           = rtoCharge;
            velCache.shipments[awb].platformFee         = platformFee;
            velCache.shipments[awb].totalVelocityCharge = totalVelCharge;
            velCache.shipments[awb].fareBreakup         = fb;
          }
          // NDR / RTO reasons from /shipments attrs (may be more detailed than tracking API)
          const attrNdr = attrs.ndr_reason || attrs.ndr_remark || attrs.ndr_remarks ||
                          attrs.remark || attrs.reason || attrs.remarks || null;
          const attrRto = attrs.rto_reason || attrs.rto_remark || attrs.rto_remarks || null;
          if (attrNdr && ['ndr','undeliver'].some(k => (velCache.shipments[awb].status||'').includes(k)))
            velCache.shipments[awb].ndrReason = attrNdr;
          if (attrRto && velCache.shipments[awb].status === 'rto')
            velCache.shipments[awb].rtoReason = attrRto;
          // If still missing, use remark/reason from attrs regardless of status
          if (!velCache.shipments[awb].ndrReason && !velCache.shipments[awb].rtoReason) {
            const anyReason = attrs.remark || attrs.reason || attrs.remarks || null;
            if (anyReason) velCache.shipments[awb].lastRemark = anyReason;
          }
        }
      } catch(e) { /* skip */ }
    }

    velCache.lastSync = new Date().toISOString();
    saveVelCache();
    console.log(`[velocity] ✅ Synced ${updated}/${allAWBs.length} AWBs, ${activeAWBs.length} COD-checked`);
  } catch(e) {
    console.error('[velocity] Sync failed:', e.message);
  } finally {
    velCache.syncing = false;
  }
}

// ════════════════════════════════════════════════════════════════
//  SHOPIFY CACHE + BACKGROUND SYNC
// ════════════════════════════════════════════════════════════════
const CACHE_PATH = path.join(__dirname, 'sanki_cache.json');
const DATA_PATH  = path.join(__dirname, 'sanki_data.json');
const META_PATH  = path.join(__dirname, 'sanki_order_meta.json');

let ordersCache = { orders: [], lastSync: null, syncing: false };
let productsCache = { products: null, lastSync: null };

function loadCache() {
  try {
    if (fs.existsSync(CACHE_PATH)) {
      const c = JSON.parse(fs.readFileSync(CACHE_PATH, 'utf8'));
      ordersCache.orders   = c.orders   || [];
      ordersCache.lastSync = c.lastSync || null;
      console.log(`[cache] Loaded ${ordersCache.orders.length} orders from disk`);
    }
  } catch(e) { console.error('[cache] Load failed:', e.message); }
}

function saveCache() {
  try {
    fs.writeFileSync(CACHE_PATH, JSON.stringify(
      { orders: ordersCache.orders, lastSync: ordersCache.lastSync }, null, 2
    ));
  } catch(e) { console.error('[cache] Save failed:', e.message); }
}

async function shopifyFetch(fetch, url) {
  return fetch(url, {
    headers: { 'X-Shopify-Access-Token': SHOPIFY_TOKEN, 'Content-Type': 'application/json' }
  });
}

async function shopifyFetchAll(fetch, startUrl) {
  let all = [], url = startUrl;
  while (url) {
    const r = await shopifyFetch(fetch, url);
    const d = await r.json();
    const key = Object.keys(d).find(k => Array.isArray(d[k]));
    if (key) all = all.concat(d[key]);
    const link = r.headers.get('Link') || '';
    const next = link.match(/<([^>]+)>;\s*rel="next"/);
    url = next ? next[1] : null;
  }
  return all;
}

function cleanOrder(order) {
  const livm = {};
  (order.line_items || []).forEach(i => {
    if (i.id && i.variant_id) livm[String(i.id)] = String(i.variant_id);
  });
  const rq = {};
  (order.refunds || []).forEach(ref =>
    (ref.refund_line_items || []).forEach(rli => {
      let vid = rli.line_item ? String(rli.line_item.variant_id) : null;
      if (!vid || vid === 'null') vid = rli.line_item_id ? livm[String(rli.line_item_id)] : null;
      if (vid && vid !== 'null') rq[vid] = (rq[vid] || 0) + (rli.quantity || 0);
    })
  );
  const items = (order.line_items || [])
    .filter(i => i.variant_id != null)
    .map(i => {
      const vid = String(i.variant_id);
      return { ...i, quantity: Math.max(0, Number(i.quantity) - (rq[vid] || 0)) };
    })
    .filter(i => i.quantity > 0);
  return { ...order, line_items: items };
}

async function backgroundSync(days = 180) {
  if (!SHOPIFY_STORE || !SHOPIFY_TOKEN) return;
  if (ordersCache.syncing) return;
  ordersCache.syncing = true;
  try {
    const fetch = require('node-fetch');
    const since = new Date(); since.setDate(since.getDate() - days);
    const all   = await shopifyFetchAll(fetch,
      `https://${SHOPIFY_STORE}/admin/api/2024-01/orders.json?status=any` +
      `&created_at_min=${since.toISOString()}&limit=250`
    );
    ordersCache.orders   = all.map(cleanOrder);
    ordersCache.lastSync = new Date().toISOString();
    // Cache products during background sync
  try {
    const pf = require('node-fetch');
    const prods = await shopifyFetchAll(pf,
      `https://${SHOPIFY_STORE}/admin/api/2024-01/products.json?limit=250&fields=id,title,status,variants,image,images,vendor,tags,product_type`,
      SHOPIFY_TOKEN);
    if (prods && prods.length > 0) { productsCache.products = prods; productsCache.lastSync = new Date().toISOString(); }
  } catch(pe) { console.error('[sync] Product cache error:', pe.message); }
  saveCache();
    console.log(`[sync] ✅ ${ordersCache.orders.length} orders at ${ordersCache.lastSync}`);
  } catch(e) {
    console.error('[sync] ❌ Background sync failed:', e.message);
  } finally {
    ordersCache.syncing = false;
  }
}

// ════════════════════════════════════════════════════════════════
//  SHOPIFY WEBHOOKS  (instant real-time order updates)
// ════════════════════════════════════════════════════════════════
app.post('/api/webhooks/shopify', (req, res) => {
  res.status(200).send('OK');
  try {
    const topic = req.headers['x-shopify-topic'] || '';
    const body  = req.body instanceof Buffer ? req.body.toString('utf8') : JSON.stringify(req.body);
    const order = JSON.parse(body);
    if (!order || !order.id) return;

    const cleaned = cleanOrder(order);
    const idx = ordersCache.orders.findIndex(o => o.id === order.id);

    if (topic === 'orders/create') {
      if (idx < 0) ordersCache.orders.unshift(cleaned);
      else         ordersCache.orders[idx] = cleaned;
    } else if (['orders/updated','orders/fulfilled','fulfillments/create','fulfillments/update'].includes(topic)) {
      if (idx >= 0) ordersCache.orders[idx] = cleaned;
      else          ordersCache.orders.unshift(cleaned);
    } else if (topic === 'orders/cancelled') {
      if (idx >= 0) ordersCache.orders[idx] = cleaned; // keep with cancelled_at set
    }

    ordersCache.lastSync = new Date().toISOString();
    saveCache();
    console.log(`[webhook/shopify] ${topic} — ${order.name || order.id}`);

    // Trigger Velocity sync if a new AWB arrived
    if (['fulfillments/create','fulfillments/update','orders/fulfilled'].includes(topic)) {
      setTimeout(() => syncVelocityShipments(), 3000);
    }
  } catch(e) { console.error('[webhook/shopify] Parse error:', e.message); }
});

// ════════════════════════════════════════════════════════════════
//  VELOCITY WEBHOOKS  (instant real-time delivery updates)
//  Configure in Velocity portal: Settings → Webhooks
//  URL: https://sanki-1.onrender.com/api/webhooks/velocity
// ════════════════════════════════════════════════════════════════
app.post('/api/webhooks/velocity', (req, res) => {
  res.status(200).send('OK');
  try {
    const body    = req.body instanceof Buffer ? JSON.parse(req.body.toString('utf8')) : req.body;
    const awb     = body.awb_code || body.tracking_number || body.awb || body.waybill;
    const rawSt   = body.current_status || body.status || body.shipment_status || '';
    const ndrR    = body.ndr_reason || null;
    const rtoR    = body.rto_reason || null;

    if (!awb) return;

    if (!velCache.shipments[awb]) velCache.shipments[awb] = { awb };
    Object.assign(velCache.shipments[awb], {
      rawStatus:    rawSt,
      status:       normalizeVelStatus(rawSt),
      ndrReason:    ndrR || velCache.shipments[awb].ndrReason,
      rtoReason:    rtoR || velCache.shipments[awb].rtoReason,
      isCod:        body.is_cod  !== undefined ? body.is_cod  : velCache.shipments[awb].isCod,
      codAmount:    body.cod_amount ? parseFloat(body.cod_amount) : velCache.shipments[awb].codAmount,
      lastUpdated:  new Date().toISOString()
    });
    velCache.lastSync = new Date().toISOString();
    saveVelCache();
    console.log(`[webhook/velocity] ${awb} → ${rawSt} (${normalizeVelStatus(rawSt)})`);
  } catch(e) { console.error('[webhook/velocity] Error:', e.message); }
});

// ════════════════════════════════════════════════════════════════
//  ONE-TIME SETUP: Register Shopify webhooks
// ════════════════════════════════════════════════════════════════
app.get('/api/setup/webhooks', async (req, res) => {
  if (!SHOPIFY_STORE || !SHOPIFY_TOKEN)
    return res.json({ success: false, error: 'SHOPIFY_STORE or SHOPIFY_ACCESS_TOKEN not set' });
  try {
    const fetch = require('node-fetch');
    const webhookUrl = `${SELF_URL}/api/webhooks/shopify`;
    const topics = ['orders/create','orders/updated','orders/cancelled','orders/fulfilled','fulfillments/create','fulfillments/update'];

    const exR = await shopifyFetch(fetch, `https://${SHOPIFY_STORE}/admin/api/2024-01/webhooks.json`);
    const exD = await exR.json();
    const existing = (exD.webhooks || []).map(w => w.topic);

    const results = [];
    for (const topic of topics) {
      if (existing.includes(topic)) { results.push({ topic, status: 'already_exists' }); continue; }
      const cr = await fetch(`https://${SHOPIFY_STORE}/admin/api/2024-01/webhooks.json`, {
        method: 'POST',
        headers: { 'X-Shopify-Access-Token': SHOPIFY_TOKEN, 'Content-Type': 'application/json' },
        body: JSON.stringify({ webhook: { topic, address: webhookUrl, format: 'json' } })
      });
      const cd = await cr.json();
      if (cd.webhook) results.push({ topic, status: 'created', id: cd.webhook.id });
      else results.push({ topic, status: 'error', detail: JSON.stringify(cd.errors || cd) });
    }
    res.json({ success: true, webhookUrl, message: 'Shopify webhooks active!', results });
  } catch(e) { res.json({ success: false, error: e.message }); }
});

// ════════════════════════════════════════════════════════════════
//  ORDERS API  (Shopify data from cache)
// ════════════════════════════════════════════════════════════════
app.get('/api/orders', async (req, res) => {
  try {
    let orders = ordersCache.orders;
    if (!orders.length && SHOPIFY_STORE && SHOPIFY_TOKEN) {
      backgroundSync(180).catch(e => console.error('[sync] Startup error:', e)); // non-blocking
      orders = ordersCache.orders;
    }
    const days = parseInt(req.query.days) || 180;
    const since = new Date(); since.setDate(since.getDate() - days);
    const filtered = orders.filter(o => new Date(o.created_at) >= since);
    res.json({ success: true, orders: filtered, total: filtered.length, lastSync: ordersCache.lastSync });
  } catch(e) { res.json({ success: false, error: e.message }); }
});

app.post('/api/orders/sync', async (req, res) => {
  try {
    await backgroundSync(parseInt(req.body.days) || 180);
    res.json({ success: true, total: ordersCache.orders.length, lastSync: ordersCache.lastSync });
  } catch(e) { res.json({ success: false, error: e.message }); }
});

// ════════════════════════════════════════════════════════════════
//  VELOCITY API ENDPOINTS
// ════════════════════════════════════════════════════════════════

// Serve Velocity shipment data from cache (instant)
app.get('/api/velocity/shipments', (req, res) => {
  res.json({
    success:  true,
    shipments: velCache.shipments,
    lastSync:  velCache.lastSync,
    total:     Object.keys(velCache.shipments).length
  });
});

// Force a fresh Velocity sync now
app.post('/api/velocity/sync', async (req, res) => {
  // Respond immediately — sync runs in background to avoid Render 30s timeout
  const awbsBefore = Object.keys(velCache.shipments).length;
  res.json({
    success:  true,
    message:  'Sync started in background',
    tracked:  awbsBefore,
    orders:   ordersCache.orders.length,
    syncing:  true
  });
  // Kick off in background (no await)
  (async () => {
    try {
      if (!ordersCache.orders.length && SHOPIFY_STORE && SHOPIFY_TOKEN) {
        console.log('[velocity/sync] ordersCache empty — running Shopify sync first');
        await backgroundSync(180);
      }
      await syncVelocityShipments();
    } catch(e) {
      console.error('[velocity/sync] Background error:', e.message);
    }
  })();
});

// Poll endpoint — client checks this to know when sync is done
app.get('/api/velocity/sync/status', (req, res) => {
  res.json({
    syncing:  velCache.syncing,
    tracked:  Object.keys(velCache.shipments).length,
    lastSync: velCache.lastSync,
    orders:   ordersCache.orders.length
  });
});

// Track specific AWBs (also updates cache)
app.post('/api/velocity/track/bulk', async (req, res) => {
  try {
    const fetch = require('node-fetch');
    const { awbs } = req.body;
    if (!awbs || !awbs.length) return res.json({ success: false, error: 'No AWBs provided' });
    const token = await getVelToken();
    const r = await fetch(`${VELOCITY_BASE}/order-tracking`, {
      method:  'POST',
      headers: {
        'Content-Type':  'application/json',
        'Authorization': token,
        'token':          token
      },
      body: JSON.stringify({ awbs })
    });
    const d = await r.json();
    // Update cache with fresh data — handle both result/data keys
    const resultMap = d.result || d.data || d.tracking_data || {};
    if (Object.keys(resultMap).length) {
      Object.keys(resultMap).forEach(awb => {
        velCache.shipments[awb] = parseVelTracking(awb, resultMap[awb]);
      });
      velCache.lastSync = new Date().toISOString();
      saveVelCache();
    }
    res.json({ success: true, results: resultMap, raw: d });
  } catch(e) { res.json({ success: false, error: e.message }); }
});

// ── Discover Velocity remittance endpoint — tries every known pattern and returns raw results ──
app.get('/api/velocity/discover-remittance', async (req, res) => {
  try {
    const fetch = require('node-fetch');
    const token = await getVelToken();
    const headers = { 'Content-Type':'application/json', 'Authorization': token, 'token': token };
    const bases = [
      'https://shazam.velocity.in/custom/api/v1',
      'https://shazam.velocity.in/api/v1',
      'https://shazam.velocity.in/api',
      'https://shazam.velocity.in',
    ];
    const paths = [
      '/passbook', '/billing/passbook', '/billing', '/cod-remittance',
      '/remittance', '/settlements', '/finance/passbook', '/wallet/transactions',
      '/cod/remittance', '/reports/cod', '/reports/remittance',
      '/shipping/billing', '/shipping/passbook',
      '/passbook/list', '/remittance/list', '/cod-collection',
    ];
    const results = [];
    for (const base of bases) {
      for (const p of paths) {
        const url = base + p;
        try {
          const r = await fetch(url, { headers, timeout: 5000 });
          const text = await r.text();
          let parsed = null;
          try { parsed = JSON.parse(text); } catch(e) {}
          const hasData = parsed && (parsed.result || parsed.data || parsed.passbook || parsed.transactions || parsed.entries || parsed.remittances);
          results.push({ url, status: r.status, hasData: !!hasData, preview: text.substring(0, 200) });
          if (r.status === 200 && hasData) console.log(`[discover] ✅ FOUND: ${url}`);
        } catch(e) {
          results.push({ url, status: 'error', error: e.message });
        }
      }
    }
    const working = results.filter(r => r.status === 200 && r.hasData);
    res.json({ success: true, working, all: results });
  } catch(e) { res.json({ success: false, error: e.message }); }
});

// ── Debug: test auth + raw API response for one AWB (helps diagnose integration issues) ──
app.get('/api/velocity/debug/:awb', async (req, res) => {
  try {
    const fetch = require('node-fetch');
    const awb   = req.params.awb;
    const token = await getVelToken();
    const headers = { 'Content-Type': 'application/json', 'Authorization': token, 'token': token };

    // Hit both APIs simultaneously to compare responses
    const [trackResp, shipResp] = await Promise.all([
      fetch(`${VELOCITY_BASE}/order-tracking`, {
        method: 'POST', headers,
        body: JSON.stringify({ awbs: [awb] })
      }),
      fetch(`${VELOCITY_BASE}/shipments`, {
        method: 'POST', headers,
        body: JSON.stringify({ search: awb, per_page: 1 })
      })
    ]);

    const trackRaw = await trackResp.json();
    const shipRaw  = await shipResp.json();

    // Extract attrs from /shipments for charge inspection
    const attrs = (shipRaw.data && shipRaw.data[0] && shipRaw.data[0].attributes) || {};
    const fareBreakup = attrs.fare_breakup || 'NOT PRESENT';
    const totalPrice  = attrs.total_price  || 'NOT PRESENT';

    res.json({
      success: true, awb,
      // What each API returns — use this to find charge fields
      trackingAPI: { httpStatus: trackResp.status, response: trackRaw },
      shipmentsAPI: {
        httpStatus: shipResp.status,
        attrsKeys:  Object.keys(attrs),          // ALL field names Velocity sends
        fare_breakup: fareBreakup,               // The documented charge field
        total_price:  totalPrice,
        fullAttrs:    attrs                      // Complete attrs object
      },
      cachedResult: velCache.shipments[awb] || null,
      hint: 'Look at shipmentsAPI.attrsKeys and shipmentsAPI.fullAttrs to find the charge field name for your account'
    });
  } catch(e) {
    res.json({ success: false, error: e.message, stack: e.stack });
  }
});

// Manually set shipping charge for an AWB (fallback when API doesn't return it)
app.post('/api/velocity/set-charge/:awb', async (req, res) => {
  try {
    const awb = req.params.awb;
    const { shippingCharge, codCharge } = req.body;
    if (!velCache.shipments[awb]) velCache.shipments[awb] = { awb };
    if (shippingCharge != null) {
      velCache.shipments[awb].shippingCharge      = parseFloat(shippingCharge) || 0;
      velCache.shipments[awb].codChargeVelocity   = parseFloat(codCharge) || 0;
      velCache.shipments[awb].totalVelocityCharge = (parseFloat(shippingCharge)||0) + (parseFloat(codCharge)||0);
      velCache.shipments[awb].chargeSource        = 'manual';
    }
    saveVelCache();
    res.json({ success: true, awb, updated: velCache.shipments[awb] });
  } catch(e) { res.json({ success: false, error: e.message }); }
});

// Shipping charge summary — all AWBs with freight data + raw attrs for debugging
app.get("/api/velocity/shipping-charges", (req, res) => {
  const ships = Object.values(velCache.shipments);
  const result = ships.map(s => ({
    awb: s.awb, status: s.status, shippingCharge: s.shippingCharge || 0,
    shippingChargeField: s.shippingChargeField || null, isCod: s.isCod, codAmount: s.codAmount || 0,
    rawAttrsKeys: s._attrsRaw ? Object.keys(s._attrsRaw) : [], rawAttrs: s._attrsRaw || {}
  }));
  const withCharge = result.filter(r => r.shippingCharge > 0);
  res.json({
    success: true, total: ships.length, withCharge: withCharge.length,
    totalFreight: withCharge.reduce((s, r) => s + r.shippingCharge, 0),
    attrsFieldsSeen: [...new Set(result.flatMap(r => r.rawAttrsKeys))].sort(),
    sample: result.slice(0, 20)
  });
});

// Auto-calculated COD collection summary
app.get('/api/velocity/cod-summary', (req, res) => {
  // COD logic mirrors Google Script:
  // delivered → collected | rto/cancelled → lost (not collected) | else → pending
  const ships = Object.values(velCache.shipments);
  let collected = 0, pending = 0, lost = 0, collectedCount = 0, pendingCount = 0;
  ships.forEach(s => {
    if (!s.isCod) return;
    const amt = parseFloat(s.codAmount) || 0;
    if (!amt) return;
    const st = s.status || '';
    if (st === 'delivered') {
      collected += amt; collectedCount++;
    } else if (['rto','cancelled'].includes(st)) {
      lost += amt; // RTO = COD came back with shipment
    } else {
      pending += amt; pendingCount++; // in_transit, ndr, out_for_delivery etc
    }
  });
  res.json({ success: true, collected, collectedCount, pending, pendingCount, lost, total: ships.length });
});

// ── Velocity Remittance / Passbook ──
// Tries multiple known endpoint patterns for COD remittance data
app.get('/api/velocity/remittance', async (req, res) => {
  try {
    const fetch = require('node-fetch');
    const token = await getVelToken();
    const headers = {
      'Content-Type':  'application/json',
      'Authorization': token,
      'token':          token
    };
    const { from, to, page = 1, limit = 50 } = req.query;
    const body = { from_date: from, to_date: to, page, limit };

    const q = `page=${page}&limit=${limit}${from?'&from_date='+from:''}${to?'&to_date='+to:''}`;
    // Try every known Velocity remittance/passbook endpoint pattern
    const endpoints = [
      // shazam API v1 patterns
      { method: 'GET',  url: `${VELOCITY_BASE}/passbook?${q}` },
      { method: 'POST', url: `${VELOCITY_BASE}/passbook`, body },
      { method: 'GET',  url: `${VELOCITY_BASE}/billing/passbook?${q}` },
      { method: 'GET',  url: `${VELOCITY_BASE}/billing?current_tab=passbook&${q}` },
      { method: 'GET',  url: `${VELOCITY_BASE}/cod-remittance?${q}` },
      { method: 'POST', url: `${VELOCITY_BASE}/cod-remittance`, body },
      { method: 'GET',  url: `${VELOCITY_BASE}/remittance?${q}` },
      { method: 'GET',  url: `${VELOCITY_BASE}/settlements?${q}` },
      { method: 'GET',  url: `${VELOCITY_BASE}/finance/passbook?${q}` },
      { method: 'GET',  url: `${VELOCITY_BASE}/wallet/transactions?${q}` },
      { method: 'GET',  url: `${VELOCITY_BASE}/shipping/billing?current_tab=passbook&${q}` },
      // Alternative base URLs
      { method: 'GET',  url: `https://shazam.velocity.in/api/v1/passbook?${q}` },
      { method: 'GET',  url: `https://shazam.velocity.in/api/passbook?${q}` },
      { method: 'GET',  url: `https://app.velocity.in/custom/api/v1/passbook?${q}` },
      { method: 'GET',  url: `https://dashboard.velocity.in/api/passbook?${q}` },
    ];

    for (const ep of endpoints) {
      try {
        const opts = { method: ep.method, headers };
        if (ep.method === 'POST' && ep.body) opts.body = JSON.stringify(ep.body);
        const r = await fetch(ep.url, opts);
        const d = await r.json();
        if (r.status === 200 && (d.result || d.data || d.passbook || d.transactions || d.remittances)) {
          return res.json({ success: true, endpoint: ep.url, data: d });
        }
      } catch(e) { /* try next */ }
    }

    res.json({ success: false, error: 'No remittance endpoint found. Share the Network tab URL from dashboard.velocity.in/shipping/billing' });
  } catch(e) {
    res.json({ success: false, error: e.message });
  }
});

// ── Velocity Remittance: Upload Excel from Velocity portal ──────────────────
// Velocity portal → Billing → Passbook → Export → upload that file here
// Auto-matches AWB/Order ID to your COD orders and marks them settled
const REMITTANCE_PATH = path.join(__dirname, 'velocity_remittance.json');
let remittanceData = { entries: [], lastImport: null };
function loadRemittance() {
  try { if (fs.existsSync(REMITTANCE_PATH)) remittanceData = JSON.parse(fs.readFileSync(REMITTANCE_PATH, 'utf8')); }
  catch(e) { console.error('[remittance] Load error:', e.message); }
}
function saveRemittance() {
  try { fs.writeFileSync(REMITTANCE_PATH, JSON.stringify(remittanceData, null, 2)); }
  catch(e) { console.error('[remittance] Save error:', e.message); }
}
loadRemittance();

app.post('/api/velocity/remittance/upload', upload.single('file'), (req, res) => {
  try {
    if (!req.file) return res.json({ success: false, error: 'No file uploaded' });
    const XLSX = require('xlsx');
    const wb   = XLSX.read(req.file.buffer, { type: 'buffer', cellDates: true });
    const ws   = wb.Sheets[wb.SheetNames[0]];
    const rows = XLSX.utils.sheet_to_json(ws, { defval: '' });

    if (!rows.length) return res.json({ success: false, error: 'Excel file is empty' });

    // Detect column names flexibly — Velocity uses different headers across reports
    const sample = rows[0];
    const keys   = Object.keys(sample).map(k => k.toLowerCase().trim());
    function findCol(variants) {
      for (const v of variants) {
        const k = keys.find(k => k.includes(v));
        if (k) return Object.keys(sample).find(ok => ok.toLowerCase().trim() === k);
      }
      return null;
    }
    const awbCol    = findCol(['awb', 'tracking', 'waybill', 'airway']);
    const amtCol    = findCol(['amount', 'cod', 'remit', 'credit', 'value']);
    const dateCol   = findCol(['date', 'credit date', 'remit date', 'transfer']);
    const utrCol    = findCol(['utr', 'ref', 'transaction', 'neft', 'imps']);
    const orderCol  = findCol(['order', 'order id', 'order_id', 'reference']);
    const statusCol = findCol(['status', 'remark']);

    // Load current order meta from disk so we can update it
    let diskMeta = {};
    try { if (fs.existsSync(META_PATH)) diskMeta = JSON.parse(fs.readFileSync(META_PATH, 'utf8')); } catch(e) {}

    const entries = [];
    let matched = 0;

    rows.forEach(row => {
      const awb    = awbCol    ? String(row[awbCol]   || '').trim() : '';
      const amount = amtCol    ? parseFloat(row[amtCol]) || 0       : 0;
      const date   = dateCol   ? String(row[dateCol]  || '').trim() : '';
      const utr    = utrCol    ? String(row[utrCol]   || '').trim() : '';
      const ordRef = orderCol  ? String(row[orderCol] || '').trim() : '';
      const status = statusCol ? String(row[statusCol]|| '').trim() : 'settled';

      if (!awb && !ordRef) return; // skip blank rows

      entries.push({ awb, amount, date, utr, ordRef, status: status || 'settled', raw: row });

      // Auto-match to order meta by AWB
      if (awb) {
        const order = ordersCache.orders.find(o =>
          (o.fulfillments || []).some(f => (f.tracking_number || '').trim() === awb)
        );
        if (order) {
          if (!diskMeta[order.id]) diskMeta[order.id] = {};
          const m = diskMeta[order.id];
          m.settlementStatus = 'settled';
          m.settlementAmount = amount || m.settlementAmount;
          m.settlementDate   = date   || m.settlementDate;
          m.settlementUTR    = utr    || m.settlementUTR;
          m.customerPayment  = 'paid';
          matched++;
        }
      }
    });

    // Persist updated meta back to disk
    fs.writeFileSync(META_PATH, JSON.stringify(diskMeta, null, 2));
    remittanceData.entries    = entries;
    remittanceData.lastImport = new Date().toISOString();
    saveRemittance();

    res.json({
      success: true,
      total:   entries.length,
      matched,
      lastImport: remittanceData.lastImport,
      columns: { awbCol, amtCol, dateCol, utrCol, orderCol }
    });
  } catch(e) {
    console.error('[remittance] Upload error:', e.message);
    res.json({ success: false, error: e.message });
  }
});

app.get('/api/velocity/remittance/data', (req, res) => {
  res.json({ success: true, ...remittanceData });
});

// Velocity webhook setup instructions
app.get('/api/setup/velocity-webhook', (req, res) => {
  res.json({
    success: true,
    webhookUrl: `${SELF_URL}/api/webhooks/velocity`,
    instructions: [
      '1. Log in to your Velocity portal',
      '2. Go to Settings → Webhooks',
      '3. Add webhook URL: ' + `${SELF_URL}/api/webhooks/velocity`,
      '4. Select all status events',
      '5. Save — Velocity will now push every status change instantly'
    ]
  });
});

// ════════════════════════════════════════════════════════════════
//  ORDER META  (call status, remarks, settlement — server-stored)
// ════════════════════════════════════════════════════════════════
app.get('/api/orders/meta', (req, res) => {
  try {
    if (fs.existsSync(META_PATH))
      return res.json({ success: true, meta: JSON.parse(fs.readFileSync(META_PATH, 'utf8')) });
    res.json({ success: true, meta: {} });
  } catch(e) { res.json({ success: false, error: e.message }); }
});

app.post('/api/orders/meta/save', (req, res) => {
  try {
    fs.writeFileSync(META_PATH, JSON.stringify(req.body.meta || req.body, null, 2));
    res.json({ success: true, savedAt: new Date().toISOString() });
  } catch(e) { res.json({ success: false, error: e.message }); }
});

app.patch('/api/orders/meta/:orderId', (req, res) => {
  try {
    let meta = {};
    if (fs.existsSync(META_PATH)) meta = JSON.parse(fs.readFileSync(META_PATH, 'utf8'));
    const id = req.params.orderId;
    meta[id] = { ...(meta[id] || {}), ...req.body, updatedAt: new Date().toISOString() };
    fs.writeFileSync(META_PATH, JSON.stringify(meta, null, 2));
    res.json({ success: true, meta: meta[id] });
  } catch(e) { res.json({ success: false, error: e.message }); }
});

// ════════════════════════════════════════════════════════════════
//  SHOPIFY — PRODUCTS
// ════════════════════════════════════════════════════════════════
app.get('/api/products', async (req, res) => {
  if (productsCache.products && productsCache.lastSync &&
      (Date.now() - new Date(productsCache.lastSync).getTime()) < 30*60*1000) {
    return res.json({ success: true, products: productsCache.products, total: productsCache.products.length, fromCache: true });
  }
  try {
    const fetch = require('node-fetch');
    const all = await shopifyFetchAll(fetch,
      `https://${SHOPIFY_STORE}/admin/api/2024-01/products.json?limit=250&fields=id,title,status,variants,image,images,vendor,tags,product_type`,
      SHOPIFY_TOKEN);
    productsCache.products = all; productsCache.lastSync = new Date().toISOString();
    res.json({ success: true, products: all, total: all.length });
  } catch(e) { res.json({ success: false, error: e.message }); }
});
    res.json({ success: true, products, total: products.length });
  } catch(e) { res.json({ success: false, error: e.message }); }
});

// ════════════════════════════════════════════════════════════════
//  SHOPIFY — CUSTOMERS
// ════════════════════════════════════════════════════════════════
app.get('/api/shopify/customers', async (req, res) => {
  try {
    const fetch = require('node-fetch');
    const customers = await shopifyFetchAll(fetch,
      `https://${SHOPIFY_STORE}/admin/api/2024-01/customers.json?limit=250`
    );
    res.json({ success: true, customers, total: customers.length });
  } catch(e) { res.json({ success: false, error: e.message }); }
});

// ════════════════════════════════════════════════════════════════
//  SHOPIFY — INVENTORY
// ════════════════════════════════════════════════════════════════
app.get('/api/inventory', async (req, res) => {
  try {
    const fetch = require('node-fetch');
    const r = await shopifyFetch(fetch,
      `https://${SHOPIFY_STORE}/admin/api/2024-01/inventory_levels.json?limit=250`
    );
    const d = await r.json();
    res.json({ success: true, inventory_levels: d.inventory_levels || [] });
  } catch(e) { res.json({ success: false, error: e.message }); }
});

app.post('/api/inventory/adjust', async (req, res) => {
  try {
    const fetch = require('node-fetch');
    const { inventoryItemId, adjustment } = req.body;
    if (!inventoryItemId || adjustment == null)
      return res.json({ success: false, error: 'Missing inventoryItemId or adjustment' });
    const lr  = await shopifyFetch(fetch,
      `https://${SHOPIFY_STORE}/admin/api/2024-01/inventory_levels.json?inventory_item_ids=${inventoryItemId}`
    );
    const ld  = await lr.json();
    const lvls = ld.inventory_levels || [];
    if (!lvls.length) return res.json({ success: false, error: 'No inventory level found' });
    const locId = lvls[0].location_id;
    const ar = await fetch(`https://${SHOPIFY_STORE}/admin/api/2024-01/inventory_levels/adjust.json`, {
      method:  'POST',
      headers: { 'X-Shopify-Access-Token': SHOPIFY_TOKEN, 'Content-Type': 'application/json' },
      body:    JSON.stringify({ location_id: locId, inventory_item_id: inventoryItemId, available_adjustment: Number(adjustment) })
    });
    const ad = await ar.json();
    if (ad.inventory_level)
      res.json({ success: true, newStock: ad.inventory_level.available, locationId: locId });
    else
      res.json({ success: false, error: JSON.stringify(ad.errors || ad) });
  } catch(e) { res.json({ success: false, error: e.message }); }
});

// ════════════════════════════════════════════════════════════════
//  BITESPEED — WhatsApp
// ════════════════════════════════════════════════════════════════
app.post('/api/whatsapp/send', async (req, res) => {
  try {
    const fetch = require('node-fetch');
    const { phone, templateName, variables, contactName, apiKey: bk, appId: bAppId } = req.body;
    const key   = BITESPEED_API_KEY || bk;
    const appId = BITESPEED_APP_ID  || bAppId;
    if (!key) return res.json({ success: false, error: 'BITESPEED_API_KEY not set' });
    const fmt = (phone || '').replace(/\s/g,'').startsWith('+')
      ? phone.replace(/\s/g,'')
      : '+91' + phone.replace(/\D/g,'');
    const payload = {
      appId, to: fmt, type: 'template',
      template: {
        name: templateName, language: { code: 'en' },
        components: variables && variables.length
          ? [{ type: 'body', parameters: variables.map(v => ({ type: 'text', text: String(v) })) }]
          : []
      }
    };
    const r = await fetch('https://api.bitespeed.co/v1/messages', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${key}` },
      body: JSON.stringify(payload)
    });
    const d = await r.json();
    if (d.messages || d.message_id || r.ok) res.json({ success: true, result: d });
    else res.json({ success: false, error: JSON.stringify(d) });
  } catch(e) { res.json({ success: false, error: e.message }); }
});

app.post('/api/whatsapp/broadcast', async (req, res) => {
  try {
    const fetch = require('node-fetch');
    const { phones, templateName, variables } = req.body;
    if (!phones || !phones.length) return res.json({ success: false, error: 'No phones provided' });
    let ok = 0, fail = 0;
    for (const phone of phones) {
      try {
        const r = await fetch(`${SELF_URL}/api/whatsapp/send`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ phone, templateName, variables })
        });
        const d = await r.json();
        if (d.success) ok++; else fail++;
      } catch { fail++; }
    }
    res.json({ success: true, sent: ok, failed: fail, total: phones.length });
  } catch(e) { res.json({ success: false, error: e.message }); }
});

// ════════════════════════════════════════════════════════════════
//  DATA PERSISTENCE (generic key-value store)
// ════════════════════════════════════════════════════════════════
app.get('/api/data/load', (req, res) => {
  try {
    if (fs.existsSync(DATA_PATH))
      return res.json({ success: true, data: JSON.parse(fs.readFileSync(DATA_PATH, 'utf8')) });
    res.json({ success: true, data: null });
  } catch(e) { res.json({ success: false, error: e.message }); }
});

app.post('/api/data/save', (req, res) => {
  try {
    fs.writeFileSync(DATA_PATH, JSON.stringify(req.body, null, 2));
    res.json({ success: true, savedAt: new Date().toISOString() });
  } catch(e) { res.json({ success: false, error: e.message }); }
});

// ════════════════════════════════════════════════════════════════
//  HEALTH CHECK
// ════════════════════════════════════════════════════════════════
app.get('/api/health', (req, res) => {
  res.json({
    status:        'ok',
    version:       '4.0',
    store:         SHOPIFY_STORE || 'not configured',
    connected:     !!(SHOPIFY_STORE && SHOPIFY_TOKEN),   // ← what frontend checks
    shopify:       !!(SHOPIFY_STORE && SHOPIFY_TOKEN),
    velocity:      !!(VELOCITY_API_KEY || VELOCITY_USERNAME),
    bitespeed:     !!BITESPEED_API_KEY,
    cachedOrders:  ordersCache.orders.length,
    cachedAWBs:    Object.keys(velCache.shipments).length,
    lastShopifySync: ordersCache.lastSync,
    lastVelocitySync: velCache.lastSync,
    velocityWebhookUrl: `${SELF_URL}/api/webhooks/velocity`,
    ts: new Date().toISOString()
  });
});

app.get('/api/debug/orders', (req, res) => {
  const sku     = req.query.sku || '';
  const matches = [];
  ordersCache.orders.forEach(o => (o.line_items || []).forEach(i => {
    if (!sku || (i.sku || '').toLowerCase().includes(sku.toLowerCase()))
      matches.push({ order_id: o.id, date: o.created_at, name: i.name, sku: i.sku, qty: i.quantity });
  }));
  res.json({ success: true, total_cached: ordersCache.orders.length, matches_found: matches.length, matches });
});

// ── Catch-all SPA ─────────────────────────────────────────────
app.get('*', (req, res) => {
  res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate');
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// ════════════════════════════════════════════════════════════════
//  START SERVER
// ════════════════════════════════════════════════════════════════
app.listen(PORT, async () => {
  const hasVelocity = VELOCITY_API_KEY || (VELOCITY_USERNAME && VELOCITY_PASSWORD);
  console.log(`🚀 SANKI Business OS v4.0 — port ${PORT}`);
  console.log(`   Shopify:   ${SHOPIFY_STORE ? '✅ ' + SHOPIFY_STORE : '❌ not configured'}`);
  console.log(`   Velocity:  ${hasVelocity ? '✅ configured' : '⚠️  set VELOCITY_USERNAME + VELOCITY_PASSWORD in Render'}`);
  console.log(`   Bitespeed: ${BITESPEED_API_KEY ? '✅ configured' : '⚠️  key missing'}`);
  console.log(`   Velocity Webhook URL: ${SELF_URL}/api/webhooks/velocity`);

  // Load caches from disk
  loadCache();
  loadVelCache();

  // Initial syncs on startup
  if (SHOPIFY_STORE && SHOPIFY_TOKEN) {
    console.log('[sync] Starting initial Shopify sync...');
    await backgroundSync(180);
  }
  if (hasVelocity) {
    console.log('[velocity] Starting initial Velocity sync...');
    setTimeout(() => syncVelocityShipments(), 5000); // 5s after Shopify sync
  }

  // Shopify: sync every 15 min
  setInterval(() => backgroundSync(180), 15 * 60 * 1000);

  // Velocity: sync every 30 min
  setInterval(() => syncVelocityShipments(), 30 * 60 * 1000);

  // Velocity remittance: try API every 6 hours — auto-settles COD if endpoint found
  async function autoSyncRemittance() {
    try {
      const fetch  = require('node-fetch');
      const token  = await getVelToken().catch(() => null);
      if (!token) return;
      const headers = { 'Content-Type':'application/json', 'Authorization': token, 'token': token };
      const patterns = [
        `${VELOCITY_BASE}/passbook?limit=200`,
        `${VELOCITY_BASE}/billing/passbook?limit=200`,
        `${VELOCITY_BASE}/cod-remittance?limit=200`,
        `${VELOCITY_BASE}/remittance?limit=200`,
        `${VELOCITY_BASE}/settlements?limit=200`,
      ];
      for (const url of patterns) {
        try {
          const r = await fetch(url, { headers });
          const d = await r.json();
          const list = d.result || d.data || d.passbook || d.transactions || d.remittances || d.entries;
          if (r.status === 200 && Array.isArray(list) && list.length) {
            console.log(`[remittance] ✅ Auto-sync found endpoint: ${url} — ${list.length} entries`);
            // Save discovered endpoint for future use
            remittanceData.autoEndpoint = url;
            // Process entries same way as upload
            let matched = 0;
            let diskMeta = {};
            try { if (fs.existsSync(META_PATH)) diskMeta = JSON.parse(fs.readFileSync(META_PATH,'utf8')); } catch(e) {}
            list.forEach(row => {
              const awb = String(row.awb_code||row.awb||row.tracking_number||row.waybill||'').trim();
              const amt = parseFloat(row.amount||row.cod_amount||row.remittance_amount||0);
              const dt  = String(row.date||row.credit_date||row.remittance_date||'').trim();
              const utr = String(row.utr||row.transaction_id||row.reference||'').trim();
              if (!awb) return;
              const order = ordersCache.orders.find(o => (o.fulfillments||[]).some(f=>(f.tracking_number||'').trim()===awb));
              if (order) {
                if (!diskMeta[order.id]) diskMeta[order.id]={};
                Object.assign(diskMeta[order.id], { settlementStatus:'settled', settlementAmount:amt||diskMeta[order.id].settlementAmount, settlementDate:dt||diskMeta[order.id].settlementDate, settlementUTR:utr||diskMeta[order.id].settlementUTR, customerPayment:'paid' });
                matched++;
              }
            });
            if (matched) { fs.writeFileSync(META_PATH, JSON.stringify(diskMeta,null,2)); console.log(`[remittance] Auto-settled ${matched} orders`); }
            saveRemittance();
            return; // found working endpoint, stop trying
          }
        } catch(e) { /* try next */ }
      }
    } catch(e) { console.log('[remittance] Auto-sync skipped:', e.message); }
  }
  setInterval(autoSyncRemittance, 6 * 60 * 60 * 1000); // every 6 hours
  setTimeout(autoSyncRemittance, 30 * 1000); // also try 30s after startup

  // Self-ping every 14 min to keep Render free tier alive
  setInterval(async () => {
    try { const fetch = require('node-fetch'); await fetch(`${SELF_URL}/api/health`); }
    catch(e) { /* silent */ }
  }, 14 * 60 * 1000);
});
