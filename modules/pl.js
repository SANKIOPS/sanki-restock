// ═══════════════════════════════════════════════════════════════
// Profit & Loss — in-app accounting (Step 1: Sales + COGS → Gross Profit)
//
// Replaces the "Till Date P&L" tab of the founder's Google Sheet. Reads the
// Shopify order mirror (orders.json, already normalized + channel-tagged by
// orders.js) and lays sales out in TWO channels — POS (physical store) and
// WEBSITE (online) — plus a Combined column, in the exact row order the sheet
// uses:  Sales → COGS → Gross Profit → Fixed Cost → Variable Cost → Marketing
//        → Net Profit.
//
// Step 1 fills the TOP half (Sales, COGS, Gross Profit). COGS uses a
// per-channel percentage (the same method the sheet uses today: POS ~40.8%,
// Web ~44%), editable in the UI so the numbers reconcile against the sheet.
// Fixed Cost / Variable Cost / Marketing / Salary get real inputs in later
// steps; for now they return 0 so the skeleton is visible end to end.
// ═══════════════════════════════════════════════════════════════
const express = require('express');
const fs = require('fs');
const path = require('path');
// Expenses module feeds the Fixed / Running / Variable / Marketing rows. Only
// approved BUSINESS (A3) expenses come through; see expenses.summaryForPL.
let expenseSummary;
try { expenseSummary = require('./expenses').summaryForPL; } catch { expenseSummary = null; }

const router = express.Router();

const DATA_DIR = process.env.DATA_PATH
  ? path.dirname(process.env.DATA_PATH)
  : path.join(__dirname, '..');
const ORDERS_PATH = process.env.ORDERS_PATH || path.join(DATA_DIR, 'orders.json');
const PROC_PATH = path.join(DATA_DIR, 'procurement.json');
const PL_PATH = path.join(DATA_DIR, 'pl.json');

// GST is NOT stored per order in Shopify (SANKI prices are GST-INCLUSIVE, so
// total_tax comes back ~0). To show a real ex-GST figure we back-calculate GST
// out of the inclusive MRP at the statutory tier — the same rates the app's
// procurement module uses: < ₹2500/unit → 5%, ≥ ₹2500 → 18%.
function gstRates() {
  try {
    const s = JSON.parse(fs.readFileSync(PROC_PATH, 'utf8'));
    const g = (s && s.settings) || {};
    return {
      low: numOr(g.gstLow, 0.05),
      high: numOr(g.gstHigh, 0.18),
      threshold: numOr(g.gstLowThreshold, 2500)
    };
  } catch { return { low: 0.05, high: 0.18, threshold: 2500 }; }
}
// GST contained in a GST-inclusive amount at a given rate: amt - amt/(1+rate).
function gstInInclusive(inclAmount, unitPrice, rates) {
  const rate = unitPrice >= rates.threshold ? rates.high : rates.low;
  return inclAmount - inclAmount / (1 + rate);
}

const CHANNELS = ['POS', 'Website'];

// ── Settings store (COGS %, editable) ────────────────────────────
// Defaults come straight from the founder's July sheet so the app's first P&L
// lines up with what they already trust.
const DEFAULTS = {
  cogsPct: { POS: 0.408, Website: 0.44 }
};
// Unit-economics assumptions. All default 0 = "not logged yet" — the founder
// fills these when they have the numbers, and the waterfall lights up. Kept in
// the SAME pl.json under `ue` so one file holds all accounting settings.
const UE_DEFAULTS = {
  shipPerOrder: { POS: 0, Website: 0 },  // forward courier ₹/order (POS usually 0)
  packPerOrder: 0,                       // packaging ₹/order
  gatewayPct: 0,                         // payment/COD gateway fee, fraction of order value
  rtoPct: 0                              // RTO/return rate, fraction (drives the return provision)
};
// Read the whole pl.json blob so writers can merge without clobbering siblings
// (cogsPct and ue live side by side).
function loadRaw() { try { return JSON.parse(fs.readFileSync(PL_PATH, 'utf8')) || {}; } catch { return {}; } }
function saveRaw(o) {
  const tmp = PL_PATH + '.tmp-' + process.pid + '-' + Date.now();
  fs.writeFileSync(tmp, JSON.stringify(o));
  fs.renameSync(tmp, PL_PATH);
}
function loadSettings() {
  const s = loadRaw();
  return {
    cogsPct: {
      POS: numOr(s.cogsPct && s.cogsPct.POS, DEFAULTS.cogsPct.POS),
      Website: numOr(s.cogsPct && s.cogsPct.Website, DEFAULTS.cogsPct.Website)
    }
  };
}
function saveSettings(s) { const raw = loadRaw(); raw.cogsPct = s.cogsPct; saveRaw(raw); }
function loadUE() {
  const u = loadRaw().ue || {};
  return {
    shipPerOrder: {
      POS: numOr(u.shipPerOrder && u.shipPerOrder.POS, 0),
      Website: numOr(u.shipPerOrder && u.shipPerOrder.Website, 0)
    },
    packPerOrder: numOr(u.packPerOrder, 0),
    gatewayPct: numOr(u.gatewayPct, 0),
    rtoPct: numOr(u.rtoPct, 0)
  };
}
function saveUE(u) { const raw = loadRaw(); raw.ue = u; saveRaw(raw); }
function numOr(v, d) { const n = parseFloat(v); return isNaN(n) ? d : n; }

// ── Orders (read-only mirror) ────────────────────────────────────
// Returns { orders:[...], dispatch:{id:{packingStatus,...}} } so we can drop
// RTO / cancelled shipments from revenue.
function loadOrders() {
  try {
    const s = JSON.parse(fs.readFileSync(ORDERS_PATH, 'utf8'));
    return { orders: s && s.orders ? Object.values(s.orders) : [], dispatch: (s && s.dispatch) || {} };
  } catch { return { orders: [], dispatch: {} }; }
}

// ── Actual per-SKU landed cost map (from Purchases) ──────────────
// The founder wants COGS built on ACTUAL landed cost, not a tentative %. The
// real cost per piece lives in the Purchases module (procurement.json), where
// each PO line carries { sku, qty, landed }. We flatten every PO line into a
// SKU→landed map. When the same SKU appears on more than one PO, the MOST
// RECENT purchase wins (latest createdAt), so COGS reflects current cost.
//
// Coverage caveat: this only knows the cost of SKUs bought THROUGH the app.
// SKUs sold but never purchased in-app (older stock, Casuals, manual) have no
// landed cost here — those fall back to the editable channel % so the P&L is
// still complete. The summary reports how much of COGS is actual vs estimated.
//
// Landed cost is NOT persisted on the PO line — Purchases stores only the raw
// inputs (perPcsYuan, weightGrams) and computes landed on the fly. So we
// replicate that formula here (must stay in sync with procurement.js
// landedCost()):
//   china: landed = perPcsYuan × exRate + weightGrams × freightPerGram
//   india: landed = perPcsYuan(₹) + transportTotal / (pieces in the PO)
// exRate / freightPerGram are captured per-PO (fallback to current settings).
function poLanded(po, line, settings, transportPerPc) {
  const india = po.origin === 'india';
  const exRate = num(po.exRate) || num(settings.exRate);
  const freightPerGram = num(po.freightPerGram) || num(settings.freightPerGram);
  const inrValue = india ? num(line.perPcsYuan) : num(line.perPcsYuan) * exRate;
  const freightPerPc = india ? transportPerPc : num(line.weightGrams) * freightPerGram;
  return inrValue + freightPerPc;
}
function buildCostMap() {
  const map = {};
  try {
    const s = JSON.parse(fs.readFileSync(PROC_PATH, 'utf8'));
    const settings = (s && s.settings) || {};
    const pos = (s && s.pos) || {};
    // Sort POs oldest→newest so the newest overwrites (most-recent cost wins).
    Object.values(pos)
      .sort((a, b) => String(a && a.createdAt || '').localeCompare(String(b && b.createdAt || '')))
      .forEach(po => {
        if (!po) return;
        const lines = po.lines || [];
        const totalQty = lines.reduce((n, l) => n + num(l && l.qty), 0);
        const transportPerPc = (po.origin === 'india' && totalQty > 0)
          ? num(po.transportTotal) / totalQty : 0;
        lines.forEach(l => {
          const sku = (l && l.sku ? String(l.sku) : '').toUpperCase().trim();
          if (!sku) return;
          const landed = poLanded(po, l, settings, transportPerPc);
          if (landed > 0) map[sku] = landed;
        });
      });
  } catch { /* no purchases yet → empty map, pure % fallback */ }
  return map;
}

// ── Date defaults: current month in IST ──────────────────────────
function istMonthRange() {
  // IST = UTC+5:30. Shift now into IST, take year-month, build first→last day.
  const nowIst = new Date(Date.now() + 5.5 * 3600 * 1000);
  const y = nowIst.getUTCFullYear();
  const m = nowIst.getUTCMonth(); // 0-based
  const pad = n => String(n).padStart(2, '0');
  const first = `${y}-${pad(m + 1)}-01`;
  const lastDay = new Date(Date.UTC(y, m + 1, 0)).getUTCDate();
  const last = `${y}-${pad(m + 1)}-${pad(lastDay)}`;
  return { from: first, to: last };
}

function round0(n) { return Math.round(n); }

// ── P&L aggregation (shared by /summary and /unit-economics) ─────
// Runs the order loop once and returns the full channel/combined row-set plus
// settings + meta. Both the P&L view and the per-order unit-economics view read
// from this, so COGS/GST/refund logic never diverges between the two.
function aggregate(from, to) {
  const settings = loadSettings();

  const store = loadOrders();
  const dispatch = store.dispatch;
  const rates = gstRates();
  const costMap = buildCostMap();          // SKU → actual landed cost (per pc)
  // Blank per-channel accumulator. salesIncl = collected incl GST (net of
  // refunds/RTO); gst = the GST portion; units/orders/returned counters.
  // actualCogs = Σ (qty × landed) for lines whose SKU has a known landed cost;
  // covered/uncovered Units & Sales split so we can (a) apply the % fallback to
  // only the uncovered slice and (b) report COGS coverage.
  const blank = () => ({
    salesIncl: 0, gst: 0, units: 0, orders: 0, refunds: 0, returned: 0,
    actualCogs: 0, coveredUnits: 0, uncoveredUnits: 0, coveredSales: 0, uncoveredSales: 0
  });
  const acc = { POS: blank(), Website: blank() };

  store.orders.forEach(o => {
    if (!o || o.cancelledAt) return;                 // skip cancelled orders
    const d = (o.createdAt || '').slice(0, 10);
    if (!d || d < from || d > to) return;            // date window (IST-naive, matches orders ledger)
    const ch = o.channel === 'POS' ? 'POS' : 'Website';

    // RTO / dispatch-cancelled = goods came back → no revenue realised.
    const dstat = (dispatch[o.id] && dispatch[o.id].packingStatus) || '';
    if (dstat === 'rto' || dstat === 'cancelled') { acc[ch].returned += 1; return; }

    const total = num(o.total);                      // collected incl GST + shipping
    const refund = Math.min(num(o.refundAmount), total);
    const netCollected = total - refund;             // (2) subtract refunds
    const factor = total > 0 ? netCollected / total : 0;
    // De-gross GST from each inclusive line (Shopify's total_tax is ~0 here),
    // then scale by the kept fraction so refunds pull GST down too. In the same
    // pass, split each line into ACTUAL-cost (SKU landed cost known) vs
    // ESTIMATED (unknown → % fallback), and total the real COGS.
    let gstGross = 0;
    (o.lineItems || []).forEach(li => {
      const qty = num(li.qty);
      const lineTotal = num(li.price) * qty;
      if (lineTotal > 0) gstGross += gstInInclusive(lineTotal, num(li.price), rates);
      const sku = (li.sku ? String(li.sku) : '').toUpperCase().trim();
      const landed = sku ? costMap[sku] : undefined;
      if (landed != null) {
        acc[ch].actualCogs += landed * qty * factor;   // real cost, refund-scaled
        acc[ch].coveredUnits += qty;
        acc[ch].coveredSales += lineTotal * factor;
      } else {
        acc[ch].uncoveredUnits += qty;
        acc[ch].uncoveredSales += lineTotal * factor;
      }
    });
    const gst = gstGross * factor;

    acc[ch].salesIncl += netCollected;
    acc[ch].gst += gst;
    acc[ch].units += num(o.itemCount);
    acc[ch].orders += 1;
    if (refund > 0) acc[ch].refunds += refund;
  });

  // ── Expenses → Fixed / Running / Variable / Marketing rows ───────
  // Pull approved business (A3) expenses for the range. Channel-tagged ones go
  // direct; SHARED ones are split between POS and Website by REVENUE share (a
  // simple, defensible default — floor-space/order-count keys come later).
  const exp = expenseSummary ? expenseSummary(from, to) : null;
  const revTotal = acc.POS.salesIncl + acc.Website.salesIncl;
  const posShare = revTotal > 0 ? acc.POS.salesIncl / revTotal : 0.5;
  function expFor(ch, kind) {
    if (!exp) return 0;
    const share = ch === 'POS' ? posShare : (1 - posShare);
    return (exp[ch][kind] || 0) + (exp.Shared[kind] || 0) * share;
  }

  // Build a full P&L row-set per channel + combined. COGS is now ACTUAL where
  // the SKU's landed cost is known (from Purchases), and falls back to the
  // editable channel % for the uncovered slice only. salesExGst is shown
  // alongside for the net-of-GST view they asked for.
  const rows = a => {
    const cogsPct = a.cogsPct;
    const estCogs = a.uncoveredSales * cogsPct;    // % applied to uncovered slice only
    const cogs = a.actualCogs + estCogs;
    const grossProfit = a.salesIncl - cogs;
    const coveredCost = a.coveredSales + a.uncoveredSales;
    const fixedCost = a.fixedCost, runningCost = a.runningCost, variableCost = a.variableCost, marketing = a.marketing;
    const netProfit = grossProfit - fixedCost - runningCost - variableCost - marketing;
    return {
      sales: round0(a.salesIncl),            // headline: incl GST, net of returns
      salesExGst: round0(a.salesIncl - a.gst),
      gst: round0(a.gst),
      units: a.units, orders: a.orders,
      refunds: round0(a.refunds), returned: a.returned,
      cogsPct,
      cogs: round0(cogs),
      cogsActual: round0(a.actualCogs),
      cogsEstimated: round0(estCogs),
      cogsEffPct: a.salesIncl ? cogs / a.salesIncl : 0,
      grossProfit: round0(grossProfit),
      grossMargin: a.salesIncl ? grossProfit / a.salesIncl : 0,
      coverage: {
        actualUnits: a.coveredUnits,
        estimatedUnits: a.uncoveredUnits,
        unitPct: (a.coveredUnits + a.uncoveredUnits) ? a.coveredUnits / (a.coveredUnits + a.uncoveredUnits) : 0,
        salesPct: coveredCost ? a.coveredSales / coveredCost : 0
      },
      fixedCost: round0(fixedCost), runningCost: round0(runningCost),
      variableCost: round0(variableCost), marketing: round0(marketing),
      netProfit: round0(netProfit)
    };
  };
  const channels = {};
  CHANNELS.forEach(ch => {
    channels[ch] = rows(Object.assign({
      cogsPct: settings.cogsPct[ch],
      fixedCost: expFor(ch, 'fixed'), runningCost: expFor(ch, 'running'),
      variableCost: expFor(ch, 'variable'), marketing: expFor(ch, 'marketing')
    }, acc[ch]));
  });

  // Combined = sum of the two channels (actual COGS + estimated fallback).
  const cIncl = acc.POS.salesIncl + acc.Website.salesIncl;
  const cGst = acc.POS.gst + acc.Website.gst;
  const cActual = acc.POS.actualCogs + acc.Website.actualCogs;
  const cEst = channels.POS.cogsEstimated + channels.Website.cogsEstimated;
  const cCogs = cActual + cEst;
  const cGross = cIncl - cCogs;
  const cCoveredUnits = acc.POS.coveredUnits + acc.Website.coveredUnits;
  const cEstUnits = acc.POS.uncoveredUnits + acc.Website.uncoveredUnits;
  const cCoveredSales = acc.POS.coveredSales + acc.Website.coveredSales;
  const cCostBase = cCoveredSales + acc.POS.uncoveredSales + acc.Website.uncoveredSales;
  const combined = {
    sales: round0(cIncl),
    salesExGst: round0(cIncl - cGst),
    gst: round0(cGst),
    units: channels.POS.units + channels.Website.units,
    orders: channels.POS.orders + channels.Website.orders,
    refunds: channels.POS.refunds + channels.Website.refunds,
    returned: channels.POS.returned + channels.Website.returned,
    cogsPct: cIncl ? cCogs / cIncl : 0,
    cogs: round0(cCogs),
    cogsActual: round0(cActual),
    cogsEstimated: round0(cEst),
    cogsEffPct: cIncl ? cCogs / cIncl : 0,
    grossProfit: round0(cGross),
    grossMargin: cIncl ? cGross / cIncl : 0,
    coverage: {
      actualUnits: cCoveredUnits,
      estimatedUnits: cEstUnits,
      unitPct: (cCoveredUnits + cEstUnits) ? cCoveredUnits / (cCoveredUnits + cEstUnits) : 0,
      salesPct: cCostBase ? cCoveredSales / cCostBase : 0
    },
    fixedCost: channels.POS.fixedCost + channels.Website.fixedCost,
    runningCost: channels.POS.runningCost + channels.Website.runningCost,
    variableCost: channels.POS.variableCost + channels.Website.variableCost,
    marketing: channels.POS.marketing + channels.Website.marketing,
    netProfit: round0(cGross) - (channels.POS.fixedCost + channels.Website.fixedCost)
      - (channels.POS.runningCost + channels.Website.runningCost)
      - (channels.POS.variableCost + channels.Website.variableCost)
      - (channels.POS.marketing + channels.Website.marketing)
  };

  return {
    range: { from, to },
    channels,
    combined,
    settings,
    meta: {
      ordersCounted: combined.orders,
      returnedOrders: combined.returned,
      refundsTotal: combined.refunds,
      cogsMethod: 'actual+estimate',
      cogsCoverage: combined.coverage,
      knownSkus: Object.keys(costMap).length,
      note: 'Sales = collected incl GST, net of refunds and RTO/returned orders (ex-GST shown alongside). COGS is ACTUAL landed cost per SKU where the item was bought through Purchases; the editable channel % fills the rest. Coverage shows how much is actual vs estimated. Fixed cost, expenses, marketing & salary arrive in the next steps.'
    }
  };
}

// GET /api/pl/summary?from=YYYY-MM-DD&to=YYYY-MM-DD
router.get('/api/pl/summary', (req, res) => {
  const def = istMonthRange();
  const from = (req.query.from || def.from).toString();
  const to = (req.query.to || def.to).toString();
  res.json(Object.assign({ success: true }, aggregate(from, to)));
});

// POST /api/pl/settings  { cogsPct: { POS: 0.41, Website: 0.44 } }
// Accepts either a fraction (0.41) or a percent number (41) for convenience.
router.post('/api/pl/settings', (req, res) => {
  const b = req.body || {};
  const s = loadSettings();
  if (b.cogsPct) {
    CHANNELS.forEach(ch => {
      if (b.cogsPct[ch] != null) {
        let v = parseFloat(b.cogsPct[ch]);
        if (!isNaN(v)) { if (v > 1) v = v / 100; s.cogsPct[ch] = Math.max(0, Math.min(v, 1)); }
      }
    });
  }
  saveSettings(s);
  res.json({ success: true, settings: s });
});

// ── Unit economics: per-order contribution waterfall ─────────────
// Turns the P&L totals into a PER-ORDER story so the founder can see whether a
// single sale actually makes money after everything. Each channel:
//   Avg selling price (incl GST, net of returns)
//     − GST
//     = Net revenue (ex GST)
//     − COGS (landed cost)
//     = Gross contribution
//     − Forward shipping   (optional input, ₹/order)
//     − Packaging          (optional input, ₹/order)
//     − Payment/COD fee    (optional input, % of order value)
//     − Return provision   (optional input, RTO% × forward+reverse shipping)
//     − CAC                (real: marketing spend ÷ orders)
//     − Allocated overhead (real: fixed+running+other-variable expenses ÷ orders)
//     = Contribution per order
// The four "optional input" lines default to 0 until the founder logs them, so
// the waterfall is honest about what's still an estimate.
function ueRound(n) { return Math.round(n * 100) / 100; }
router.get('/api/pl/unit-economics', (req, res) => {
  const def = istMonthRange();
  const from = (req.query.from || def.from).toString();
  const to = (req.query.to || def.to).toString();
  const agg = aggregate(from, to);
  const ue = loadUE();

  // Order-weighted blended shipping for the Combined column.
  const posOrders = agg.channels.POS.orders, webOrders = agg.channels.Website.orders;
  const totOrders = posOrders + webOrders;
  const blendedShip = totOrders
    ? (posOrders * ue.shipPerOrder.POS + webOrders * ue.shipPerOrder.Website) / totOrders
    : 0;

  function perOrder(chKey, data, shipPerOrder) {
    const n = data.orders || 0;
    const per = v => (n ? v / n : 0);
    const avgSale = per(data.sales);            // incl GST, net of returns
    const gst = per(data.gst);
    const cogs = per(data.cogs);
    const netRevenue = avgSale - gst;
    const grossContribution = netRevenue - cogs;
    const ship = shipPerOrder;
    const pack = ue.packPerOrder;
    const gatewayFee = ue.gatewayPct * avgSale;
    // Return provision: on an RTO you eat forward + reverse courier. Amortised
    // across ALL orders at the return rate. Packaging is also lost on a return.
    const returnProvision = ue.rtoPct * (ship * 2 + pack);
    const cac = per(data.marketing);
    const overhead = per((data.fixedCost || 0) + (data.runningCost || 0) + (data.variableCost || 0));
    const contribution = grossContribution - ship - pack - gatewayFee - returnProvision - cac - overhead;
    const r = {
      orders: n,
      avgSale: ueRound(avgSale), gst: ueRound(gst), netRevenue: ueRound(netRevenue),
      cogs: ueRound(cogs), grossContribution: ueRound(grossContribution),
      ship: ueRound(ship), pack: ueRound(pack), gatewayFee: ueRound(gatewayFee),
      returnProvision: ueRound(returnProvision), cac: ueRound(cac), overhead: ueRound(overhead),
      contribution: ueRound(contribution),
      contributionPct: avgSale ? contribution / avgSale : 0
    };
    return r;
  }

  res.json({
    success: true,
    range: { from, to },
    ue,
    channels: {
      POS: perOrder('POS', agg.channels.POS, ue.shipPerOrder.POS),
      Website: perOrder('Website', agg.channels.Website, ue.shipPerOrder.Website)
    },
    combined: perOrder('Combined', agg.combined, blendedShip),
    meta: {
      note: 'Per-order averages over the selected range. CAC and allocated overhead are REAL (from logged marketing + expenses ÷ orders). Forward shipping, packaging, payment/COD fee % and RTO % are your assumptions — edit them below; each stays ₹0 until you log it. Contribution = what one average order leaves after everything.'
    }
  });
});

// POST /api/pl/ue-settings — save unit-economics assumptions.
// gatewayPct & rtoPct accept a fraction (0.02) or a percent number (2).
router.post('/api/pl/ue-settings', (req, res) => {
  const b = req.body || {};
  const u = loadUE();
  if (b.shipPerOrder) {
    CHANNELS.forEach(ch => {
      if (b.shipPerOrder[ch] != null && !isNaN(parseFloat(b.shipPerOrder[ch])))
        u.shipPerOrder[ch] = Math.max(0, parseFloat(b.shipPerOrder[ch]));
    });
  }
  if (b.packPerOrder != null && !isNaN(parseFloat(b.packPerOrder)))
    u.packPerOrder = Math.max(0, parseFloat(b.packPerOrder));
  ['gatewayPct', 'rtoPct'].forEach(k => {
    if (b[k] != null && !isNaN(parseFloat(b[k]))) {
      let v = parseFloat(b[k]);
      if (v > 1) v = v / 100;                   // 2 → 0.02
      u[k] = Math.max(0, Math.min(v, 1));
    }
  });
  saveUE(u);
  res.json({ success: true, ue: u });
});

function num(v) { const n = parseFloat(v); return isNaN(n) ? 0 : n; }

module.exports = { router };
