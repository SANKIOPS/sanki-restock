'use strict';

// Read-only management report. COGS follows the sold SKU; all other costs
// follow actual approved payments. Never write to accounting/order stores.
const purchaseCosts = require('../public/purchase-costs');
const { velocityCompletion } = require('./pnl-delivery');
const { resolveOrder, manualHash, resolveExpenseConfirmation } = require('./pnl-line-corrections');
const START = '2026-08-22';
const number = value => Number.isFinite(Number(value)) ? Number(value) : 0;
const money = value => Math.round((number(value) + Number.EPSILON) * 100) / 100;
const present = value => value !== null && value !== undefined && value !== '' && Number.isFinite(Number(value));
const skuKey = value => String(value || '').trim().toUpperCase();
const list = value => Array.isArray(value) ? value : Object.values(value || {});
const business = value => ['SANKI', 'A3'].includes(String(value || '').toUpperCase());
function validDate(value) {
  return /^\d{4}-\d{2}-\d{2}$/.test(String(value)) && !Number.isNaN(Date.parse(value + 'T00:00:00Z')) && new Date(value + 'T00:00:00Z').toISOString().slice(0, 10) === value;
}
function day(value) {
  if (!value) return '';
  const text = String(value);
  if (validDate(text)) return text;
  const timestamp = Date.parse(text);
  return Number.isFinite(timestamp) ? new Date(timestamp + 19800000).toISOString().slice(0, 10) : '';
}
function channel(value) {
  const text = String(value || '').toLowerCase();
  return /pos|walk.in/.test(text) ? 'POS' : /website|online|web/.test(text) ? 'Website' : 'Other';
}
const gstRate = unitValue => number(unitValue) <= 2500 ? 0.05 : 0.18;

// User-configured management Results policy: choose the slab from the final
// per-piece selling amount after discount, then extract inclusive GST.
// This is report-only, not a tax setting or statutory invoice calculation.
function lineTax(line, taxesIncluded = true) {
  const qty = number(line.qty), discount = number(line.discount);
  const signedBase=number(line.price ?? line.unitPrice)*qty-discount,base = Math.max(0,signedBase);
  const recorded = Array.isArray(line.taxLines) && line.taxLines.length ? money(line.taxLines.reduce((sum, tax) => sum + number(tax.price), 0)) : null;
  if(line.pnlKind==='rounding')return {gross:money(signedBase),sourceGross:money(signedBase+(taxesIncluded?0:number(recorded))),tax:0,recorded,calculated:0,rate:0,status:'User-confirmed rounding: no product GST or COGS',valid:qty>0};
  if(line.pnlKind==='shipping') {
    const confirmedZero=line.pnlShippingTaxZero===true,known=confirmedZero || recorded!==null || line.taxable===false, tax=confirmedZero?0:known ? recorded || 0 : null;
    const gross=money(base + (taxesIncluded ? 0 : number(tax)));
    return {gross,sourceGross:confirmedZero?money(base+(taxesIncluded?0:number(recorded))):gross,tax,recorded:confirmedZero?recorded:tax,calculated:tax,rate:confirmedZero?0:null,status:confirmedZero?'User-confirmed tax-free shipping (Results only)':known?'Shipping fee: source invoice GST retained':'Shipping fee GST evidence missing',valid:known&&qty>0};
  }
  if (!(qty > 0)) return { gross: money(base), tax: null, rate: null, recorded, status: 'Missing quantity', valid: false };
  // Confirmed non-garment exception. Match only an explicit perfume product
  // title; do not infer rates for unnamed custom items or overwrite invoices.
  const perfume = /\bperfumes?\b/i.test(String(line.title || ''));
  const rate = perfume ? 0.28 : gstRate(base / qty);
  const calculated = money(taxesIncluded ? base - base / (1 + rate) : base * rate);
  // Management Results only: invoice rates, recorded tax and Shopify's taxable
  // flag never override the configured garment calculation. sourceGross is
  // reconciliation evidence, not a tax-rate input.
  return { gross: money(base + (taxesIncluded ? 0 : calculated)), sourceGross: taxesIncluded ? money(base) : recorded === null ? null : money(base + recorded), tax: calculated, rate, recorded, calculated, difference: recorded === null ? null : money(calculated - recorded), status: 'Results configured GST', valid: true };
}

function completion(order, dispatch = {}, velocity = {}) {
  if (order.pnlCancelled) return null;
  if (order.pnlDeliveryDate) return {date:order.pnlDeliveryDate,evidence:'User-confirmed delivery (Results only)'};
  const explicit = day(order.completedAt || order.deliveredAt || dispatch.deliveredAt);
  if (explicit) return { date: explicit, evidence: 'Recorded completion date' };
  if (order.cancelledAt || ['cancelled', 'rto'].includes(dispatch.packingStatus)) return null;
  if (order.channel === 'POS' && ['paid', 'partially_refunded', 'refunded'].includes(order.financialStatus)) {
    return { date: day(order.processedAt || order.createdAt), evidence: 'Completed POS sale' };
  }
  const carrier = velocityCompletion(order, dispatch, velocity);
  if (carrier) return carrier;
  if (dispatch.packingStatus === 'delivered' || order.deliveryComplete) {
    return { date: '', evidence: 'Delivery date missing' };
  }
  return null;
}
function row(values) {
  return Object.assign({ id: '', date: '', kind: '', category: '', channel: 'Shared', particulars: '', reference: '', account: '', method: '', proof: '', source: '', grossSales: 0, discounts: 0, returns: 0, gst: 0, netSales: 0, otherIncome: 0, cogs: 0, expenses: 0, taxComplete: true, costComplete: true, details: [] }, values);
}
function costLots(purchases = {}, opening = {}) {
  const lots = [];
  for (const entry of list(opening.lots)) {
    if (entry.verified !== true || !skuKey(entry.sku) || !(number(entry.qty) > 0) || !present(entry.unitCost) || number(entry.unitCost) < 0) continue;
    lots.push({ id: entry.id || 'OPEN/' + skuKey(entry.sku), sku: skuKey(entry.sku), date: day(entry.date) || START, qty: number(entry.qty), cost: number(entry.unitCost), opening: true });
  }
  for (const po of list(purchases.pos)) {
    if (po.accountingExcluded || !['received', 'awaiting_approval', 'posting_partial', 'posted'].includes(po.status)) continue;
    const date = day(po.dateReceive || po.receivedAt || po.postedAt);
    if (!date || date < START) continue; // Pre-boundary balances require verified opening quantities.
    const costs = purchaseCosts(po, purchases.settings);
    (po.lines || []).forEach((line, index) => {
      const cost = present(line.landedUnitCost) ? number(line.landedUnitCost) : costs.lines[index]?.perPiece;
      const qty = present(line.receivedQty) ? number(line.receivedQty) : number(line.qty);
      if (!skuKey(line.sku) || !(qty > 0) || !(cost > 0) || line.accountingExcluded) return;
      lots.push({ id: po.id + '/' + (line.id || index), sku: skuKey(line.sku), date, qty, cost, purchaseId: po.id });
    });
  }
  return lots.sort((a, b) => a.date.localeCompare(b.date) || a.id.localeCompare(b.id));
}

function buildReport(sources = {}, options = {}) {
  if (options.from && !validDate(options.from) || options.to && !validDate(options.to)) throw Object.assign(new Error('Choose a valid date range.'), { status: 400 });
  const start = validDate(options.start) && options.start > START ? options.start : START;
  const from = options.from < start ? start : options.from || start;
  const to = options.to || day(new Date().toISOString());
  if (!validDate(from) || !validDate(to) || from > to) throw Object.assign(new Error('Choose a valid date range on or after 22 August 2026.'), { status: 400 });
  const selectedChannel = options.channel || 'All';
  if (!['All', 'POS', 'Website', 'Other'].includes(selectedChannel)) throw Object.assign(new Error('Choose a valid sales channel.'), { status: 400 });
  const events = [], warnings = [], tasks = [], taxRows = [], collections = [];
  const books = sources.expenses || {}, orderStore = sources.orders || {}, seen = new Set();
  const warning = (reference, message, date = '', scope = 'data', ch = 'Shared') => warnings.push({ reference, message, date, scope, channel: ch });
  const add = entry => { if (!seen.has(entry.id) && entry.date && entry.date >= start && entry.date <= to) { seen.add(entry.id); events.push(entry); } };
  const lots = costLots(sources.purchases, sources.opening).map(lot => ({ ...lot, remaining: lot.qty }));
  const allocations = new Map();
  const taxAllocations = new Map();
  const shopifyCostMode = Object.prototype.hasOwnProperty.call(sources, 'shopifyCosts');
  const shopifyCosts = new Map(), duplicateCostSkus = new Set();
  for (const item of sources.shopifyCosts?.items || []) {
    if (!item || typeof item !== 'object') continue;
    const sku = skuKey(item.sku);
    if (!sku) continue;
    const previous = shopifyCosts.get(sku);
    if (previous && (!item.id || previous.id !== item.id || number(previous.unitCost) !== number(item.unitCost))) duplicateCostSkus.add(sku);
    shopifyCosts.set(sku, item);
  }
  const allocate = (sku, qty, date, reference, ch, inventoryItemId) => {
    if (shopifyCostMode) {
      const identityMatches=inventoryItemId?(sources.shopifyCosts?.items||[]).filter(i=>i&&String(i.id)===String(inventoryItemId)&&skuKey(i.sku)===sku):[];
      const identityValid=identityMatches.length>0&&identityMatches.every(i=>number(i.unitCost)===number(identityMatches[0].unitCost)&&(i.unitCostCurrency||i.currency||'INR')==='INR');
      const item = inventoryItemId?(identityValid?identityMatches[0]:null):shopifyCosts.get(sku);
      const currency = item?.unitCostCurrency || item?.currency || 'INR';
      const valid = !!sku && (inventoryItemId?identityValid:!duplicateCostSkus.has(sku)) && present(item?.unitCost) && number(item.unitCost) > 0 && currency === 'INR';
      if (!valid) {
        const reason = inventoryItemId ? 'Selected Shopify item identity/cost is no longer valid' : duplicateCostSkus.has(sku) ? 'Duplicate Shopify SKU' : currency !== 'INR' ? 'Non-INR Shopify cost' : 'Shopify Cost per item missing';
        warning(reference, reason + ' for ' + (sku || 'unmapped SKU') + ': ' + qty + ' unit(s). Profit is unavailable until resolved.', date, 'cogs', ch);
        return { parts: [], qty, missing: qty, cost: 0 };
      }
      // Current Shopify cost is the management Results basis. Never invent
      // opening quantities or consume today's stock to price past sales.
      // Refunds reverse this sale's exact allocation, not a new estimate.
      const cost = number(item.unitCost);
      return { parts: [{ lotId: item.id || 'SHOPIFY/' + sku, purchaseId: 'Shopify Cost per item', sku, inventoryItemId:item.id, qty, unitCost: money(cost), amount: money(qty * cost), exactCost: cost, returned: 0, costAsOf: sources.shopifyCosts.at || null }], qty, missing: 0, cost: money(qty * cost) };
    }
    if(inventoryItemId){warning(reference,'Selected Shopify item cost source is unavailable; no inventory or average fallback was used.',date,'cogs',ch);return {parts:[],qty,missing:qty,cost:0};}
    let remaining = qty; const parts = [];
    for (const lot of lots) {
      if (lot.sku !== sku || lot.date > date || lot.remaining <= 0) continue;
      const used = Math.min(lot.remaining, remaining);
      lot.remaining -= used; remaining -= used;
      parts.push({ lotId: lot.id, purchaseId: lot.purchaseId || 'Opening stock', qty: used, unitCost: money(lot.cost), amount: money(used * lot.cost), exactCost: lot.cost, returned: 0 });
      if (remaining <= 0) break;
    }
    if (remaining > 0) warning(reference, 'Verified stock/cost missing for ' + (sku || 'unmapped SKU') + ': ' + remaining + ' unit(s). Profit is unavailable until resolved.', date, 'cogs', ch);
    return { parts, qty, missing: remaining, cost: money(parts.reduce((sum, part) => sum + part.qty * part.exactCost, 0)) };
  };
  const returnCost = (key, qty, date, reference, ch) => {
    const allocation = allocations.get(key);
    if (!allocation) return { cost: 0, complete: false };
    // A source line can represent several different physical products. A
    // partial return cannot identify which component came back, so never
    // consume whichever component happens to be listed first.
    if(allocation.bundle){
      if(qty!==allocation.qty||allocation.missing>0||allocation.parts.some(p=>p.returned>0)){warning(reference,'Partial or incomplete bundle return requires verified original costs and the exact returned SKU components; no component cost was guessed.',date,'cogs',ch);return {cost:0,complete:false};}
      for(const part of allocation.parts){part.returned=part.qty;lots.push({id:reference+'/'+part.lotId,sku:part.sku||allocation.sku,date,qty:part.qty,remaining:part.qty,cost:part.exactCost,purchaseId:part.purchaseId});}
      lots.sort((a,b)=>a.date.localeCompare(b.date)||a.id.localeCompare(b.id));
      return {cost:allocation.cost,complete:true};
    }
    let remaining = qty, cost = 0;
    for (const part of allocation.parts) {
      const used = Math.min(remaining, part.qty - part.returned);
      if (!(used > 0)) continue;
      part.returned += used; remaining -= used; cost += used * part.exactCost;
      lots.push({ id: reference + '/' + part.lotId, sku: allocation.sku, date, qty: used, remaining: used, cost: part.exactCost, purchaseId: part.purchaseId });
      if (remaining <= 0) break;
    }
    lots.sort((a, b) => a.date.localeCompare(b.date) || a.id.localeCompare(b.id));
    if (remaining > 0) warning(reference, 'Returned units exceed the verified original cost allocation. Review SKU return quantities.', date, 'cogs', ch);
    return { cost: money(cost), complete: remaining === 0 };
  };

  function queueSale(order, dispatch, source) {
    if (order.accountingExcluded || ['2720'].includes(String(order.number || order.orderNumber || '').replace(/^#/, ''))) return;
    const ch = channel(order.channel), id = source + '/' + order.id, reference = order.name || order.id;
    const resolved=resolveOrder(order,sources.lineCorrections);
    order=resolved.order;
    const made = completion(order, dispatch, sources.velocity);
    resolved.issues.forEach(message=>warning(reference,message,made?.date||day(order.processedAt||order.createdAt),'cogs',ch));
    const transactions = [...new Map((order.paymentTransactions || []).filter(t => ['sale', 'capture'].includes(t.kind) && t.status === 'success').map(t => [t.id || JSON.stringify(t), t])).values()];
    const paymentDetailsKnown = (transactions.length > 0 || order.financialStatus === 'pending') && transactions.every(t => day(t.processedAt));
    const periodPayments = transactions.filter(t => day(t.processedAt) && day(t.processedAt) <= to);
    const collected = paymentDetailsKnown ? money(periodPayments.reduce((sum, t) => sum + number(t.amount), 0)) : null;
    const periodRefunds = (order.refundTransactions || []).filter(t => day(t.processedAt) && day(t.processedAt) <= to);
    const cashRefunded = money(periodRefunds.filter(t => !/store.?credit|gift.?card/i.test(t.gateway)).reduce((sum, t) => sum + number(t.amount), 0));
    const placed = day(order.processedAt || order.createdAt);
    const completedInTime = made?.date && made.date <= to;
    const cancelledInTime = order.pnlCancelled || (order.cancelledAt ? day(order.cancelledAt) <= to : ['cancelled', 'rto'].includes(dispatch.packingStatus) && !completedInTime);
    const refundedValue = money(list(order.refunds).filter(r => day(r.date || r.createdAt) && day(r.date || r.createdAt) <= to).reduce((sum, r) => sum + number(r.amount), 0));
    if (placed >= start && placed <= to) collections.push({ id, reference, date: completedInTime ? made.date : placed, channel: ch, orderValue: money(order.total), collected, cashRefunded, balanceToCollect: cancelledInTime ? 0 : collected === null ? null : money(Math.max(0, number(order.total) - refundedValue - collected + cashRefunded)), status: completedInTime ? 'Completed sale' : (cancelledInTime ? 'Cancelled / RTO' : 'Awaiting completion'), customerAdvance: collected === null ? null : (completedInTime ? 0 : money(Math.max(0, collected - cashRefunded))), payments: periodPayments.map(t => ({ id: t.id, date: day(t.processedAt), method: t.gateway, amount: money(t.amount) })), source });
    if (order.pnlCancelled && collections.length && collections[collections.length-1].id===id) {
      collections[collections.length-1].status='Cancelled (Results-only confirmation)';
      collections[collections.length-1].correction=order.pnlStatusCorrection;
    }
    if (!made) return;
    if (!made.date) { warning(reference, made.issue || 'Delivery is marked complete but its actual date is missing. This sale is excluded until the source date is confirmed.', '', 'sales', ch); return; }
    if (made.date < start || made.date > to) return;
    tasks.push({ date: made.date, sort: 1, id, run() {
      const lines = order.lineItems || [], taxesIncluded = order.taxesIncluded !== false;
      const detail = [], taxDetail = []; let gst = 0, taxComplete = lines.length > 0, cost = 0, costComplete = lines.length > 0, billed = 0, sourceBilled = 0;
      lines.forEach((line, index) => {
        const key = id + '/' + (line.id || index), sku = skuKey(line.sku), qty = number(line.qty);
        const tax = lineTax(line, taxesIncluded);
        const components=(line.pnlComponents||[]).map(component=>({...component,allocation:allocate(skuKey(component.sku),component.qty,made.date,reference,ch,component.inventoryItemId)}));
        const allocation = components.length?{qty,bundle:true,missing:components.reduce((n,c)=>n+c.allocation.missing,0),cost:money(components.reduce((n,c)=>n+c.allocation.cost,0)),parts:components.flatMap(c=>c.allocation.parts)}:present(line.pnlUnitCost) ? {qty,missing:0,cost:money(qty*number(line.pnlUnitCost)),parts:['shipping','rounding'].includes(line.pnlKind)?[]:[{lotId:'PNL-CORRECTION/'+order.id+'/'+line.id,purchaseId:'User-confirmed Results custom-item cost',qty,unitCost:line.pnlUnitCost,amount:money(qty*line.pnlUnitCost),exactCost:line.pnlUnitCost,returned:0}]} : allocate(sku, Math.max(0, qty), made.date, reference, ch,line.pnlInventoryItemId);
        allocations.set(key, { ...allocation, sku });
        taxAllocations.set(key, { ...tax, qty, taxesIncluded, returnedQty: 0, returnedShare: 0, returnedTax: 0, returnedGross: 0 });
        detail.push({ sku, qty, title:line.title||'', lineId: line.id || index, gross: tax.gross, gst: tax.tax, netSales: tax.valid ? money(tax.gross - tax.tax) : null, cogs: allocation.missing ? null : allocation.cost, correction:line.pnlCorrection||null, itemKind:components.length?'bundle':line.pnlKind||'product', components:components.map(({allocation,...component})=>({...component,cogs:allocation.missing?null:allocation.cost})), allocations: allocation.parts.map(({ exactCost, returned, ...part }) => part) });
        const taxRecord = { id: key, date: made.date, reference, sku, qty, channel: ch, ...tax, taxesIncluded };
        taxRows.push(taxRecord); taxDetail.push(taxRecord);
        taxComplete = taxComplete && tax.valid; costComplete = costComplete && qty > 0 && allocation.missing === 0;
        billed += tax.gross; sourceBilled += tax.sourceGross ?? tax.gross; gst += number(tax.tax); cost += allocation.cost;
        if (!tax.valid) warning(reference, tax.status + ' for ' + sku + '.', made.date, 'gst', ch);
      });
      const shipping = number(order.shipping);
      if (shipping > 0) {
        const shippingTax = present(order.shippingTax) ? number(order.shippingTax) : null;
        billed += shipping + (!taxesIncluded ? number(shippingTax) : 0); sourceBilled += shipping + (!taxesIncluded ? number(shippingTax) : 0); gst += number(shippingTax);
        if (shippingTax === null) { taxComplete = false; warning(reference, 'Shipping tax is missing; no garment rate has been guessed for shipping.', made.date, 'gst', ch); }
      }
      if (!lines.length) warning(reference, 'Sale has no SKU lines. GST and COGS need source details.', made.date, 'sales', ch);
      if (order.taxesIncluded !== true && order.taxesIncluded !== false) {
        taxComplete = false; warning(reference, 'Tax-inclusive/exclusive price basis is missing. Refresh or confirm the source invoice before finalising profit.', made.date, 'gst', ch);
      }
      if (order.currency && order.currency !== 'INR') {
        taxComplete = false; warning(reference, 'Non-INR sale requires a verified exchange-rate allocation.', made.date, 'sales', ch);
      }
      if (Math.abs(money(sourceBilled) - number(order.total)) > 0.03) {
        taxComplete = false; warning(reference, 'Line values, discounts, shipping and invoice total do not reconcile. Review the source invoice.', made.date, 'gst', ch);
      }
      const reportGross = taxesIncluded ? number(order.total) : money(billed);
      add(row({ id, date: made.date, kind: 'sale', category: ch + ' sales', channel: ch, particulars: order.customer?.name || 'Completed sale', reference, source, completionEvidence: made.evidence, deliveryAwbs: made.awbs || [], sourceInvoiceTotal: money(order.total), grossSales: money(reportGross + number(order.discount)), discounts: money(order.discount), gst: money(gst), netSales: taxComplete ? money(reportGross - gst) : null, cogs: money(cost), taxComplete, costComplete, details: detail }));
    } });
    const refunds = list(order.refunds);
    if (!refunds.length && (order.refundTransactions || []).length) {
      for (const tx of order.refundTransactions) refunds.push({ id: tx.id, date: day(tx.processedAt), amount: number(tx.amount), lineItems: [], legacy: true });
    } else if (!refunds.length && number(order.refundAmount) > 0) warning(reference, 'Refund amount exists without a dated refund/credit note. Review the source before treating profit as complete.', '', 'sales', ch);
    for (const refund of refunds) {
      const date = day(refund.date || refund.createdAt), rid = id + '/REFUND/' + refund.id;
      if (!date || date < made.date) { warning(reference, 'Refund date is missing or precedes the completed sale.', '', 'sales', ch); continue; }
      tasks.push({ date, sort: 2, id: rid, run() {
        let amount = number(refund.amount), refundGst = 0, recordedRefundGst = 0, recordedRefundKnown = true, reportGrossDifference = 0, reversedCost = 0, taxComplete = true, costComplete = true; const detail = [];
        for (const line of refund.lineItems || []) {
          const originalIndex = linesIndex(order.lineItems, line.lineItemId);
          const original = (order.lineItems || [])[originalIndex];
          if (!original) { taxComplete = false; costComplete = false; continue; }
          const qty = number(line.qty), originalTax = taxAllocations.get(id + '/' + (original.id || originalIndex));
          const sourceLineGross = present(line.subtotal) && present(line.tax) ? money(number(line.subtotal) + number(line.tax)) : null;
          recordedRefundGst += number(line.tax);
          recordedRefundKnown = recordedRefundKnown && present(line.tax);
          let tax = null;
          if (!originalTax?.valid || !(qty > 0) || qty + originalTax.returnedQty > originalTax.qty + 1e-8) taxComplete = false;
          else {
            // Reverse the original Results allocation, never the source refund
            // GST or a newly selected slab. Documented value-only partial
            // credits use their fraction of the original source line.
            const quantityShare = qty / originalTax.qty;
            const share = sourceLineGross !== null && originalTax.sourceGross > 0 ? Math.min(quantityShare, sourceLineGross / originalTax.sourceGross) : quantityShare;
            if (share < 0 || originalTax.returnedShare + share > 1 + 1e-8) taxComplete = false;
            else {
              const nextShare = Math.min(1, originalTax.returnedShare + share);
              tax = money(money(originalTax.tax * nextShare) - originalTax.returnedTax);
              const reportLineGross = money(money(originalTax.gross * nextShare) - originalTax.returnedGross);
              originalTax.returnedQty += qty; originalTax.returnedShare = nextShare;
              originalTax.returnedTax = money(originalTax.returnedTax + tax); originalTax.returnedGross = money(originalTax.returnedGross + reportLineGross);
              refundGst += tax;
              if (!originalTax.taxesIncluded && sourceLineGross !== null) reportGrossDifference += reportLineGross - sourceLineGross;
            }
          }
          if (line.restockType === 'return' && !['shipping','rounding'].includes(original.pnlKind)) {
            const returned = returnCost(id + '/' + (original.id || originalIndex), qty, date, rid, ch);
            reversedCost += returned.cost; costComplete = costComplete && returned.complete;
          }
          detail.push({ sku: original.sku, qty, returnedToStock: line.restockType === 'return', subtotal: line.subtotal, gst: tax });
        }
        if (!(amount > 0)) amount = money((refund.lineItems || []).reduce((sum, l) => sum + number(l.subtotal) + number(l.tax), 0) + number(refund.adjustmentAmount));
        if (!(amount > 0)) return;
        if (!(refund.lineItems || []).length) {
          taxComplete = false;
          warning(reference, 'Refund has no item/tax breakdown. Revenue deduction is shown; GST and any returned SKU cost need review.', date, 'gst', ch);
        }
        if (present(refund.shippingTax)) { refundGst += number(refund.shippingTax); recordedRefundGst += number(refund.shippingTax); }
        if (!taxComplete) warning(reference, 'Refund GST/item linkage is incomplete. Review the source credit note.', date, 'gst', ch);
        if (amount > number(order.total) + .01 || refundGst > amount + .01 || refundGst < 0) {
          taxComplete = false; warning(reference, 'Refund amount or GST exceeds the original invoice allocation.', date, 'gst', ch);
        }
        if (!costComplete) warning(reference, 'Refund could not be linked to its original SKU cost allocation.', date, 'cogs', ch);
        const reportRefund = money(amount + reportGrossDifference);
        taxRows.push({ id: rid, date, reference, sku: 'Refund / credit note', qty: 0, channel: ch, gross: -reportRefund, tax: taxComplete ? -money(refundGst) : null, recorded: recordedRefundKnown ? -money(recordedRefundGst) : null, calculated: taxComplete ? -money(refundGst) : null, rate: null, valid: taxComplete, status: taxComplete ? 'Original Results GST reversed' : 'Refund tax needs review' });
        add(row({ id: rid, date, kind: 'return', category: 'Returns / refunds', channel: ch, particulars: 'Refund / credit note for ' + reference, reference, source, sourceRefundAmount: money(amount), returns: reportRefund, gst: -money(refundGst), netSales: taxComplete ? -money(reportRefund - refundGst) : null, cogs: -money(reversedCost), taxComplete, costComplete, details: detail }));
      } });
    }
  }
  for (const order of list(orderStore.orders)) queueSale(order, orderStore.dispatch?.[order.id] || {}, 'Shopify');
  for (const sale of list(sources.manualSales?.sales)) {
    if (sale.voided || sale.shopifyOrderId || sale.orderId) continue;
    queueSale({ ...sale, taxesIncluded: sale.taxesIncluded ?? true, channel: channel(sale.channel), financialStatus: 'paid', processedAt: sale.day || sale.ts, completedAt: sale.day || sale.ts, lineItems: (sale.items || []).map((l, i) => ({ ...l, id: l.id || String(i), price: l.unitPrice, discount: (sale.items || []).reduce((s, x) => s + number(x.lineTotal), 0) ? number(sale.discount) * number(l.lineTotal) / (sale.items || []).reduce((s, x) => s + number(x.lineTotal), 0) : 0 })), paymentTransactions: [{ id: sale.id, kind: 'sale', status: 'success', amount: sale.total, gateway: sale.paymentMode, processedAt: sale.ts || sale.day }] }, {}, 'Manual sale');
  }
  tasks.sort((a, b) => a.date.localeCompare(b.date) || a.sort - b.sort || a.id.localeCompare(b.id)).filter(task => task.date <= to).forEach(task => task.run());

  const activeDraft = record => !!(record.reconciliationDraft && books.bankReconciliationDrafts?.[record.reconciliationDraft]);
  const paidIds = new Set(), refundPayments = new Map();
  function paymentExpense(expense, payment, amount, kind = 'expense') {
    const date = day(payment.date || payment.paidAt), id = expense.id + '/' + payment.id;
    if (!date) { warning(expense.id, 'Actual payment date is missing; this payment is excluded.', '', 'expense'); return; }
    paidIds.add(id);
    if (date >= start && date <= to && amount > 0) refundPayments.set(expense.id + '/' + (payment.refundPaymentId || payment.id), { date, remaining: money(amount) });
    const type = ['fixed', 'running', 'variable', 'marketing'].includes(expense.type) ? expense.type : 'variable';
    add(row({ id, date, kind, category: expense.ledger || expense.category || 'Unclassified paid expense', group: type, channel: ['POS', 'Website'].includes(expense.channel) ? expense.channel : 'Shared', particulars: expense.particulars || expense.vendor || '', reference: expense.id + '/' + payment.id, expenseDate: day(expense.date), account: payment.account || expense.account || '', method: payment.paymentType || '', proof: payment.proof || expense.billPhoto || '', source: 'Expenses', expenses: money(amount), details: [{ vendor: expense.vendor || '', paymentAmount: money(payment.amount), includedAmount: money(amount), note: payment.note || '' }] }));
  }
  for (const expense of list(books.expenses)) {
    if (!business(expense.nature) || expense.accountingExcluded || expense.nonExpenseReimbursement || expense.salaryAdvanceId || expense.inventoryPurchase || expense.procurementPoId || expense.purchaseOrderId || !['approved', 'partially_paid', 'paid'].includes(expense.status)) continue;
    let remaining = Math.max(0, number(expense.amount));
    const regularPayments = (expense.payments || []).filter(p => !p.voided && !p.accountingExcluded && !(p.bankReconciliationDraft && books.bankReconciliationDrafts?.[p.bankReconciliationDraft])).map((p, i) => ({ ...p, id: p.id || 'PAY-LEGACY/' + i }));
    const advanceApplications = (expense.vendorAdvanceApplications || []).map((p, i) => ({ ...p, id: 'ADV-APPLICATION/' + (p.id || (p.vendorAdvanceId || 'LEGACY') + '/' + (p.batchPaymentId || p.appliedAt || i)), refundPaymentId: 'ADV-' + p.vendorAdvanceId + '-' + i, paymentType: 'Applied vendor advance' })).filter(p => !p.voided && !p.accountingExcluded && !activeDraft((books.vendorAdvances || []).find(a => a.id === p.vendorAdvanceId) || {}));
    const payments = [...regularPayments, ...advanceApplications].sort((a, b) => day(a.date || a.paidAt).localeCompare(day(b.date || b.paidAt)) || String(a.id).localeCompare(String(b.id)));
    const unique = new Set();
    for (const payment of payments) {
      if (unique.has(payment.id) || !(number(payment.amount) > 0)) continue;
      unique.add(payment.id);
      const included = Math.min(remaining, number(payment.amount)); remaining -= included;
      if (included > 0) paymentExpense(expense, payment, included);
    }
    const confirmation=resolveExpenseConfirmation(expense,sources);
    for(const message of confirmation.issues)warning(expense.id,message,confirmation.entry?.paymentDate||day(expense.date),'expense');
    if(confirmation.amount>0){
      const fact=confirmation.entry,type=['fixed','running','variable','marketing'].includes(expense.type)?expense.type:'variable';
      // Management-report evidence only. Do not add a payment ID, refund
      // allocation, salary link, account debit or reconciliation marker.
      add(row({id:'RESULTS-CONFIRMATION/'+expense.id,date:confirmation.date,kind:'expense_confirmation',category:expense.ledger||expense.category||'Unclassified paid expense',group:type,channel:['POS','Website'].includes(expense.channel)?expense.channel:'Shared',particulars:expense.particulars||expense.vendor||'',reference:expense.id+'/RESULTS-CONFIRMED',expenseDate:day(expense.date),method:'User-confirmed reimbursement/payment; report only',source:'Results-only paid-expense confirmation',expenses:confirmation.amount,details:[{expenseId:expense.id,reportOnly:true,confirmedTotal:fact.amount,includedAmount:confirmation.amount,eligibleSourcePaidAmount:confirmation.eligiblePaidAmount,confirmedPaymentDate:fact.paymentDate,adjustmentReference:fact.adjustmentReference||'',adjustmentReferenceBasis:'User-reported only; not verified bank or adjustment evidence',actor:fact.updatedBy||'',confirmedAt:fact.updatedAt||'',reason:fact.note||'',sourceFinancialRecordsUnchanged:true}]}));
    }
    const evidenceTotal=money(payments.reduce((sum,p)=>sum+number(p.amount),0)+confirmation.amount);
    if (number(expense.paidAmount) > evidenceTotal) warning(expense.id, 'Paid total exceeds available payment evidence. Only documented payments or an audited Results-only confirmation are included.', day(expense.date), 'expense');
    if (!payments.length && !confirmation.amount && number(expense.paidAmount) > 0) warning(expense.id, 'Paid amount has no eligible payment record. It is excluded until evidence is restored or an audited Results-only confirmation is supplied.', day(expense.date), 'expense');
  }
  for (const expense of list(books.reconciliationExpenses)) {
    if (!business(expense.nature) || expense.accountingExcluded || activeDraft(expense)) continue;
    // A card credit can be the evidence for a linked supplier refund, not a
    // second expense reversal. Recognise the dated source allocation below.
    if (expense.expenseRefundReceiptId || list(books.expenseRefunds).some(r => r.status === 'received' && (r.components || []).some(c => c.externalMovementId === expense.id))) {
      if (!list(books.expenseRefunds).some(r => r.status === 'received' && (r.components || []).some(c => expense.expenseRefundReceiptId && c.receiptId === expense.expenseRefundReceiptId || c.externalMovementId === expense.id))) warning(expense.id, 'Card refund evidence has no received source allocation. Review its expense-refund link.', day(expense.date), 'expense');
      continue;
    }
    paymentExpense({ ...expense, ledger: expense.category }, { id: 'BANK', refundPaymentId: expense.creditCardId ? 'STATEMENT' : 'BANK', date: expense.date, amount: expense.amount, account: expense.account, paymentType: expense.creditCardId ? 'Credit card' : 'Bank statement' }, number(expense.amount));
  }
  const receivedRefunds = list(books.expenseRefunds).filter(r => business(r.nature) && r.status === 'received' && !r.accountingExcluded && !activeDraft(r) && !r.creditNote).sort((a, b) => String(a.date || '').localeCompare(String(b.date || '')) || String(a.id).localeCompare(String(b.id)));
  for (const refund of receivedRefunds) {
    const date = day(refund.date);
    if (date && (date < start || date > to)) continue;
    for (const [index, source] of (refund.sources || []).entries()) {
      if (source.kind === 'advance' || !source.expenseId) continue; // Returned advances are balance-sheet movements.
      const expense = books.expenses?.[source.expenseId] || list(books.reconciliationExpenses).find(e => e.id === source.expenseId);
      if (!expense || expense.accountingExcluded || !business(expense.nature)) { warning(refund.id, 'Received expense refund has no included business source. Review the original paid expense.', date, 'expense'); continue; }
      if (expense.inventoryPurchase || expense.procurementPoId || expense.purchaseOrderId) { warning(refund.id, 'Inventory supplier refund needs a verified SKU/lot cost correction, not an operating-expense deduction.', date, 'cogs'); continue; }
      const ch = ['POS', 'Website'].includes(expense.channel) ? expense.channel : 'Shared';
      const parts = source.paymentAllocations || []; let reversed = 0;
      for (const allocation of parts) {
        const paid = refundPayments.get(source.expenseId + '/' + allocation.paymentId), amount = number(allocation.amount);
        const included = date && paid && paid.date <= date ? Math.min(paid.remaining, Math.max(0, amount)) : 0;
        if (paid && included) paid.remaining = money(paid.remaining - included);
        reversed += included;
        if (money(included) < money(amount)) warning(refund.id, 'Refund allocation exceeds or cannot identify the original included payment. Only verified paid costs have been reversed.', date, 'expense', ch);
      }
      if (Math.abs(money(reversed) - number(source.amount)) > .01 || !parts.length) warning(refund.id, 'Received supplier refund is not fully linked to paid-expense evidence. Review its source allocation.', date, 'expense', ch);
      if (!(reversed > 0) || !date) continue;
      const type = ['fixed', 'running', 'variable', 'marketing'].includes(expense.type) ? expense.type : 'variable';
      add(row({ id: 'EXPENSE-REFUND/' + refund.id + '/' + index, date, kind: 'expense_refund', category: expense.ledger || expense.category || source.category || 'Unclassified paid expense', group: type, channel: ['POS', 'Website'].includes(expense.channel) ? expense.channel : 'Shared', particulars: 'Supplier refund · ' + (refund.vendor || expense.vendor || '') + ' · ' + (refund.reason || ''), reference: refund.id, expenseDate: day(expense.date), account: (refund.components || []).map(c => c.account || c.issuer || c.mode).filter(Boolean).join(' + '), method: (refund.components || []).map(c => c.mode).join(' + '), proof: refund.proofs?.[0] || '', source: 'Expense refunds', expenses: -money(reversed), details: [{ expenseId: expense.id, paymentAllocations: parts }] }));
    }
  }
  for (const payment of list(sources.incentives?.payments)) {
    if (payment.active === false || payment.accountingExcluded || !(number(payment.amount) > 0)) continue;
    paymentExpense({ id: payment.id, date: payment.date, type: 'variable', ledger: 'Sales incentives', vendor: payment.salesperson, particulars: 'Sales incentive · ' + payment.salesperson, channel: 'POS' }, { ...payment, proof: payment.proofs?.[0] || '', paymentType: 'Incentive payment' }, number(payment.amount), 'incentive');
  }
  for (const payment of list(sources.salary?.salaryPayments)) {
    if (payment.active === false || payment.accountingExcluded || !(number(payment.amount) > 0)) continue;
    if (payment.linkedLedgerEntryId) {
      if (!paidIds.has(payment.linkedLedgerEntryId) && !events.some(e => e.id.startsWith(payment.linkedLedgerEntryId + '/'))) warning(payment.id, 'Salary links to a ledger entry not included in paid expenses. Review its classification to avoid missing or double-counted salary.', day(payment.date), 'expense');
      continue;
    }
    const employee = sources.salary?.employees?.[payment.empId] || {};
    paymentExpense({ id: payment.id, date: payment.date, type: 'fixed', ledger: 'Salary', vendor: payment.employeeName, particulars: 'Salary · ' + payment.employeeName, channel: employee.channel }, { ...payment, paymentType: 'Salary payment' }, number(payment.amount), 'salary');
  }
  const manualReceipts = new Map();
  for (const receipt of list(books.receipts)) {
    if (!business(receipt.nature) || receipt.accountingExcluded || activeDraft(receipt)) continue;
    const linkedRefundAmount=receivedRefunds.reduce((sum,refund)=>sum+(refund.date===receipt.date?(refund.components||[]).filter(c=>c.externalMovementId===receipt.id||c.receiptId===receipt.id).reduce((n,c)=>n+number(c.amount),0):0),0);
    if(receipt.receiptType==='refund'&&number(receipt.amount)>0&&money(linkedRefundAmount)>=money(receipt.amount))continue;
    if (['bank_interest', 'other_income'].includes(receipt.receiptType)) add(row({ id: 'RECEIPT/' + receipt.id, date: day(receipt.date), kind: 'income', category: receipt.category || 'Other income', particulars: receipt.source || '', reference: receipt.id, account: receipt.account || '', proof: receipt.proof || '', source: 'Receipts', otherIncome: money(receipt.amount) }));
    else if (receipt.receiptType === 'product_sale' && !receipt.orderId && !receipt.shopifyOrderId) {
      const key = receipt.manualSaleId || receipt.id, group = manualReceipts.get(key) || []; group.push(receipt); manualReceipts.set(key, group);
    }
    else if (['asset_sale', 'refund'].includes(receipt.receiptType)) warning(receipt.id, 'Receipt requires profit classification (' + receipt.receiptType + '); it has not been treated as ordinary income automatically.', day(receipt.date), 'income');
  }
  for (const [id, receipts] of manualReceipts) {
    if (list(sources.manualSales?.sales).some(sale => sale.id === id)) continue;
    const date = day(receipts[0].date);
    const amount = money(receipts.reduce((sum, r) => sum + number(r.amount), 0));
    const fact=sources.lineCorrections?.entries?.['manual:'+id+'/$manual'];
    const costKnown=fact?.type==='manual_cost'&&fact.fingerprint===manualHash(receipts)&&present(fact.unitCost)&&fact.unitCost>0&&Number.isInteger(fact.qty)&&fact.qty>0;
    const taxKnown=costKnown&&present(fact.taxRate)&&fact.taxRate>=0&&fact.taxRate<=1&&typeof fact.taxesIncluded==='boolean';
    const tax=taxKnown?money(fact.taxesIncluded?amount-amount/(1+fact.taxRate):amount*fact.taxRate):null;
    const gross=taxKnown&&!fact.taxesIncluded?money(amount+tax):amount;
    taxRows.push({id:'MANUAL-RECEIPT/'+id,date,reference:id,channel:'Other',sku:'Manual non-garment sale',qty:costKnown?fact.qty:1,gross,recorded:null,calculated:tax,tax,rate:taxKnown?fact.taxRate:null,valid:taxKnown,status:taxKnown?'User-confirmed manual-sale tax':'Manual-sale tax basis needs review'});
    add(row({ id: 'MANUAL-RECEIPT/' + id, date, kind: 'sale', channel: 'Other', category: 'Other sales', particulars: receipts[0].source, reference: id, source: 'Receipts', sourceInvoiceTotal:amount,grossSales:gross,gst:tax||0,netSales:taxKnown?money(gross-tax):null,cogs:costKnown?money(fact.qty*fact.unitCost):0,taxComplete:taxKnown,costComplete:costKnown,details:costKnown?[{title:receipts[0].source,qty:fact.qty,cogs:money(fact.qty*fact.unitCost),correction:fact,allocations:[{purchaseId:'User-confirmed manual-sale COGS',qty:fact.qty,unitCost:fact.unitCost,amount:money(fact.qty*fact.unitCost)}]}]:[] }));
    if(!costKnown)warning(id, fact?'Manual-sale COGS confirmation no longer matches its receipts; review it again.':'Manual product sale has no SKU/cost allocation. Its gross value is shown; profit is unavailable until mapped.',date,'cogs','Other');
    if(!taxKnown)warning(id,'Manual non-garment sale needs its confirmed GST rate and inclusive/exclusive price basis.',date,'gst','Other');
  }
  for (const refund of list(books.salesRefunds)) {
    if (!business(refund.nature) || refund.accountingExcluded || refund.voided || activeDraft(refund)) continue;
    const date = day(refund.date), ref = String(refund.saleReference || '').replace(/^#/, '');
    const linked = events.some(e => e.kind === 'return' && e.date === date && String(e.reference).replace(/^#/, '') === ref && Math.abs(e.returns - number(refund.amount)) < .01);
    if (linked || !(number(refund.amount) > 0)) continue;
    add(row({ id: 'LEDGER-REFUND/' + refund.id, date, kind: 'return', channel: 'Other', category: 'Returns / refunds', particulars: refund.reason, reference: refund.saleReference || refund.id, account: refund.refundAccount, proof: refund.proof, source: 'Ledger refund', returns: money(refund.amount), netSales: null, taxComplete: false, costComplete: false }));
    warning(refund.id, 'Ledger-recorded customer refund is not linked to a source credit note/SKU return. Review tax, original order and restock evidence before profit is final.', date, 'sales', 'Other');
  }
  const choose = entry => selectedChannel === 'All' || entry.channel === selectedChannel;
  const inRange = (entry, lo, hi) => entry.date >= lo && entry.date <= hi && choose(entry);
  const included = events.filter(entry => inRange(entry, from, to));
  const scopedWarnings = warnings.filter(w => (!w.date || w.date >= from && w.date <= to) && choose(w));
  const sourceErrors = (sources.feeds || []).filter(feed => (feed.required || feed.status === 'unreadable') && feed.status !== 'ready');
  for (const feed of sourceErrors) scopedWarnings.push({ reference: feed.name, message: 'Source ' + feed.name + ' is ' + feed.status + '. Profit is unavailable.', scope: 'source' });
  function summarize(rows, issues = []) {
    const fields = ['grossSales', 'discounts', 'returns', 'gst', 'netSales', 'otherIncome', 'cogs', 'expenses'];
    const totals = Object.fromEntries(fields.map(field => [field, money(rows.reduce((sum, entry) => sum + number(entry[field]), 0))]));
    totals.taxComplete = rows.every(e => e.taxComplete) && !issues.some(w => ['gst', 'sales', 'source'].includes(w.scope));
    totals.costComplete = rows.every(e => e.costComplete) && !issues.some(w => ['cogs', 'source'].includes(w.scope));
    totals.complete = totals.taxComplete && totals.costComplete && !issues.some(w => ['expense', 'income'].includes(w.scope));
    totals.knownCogs = totals.cogs; totals.knownGst = totals.gst;
    if (!totals.taxComplete) { totals.gst = null; totals.netSales = null; }
    if (!totals.costComplete) totals.cogs = null;
    totals.totalIncome = totals.taxComplete ? money(totals.netSales + totals.otherIncome) : null;
    totals.grossProfit = totals.taxComplete && totals.costComplete ? money(totals.netSales - totals.cogs) : null;
    totals.netProfit = totals.complete ? money(totals.totalIncome - totals.cogs - totals.expenses) : null;
    totals.margin = totals.netProfit !== null && totals.netSales > 0 ? totals.netProfit / totals.netSales : null;
    totals.salesCount = rows.filter(e => e.kind === 'sale').length;
    return totals;
  }
  const span = (Date.parse(to) - Date.parse(from)) / 86400000 + 1;
  const previousTo = new Date(Date.parse(from) - 86400000).toISOString().slice(0, 10);
  const previousFrom = new Date(Date.parse(from) - span * 86400000).toISOString().slice(0, 10);
  const previousRows = events.filter(e => inRange(e, previousFrom < start ? start : previousFrom, previousTo));
  const previous = previousTo < start ? null : { from: previousFrom < start ? start : previousFrom, to: previousTo, partial: previousFrom < start, totals: summarize(previousRows, [...warnings.filter(w => (!w.date || w.date >= previousFrom && w.date <= previousTo) && choose(w)), ...scopedWarnings.filter(w => w.scope === 'source')]) };
  const categories = new Map();
  for (const entry of included.filter(e => ['expense', 'expense_confirmation', 'expense_refund', 'salary', 'incentive'].includes(e.kind))) {
    const key = entry.group + '/' + entry.category, group = categories.get(key) || { id: key, group: entry.group, category: entry.category, amount: 0, count: 0 };
    group.amount = money(group.amount + entry.expenses); group.count++; categories.set(key, group);
  }
  const months = [...new Set(included.map(e => e.date.slice(0, 7)))].sort();
  return { success: true, range: { from, to, requestedFrom: options.from || from, partial: from !== options.from && !!options.from, start }, channel: selectedChannel, basis: shopifyCostMode ? 'Completed sales excluding Results-calculated GST − synced Shopify SKU COGS − approved paid operating expenses' : 'Completed sales excluding Results-calculated GST − FIFO SKU COGS − approved paid operating expenses', totals: summarize(included, scopedWarnings), previous, categories: [...categories.values()].sort((a, b) => a.group.localeCompare(b.group) || a.category.localeCompare(b.category)), transactions: included.sort((a, b) => b.date.localeCompare(a.date) || a.id.localeCompare(b.id)), taxes: taxRows.filter(e => inRange(e, from, to)), collections: collections.filter(e => inRange(e, from, to)), trends: months.map(month => ({ month, ...summarize(included.filter(e => e.date.startsWith(month)), scopedWarnings.filter(w => !w.date || w.date.startsWith(month))) })), warnings: scopedWarnings, feeds: sources.feeds || [], costSource: shopifyCostMode ? { name: 'Shopify Cost per item', at: sources.shopifyCosts?.at || null } : { name: 'Verified FIFO' }, policy: { operatingExpenses: 'Approved paid portions, including explicitly audited Results-only paid-expense confirmations. Source payments take priority across all dates, so restored payments supersede report-only amounts. Reimbursements and vendor advances are not counted twice. Report-only confirmations never post bank movements or alter financial approvals.', cogs: shopifyCostMode ? 'Shopify Cost per item by SKU from the synced local snapshot. Current stock quantities and supplier payments do not limit COGS. Refunds reverse the original sale allocation. Missing or duplicate costs remain exceptions. Cost changes restate management Results history.' : 'Received inventory allocated by SKU using FIFO, irrespective of supplier payment. Missing opening costs are never estimated.', gst: 'Results only: final per-piece selling value after discounts ≤ ₹2,500 uses 5%; above ₹2,500 uses 18%. Invoice GST and exempt flags do not override this calculation. Refunds reverse original Results GST. Inclusive GST is extracted from the final selling amount; exclusive GST is added. Missing price basis and non-garment tax still require review. Invoices, Shopify, collections and ledgers remain unchanged.', sales: 'POS completion or recorded delivery date. Customer advances and courier/bank settlements are not extra sales.', channels: selectedChannel === 'All' ? 'All channels and Shared overhead included.' : 'Direct channel only. Shared overhead is excluded, not silently allocated.' } };
}
function linesIndex(lines, id) { return (lines || []).findIndex(line => String(line.id) === String(id)); }
module.exports = { START, validDate, day, money, gstRate, lineTax, completion, costLots, buildReport };
