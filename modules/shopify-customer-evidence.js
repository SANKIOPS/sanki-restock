'use strict';

const fs = require('fs');
const crypto = require('crypto');
const { fingerprint } = require('./sale-store-credit-corrections');
const copy = value => JSON.parse(JSON.stringify(value));
const text = value => String(value == null ? '' : value).trim();
function fail(message, status = 409) { const error = new Error(message); error.status = status; throw error; }
function id(value, label) {
  if (typeof value === 'number' && !Number.isSafeInteger(value)) fail(label + ' is not an exact identifier.');
  const valueText = text(value);
  if (!/^[1-9]\d*$/.test(valueText)) fail(label + ' is unavailable.');
  return valueText;
}
function number(value) {
  const valueText = text(value).replace(/^#\s*/, '').toLowerCase();
  return /^\d+$/.test(valueText) ? valueText.replace(/^0+(?=\d)/, '') : valueText;
}
function cents(value) {
  const amount = typeof value === 'string' && !value.trim() ? NaN : Number(value);
  if (!Number.isFinite(amount) || amount < 0 || !Number.isSafeInteger(Math.round(amount * 100)) || Math.abs(amount * 100 - Math.round(amount * 100)) > 0.000001) fail('The selected order total is not an exact INR amount.');
  return Math.round(amount * 100);
}
function read(io, filePath) {
  let raw, data;
  try { raw = io.readFileSync(filePath,'utf8'); data = JSON.parse(raw); }
  catch { fail('The saved Shopify evidence cannot be read safely. No customer identity was changed.'); }
  if (!data || typeof data !== 'object' || Array.isArray(data) || !data.orders || typeof data.orders !== 'object' || Array.isArray(data.orders) || data.customerIdentityRefreshes != null && !Array.isArray(data.customerIdentityRefreshes)) fail('The saved Shopify evidence has an invalid shape. No customer identity was changed.');
  return {raw,data};
}
function cachedOrder(store, orderId) {
  const order = Object.prototype.hasOwnProperty.call(store.orders,orderId) && store.orders[orderId];
  if (!order || text(order.id) !== orderId || text(order.channel).toUpperCase() !== 'POS') fail('Choose two existing POS Shopify orders.');
  if (text(order.currency || 'INR').toUpperCase() !== 'INR') fail('Only INR order identity evidence is supported.');
  if (order.customer != null && (typeof order.customer !== 'object' || Array.isArray(order.customer))) fail('The saved customer evidence has an invalid shape.');
  return order;
}
function sameDate(raw, cached, label) {
  if (!cached && !raw) return;
  const expected = Date.parse(cached), actual = Date.parse(raw);
  if (!cached || !raw || !Number.isFinite(expected) || expected !== actual) fail('Shopify ' + label + ' changed. Refresh the order through its source workflow first.');
}
function verifySource(raw, cached, orderId) {
  if (!raw || id(raw.id,'Shopify order ID') !== orderId) fail('Shopify returned a different order identity.');
  const expectedNumber = cached.orderNumber || cached.number || cached.name;
  if (!expectedNumber || number(raw.order_number || raw.name) !== number(expectedNumber) || cached.name && number(raw.name) !== number(cached.name)) fail('Shopify order number changed. No customer identity was changed.');
  if (text(raw.source_name).toLowerCase() !== 'pos' || text(raw.currency).toUpperCase() !== 'INR') fail('Shopify no longer identifies this as an INR POS order.');
  if (cents(raw.total_price) !== cents(cached.total)) fail('Shopify order total changed. Refresh its financial source evidence first.');
  const createdAt = cached.createdAt || cached.created_at;
  if (!createdAt) fail('The saved order creation date is unavailable.');
  sameDate(raw.created_at,createdAt,'order creation date');
  sameDate(raw.processed_at || raw.created_at,cached.processedAt || cached.processed_at || cached.createdAt || cached.created_at,'order payment date');
  return id(raw.customer && raw.customer.id,'Shopify customer ID');
}
function hash(value) { return crypto.createHash('sha256').update(String(value)).digest('hex'); }
function atomicCompareWrite(io, filePath, before, after) {
  const temporary = filePath + '.customer-identity-' + crypto.randomBytes(12).toString('hex') + '.tmp';
  let descriptor, created = false;
  try {
    if (io.readFileSync(filePath,'utf8') !== before) fail('The Shopify cache changed during identity verification. Retry the refresh.');
    descriptor = io.openSync(temporary,'wx',0o600); created = true;
    io.writeFileSync(descriptor,after,'utf8'); io.fsyncSync(descriptor);
    io.closeSync(descriptor); descriptor = undefined;
    if (io.readFileSync(filePath,'utf8') !== before) fail('The Shopify cache changed during identity verification. Retry the refresh.');
    io.renameSync(temporary,filePath); created = false;
  } finally {
    if (descriptor !== undefined) io.closeSync(descriptor);
    if (created) { try { io.unlinkSync(temporary); } catch {} }
  }
}
async function refresh(options) {
  const {filePath,client,store,validate} = options || {}, io = options && options.io || fs;
  if (!filePath || !client || typeof client.request !== 'function' || !store) fail('The Shopify evidence connection is not configured.',503);
  if (typeof validate !== 'function') fail('A synchronous accounting evidence check is required.',400);
  const actor = text(options.actor);
  if (!actor) fail('An authorized actor is required.',400);
  if (!Array.isArray(options.orderIds) || options.orderIds.length !== 2) fail('Choose exactly two Shopify orders.',400);
  const orderIds = options.orderIds.map(value => id(value,'Selected Shopify order ID'));
  if (new Set(orderIds).size !== 2) fail('Choose two distinct Shopify orders.',400);
  const initial = read(io,filePath), selected = orderIds.map(orderId => cachedOrder(initial.data,orderId));
  const beforeFingerprints = selected.map(fingerprint), customerIds = [];
  for (let index = 0; index < orderIds.length; index++) {
    const url = 'https://' + store + '/admin/api/2024-01/orders/' + orderIds[index] + '.json?fields=id,order_number,name,source_name,currency,total_price,created_at,processed_at,customer';
    const response = await client.request(url,{method:'GET'});
    if (!response || !response.ok) fail('Shopify customer evidence lookup failed' + (response && response.status ? ' (' + response.status + ')' : '') + '. No customer identity was changed.',502);
    let payload;
    try { payload = await response.json(); } catch { fail('Shopify returned unreadable order evidence. No customer identity was changed.',502); }
    customerIds.push(verifySource(payload && payload.order,selected[index],orderIds[index]));
  }
  if (customerIds[0] !== customerIds[1]) fail('Shopify identifies different customers on these orders. Select the same customer in Shopify before refreshing this evidence.');
  const latest = read(io,filePath), next = copy(latest.data), changedOrderIds = [], beforeCustomerIdHashes = {};
  orderIds.forEach((orderId,index) => {
    const order = cachedOrder(latest.data,orderId);
    if (fingerprint(order) !== beforeFingerprints[index]) fail('Selected order evidence changed during the Shopify lookup. Review a fresh refresh; no customer identity was changed.');
    const previousId = text(order.customer && order.customer.id);
    if (previousId && previousId !== customerIds[index]) fail('A saved customer ID conflicts with Shopify. A protected source review is required; it was not reassigned.');
    beforeCustomerIdHashes[orderId] = previousId ? hash(previousId) : '';
    if (!previousId) {
      next.orders[orderId].customer = {...next.orders[orderId].customer,id:customerIds[index]};
      changedOrderIds.push(orderId);
    }
  });
  const checkedAt = typeof options.now === 'function' ? options.now() : new Date().toISOString();
  next.customerIdentityRefreshes = (next.customerIdentityRefreshes || []).concat({
    id:'SCID-' + crypto.randomBytes(12).toString('hex'),action:'SHOPIFY_CUSTOMER_IDENTITY_HYDRATED',actor,checkedAt,
    source:'exact_shopify_order_get',orderIds,changedOrderIds,beforeCustomerIdHashes,verifiedCustomerIdHash:hash(customerIds[0]),customerMatchVerified:true
  });
  const outcome = validate(copy(latest.data.orders),copy(next.orders));
  if (outcome && typeof outcome.then === 'function') fail('The accounting evidence check must complete synchronously.',400);
  atomicCompareWrite(io,filePath,latest.raw,JSON.stringify(next));
  return {success:true,updated:changedOrderIds.length,customerMatchVerified:true};
}

module.exports = {refresh};
