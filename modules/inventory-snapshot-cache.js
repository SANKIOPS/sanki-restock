'use strict';
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

// Display snapshots can survive a restart. They are never used to authorize a
// stock write: snapshot(true) always waits for a current Shopify result.
function createSnapshotCache({ fetchSnapshot, file, now = Date.now, ttl = 60000, retryDelay = 30000 }) {
  let current, display, inflight, generation = 0, error = null, retryAt = 0;
  if (file) {
    try {
      const saved = JSON.parse(fs.readFileSync(file, 'utf8'));
      const names = ['available', 'on_hand', 'committed', 'quality_control', 'damaged', 'reserved', 'safety_stock'];
      if (Number.isFinite(Date.parse(saved.at)) && saved.mapping && Array.isArray(saved.items) && saved.items.every(i => i.id && i.product?.handle && Array.isArray(i.levels) && i.levels.every(l => names.every(k => Number.isSafeInteger(l[k]))))) display = saved;
    } catch (_) { /* A missing or invalid display cache requires an initial read. */ }
  }
  function save(value) {
    if (!file) return;
    const temporary = file + '.' + crypto.randomUUID() + '.tmp';
    try {
      fs.mkdirSync(path.dirname(file), { recursive: true });
      fs.writeFileSync(temporary, JSON.stringify(value), { mode: 0o600 });
      fs.renameSync(temporary, file);
    } catch (_) { try { fs.unlinkSync(temporary); } catch (_) {} }
  }
  async function snapshot(force = false) {
    if (!force && current && now() - Date.parse(current.at) < ttl) return current;
    if (inflight) {
      await inflight;
      // Joining a current refresh is sufficient; do not queue a second full
      // catalogue read for every simultaneous caller requesting fresh stock.
      return snapshot(false);
    }
    const version = generation;
    const pending = Promise.resolve().then(fetchSnapshot).then(value => {
      if (version === generation) { current = display = value; error = null; retryAt = 0; save(value); }
      return value;
    }).catch(e => { error = e.message; retryAt = now() + retryDelay; throw e; });
    inflight = pending;
    let value;
    try { value = await pending; } finally { if (inflight === pending) inflight = null; }
    if (version !== generation) return snapshot(force);
    return value;
  }
  function read(force = false) {
    if (!inflight && (force || now() >= retryAt && (!current || now() - Date.parse(current.at) >= ttl))) snapshot(force).catch(() => {});
    return { snapshot: display || null, refreshing: !!inflight, stale: !current || !!inflight || now() - Date.parse(current.at) >= ttl, refreshError: error };
  }
  function invalidate() { generation++; current = null; error = null; retryAt = 0; }
  return { snapshot, read, invalidate };
}
module.exports = { createSnapshotCache };
