'use strict';

// A read-only projection of verified physical-bank cash flow. Imported reports,
// invoices, salary entitlements and allocation records remain source evidence.
// This module creates no adjustment, receipt, payment or additional store write.
const text = value => String(value == null ? '' : value).trim();
const list = value => Array.isArray(value) ? value : [];
const accountName = value => text(value).replace(/\s+/g, ' ').toLowerCase();
const own = (value, name) => Object.prototype.hasOwnProperty.call(value || {}, name);

function key(account, nature, movementId) {
  // SAMAST/PERSONAL can share one physical account. Entity is provenance, not
  // permission to apply the same physical movement twice under another key.
  return JSON.stringify([accountName(account), text(movementId)]);
}
function cents(value) {
  if (value == null || value === '') return 0;
  if (typeof value === 'boolean' || !Number.isFinite(Number(value))) throw Error('Invalid bank movement amount.');
  const amount = Number(value), result = Math.round(amount * 100);
  if (!Number.isSafeInteger(result) || Math.abs(amount * 100 - result) > .00001) throw Error('Bank movement amounts require precise paise.');
  return result;
}
function snapshot(row) {
  const value = { debit: cents(row && row.debit), credit: cents(row && row.credit), date: text(row && row.date) };
  if (value.debit < 0 || value.credit < 0 || !/^\d{4}-\d{2}-\d{2}$/.test(value.date)) throw Error('Invalid bank movement direction or date.');
  const date = new Date(value.date + 'T00:00:00Z');
  if (!Number.isFinite(date.getTime()) || date.toISOString().slice(0, 10) !== value.date) throw Error('Invalid bank movement date.');
  return value;
}
const same = (left, right) => left.debit === right.debit && left.credit === right.credit && left.date === right.date;
const sourceSnapshot = row => snapshot(row && row.bankDateOverride && row.originalDate ? { ...row, date: row.originalDate } : row);
const net = row => cents(row.credit) - cents(row.debit);
const amountFields = row => ({ debit: row.debit / 100, credit: row.credit / 100, date: row.date });
function members(row) {
  return [...new Set([...list(row && row.sourceIds), ...list(row && row.linkedEntryIds),
    ...list(row && row.linkedEntries).map(entry => entry && entry.id)].filter(id => typeof id === 'string' && text(id)).map(text))];
}
function collect(store, account, conflicts) {
  const records = new Map(), duplicates = new Set();
  for (const [storedKey, record] of Object.entries(store && store.bankMovementAuthority || {})) {
    if (!record || typeof record !== 'object' || accountName(record.account) !== accountName(account)) continue;
    const id = text(record.movementId);
    try {
      if (!id || storedKey !== key(account, record.nature, id)) throw Error('Bank authority identity does not agree with its physical account and movement.');
      const original = snapshot(record.original), after = snapshot(record.after);
      if ((!after.debit) === (!after.credit) || !(original.debit || original.credit) ||
        Math.sign(after.credit - after.debit) !== Math.sign(original.credit - original.debit)) throw Error('Bank authority must retain one positive movement in the original money direction.');
      if (records.has(id)) { duplicates.add(id); throw Error('More than one bank authority record claims this movement.'); }
      records.set(id, { ...record, movementId: id, _original: original, _after: after, _members: members(record) });
    } catch (error) {
      conflicts.push({ type: 'invalid_bank_movement_authority', movementId: id, planId: text(record.planId), message: error.message });
    }
  }
  for (const id of duplicates) records.delete(id);
  return records;
}
function metadata(records, sourceChanged) {
  const planIds = [...new Set(records.map(record => text(record.planId)).filter(Boolean))];
  return { planId: planIds.length === 1 ? planIds[0] : '', planIds,
    movementId: records.length === 1 ? records[0].movementId : '',
    movementIds: records.map(record => record.movementId), sourceChanged: !!sourceChanged };
}
function alreadyProjected(row, records) {
  const marker = row && row.bankMovementAuthority;
  return marker && records.every(record => list(marker.planIds).includes(text(record.planId)) && list(marker.movementIds).includes(record.movementId));
}
function sourceConflict(conflicts, record, reason = 'The original source amount or date changed after bank finalization. Verified bank cash flow is retained; review the source change.') {
  conflicts.push({ type: 'bank_movement_authority_source_changed', movementId: record.movementId, planId: text(record.planId), sourceChanged: true, message: reason });
}
function canonicalRow(record, sourceIds, sourceChanged) {
  return { id: record.movementId, ...amountFields(record._after),
    description: text(record.description || record.original && record.original.description) || 'Verified bank movement',
    reference: text(record.reference || record.bankReference || record.original && record.original.reference),
    entity: text(record.sourceNature || record.nature), sourceIds: sourceIds.slice(),
    bankMovementAuthority: metadata([record], sourceChanged) };
}
function project(store, account, nature, rows) {
  const input = list(rows), conflicts = [], records = collect(store, account, conflicts);
  const result = input.map(row => ({ ...row }));
  const removed = new Set(), consumed = new Set(), byId = new Map(), additions = [];
  input.forEach((row, index) => { const id = text(row && row.id); const indexes = byId.get(id) || []; indexes.push(index); byId.set(id, indexes); });
  const ordered = [...records.values()].sort((left, right) => right._members.length - left._members.length);
  for (const record of ordered) {
    if (consumed.has(record.movementId)) continue;
    const direct = byId.get(record.movementId) || [];
    if (direct.length > 1) {
      conflicts.push({ type: 'ambiguous_bank_movement_source', movementId: record.movementId, planId: text(record.planId), message: 'This movement is represented more than once. Review the duplicate source identity before applying a bank projection.' });
      consumed.add(record.movementId); continue;
    }
    if (direct.length === 1) {
      const index = direct[0], row = input[index];
      if (removed.has(index)) { consumed.add(record.movementId); continue; }
      let changed = false;
      try { const current = snapshot(row); changed = !(alreadyProjected(row, [record]) && same(current, record._after)) && !same(sourceSnapshot(row), record._original); }
      catch { changed = true; }
      if (changed) sourceConflict(conflicts, record);
      const currentChildIds = members(row), childIds = record._members;
      if (childIds.length !== currentChildIds.length || childIds.some(id => !currentChildIds.includes(id))) {
        changed = true;
        sourceConflict(conflicts, record, 'The source identities belonging to this aggregate changed after bank finalization. The verified bank amount and original known identities are retained.');
      }
      for (const childId of childIds) {
        const represented = byId.get(childId) || [];
        if (represented.length === 1 && represented[0] !== index) removed.add(represented[0]);
        else if (represented.length > 1) conflicts.push({ type: 'ambiguous_bank_movement_source', movementId: childId, parentMovementId: record.movementId, message: 'An aggregate member is represented more than once; its duplicate source identities require review.' });
      }
      for (const childId of childIds) if (records.has(childId)) {
        consumed.add(childId);
        conflicts.push({ type: 'overlapping_bank_movement_authority', movementId: childId, parentMovementId: record.movementId, message: 'A canonical aggregate bank movement already covers this child correction. It was not applied twice.' });
      }
      result[index] = { ...row, ...amountFields(record._after), bankMovementAuthority: metadata([record], changed || row.bankMovementAuthority && row.bankMovementAuthority.sourceChanged) };
      if (childIds.length || currentChildIds.length) result[index].sourceIds = childIds.slice();
      if (own(row, 'linkedEntryIds')) result[index].linkedEntryIds = childIds.slice();
      if (own(row, 'linkedEntries')) result[index].linkedEntries = childIds.map(id => ({ id }));
      consumed.add(record.movementId); continue;
    }

    // A source picker may expose a batch, while the account ledger exposes its
    // individual allocations. Replace only the identified members, retaining
    // their IDs; unrelated cash flow is never swallowed by the aggregate.
    if (record._members.length) {
      const covered = [], missing = [], ambiguous = [];
      for (const id of record._members) {
        const indexes = byId.get(id) || [];
        if (!indexes.length) missing.push(id);
        else if (indexes.length !== 1 || removed.has(indexes[0])) ambiguous.push(id);
        else covered.push(indexes[0]);
      }
      if (ambiguous.length) {
        conflicts.push({ type: 'ambiguous_bank_movement_members', movementId: record.movementId, sourceIds: ambiguous, message: 'Aggregate members overlap another projection or duplicate source. No additional movement was created.' });
        consumed.add(record.movementId); continue;
      }
      let changed = missing.length > 0;
      if (covered.length) {
        try {
          const total = covered.reduce((sum, index) => { const row = sourceSnapshot(input[index]); sum.debit += row.debit; sum.credit += row.credit; if (row.date > sum.date) sum.date = row.date; return sum; }, { debit: 0, credit: 0, date: '' });
          changed = changed || !same(total, record._original);
        } catch { changed = true; }
      } else changed = true;
      if (changed) sourceConflict(conflicts, record, missing.length ? 'One or more original batch members are no longer present. The finalized canonical bank movement and member identities are retained.' : undefined);
      for (const index of covered) removed.add(index);
      for (const id of record._members) consumed.add(id);
      const aggregate = canonicalRow(record, record._members, changed);
      if (covered.length) { const anchor = Math.min(...covered); removed.delete(anchor); result[anchor] = aggregate; }
      else additions.push(aggregate);
      consumed.add(record.movementId); continue;
    }

    // Child corrections are handled together below, because one rendered row
    // may represent several original movements.
    const parents = input.map((row, index) => ({ row, index })).filter(item => !removed.has(item.index) && members(item.row).includes(record.movementId));
    if (parents.length === 1) continue;
    if (parents.length > 1) {
      conflicts.push({ type: 'ambiguous_bank_movement_members', movementId: record.movementId, message: 'The child movement belongs to more than one rendered aggregate. No duplicate bank projection was added.' });
      consumed.add(record.movementId); continue;
    }
    sourceConflict(conflicts, record, 'The original source movement is no longer present. The finalized bank amount and date are retained without creating another accounting entry.');
    additions.push({ ...canonicalRow(record, [], true), bankSourceMissing: true });
    consumed.add(record.movementId);
  }

  input.forEach((row, index) => {
    if (removed.has(index) || records.has(text(row.id))) return;
    const ids = members(row), children = ids.map(id => records.get(id)).filter(record => record && !consumed.has(record.movementId));
    if (!children.length) return;
    if (alreadyProjected(row, children)) { children.forEach(record => consumed.add(record.movementId)); return; }
    let changed = false;
    try {
      const base = snapshot(row), after = { ...base }, dates = new Set();
      for (const record of children) {
        const explicit = list(row.linkedEntries).filter(entry => text(entry && entry.id) === record.movementId && (entry.debit != null || entry.credit != null));
        const original = explicit.length === 1 ? sourceSnapshot(explicit[0]) : record._original;
        if (explicit.length === 1 && !same(original, record._original)) { changed = true; sourceConflict(conflicts, record); }
        after.debit += record._after.debit - original.debit;
        after.credit += record._after.credit - original.credit;
        dates.add(record._after.date);
      }
      if (after.debit < 0 || after.credit < 0) throw Error('The aggregate no longer covers its original member amounts.');
      if (children.length === ids.length && dates.size === 1) after.date = [...dates][0];
      else if (children.some(record => record._after.date !== record._original.date)) {
        conflicts.push({ type: 'bank_movement_aggregate_date_spread', movementId: text(row.id), sourceIds: ids,
          message: 'Only part of this aggregate changed bank date. Its source identities remain grouped; review the individual dates before using this aggregate as one bank transaction.' });
      }
      const previous = row.bankMovementAuthority || {}, marker = metadata(children, changed || previous.sourceChanged);
      marker.planIds = [...new Set([...list(previous.planIds), ...marker.planIds])];
      marker.movementIds = [...new Set([...list(previous.movementIds), ...marker.movementIds])];
      result[index] = { ...row, ...amountFields(after), sourceIds: ids,
        bankMovementAuthority: marker, ...(own(row, 'linkedEntries') ? { linkedEntries: ids.map(id => ({ id })) } : {}) };
    } catch (error) {
      conflicts.push({ type: 'bank_movement_aggregate_source_changed', movementId: text(row.id), sourceIds: ids, sourceChanged: true, message: error.message + ' Nothing was silently clamped or counted twice.' });
    }
    children.forEach(record => consumed.add(record.movementId));
  });
  const output = result.filter((row, index) => !removed.has(index)).concat(additions);
  Object.defineProperty(output, 'bankMovementAuthorityConflicts', { value: conflicts, enumerable: false });
  return output;
}
function delta(store, account, nature, opts = {}) {
  const within = row => (!opts.from || text(row.date) >= opts.from) && (!opts.to || text(row.date) <= opts.to);
  const total = rows => rows.filter(within).reduce((sum, row) => sum + net(row), 0);
  if (Array.isArray(opts.sourceRows)) {
    const projected = project(store, account, nature, opts.sourceRows);
    let difference = total(projected) - total(opts.sourceRows);
    const records = collect(store, account, []);
    // A compact batch row cannot display two dates at once. Its warning is
    // explicit, but period balances can still place each corrected member on
    // its exact bank date without moving the unrelated remainder of the batch.
    for (const row of projected) for (const id of list(row.bankMovementAuthority && row.bankMovementAuthority.movementIds)) {
      const record = records.get(id);
      if (!record || text(row.id) === id || row.date === record._after.date) continue;
      const actual = record._after.credit - record._after.debit;
      if (within(row)) difference -= actual;
      if (within(record.after)) difference += actual;
    }
    return difference / 100;
  }
  const records = collect(store, account, []), children = new Set([...records.values()].flatMap(record => record._members));
  let difference = 0;
  for (const record of records.values()) {
    if (children.has(record.movementId)) continue;
    if (within(record.after)) difference += record._after.credit - record._after.debit;
    if (within(record.original)) difference -= record._original.credit - record._original.debit;
  }
  return difference / 100;
}

module.exports = { key, project, delta };
