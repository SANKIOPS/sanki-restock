'use strict';

const key = value => String(value || '').trim().toLowerCase();

// Read-only reuse of approved PERSONAL classifications. This is not a category
// migration: the reviewed shared catalogue and all historical records stay put.
function personalCategoryChoices(store, { base, retiredNames = [], types = [] }) {
  const choices = new Map(base.map(category => [key(category.name), { ...category }]));
  const retired = new Set(retiredNames.map(key));
  const removals = new Map();
  for (const entry of store.auditLog || []) {
    if (!['CATEGORY_REMOVED', 'CATEGORY_CREATED'].includes(entry.action)) continue;
    const name = key(entry.subjectId || entry.before?.name || entry.after?.name);
    if (name) removals.set(name, entry.action === 'CATEGORY_REMOVED');
  }
  const custom = new Map(Object.entries(store.customLedgers || {}).map(([name, value]) => [key(name), value]));
  const overrides = new Map(Object.entries(store.ledgerOverrides || {}).map(([name, value]) => [key(name), value]));
  // Deterministic ordering: newest approved metadata wins, never a pending edit.
  const approved = Object.values(store.expenses || {}).filter(expense =>
    expense && String(expense.nature || '').toUpperCase() === 'PERSONAL' &&
    ['approved', 'partially_paid', 'paid'].includes(expense.status) &&
    expense.approvedBy && expense.approvedAt
  ).sort((a, b) => String(b.approvedAt).localeCompare(String(a.approvedAt)) || String(a.id).localeCompare(String(b.id)));
  for (const expense of approved) {
    const name = String(expense.ledger || '').trim(), normalized = key(name);
    if (!name || name.length > 120 || choices.has(normalized) || retired.has(normalized) || removals.get(normalized)) continue;
    const master = custom.get(normalized) || {}, override = overrides.get(normalized) || {};
    if (master.active === false || master.deleted === true || master.disabled === true) continue;
    const type = [override.type, master.type, expense.type, 'variable'].find(value => types.includes(value)) || 'variable';
    choices.set(normalized, { name, nature: 'PERSONAL', group: String(master.group || expense.categoryGroup || ''), type });
  }
  return [...choices.values()].sort((a, b) => a.group.localeCompare(b.group) || a.name.localeCompare(b.name));
}

module.exports = { personalCategoryChoices };
