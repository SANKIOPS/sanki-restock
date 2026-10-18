'use strict';
const crypto = require('node:crypto');
const previewSecret = crypto.randomBytes(32);

// Groups are display labels, not new financial ledgers. Keep an independent
// list so an empty group survives until its first category is added.
function cleanGroupName(value) {
  if (typeof value !== 'string' || /[\p{C}\u2028\u2029]/u.test(value)) return '';
  const name = value.trim().replace(/\s+/gu, ' ');
  return name && name.length <= 120 ? name : '';
}
function groupKey(value) { return cleanGroupName(value).toLowerCase(); }
function resolveGroupName(store, value) {
  let name = cleanGroupName(value), key = groupKey(name);
  const aliases = new Map(Object.entries(store.categoryGroupAliases || {})), seen = new Set();
  while (name && aliases.has(key) && !seen.has(key)) {
    seen.add(key); name = cleanGroupName(aliases.get(key)); key = groupKey(name);
  }
  return name;
}
function categoryRows(store, categories = []) {
  const seen = new Set();
  // The current picker/catalog is authoritative. A legacy custom-ledger cache
  // for the same category must not manufacture a second category or group.
  return categories.concat(Array.isArray(store.sankiCategoryCatalog && store.sankiCategoryCatalog.categories) ? store.sankiCategoryCatalog.categories : [], Object.values(store.customLedgers || {})).filter(category => {
    if (!category || typeof category.name !== 'string') return false;
    const key = category.name.trim().toLowerCase();
    if (!key || seen.has(key)) return false;
    seen.add(key); return true;
  });
}
function entries(store) {
  return Object.entries(store.expenses || {}).map(([id, record]) => ({ id, collection: 'expenses', record })).concat((store.reconciliationExpenses || []).map((record, index) => ({ id: record.id || String(index), collection: 'reconciliationExpenses', record })));
}
function categoryGroupNames(store, categories = []) {
  const seen = new Set(), groups = [], rows = categoryRows(store, categories), categoryNames = new Set(rows.map(category => category.name.trim().toLowerCase()));
  for (const value of rows.map(category => category.group).concat(Array.isArray(store.customCategoryGroups) ? store.customCategoryGroups : [], entries(store).filter(entry => !categoryNames.has(String(entry.record.ledger || entry.record.category || '').trim().toLowerCase())).map(entry => entry.record.categoryGroup))) {
    const name = resolveGroupName(store, value), key = groupKey(name);
    if (!name || seen.has(key)) continue;
    seen.add(key); groups.push(name);
  }
  return groups;
}
function revision(store) { return crypto.createHash('sha256').update(JSON.stringify(store)).digest('hex'); }
function canonicalGroup(store, value, categories = []) {
  const name = resolveGroupName(store, value);
  return categoryGroupNames(store, categories).find(group => groupKey(group) === groupKey(name)) || '';
}
function catalog(store, categories = []) {
  const groups = categoryGroupNames(store, categories).map(name => ({ name, categoryCount: 0, entryCount: 0, businesses: [], categories: [] }));
  const byGroup = new Map(groups.map(group => [groupKey(group.name), group])), byCategory = new Map();
  for (const row of categoryRows(store, categories)) {
    if (!row || typeof row.name !== 'string') continue;
    const key = row.name.trim().toLowerCase(), group = byGroup.get(groupKey(resolveGroupName(store, row.group)));
    if (!group) continue;
    if (!byCategory.has(key)) byCategory.set(key, group);
    if (!group.categories.some(category => category.name.trim().toLowerCase() === key)) group.categories.push({ name: row.name, entryCount: 0, businesses: [] });
  }
  for (const { record } of entries(store)) {
    const categoryName = String(record.ledger || record.category || '').trim(), key = categoryName.toLowerCase();
    const group = byCategory.get(key) || byGroup.get(groupKey(resolveGroupName(store, record.categoryGroup)));
    if (!group) continue;
    const business = String(record.nature || 'SANKI').trim().toUpperCase();
    group.entryCount += 1;
    if (!group.businesses.includes(business)) group.businesses.push(business);
    let category = group.categories.find(category => category.name.trim().toLowerCase() === key);
    if (!category && categoryName) { category = { name: categoryName, entryCount: 0, businesses: [] }; group.categories.push(category); }
    if (category) { category.entryCount += 1; if (!category.businesses.includes(business)) category.businesses.push(business); }
  }
  for (const group of groups) {
    group.categoryCount = group.categories.length; group.businesses.sort(); group.categories.sort((a, b) => a.name.localeCompare(b.name));
    group.categories.forEach(category => category.businesses.sort());
  }
  return { revision: revision(store), groups: groups.sort((a, b) => a.name.localeCompare(b.name)) };
}
function fail(status, error, code) { const failure = new Error(error); failure.status = status; failure.code = code; throw failure; }
function input(body) {
  body = body || {};
  const action = body.action, target = cleanGroupName(body.target), sources = Array.isArray(body.sources) ? body.sources.map(cleanGroupName) : [];
  const reason = typeof body.reason === 'string' ? body.reason.trim() : '';
  if (!['rename', 'merge'].includes(action)) fail(400, 'Choose rename or merge.', 'INVALID_ACTION');
  if (!sources.length || sources.length > 100 || sources.some(name => !name) || (action === 'rename' && sources.length !== 1)) fail(400, 'Choose valid source groups; rename accepts one source.', 'INVALID_SOURCES');
  if (new Set(sources.map(groupKey)).size !== sources.length) fail(400, 'Select each source group only once.', 'DUPLICATE_SOURCES');
  if (!target) fail(400, 'Enter a final group name using printable characters (up to 120 characters).', 'INVALID_TARGET');
  if (!reason || reason.length > 1000 || /[\p{C}\u2028\u2029]/u.test(reason)) fail(400, 'Enter a printable reason for this change (up to 1000 characters).', 'INVALID_REASON');
  return { action, sources, target, reason };
}
function fingerprint(body, actor) {
  const normalized = input(body);
  normalized.sources = normalized.sources.map(groupKey).sort();
  if (normalized.action === 'merge') normalized.target = groupKey(normalized.target);
  return crypto.createHash('sha256').update(JSON.stringify({ ...normalized, actor: String(actor || '').trim().toLowerCase() })).digest('hex');
}
function planChange(store, body, categories = []) {
  const normalized = input(body), view = catalog(store, categories);
  if (body.revision !== view.revision) fail(409, 'The groups or accounting records changed. Refresh and review a new preview before confirming.', 'STALE_PREVIEW');
  const sourceGroups = normalized.sources.map(source => view.groups.find(group => groupKey(group.name) === groupKey(source)));
  if (sourceGroups.some(group => !group)) fail(409, 'A source group is no longer available. Refresh the group list.', 'SOURCE_CHANGED');
  const existingTarget = view.groups.find(group => groupKey(group.name) === groupKey(normalized.target));
  let target = normalized.target;
  if (normalized.action === 'rename') {
    if (existingTarget && groupKey(existingTarget.name) !== groupKey(sourceGroups[0].name)) fail(409, 'That group already exists. Use Merge to combine these groups instead.', 'TARGET_EXISTS');
    if (!existingTarget && groupKey(resolveGroupName(store, target)) !== groupKey(target)) fail(409, 'That name belongs to a previously renamed or merged group. Use its current final group instead.', 'TARGET_RETIRED');
  } else {
    if (!existingTarget) fail(400, 'Choose an existing final group for the merge.', 'TARGET_MISSING');
    if (sourceGroups.some(group => groupKey(group.name) === groupKey(existingTarget.name))) fail(400, 'The final group cannot also be a source group.', 'TARGET_IS_SOURCE');
    target = existingTarget.name;
  }
  const uniqueCategories = new Map();
  sourceGroups.forEach(group => group.categories.forEach(category => { if (!uniqueCategories.has(category.name.toLowerCase())) uniqueCategories.set(category.name.toLowerCase(), category); }));
  const preview = { revision: view.revision, action: normalized.action, sources: sourceGroups.map(group => group.name), target, reason: normalized.reason, categoryCount: uniqueCategories.size, entryCount: sourceGroups.reduce((sum, group) => sum + group.entryCount, 0), businesses: Array.from(new Set(sourceGroups.flatMap(group => group.businesses))).sort(), categories: Array.from(uniqueCategories.values()).sort((a, b) => a.name.localeCompare(b.name)), warnings: ['Categories stay separate. Only group labels and organization change; amounts, dates, payments and reconciliations stay unchanged.'], already: normalized.action === 'rename' && sourceGroups[0].name === target };
  preview.previewToken = crypto.createHmac('sha256', previewSecret).update(JSON.stringify(preview)).digest('hex');
  return preview;
}
function financialInvariant(store) {
  const copy = JSON.parse(JSON.stringify(store));
  for (const key of ['customCategoryGroups', 'categoryGroupAliases', 'categoryGroupOperations', 'auditLog', 'auditSeq']) delete copy[key];
  for (const category of Object.values(copy.customLedgers || {})) if (category && typeof category === 'object') delete category.group;
  for (const category of copy.sankiCategoryCatalog && copy.sankiCategoryCatalog.categories || []) delete category.group;
  for (const { record } of entries(copy)) delete record.categoryGroup;
  return JSON.stringify(copy);
}
function classificationSnapshot(store, preview, categories = []) {
  const sources = new Set(preview.sources.map(groupKey)), definitions = categoryRows(store, categories);
  const assignments = new Map(definitions.map(category => [category.name.trim().toLowerCase(), resolveGroupName(store, category.group)]));
  const changedCategories = [];
  const captureCategory = (category, collection, key) => {
    if (!category) return;
    const authoritative = assignments.get(String(category.name || '').trim().toLowerCase());
    if (!sources.has(groupKey(authoritative)) && !sources.has(groupKey(resolveGroupName(store, category.group)))) return;
    changedCategories.push({ collection, key, name: category.name, hasGroup: Object.hasOwn(category, 'group'), group: Object.hasOwn(category, 'group') ? category.group : null });
  };
  Object.entries(store.customLedgers || {}).forEach(([key, category]) => captureCategory(category, 'customLedgers', key));
  (store.sankiCategoryCatalog && store.sankiCategoryCatalog.categories || []).forEach((category, index) => captureCategory(category, 'sankiCategoryCatalog.categories', index));
  const changedEntries = entries(store).filter(({ record }) => {
    const group = assignments.get(String(record.ledger || record.category || '').trim().toLowerCase()) || resolveGroupName(store, record.categoryGroup);
    return sources.has(groupKey(group));
  }).map(({ id, collection, record }) => ({ collection, id, hasCategoryGroup: Object.hasOwn(record, 'categoryGroup'), categoryGroup: Object.hasOwn(record, 'categoryGroup') ? record.categoryGroup : null }));
  // Recovery evidence deliberately contains classification metadata only. It
  // cannot be used to roll back later payments, amounts, dates or bank links.
  return JSON.parse(JSON.stringify({
    customCategoryGroupsPresent: Object.hasOwn(store, 'customCategoryGroups'),
    customCategoryGroups: Array.isArray(store.customCategoryGroups) ? store.customCategoryGroups : null,
    categoryGroupAliasesPresent: Object.hasOwn(store, 'categoryGroupAliases'),
    categoryGroupAliases: store.categoryGroupAliases || null,
    categoryAssignments: definitions.filter(category => sources.has(groupKey(resolveGroupName(store, category.group)))).map(category => ({ name: category.name, group: resolveGroupName(store, category.group) })),
    categories: changedCategories,
    entries: changedEntries
  }));
}
function applyChange(store, preview, categories = []) {
  const before = financialInvariant(store), sourceKeys = new Set(preview.sources.map(groupKey)), definitions = categoryRows(store, categories), categoryGroups = new Map();
  for (const category of definitions) if (category && typeof category.name === 'string' && !categoryGroups.has(category.name.toLowerCase())) categoryGroups.set(category.name.toLowerCase(), groupKey(resolveGroupName(store, category.group)));
  for (const category of Object.values(store.customLedgers || {}).concat(store.sankiCategoryCatalog && store.sankiCategoryCatalog.categories || [])) {
    if (!category) continue;
    const authoritative = definitions.find(definition => definition.name.trim().toLowerCase() === String(category.name || '').trim().toLowerCase());
    const currentGroup = authoritative && resolveGroupName(store, authoritative.group);
    if (sourceKeys.has(groupKey(currentGroup))) category.group = preview.target;
    else if (sourceKeys.has(groupKey(resolveGroupName(store, category.group)))) category.group = currentGroup || preview.target;
  }
  for (const { record } of entries(store)) {
    const key = categoryGroups.get(String(record.ledger || record.category || '').toLowerCase()) || groupKey(resolveGroupName(store, record.categoryGroup));
    if (sourceKeys.has(key)) record.categoryGroup = preview.target;
  }
  store.customCategoryGroups = categoryGroupNames(store, categories).filter(name => !sourceKeys.has(groupKey(name)));
  if (!store.customCategoryGroups.some(name => groupKey(name) === groupKey(preview.target))) store.customCategoryGroups.push(preview.target);
  const aliases = new Map(Object.entries(store.categoryGroupAliases || {}));
  for (const [key, value] of aliases) if (sourceKeys.has(groupKey(resolveGroupName(store, value)))) aliases.set(key, preview.target);
  // Same-key aliases retain the final display casing for legacy/default
  // category providers that cannot themselves be rewritten in this store.
  for (const source of preview.sources) aliases.set(groupKey(source), preview.target);
  store.categoryGroupAliases = Object.fromEntries(aliases);
  if (financialInvariant(store) !== before) throw new Error('Group management attempted to change non-group accounting data. No changes were saved.');
  return { categoryCount: preview.categoryCount, entryCount: preview.entryCount, businesses: preview.businesses };
}
module.exports = { cleanGroupName, groupKey, categoryGroupNames, resolveGroupName, canonicalGroup, catalog, revision, input, fingerprint, planChange, applyChange, financialInvariant, classificationSnapshot };
