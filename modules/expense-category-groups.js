'use strict';

// Groups are display labels, not new financial ledgers. Keep an independent
// list so an empty group survives until its first category is added.
function cleanGroupName(value) {
  if (typeof value !== 'string' || /[\p{C}\u2028\u2029]/u.test(value)) return '';
  const name = value.trim().replace(/\s+/gu, ' ');
  return name && name.length <= 120 ? name : '';
}
function groupKey(value) { return cleanGroupName(value).toLowerCase(); }
function categoryGroupNames(store, categories = []) {
  const seen = new Set(), groups = [];
  for (const value of categories.map(category => category && category.group).concat(Array.isArray(store.customCategoryGroups) ? store.customCategoryGroups : [])) {
    const name = cleanGroupName(value), key = groupKey(name);
    if (!name || seen.has(key)) continue;
    seen.add(key); groups.push(name);
  }
  return groups;
}
module.exports = { cleanGroupName, groupKey, categoryGroupNames };
