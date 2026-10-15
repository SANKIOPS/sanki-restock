'use strict';

function normalized(value) {
  return String(value || '').normalize('NFKC').replace(/[\u200B-\u200D\u2060\uFEFF]/g, '').toLowerCase().replace(/\s+/g, ' ').trim();
}
function compact(value) { return normalized(value).replace(/[^\p{L}\p{N}]/gu, ''); }
function fuzzyIncludes(value, query) {
  const text = normalized(value), needle = normalized(query);
  if (!needle || text.includes(needle)) return true;
  let matched = 0;
  for (const character of text) {
    if (character === needle[matched]) matched += 1;
    if (matched === needle.length) return true;
  }
  return false;
}

// Keep broad particulars/reference searching, but never let a weak description
// match outrank a vendor whose name is exactly what the user entered.
function vendorSearchRank(vendor, query) {
  const needle = normalized(query);
  if (!needle) return 0;
  const name = normalized(vendor && vendor.name), compactName = compact(name), compactNeedle = compact(needle);
  if (name === needle) return 0;
  if (compactNeedle && compactName === compactNeedle) return 1;
  if (name.startsWith(needle)) return 2;
  if (compactNeedle && compactName.startsWith(compactNeedle)) return 3;
  if (name.split(' ').includes(needle)) return 4;
  if (name.includes(needle)) return 5;
  if (fuzzyIncludes(name, needle)) return 6;
  if ((vendor && vendor.entities || []).some(entity => fuzzyIncludes(entity, needle))) return 7;
  if ((vendor && vendor.ledgerRows || []).some(row =>
    fuzzyIncludes(row.particulars, needle) || fuzzyIncludes(row.type, needle) ||
    normalized(row.reference).includes(needle) || String(row.in || '').includes(needle) || String(row.out || '').includes(needle))) return 8;
  return Infinity;
}

function compareVendorSearch(a, b, query) {
  return vendorSearchRank(a, query) - vendorSearchRank(b, query) || String(a.name || '').localeCompare(String(b.name || ''));
}

module.exports = { vendorSearchRank, compareVendorSearch };
