'use strict';

function normalizeSku(value) {
  return String(value || '').toUpperCase().replace(/\s+/g, '').trim();
}

function canonicalSku(value, aliases) {
  let sku = normalizeSku(value);
  const seen = new Set();
  while (sku && aliases[sku] && !seen.has(sku)) {
    seen.add(sku);
    sku = normalizeSku(aliases[sku]);
  }
  return sku;
}

function normalizeDesired(rows, aliases) {
  const desired = new Map();
  for (const row of rows || []) {
    const sku = canonicalSku(row && row.sku, aliases);
    if (!sku) continue;
    const frontQty = Number(row.frontQty);
    const backQty = Number(row.backQty);
    if (!Number.isInteger(frontQty) || frontQty < 0 || !Number.isInteger(backQty) || backQty < 0) {
      throw new Error(`Invalid quantities for ${sku}`);
    }
    const current = desired.get(sku) || { sku, frontQty: 0, backQty: 0 };
    current.frontQty += frontQty;
    current.backQty += backQty;
    desired.set(sku, current);
  }
  return desired;
}

function buildExactPlan({ variants, levels, rows, aliases = {}, frontLocationId, backLocationId, zeroUnlisted = true }) {
  const normalizedAliases = Object.fromEntries(
    Object.entries(aliases || {}).map(([from, to]) => [normalizeSku(from), normalizeSku(to)])
  );
  const desired = normalizeDesired(rows, normalizedAliases);
  const groups = new Map();
  for (const variant of variants || []) {
    const rawSku = normalizeSku(variant.sku);
    if (!rawSku || !variant.inventoryItemId) continue;
    const canonical = canonicalSku(rawSku, normalizedAliases);
    if (!groups.has(canonical)) groups.set(canonical, []);
    groups.get(canonical).push({ ...variant, rawSku, canonical });
  }

  const missing = [...desired.keys()].filter(sku => !groups.has(sku)).sort();
  const changes = [];
  const duplicateVariants = [];
  let currentFront = 0;
  let currentBack = 0;
  let desiredFront = 0;
  let desiredBack = 0;

  for (const [canonical, group] of groups) {
    group.sort((a, b) => {
      const aExact = a.rawSku === canonical ? 0 : 1;
      const bExact = b.rawSku === canonical ? 0 : 1;
      return aExact - bExact || String(a.variantId).localeCompare(String(b.variantId));
    });
    const wanted = desired.get(canonical);
    if (group.length > 1) {
      duplicateVariants.push({ canonical, variants: group.map(v => ({ sku: v.rawSku, variantId: v.variantId })) });
    }
    for (let index = 0; index < group.length; index += 1) {
      const variant = group[index];
      const current = levels[String(variant.inventoryItemId)] || {};
      const nowFront = Number(current[String(frontLocationId)] || 0);
      const nowBack = Number(current[String(backLocationId)] || 0);
      currentFront += nowFront;
      currentBack += nowBack;

      let wantFront = nowFront;
      let wantBack = nowBack;
      if (wanted) {
        wantFront = index === 0 ? wanted.frontQty : 0;
        wantBack = index === 0 ? wanted.backQty : 0;
      } else if (zeroUnlisted) {
        wantFront = 0;
        wantBack = 0;
      }
      desiredFront += wantFront;
      desiredBack += wantBack;

      if (nowFront !== wantFront) {
        changes.push({
          sku: variant.rawSku,
          canonicalSku: canonical,
          inventoryItemId: String(variant.inventoryItemId),
          variantId: String(variant.variantId || ''),
          locationId: String(frontLocationId),
          location: 'Display',
          from: nowFront,
          to: wantFront,
        });
      }
      if (nowBack !== wantBack) {
        changes.push({
          sku: variant.rawSku,
          canonicalSku: canonical,
          inventoryItemId: String(variant.inventoryItemId),
          variantId: String(variant.variantId || ''),
          locationId: String(backLocationId),
          location: 'Warehouse',
          from: nowBack,
          to: wantBack,
        });
      }
    }
  }

  return {
    changes,
    missing,
    duplicateVariants,
    totals: { currentFront, currentBack, desiredFront, desiredBack },
    desiredSkuCount: desired.size,
  };
}

module.exports = { buildExactPlan, canonicalSku, normalizeDesired, normalizeSku };
