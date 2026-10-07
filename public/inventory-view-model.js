(function (root, factory) {
  if (typeof module === 'object' && module.exports) module.exports = factory();
  else root.SankiInventoryView = factory();
})(typeof window === 'undefined' ? this : window, function () {
  'use strict';
  function visibleVariants(product, mode) {
    return (product.variants || []).filter(function (v) {
      if (mode === 'outOfStock') return Number(v.availableQty || 0) <= 0;
      // Explicit care views must still show the pieces set aside from sale.
      if (mode === 'cleaning') return Number(v.cleaningQty) > 0;
      if (mode === 'miscellaneous') return Number(v.notForSaleQty) > 0;
      if (mode === 'other') return Number(v.otherUnavailableQty || 0) + Number(v.committedQty || 0) > 0;
      if (mode === 'display') return Number(v.displayQty) > 0;
      if (mode === 'warehouse') return Number(v.warehouseQty) > 0;
      return Number(v.availableQty) > 0;
    });
  }
  function includes(product, mode) {
    if (mode === 'funky' && product.collection !== 'SANKI Funky') return false;
    if (mode === 'casuals' && product.collection !== 'SANKI Casuals') return false;
    return visibleVariants(product, mode).length > 0;
  }
  function listProduct(product, mode) {
    var variants = visibleVariants(product, mode), view = Object.assign({}, product, { variants: variants, hiddenSkuCount: (product.variants || []).length - variants.length });
    if (mode === 'outOfStock') {
      ['displayQty','warehouseQty','otherQty','availableQty','cleaningQty','notForSaleQty','otherUnavailableQty','committedQty','totalQty'].forEach(function (key) {
        view[key] = variants.reduce(function (n, v) { return n + Number(v[key] || 0); }, 0);
      });
    }
    return view;
  }
  return { visibleVariants: visibleVariants, includes: includes, listProduct: listProduct };
});
