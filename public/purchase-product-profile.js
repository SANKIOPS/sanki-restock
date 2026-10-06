(function(root, factory) {
  if (typeof module === 'object' && module.exports) module.exports = factory();
  else root.PurchaseProductProfile = factory();
})(typeof globalThis !== 'undefined' ? globalThis : this, function() {
  'use strict';
  return function(group, hasBack) {
    var type = String(group.productType || '').trim().toLowerCase();
    var kind = { perfumes:'perfume', perfume:'perfume', belts:'belt', belt:'belt', bag:'bag', bags:'bag' }[type];
    var back = hasBack ? ['back'] : [];
    if (kind) return { kind:kind, productOnly:true, views:['front'].concat(back, ['detail']) };
    var audience = String(group.audience || '').toLowerCase();
    var models = audience === 'unisex' ? ['female','model-side-female','male','model-side-male']
      : ['women','men'].indexOf(audience) >= 0 ? ['model-front','model-side'] : [];
    return { kind:'clothing', productOnly:false, views:models.length ? ['front'].concat(back, models) : [] };
  };
});
