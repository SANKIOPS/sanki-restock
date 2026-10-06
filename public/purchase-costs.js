(function(root, factory) {
  if (typeof module === 'object' && module.exports) module.exports = factory();
  else root.PurchaseCosts = factory();
})(typeof globalThis !== 'undefined' ? globalThis : this, function() {
  'use strict';
  var positive = function(value) { return Math.max(0, Number(value) || 0); };
  return function(po, settings) {
    settings = settings || {};
    var rows = po.lines || [], qty = rows.reduce(function(sum, line) { return sum + positive(line.qty); }, 0);
    var rate = positive(po.exRate != null ? po.exRate : settings.exRate);
    var freight = positive(po.freightPerGram != null ? po.freightPerGram : settings.freightPerGram);
    var charges = (positive(po.localTransportYuan) + positive(po.otherCostsYuan)) * rate;
    var transport = po.origin === 'india' ? positive(po.transportTotal) : 0;
    var exact = rows.map(function(line) {
      var count = positive(line.qty);
      var goods = positive(line.perPcsYuan) * (po.origin === 'india' ? 1 : rate);
      var delivery = po.origin === 'india' ? 0 : positive(line.weightGrams) * freight;
      var perPiece = goods + delivery + (qty ? (transport + charges) / qty : 0);
      return { qty:count, perPiece:count ? perPiece : 0, exact:count * perPiece };
    });
    var total = Math.round(exact.reduce(function(sum, row) { return sum + row.exact; }, 0));
    var allocated = exact.map(function(row) { return Math.floor(row.exact); });
    var remainder = total - allocated.reduce(function(sum, value) { return sum + value; }, 0);
    var rank = exact.map(function(row, index) { return { index:index, fraction:row.exact - allocated[index], qty:row.qty }; })
      .filter(function(row) { return row.qty > 0; }).sort(function(a,b) { return b.fraction - a.fraction || a.index - b.index; });
    for (var i = 0; i < remainder; i++) allocated[rank[i % rank.length].index]++;
    return { qty:qty, total:total, charges:charges, transport:transport, lines:exact.map(function(row, index) {
      return { qty:row.qty, perPiece:row.perPiece, amount:allocated[index] };
    }) };
  };
});
