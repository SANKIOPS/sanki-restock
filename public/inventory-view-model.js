(function (root, factory) {
  if (typeof module === 'object' && module.exports) module.exports = factory();
  else root.SankiInventoryView = factory();
})(typeof window === 'undefined' ? this : window, function () {
  'use strict';
  var balanceKeys = ['displayQty','warehouseQty','otherQty','availableQty','cleaningQty','notForSaleQty','otherUnavailableQty','committedQty','totalQty'];
  // A misnamed Shopify colour option can contain a clothing size. Only reject
  // complete size labels; custom shades such as "Blue 2" remain valid colours.
  var sizeToken = '(?:x{0,4}[sl]|m|[2-9]x[sl]|fs|os|o/s|small|medium|large|extra[ -]?(?:small|large)|\\d{1,3}(?:\\.\\d)?)';
  var sizeRange = sizeToken + '(?:\\s*(?:[-\\u2013\\u2014/]|to)\\s*' + sizeToken + ')*';
  var sizeOnly = new RegExp('^(?:size\\s*:?\\s*)?(?:' + sizeRange + '|(?:one|free|universal)\\s*size(?:\\s+fits?\\s+all|\\s*\\(\\s*(?:fits?\\s+)?' + sizeRange + '\\s*\\))?)$', 'i');
  function colourLabel(value) {
    var label = String(value == null ? '' : value).trim().replace(/\s+/g, ' ');
    if (!label || /^(see product|see variant|default title|default|n\/?a|none|unknown|not specified)$/i.test(label) || sizeOnly.test(label)) return '';
    label = label.toLowerCase().replace(/\bgray\b/g, 'grey').replace(/\s*([/&])\s*/g, ' $1 ');
    if (/^(multi[ -]?colou?r(?:ed)?|multicolou?r)$/.test(label)) return 'Multicolour';
    if (/^off[ -]?white$/.test(label)) return 'Off White';
    return label.replace(/(^|[\s/,&-])([a-z])/g, function (_, separator, letter) { return separator + letter.toUpperCase(); });
  }
  function variantColour(variant, product) {
    var options = variant.selectedOptions || [], option = options.find(function (o) { return /^(colou?r|shade)$/i.test(String(o.name || '').trim()); });
    var explicit = colourLabel(option && option.value) || colourLabel(variant.colour) || colourLabel(variant.color);
    if (explicit) return explicit;
    // Older retained snapshots do not contain named Shopify options. Recognise
    // a colour token in their variant title without treating a size as a colour.
    var colours = /^(black|white|off[ -]?white|cream|ivory|beige|brown|tan|camel|grey|gray|charcoal|blue|navy(?: blue)?|light blue|dark blue|sky blue|green|olive(?: green)?|mehndi green|mint(?: green)?|red|maroon|burgundy|pink|peach|purple|lavender|yellow|mustard|orange|rust|teal|turquoise|silver|gold|multicolou?r|multi[ -]?colou?red?)$/i;
    var tokens = String(variant.variant || '').split(/\s*\/\s*/).filter(function (part) { return colours.test(part.trim()); }).map(colourLabel);
    if (tokens.length) return Array.from(new Set(tokens)).join(' / ');
    // A size-only variant may inherit a single-colour product title. Multiple
    // colours stay combined so we cannot claim a particular colour for its SKU.
    var title = String(product && product.title || ''), found = [], pattern = /\b(off[ -]?white|light blue|dark blue|sky blue|navy blue|olive green|mehndi green|mint green|black|white|cream|ivory|beige|brown|tan|camel|grey|gray|charcoal|blue|navy|green|olive|red|maroon|burgundy|pink|peach|purple|lavender|yellow|mustard|orange|rust|teal|turquoise|silver|gold|multicolou?r)\b/gi, match;
    while ((match = pattern.exec(title))) { var label = colourLabel(match[0]); if (found.indexOf(label) < 0) found.push(label); }
    return found.join(' / ');
  }
  function colourVariants(product, colour) {
    var selected = colourLabel(colour);
    return (product.variants || []).filter(function (variant) { return !selected || variantColour(variant, product) === selected; });
  }
  function visibleVariants(product, mode, colour) {
    return colourVariants(product, colour).filter(function (v) {
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
  function includes(product, mode, colour) {
    if (mode === 'funky' && product.collection !== 'SANKI Funky') return false;
    if (mode === 'casuals' && product.collection !== 'SANKI Casuals') return false;
    return visibleVariants(product, mode, colour).length > 0;
  }
  function listProduct(product, mode, colour) {
    var variants = visibleVariants(product, mode, colour), view = Object.assign({}, product, { variants: variants, hiddenSkuCount: (product.variants || []).length - variants.length });
    if (mode === 'outOfStock' || colourLabel(colour)) {
      // Owned totals include cleaning/committed pieces of the chosen colour.
      // The explicit out-of-stock view retains its unavailable-only totals.
      var balances = mode === 'outOfStock' ? variants : colourVariants(product, colour);
      balanceKeys.forEach(function (key) {
        view[key] = balances.reduce(function (n, v) { return n + Number(v[key] || 0); }, 0);
      });
    }
    return view;
  }
  function colours(product, mode) {
    return visibleVariants(product, mode).map(function (variant) { return variantColour(variant, product); }).filter(Boolean);
  }
  return { colourLabel: colourLabel, variantColour: variantColour, colours: colours, visibleVariants: visibleVariants, includes: includes, listProduct: listProduct };
});
