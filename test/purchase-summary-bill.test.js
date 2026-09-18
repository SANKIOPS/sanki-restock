const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const html = fs.readFileSync(require('node:path').join(__dirname, '../public/procurement.html'), 'utf8');
const source = fs.readFileSync(require('node:path').join(__dirname, '../modules/procurement.js'), 'utf8');

test('summary bill uses actual received lines and totals yuan and freight', () => {
  const start = html.indexOf('    function purchaseSummaryBill(po)');
  const end = html.indexOf('    function purchaseCostPanel(po)', start);
  const render = vm.runInNewContext(html.slice(start, end) + '\npurchaseSummaryBill;', {
    me: { canManage: true }, settings: {}, esc: String, money: n => '₹' + n, yuan: n => '¥' + n, window: {}
  });
  const po = { id: 'PO-7', status: 'received', origin: 'china', exRate: 15, freightPerGram: .5,
    billNo: 'B1', vendor: 'Vendor', vendorBill: { billNumber: 'B1', totalQuantity: 4, totalValue: 90, localTransportation: 5 },
    lines: [
      { sku: 'MISSING', qty: 0, perPcsYuan: 10, weightGrams: 100 },
      { sku: 'EXPECTED', qty: 2, perPcsYuan: 20, weightGrams: 100 },
      { sku: 'EXTRA', qty: 2, perPcsYuan: 20, weightGrams: 200, receiptAdded: { at: 'today' } }
    ] };
  const out = render(po);
  assert.match(out, /Received Extra/);
  assert.match(out, /<th class="money">4<\/th>/);
  assert.match(out, /<th class="money">¥80<\/th>/);
  assert.match(out, /<th>Total weight<\/th>/);
  assert.match(out, /<th class="money">600 g<\/th>/);
  assert.match(out, /<th class="money">₹300<\/th>/);
  assert.match(out, /<th class="money">₹1500<\/th>/);
  assert.match(out, /Edit calculation in this table/);
  assert.match(out, /data-vendor-bill="PO-7"/);
  assert.match(out, /¥10/); // Vendor's ¥90 less our ¥80.
  assert.match(out, /Total Amount \(Yuan\)/);
  assert.match(out, /Total Amount \(INR\)/);
  assert.match(out, /data-vendor-field="freight"/);
  assert.match(out, /data-vendor-field="totalAmount"/);
  assert.doesNotMatch(out, /MISSING/);
});

test('pending inline corrections reject invalid input before changing any line', () => {
  const start = source.indexOf("router.patch('/api/procurement/pos/:id/summary-calculation'");
  const end = source.indexOf('\n});', start) + 4;
  const po = { id: 'PO-7', status: 'received', lines: [{ qty: 2, perPcsYuan: 20, weightGrams: 100 }] };
  let handler, saves = 0;
  vm.runInNewContext(source.slice(start, end), {
    router: { patch: (_, fn) => { handler = fn; } }, canManagePurchases: () => true,
    loadStore: () => ({ pos: { 'PO-7': po }, settings: {} }), isLockedPo: () => false,
    poCostBreakdown: () => ({ total: 1 }), saveStore: () => saves++, publicPo: x => x
  });
  let code, body;
  handler({ params: { id: 'PO-7' }, body: { lines: [{ qty: -1, unitPrice: 20, weightGrams: 100 }] }, user: {} },
    { status: n => { code = n; return { json: x => { body = x; } }; }, json: x => { body = x; } });
  assert.equal(code, 400); assert.equal(saves, 0); assert.equal(po.lines[0].qty, 2);
  handler({ params: { id: 'PO-7' }, body: { lines: [{ qty: 3, unitPrice: 25, weightGrams: 150 }] }, user: {} },
    { status: n => { code = n; return { json: x => { body = x; } }; }, json: x => { body = x; } });
  assert.equal(body.success, true); assert.equal(po.lines[0].qty, 3); assert.equal(po.lines[0].perPcsYuan, 25); assert.equal(saves, 1);
});

test('vendor bill figures are saved separately and do not change purchase quantities', () => {
  const start = source.indexOf("router.patch('/api/procurement/pos/:id/vendor-bill'");
  const end = source.indexOf('\n});', start) + 4;
  const po = { id: 'PO-7', status: 'received', billNo: 'OUR-1', lines: [{ qty: 4, perPcsYuan: 20 }] };
  let handler, saves = 0, code, body;
  vm.runInNewContext(source.slice(start, end), {
    router: { patch: (_, fn) => { handler = fn; } }, canReconcileVendorBill: () => true,
    loadStore: () => ({ pos: { 'PO-7': po } }), saveStore: () => saves++, publicPo: x => x
  });
  const response = { status: n => { code = n; return { json: x => { body = x; } }; }, json: x => { body = x; } };
  handler({ params: { id: 'PO-7' }, body: { totalValue: -1 }, user: {} }, response);
  assert.equal(code, 400); assert.equal(saves, 0);
  handler({ params: { id: 'PO-7' }, body: { billNumber: 'V-1', totalValue: 85, totalQuantity: 4, localTransportation: 5, exchangeRate: 15, currency: 'CNY' }, user: {} }, response);
  assert.equal(body.success, true); assert.equal(po.vendorBill.totalValue, 85);
  assert.equal(po.billNo, 'OUR-1'); assert.equal(po.lines[0].qty, 4); assert.equal(saves, 1);
});

test('billing amount includes recorded local transport and other purchase costs',()=>{
 const {purchaseBillingAmount}=require('../modules/purchase-payment-status');
 const po={status:'received',origin:'china',exRate:12,freightPerGram:0,localTransportYuan:300,otherCostsYuan:50,lines:[{qty:2,perPcsYuan:100,weightGrams:0}]};
 assert.equal(purchaseBillingAmount(po),6600);
});

test('vendor comparison totals include articles, freight, local transport and other costs in both currencies',()=>{
 const start=html.indexOf('    function purchaseSummaryBill(po)'),end=html.indexOf('    function purchaseCostPanel(po)',start);
 const render=vm.runInNewContext(html.slice(start,end)+'\npurchaseSummaryBill;',{
   me:{canManage:true},settings:{},esc:String,money:n=>'₹'+n,yuan:n=>'¥'+n,window:{}
 });
 const po={id:'PO-8',status:'received',origin:'china',exRate:10,freightPerGram:1,localTransportYuan:5,otherCostsYuan:2,
   lines:[{sku:'A',qty:2,perPcsYuan:100,weightGrams:50}],
   vendorBill:{currency:'CNY',exchangeRate:10,totalValue:210,freight:11,localTransportation:6,otherCosts:3,totalAmount:230}};
 const out=render(po);
 assert.match(out, /<th>Total weight<\/th>/);
 assert.match(out, /<th class="money">100 g<\/th>/);
 assert.match(out, /Billing amount ₹2170/);
 assert.match(out, /Total Amount \(Yuan\)<\/td><td class="money">¥217<\/td><td class="money">¥230<\/td><td class="money">¥13/);
 assert.match(out, /Total Amount \(INR\)<\/td><td class="money">₹2170<\/td><td class="money">₹2300<\/td><td class="money">₹130/);
});

test('accounting can reconcile vendor bill without purchase line edit access',()=>{
 const start=source.indexOf('function canReconcileVendorBill(req)'),end=source.indexOf('\n}',start)+2;
 const can=vm.runInNewContext(source.slice(start,end)+'\ncanReconcileVendorBill;', {canManagePurchases:()=>false});
 assert.equal(can({user:{role:'accounting',roles:[]}}),true);
 assert.equal(can({user:{role:'viewer',roles:[]}}),false);
});

test('posted billing adds purchase-related costs without altering Shopify line snapshots',()=>{
 const {purchaseBillingAmount}=require('../modules/purchase-payment-status');
 const po={status:'posted',origin:'china',exRate:12,localTransportYuan:300,otherCostsYuan:50,newProducts:[{variants:[{qty:2,landed:1000}]}]};
 assert.equal(purchaseBillingAmount(po),6200);
 assert.equal(po.newProducts[0].variants[0].landed,1000);
});
