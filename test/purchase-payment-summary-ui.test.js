const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm'),path=require('node:path');
const html=fs.readFileSync(path.join(__dirname,'../public/procurement.html'),'utf8');
test('vendor payment summary remains empty until an invoice calculation is selected',()=>{
 const start=html.indexOf('    var selectedPaymentBills='),end=html.indexOf('    function renderPurchaseHistory(meta)',start);
 const host={innerHTML:'',querySelectorAll:()=>[]};
 const context={me:{isAdmin:true,role:'owner',roles:['owner']},purchaseHistory:[
  {id:'PO-1',vendor:'Vendor A',billNo:'101',datePurchase:'2026-09-10',paymentSummary:{status:'partially_paid',amount:50000,paidAmount:20000,balanceDue:30000,payments:[{date:'2026-09-12',reference:'R1'}]}},
  {id:'PO-2',vendor:'Vendor A',billNo:'102',datePurchase:'2026-09-15',paymentSummary:{status:'unpaid',amount:40000,paidAmount:0,balanceDue:40000,payments:[]}},
  {id:'PO-3',vendor:'Vendor B',billNo:'103',datePurchase:'2026-09-15',paymentSummary:{status:'unpaid',amount:10000,paidAmount:0,balanceDue:10000,payments:[]}}
 ],combinedVendorInvoices:[],activeInvoiceId:'',el:id=>id==='vendorPaymentSummary'?host:null,esc:String,money:n=>'₹'+n};
 vm.runInNewContext(html.slice(start,end)+'\nthis.render=renderVendorPaymentSummary;this.select=selectedPaymentBills;',context);
 context.render();assert.match(host.innerHTML,/Select bill\(s\) above/);assert.doesNotMatch(host.innerHTML,/PO-1|PO-2|PO-3/);
});

test('mixed-vendor combined invoice replaces child payable rows but preserves child IDs',()=>{
 const start=html.indexOf('    var selectedPaymentBills='),end=html.indexOf('    function renderPurchaseHistory(meta)',start);
 const host={innerHTML:'',querySelectorAll:()=>[]};
 const context={me:{isAdmin:true,role:'owner',roles:['owner']},purchaseHistory:[
  {id:'PO-1',vendor:'Vendor A',billNo:'101',datePurchase:'2026-09-10',paymentSummary:{amount:50000,paidAmount:0,balanceDue:50000}},
  {id:'PO-2',vendor:'Vendor B',billNo:'102',datePurchase:'2026-09-15',paymentSummary:{amount:40000,paidAmount:0,balanceDue:40000}}
 ],activeInvoiceId:'CVI-1',combinedVendorInvoices:[{id:'CVI-1',vendor:'Multiple vendors',vendors:['Vendor A','Vendor B'],poIds:['PO-1','PO-2'],lgDate:'2026-09-18',lgBillNumber:'LG-501',finalized:{basis:'vendor',amountInr:90000}}],
 combinedInvoiceDetail:()=>'<div>PO-1 + PO-2</div>',el:id=>id==='vendorPaymentSummary'?host:null,esc:String,money:n=>'₹'+n};
 vm.runInNewContext(html.slice(start,end)+'\nthis.render=renderVendorPaymentSummary;',context);
 context.render();
 assert.match(host.innerHTML,/data-payment-group="CVI-1"/);
 assert.match(host.innerHTML,/LG-501/);
 assert.match(host.innerHTML,/2026-09-18/);
 assert.match(host.innerHTML,/101 \+ 102/);
 assert.doesNotMatch(host.innerHTML,/data-payment-po="PO-1"/);
 assert.doesNotMatch(host.innerHTML,/data-payment-po="PO-2"/);
});
