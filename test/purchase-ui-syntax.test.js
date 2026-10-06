const test=require('node:test'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
for(const file of ['procurement.html','fresh-procurement.html','size-tracker.html'])test(file+' compiles all inline browser scripts',()=>{
  const html=fs.readFileSync(path.join(__dirname,'../public',file),'utf8');
  for(const script of html.matchAll(/<script\b[^>]*>([\s\S]*?)<\/script>/g))if(script[1].trim())new vm.Script(script[1],{filename:file});
});
test('purchase history renders a received bill with allocated costs without a runtime error',()=>{
  const assert=require('node:assert/strict'),html=fs.readFileSync(path.join(__dirname,'../public/procurement.html'),'utf8');
  const section=(start,end)=>html.slice(html.indexOf(start),html.indexOf(end));
  const context={settings:{},PurchaseCosts:require('../public/purchase-costs'),esc:String,money:n=>'INR '+n,historyStatus:()=> 'Received',purchasePaymentBadge:()=>'',purchasePaymentPanel:()=>'',purchaseCostPanel:()=>'',combinedVendorInvoices:[],selectedInvoiceBills:{}};
  vm.createContext(context);
  vm.runInContext(section('    function historyTotals(po)','    function historyStatusCode(po)')+section('    function historyLineCost(po,line)','    function purchaseCalculationPanel(po)')+section('    function purchaseHistoryHead(po,','    function historicalPurchaseRow(po,idx)'),context);
  const po={id:'PO-TEST',status:'received',origin:'china',exRate:12,freightPerGram:0.1,localTransportYuan:2,lines:[{productType:'Shirt',qty:2,perPcsYuan:10,weightGrams:100}]};
  const row=context.historyPoRow(po,0);
  assert.match(row,/PO-TEST/);assert.match(row,/2 pcs/);assert.match(row,/INR 284/);
});
