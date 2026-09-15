const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const vm=require('node:vm');
const html=fs.readFileSync(require('node:path').join(__dirname,'../public/procurement.html'),'utf8');
test('purchase history is one filtered chronological list including historical records',()=>{
  const host={innerHTML:'',querySelectorAll:()=>[]};
  const context={historyCategory:'all',historyDrillCategory:'',historyScope:'all',purchaseHistory:[{id:'historical',historical:true,datePurchase:'2026-08-24'}],el:id=>{assert.equal(id,'sumHost');return host;},historyMatchingLines:po=>po.lines||[],historyPoRow:po=>po.id+';'};
  context.purchaseHistoryTable=pos=>pos.map(context.historyPoRow).join('');
  const start=html.indexOf('function renderHistoryExplorerResults('),end=html.indexOf('function historyExplorerPoRow(',start);
  const render=vm.runInNewContext(html.slice(start,end)+'\nrenderHistoryExplorerResults;',context);
  render([{id:'old',datePurchase:'2026-08-25',lines:[1]},{id:'new',datePurchase:'2026-09-02',lines:[1]}]);
  assert.equal(host.innerHTML,'new;old;historical;');
  context.historyCategory='Shirt';render([{id:'selected',datePurchase:'2026-08-25',lines:[1]}]);
  assert.equal(host.innerHTML,'selected;');
  assert.doesNotMatch(html,/id="historyExplorerResults"|Matching purchase details/);
});
test('all purchase headers use the same six-column layout',()=>{
  const start=html.indexOf('function purchaseHistoryTable('),end=html.indexOf('function historyPoRow(',start);
  const context={esc:String,historyStatus:()=>'',historyPoRow:()=>''};
  const table=vm.runInNewContext(html.slice(start,end)+'\npurchaseHistoryTable;',context)([]);
  assert.match(table,/Received date.*Purchase date.*PO number.*Vendor name.*Quantity.*Amount/);
  const head=vm.runInNewContext(html.slice(start,end)+'\npurchaseHistoryHead;',context)({id:'PO-1'},'Not received','2026-09-02','PO-1','Vendor','48 pcs','₹100');
  assert.match(head,/purchase-table-row purchase-table-record/);
  assert.match(head,/Not received.*2026-09-02.*PO-1.*Vendor.*48 pcs.*₹100/);
  assert.match(html,/purchaseHistoryHead\(po,'Not recorded','Not recorded'/);
});
