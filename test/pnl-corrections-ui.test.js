'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const source=fs.readFileSync(path.join(__dirname,'../public/pnl-corrections.js'),'utf8');
function harness(editable=true){
 const elements=new Map(),selectors=new Map(),calls=[],actions=[],rows=[{orderId:'O',lineId:'L',reference:'#DEMO',date:'2026-10-01',title:'<Custom>',qty:2,price:100,discount:0,current:null},{orderId:'P',lineId:'D',reference:'#DUP',date:'2026-10-02',title:'Duplicate',sourceSku:'DUP',qty:1,price:200,candidates:[{inventoryItemId:'I-BLUE',sku:'DUP',productTitle:'Example <product>',variant:'Blue / L',unitCost:50,currency:'INR'}],current:null}];
 const element=id=>{if(!elements.has(id))elements.set(id,{id,value:'',disabled:false,hidden:false,innerHTML:'',textContent:'',listeners:{},addEventListener(n,f){this.listeners[n]=f;},querySelector(s){return selectors.get(s);},querySelectorAll(s){return s==='[data-correction-action]'?actions:[];}});return elements.get(id);};
 rows.forEach((r,i)=>{const action={dataset:{correctionAction:String(i)},value:'',matches:s=>s==='[data-correction-action]'};actions.push(action);selectors.set('[data-correction-value="'+i+'"]',{value:'',hidden:true,placeholder:''});selectors.set('[data-correction-inventory="'+i+'"]',{value:'',hidden:true});});
 element('from').value='2026-08-22';element('to').value='2026-10-18';
 let reloads=0;
 const context=vm.createContext({document:{getElementById:element},URLSearchParams,location:{reload(){reloads++;}},fetch:async(url,options)=>{const body=options.body?JSON.parse(options.body):null;calls.push({url,body});return {ok:true,json:async()=>url.endsWith('/preview')?{success:true,items:body.items.map(i=>({...i,reference:rows.find(r=>r.orderId===i.orderId).reference,title:'<Preview>'}))}:body?{success:true,count:body.items.length}:{success:true,rows,canEdit:editable,historyCount:7}};}});
 vm.runInContext(source,context);
 return {element,actions,calls,rows,reloads:()=>reloads,value:i=>selectors.get('[data-correction-value="'+i+'"]'),identity:i=>selectors.get('[data-correction-inventory="'+i+'"]'),async load(){await element('pnlCorrectionLoad').onclick();},async preview(){await element('pnlCorrectionCheck').onclick();},change(i,type){actions[i].value=type;element('pnlCorrectionRows').listeners.change({target:actions[i]});}};
}
test('combined correction UI exposes exact identities and escapes source text',async()=>{
 const h=harness();await h.load();const html=h.element('pnlCorrectionRows').innerHTML;
 assert.match(html,/&lt;Custom&gt;/);assert.doesNotMatch(html,/<Custom>/);
 assert.match(html,/Split combined SKU line/);assert.match(html,/confirmed no GST/);assert.match(html,/Rounding/);
 assert.match(html,/Example &lt;product&gt; · Blue \/ L · ₹50 INR/);
 h.change(1,'sku_identity');assert.equal(h.identity(1).hidden,false);assert.equal(h.value(1).hidden,true);
});
test('bundle form sends exact component quantities and never changes source quantity',async()=>{
 const h=harness();await h.load();h.change(0,'bundle');h.value(0).value='SKU-A:1, sku-b:1';await h.preview();
 const item=h.calls.at(-1).body.items[0];assert.deepEqual(item.components,[{sku:'SKU-A',qty:1},{sku:'SKU-B',qty:1}]);assert.equal(item.expectedQty,2);
 assert.match(h.element('pnlCorrectionPreview').innerHTML,/SKU-A × 1 \+ SKU-B × 1/);assert.equal(h.element('pnlCorrectionSave').disabled,false);
 h.value(0).value='SKU-A';h.element('pnlCorrectionRows').listeners.input();await h.preview();
 assert.match(h.element('pnlCorrectionStatus').textContent,/SKU:quantity/);assert.equal(h.element('pnlCorrectionSave').disabled,true);
});
test('exact variant and non-product treatments preserve preview-before-save and reason',async()=>{
 const h=harness();await h.load();h.change(1,'sku_identity');h.identity(1).value='I-BLUE';await h.preview();assert.equal(h.calls.at(-1).body.items[0].inventoryItemId,'I-BLUE');assert.match(h.element('pnlCorrectionPreview').innerHTML,/Blue \/ L/);
 h.change(1,'');h.change(0,'rounding');await h.preview();assert.match(h.element('pnlCorrectionPreview').innerHTML,/amount retained/);
 h.change(0,'shipping_no_tax');await h.preview();assert.match(h.element('pnlCorrectionPreview').innerHTML,/confirmed zero GST/);
 h.element('pnlCorrectionNote').value='Confirmed source evidence';await h.element('pnlCorrectionSave').onclick();assert.equal(h.calls.at(-1).body.note,'Confirmed source evidence');assert.equal(h.reloads(),1);
 assert.ok(h.calls.every(c=>c.url.startsWith('/api/pl/report/line-corrections')));
});
test('read-only report users cannot preview or save item corrections',async()=>{
 const h=harness(false);await h.load();h.change(0,'rounding');await h.preview();await h.element('pnlCorrectionSave').onclick();assert.equal(h.calls.length,1);assert.equal(h.element('pnlCorrectionCheck').disabled,true);assert.match(h.element('pnlCorrectionRows').innerHTML,/ disabled/);
});
test('Excel component allocations show component SKU and quantity instead of parent bundle totals',()=>{
 const XLSX=require('xlsx'),{exportWorkbook}=require('../modules/pnl-routes'),{buildReport}=require('../modules/pnl-report');
 const r=buildReport({}, {from:'2026-10-01',to:'2026-10-18'});r.transactions=[{date:'2026-10-01',reference:'#DEMO',details:[{sku:'',qty:3,components:[{sku:'A',qty:1},{sku:'B',qty:2}],allocations:[{sku:'A',qty:1,unitCost:20,amount:20,purchaseId:'Shopify'},{sku:'B',qty:2,unitCost:30,amount:60,purchaseId:'Shopify'}]}]}];
 const workbook=XLSX.read(exportWorkbook(r),{type:'buffer'}),rows=XLSX.utils.sheet_to_json(workbook.Sheets['SKU allocations'],{header:1});
 assert.deepEqual(rows.slice(1).map(row=>[row[2],row[3],row[5],row[7]]),[['A',1,1,20],['B',2,2,60]]);
});
