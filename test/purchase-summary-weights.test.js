const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm'),path=require('node:path');
const source=fs.readFileSync(path.join(__dirname,'../modules/procurement.js'),'utf8');
const html=fs.readFileSync(path.join(__dirname,'../public/procurement.html'),'utf8');
function handler(store,allowed=true){
 const start=source.indexOf("router.patch('/api/procurement/pos/:id/weights'"),end=source.indexOf('\n});',start)+4;let fn,saves=0;
 vm.runInNewContext(source.slice(start,end),{router:{patch:(url,f)=>fn=f},canManagePurchases:()=>allowed,loadStore:()=>store,saveStore:()=>saves++,num:x=>Number(x)||0,publicPo:po=>po});
 return weights=>{let code=200,body;fn({params:{id:'PO-5'},body:{weights},user:{username:'owner'}},{status:n=>{code=n;return {json:b=>body=b}},json:b=>body=b});return {code,body,saves};};
}
test('summary weights save atomically without changing receipt status, price or quantity',()=>{
 const po={id:'PO-5',status:'advance',lines:[{sku:'A',qty:3,perPcsYuan:40,weightGrams:0},{sku:'B',qty:3,weightGrams:0}]};
 const save=handler({pos:{'PO-5':po}});
 assert.equal(save({'0':250,'1':-1}).code,400);assert.equal(po.lines[0].weightGrams,0);
 const result=save({'0':250.5});assert.equal(result.code,200);assert.equal(result.saves,1);
 assert.equal(po.lines[0].weightGrams,250.5);assert.equal(po.lines[1].weightGrams,0);
 assert.equal(po.status,'advance');assert.equal(po.dateReceive,undefined);assert.equal(po.lines[0].qty,3);assert.equal(po.lines[0].perPcsYuan,40);
 assert.equal(po.weightHistory[0].changes[0].before,0);
});
test('posted POs and unauthorized users cannot use summary weight saving',()=>{
 assert.equal(handler({pos:{'PO-5':{status:'posted'}}})({'0':10}).code,400);
 assert.equal(handler({pos:{}},false)({'0':10}).code,403);
});
test('summary uses one editable calculation table with zoomable photos',()=>{
 const a=html.indexOf('function purchaseCalculationPanel('),b=html.indexOf('window.attachPoInvoice',a);
 const c={window:{},canReviewHeldImage:()=>true,me:{canManage:true},settings:{exRate:15,freightPerGram:0.45},esc:String,money:x=>'₹'+x,yuan:x=>'¥'+x};
 const render=vm.runInNewContext(html.slice(a,b)+'\npurchaseCostPanel;',c),po={id:'PO-5',status:'advance',origin:'china',lines:[{sku:'A',qty:3,perPcsYuan:40,weightGrams:250,photoUrl:'/api/procurement/photo/a'}]};
 const out=render(po);
 assert.match(out,/Edit calculation in this table/);assert.match(out,/data-bill-line="0" data-field="weightGrams"/);assert.match(out,/data-zoom src="\/api\/procurement\/photo\/a"/);assert.match(out,/Goods ₹1800.*freight.*₹337.5/);
 assert.doesNotMatch(out,/Complete purchase record|data-summary-weight-editor|Save weights & recalculate/);
 assert.match(out,/Attach original bill/);
 c.me.canManage=false;assert.doesNotMatch(render(po),/Edit calculation in this table/);
});
test('pending bill totals recalculate from weights instead of an older preview',()=>{
 const {purchaseBillingAmount}=require('../modules/purchase-payment-status');
 const po={status:'received',origin:'china',exRate:15,freightPerGram:0.45,lines:[{qty:3,perPcsYuan:40,weightGrams:250}],newProducts:[{variants:[{qty:3,landed:600}]}]};
 assert.equal(purchaseBillingAmount(po),2138);
 assert.equal(purchaseBillingAmount({...po,status:'posted'}),1800);
});
