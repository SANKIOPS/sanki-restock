const test=require('node:test'),assert=require('node:assert/strict');
const {normalizeShipment,fetchShipmentIndex,matchShipments}=require('../modules/velocity-order-sync');
const row=(id,orderId='123')=>({id,attributes:{tracking_number:'AWB'+id,status:'delivered',carrier:{name:'DTDC'},order:{channel:'shopify',external_id:orderId,display_id:'#2794'},applicable_weight:{value:'753.3'}}});
test('discovery uses Shopify external IDs, never customer names or display numbers',()=>{
 const shipment=normalizeShipment(row('1'));assert.equal(shipment.weight,.7533);
 const match=matchShipments({'123':{}},[shipment,normalizeShipment(row('2','999'))]);
 assert.equal(match.byOrder['123'][0].awb,'AWB1');assert.equal(match.unmatched,1);
 assert.equal(normalizeShipment({id:'x',attributes:{tracking_number:'a',order:{channel:'manual',external_id:'123'}}}),null);
});
test('full pagination includes later pages and rejects repeated data',async()=>{
 const pages=[];
 const fetchImpl=async(url,opt)=>{const p=JSON.parse(opt.body).page;pages.push(p);return {ok:true,json:async()=>({data:[row(String(p))],meta:{current_page:p,total:2}})};};
 const index=await fetchShipmentIndex({token:'secret',fetchImpl,pause:async()=>{}});
 assert.deepEqual(pages,[1,2]);assert.equal(index.rows.length,2);
 await assert.rejects(fetchShipmentIndex({token:'secret',pause:async()=>{},fetchImpl:async(url,opt)=>({ok:true,json:async()=>({data:[row('1')],meta:{current_page:JSON.parse(opt.body).page,total:2}})})}),/repeated/);
});
test('provider failure cannot be treated as an empty successful index',async()=>{
 await assert.rejects(fetchShipmentIndex({token:'secret',fetchImpl:async()=>({ok:false,status:401})}),/401/);
});
