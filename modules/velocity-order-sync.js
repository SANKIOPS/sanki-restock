'use strict';
const BASE='https://shazam.velocity.in/custom/api/v1';
function normalizeShipment(row){
 const a=row.attributes||{},o=a.order||{},awb=String(a.tracking_number||'').trim();
 if(!o.external_id||o.channel!=='shopify'||!awb)return null;
 const grams=a.applicable_weight&&a.applicable_weight.value;
 return {id:String(row.id),orderId:String(o.external_id),orderName:o.display_id||'',awb,courier:(a.carrier||{}).name||'',status:a.status||'Tracking update pending',weight:grams!=null&&Number.isFinite(Number(grams))?Number(grams)/1000:null,trackingUrl:'https://www.velocityshipping.in/track/'+encodeURIComponent(awb)};
}
async function fetchShipmentIndex({token,fetchImpl=require('node-fetch'),pause=ms=>new Promise(r=>setTimeout(r,ms))}){
 const rows=[],seen=new Set();let total=null;
 for(let page=1;page<=1000;page++){
  if(page>1)await pause(1000);
  const response=await fetchImpl(BASE+'/shipments',{method:'POST',headers:{'Content-Type':'application/json',Authorization:'Bearer '+String(token).replace(/^Bearer\s+/i,'')},body:JSON.stringify({per_page:100,page}),timeout:90000});
  if(!response.ok)throw new Error('Velocity shipment discovery HTTP '+response.status);
  const body=await response.json();
  if(!Array.isArray(body.data)||!body.meta||!Number.isFinite(Number(body.meta.total)))throw new Error('Unexpected Velocity shipment index response');
  if(total===null)total=Number(body.meta.total);
  if(Number(body.meta.current_page)!==page)throw new Error('Velocity shipment pagination did not advance');
  for(const row of body.data){if(!row.id||seen.has(String(row.id)))throw new Error('Velocity shipment pagination repeated a shipment');seen.add(String(row.id));const normalized=normalizeShipment(row);if(normalized)rows.push(normalized);}
  if(seen.size>=Number(body.meta.total))return {rows,total:seen.size};
  if(!body.data.length)throw new Error('Velocity shipment index ended before all shipments were read');
 }
 throw new Error('Velocity shipment pagination limit exceeded');
}
function matchShipments(orders,rows){
 const byOrder={};let unmatched=0;
 for(const row of rows){if(!orders[row.orderId]){unmatched++;continue;} (byOrder[row.orderId]||=([])).push(row);}
 return {byOrder,unmatched};
}
module.exports={normalizeShipment,fetchShipmentIndex,matchShipments};
