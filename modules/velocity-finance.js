'use strict';
const BASE='https://shazam.velocity.in/custom/api/v1';
// Official finance API: maximum 100 AWBs and five requests/minute.
function normalize(row, at) {
 const awb=String(row.tracking_number||'').trim();
 if(!awb)throw new Error('Velocity response is missing tracking_number');
 if(row.error)return {awb,error:String(row.error),checkedAt:at};
 const amount=Number(row.cod_amount);
 if(!Number.isFinite(amount)||amount<0)throw new Error('Invalid COD amount for '+awb);
 const utr=String(row.utr_no||'').trim();
 return {awb,amount,deliveryDate:row.delivery_date||null,date:row.remittance_date||null,utr,status:utr?'provider_reported_settled':'pending',bankVerified:false,checkedAt:at,source:'velocity_cod_remittance_api'};
}
async function fetchRemittances({awbs,token,fetchImpl=require('node-fetch'),sleep=ms=>new Promise(r=>setTimeout(r,ms)),now=()=>new Date().toISOString()}){
 const unique=[...new Set(awbs.map(v=>String(v).trim()).filter(Boolean))];
 if(!token)throw new Error('Velocity API key is not configured');
 const rows=[];
 for(let i=0;i<unique.length;i+=100){
  if(i)await sleep(12500);
  const response=await fetchImpl(BASE+'/cod-remittance',{method:'POST',headers:{'Content-Type':'application/json',Authorization:'Bearer '+String(token).replace(/^Bearer\s+/i,'')},body:JSON.stringify({awbs:unique.slice(i,i+100)})});
  if(!response.ok)throw new Error('Velocity COD API returned HTTP '+response.status);
  const body=await response.json();if(!Array.isArray(body.data))throw new Error('Velocity COD API returned an unexpected response');
  const expected=new Set(unique.slice(i,i+100));const seen=new Set();
  for(const row of body.data){const item=normalize(row,now());if(!expected.has(item.awb)||seen.has(item.awb))throw new Error('Unexpected or duplicate AWB in Velocity response');seen.add(item.awb);rows.push(item);}
  for(const awb of expected)if(!seen.has(awb))rows.push({awb,error:'AWB omitted from API response',checkedAt:now()});
 }
 return rows;
}
module.exports={fetchRemittances,normalize};
