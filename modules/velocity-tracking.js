'use strict';
async function fetchTracking({awbs,token,fetchImpl=require('node-fetch'),now=()=>new Date().toISOString()}){
 const shipments={};const unique=[...new Set(awbs)];
 for(let i=0;i<unique.length;i+=50){const batch=unique.slice(i,i+50);const response=await fetchImpl('https://shazam.velocity.in/custom/api/v1/order-tracking',{method:'POST',headers:{'Content-Type':'application/json',Authorization:'Bearer '+String(token).replace(/^Bearer\s+/i,'')},body:JSON.stringify({awbs:batch}),timeout:30000});if(!response.ok)throw new Error('Velocity tracking API returned HTTP '+response.status);const body=await response.json();if(!body.result||typeof body.result!=='object')throw new Error('Unexpected Velocity tracking response');
  for(const [awb,row] of Object.entries(body.result)){
   if(!batch.includes(awb)||!row||typeof row!=='object')continue;
   const td=row.tracking_data||row,track=(td.shipment_track||[])[0]||td;
   const status=row.current_status||row.status||track.current_status||td.shipment_status;
   if(!status)continue;
   shipments[awb]={awb,status,courierName:track.courier_name||track.courier_company||row.courier_name||'',weight:track.weight??null,currentLocation:row.location||row.current_location||track.delivered_to||track.destination||'',deliveredDate:row.delivery_date||row.delivered_date||track.delivered_date||track.delivery_date||td.delivered_date||null,trackingUrl:td.track_url||'',recipientName:track.consignee_name||'',lastUpdated:now()};
  }
 }
 return shipments;
}
module.exports={fetchTracking};
