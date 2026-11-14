'use strict';
(function(root,factory){const api=factory();if(typeof module==='object'&&module.exports)module.exports=api;else root.UnitEconomicsDetails=api;})(typeof window==='object'?window:this,function(){
 const sale=r=>['sale','return'].includes(r.kind),known=v=>typeof v==='number'&&Number.isFinite(v);
 function relevant(entries,key){return entries.filter(r=>['cogs','grossProfit','gst','netSales','sales'].includes(key)?sale(r):key==='discountsReturns'?r.discounts||r.returns:r.values[key]!==0&&r.values[key]!==undefined);}
 function costs(entries){return relevant(entries,'cogs').flatMap(r=>{
  const items=(r.details||[]).filter(d=>!['shipping','rounding'].includes(d.itemKind));
  return (items.length?items:[{title:'Item details unavailable',cogs:null}]).map(d=>({entry:r,date:r.date,reference:r.reference,type:r.kind==='return'?'Return / cost reversed':'Sale',product:d.title||d.sku||'Product not mapped',sku:d.sku||'SKU not mapped',qty:d.qty??null,unitCost:known(d.cogs)&&d.qty>0?Math.abs(d.cogs/d.qty):null,cost:known(d.cogs)?d.cogs:null,status:known(d.cogs)?r.kind==='return'?(d.returnedToStock?'Cost reversed — restocked':'No stock cost reversed'):'Cost available':'Cost missing — profit unavailable'}));
 });}
 function sum(rows,field){return rows.reduce((n,r)=>n+(known(r[field])?r[field]:0),0);}
 return {relevant,costs,sum,known};
});
