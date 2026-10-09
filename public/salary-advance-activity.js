(function(root,factory){
  if(typeof module==='object'&&module.exports)module.exports=factory();
  else root.SalaryAdvanceActivity=factory();
})(typeof window!=='undefined'?window:this,function(){
  'use strict';
  function money(value){return Math.round((Number(value)||0)*100)/100;}
  function day(value){
    var text=String(value||'').slice(0,10);
    if(!/^\d{4}-\d{2}-\d{2}$/.test(text))return '';
    var date=new Date(text+'T00:00:00Z');
    return !isNaN(date.getTime())&&date.toISOString().slice(0,10)===text?text:'';
  }
  function today(){var now=new Date();return now.getFullYear()+'-'+String(now.getMonth()+1).padStart(2,'0')+'-'+String(now.getDate()).padStart(2,'0');}
  function dates(record){
    if(!record)return [];
    return ['date','payoutDate','createdAt','updatedAt','approvedAt','rejectedAt','postedAt','cancelledAt'].map(function(key){return day(record[key]);}).concat((record.recoveries||[]).reduce(function(all,r){return all.concat([day(r.deductionDate),day(r.recordedOn),day(r.at)]);},[])).filter(Boolean);
  }
  // This is a read-only view. Pending requests are not paid advances, and a
  // posted request plus its advance must never count as two payouts.
  function build(data,options){
    data=data||{};options=options||{};
    var end=day(options.today)||today(),days=Number(options.days)===30?30:60,onlyOutstanding=options.onlyOutstanding===true;
    var start=new Date(end+'T00:00:00Z');start.setUTCDate(start.getUTCDate()-(days-1));var cutoff=start.toISOString().slice(0,10);
    var requests=data.requests||[],byRequest=Object.create(null),byAdvance=Object.create(null),matched=Object.create(null),seen=Object.create(null);
    requests.forEach(function(r){byRequest[r.id]=r;if(r.advanceId)byAdvance[r.advanceId]=r;});
    var advances=Array.isArray(data.activityAdvances)?data.activityAdvances:(data.advances||[]).concat((data.summary||[]).reduce(function(all,x){return all.concat(x.transactions||[]);},[]));
    var rows=[];
    function add(row,activity){
      var recent=activity.some(function(date){return date>=cutoff&&date<=end;});
      if(onlyOutstanding?!(row.outstanding>0):!(recent||row.outstanding>0||row.waiting))return;
      row.activityDate=activity.filter(function(date){return date<=end;}).sort().pop()||row.date||'';
      rows.push(row);
    }
    advances.forEach(function(a){
      if(seen[a.id])return;seen[a.id]=true;
      var r=byRequest[a.requestId]||byAdvance[a.id];if(r)matched[r.id]=true;
      var recovered=money(a.recovered==null?(a.recoveries||[]).reduce(function(n,x){return n+Number(x.amount||0);},0):a.recovered);
      var outstanding=a.active===false?0:money(Math.max(0,a.outstanding==null?money(a.amount)-recovered:a.outstanding));
      add({id:a.id,requestId:r&&r.id||a.requestId||'',empId:a.empId,employeeName:a.employeeName||r&&r.employeeName||a.empId,date:a.date||a.payoutDate||'',amount:money(a.amount),recovered:recovered,outstanding:outstanding,waiting:false,account:a.account||'',reference:a.reference||'',requestStatus:r&&r.status||(a.directOwnerPost?'Paid directly by Owner':'Posted'),status:a.active===false?'Cancelled':outstanding>0?(recovered>0?'Partly deducted':'Not deducted'):'Fully deducted',note:a.active===false?a.cancelReason||'':a.note||'',request:r||null,advance:a},dates(a).concat(dates(r)));
    });
    requests.forEach(function(r){
      if(matched[r.id])return;
      var waiting=!['Posted','Rejected','Cancelled'].includes(r.status);
      add({id:r.id,requestId:r.id,empId:r.empId,employeeName:r.employeeName||r.empId,date:r.payoutDate||r.date||'',amount:money(r.amount),recovered:null,outstanding:null,waiting:waiting,account:r.account||'',reference:r.reference||'',requestStatus:r.status||'Pending approval',status:r.status||'Pending approval',note:r.rejectionReason||r.note||'',request:r,advance:null},dates(r));
    });
    rows.sort(function(a,b){return String(b.activityDate+b.id).localeCompare(String(a.activityDate+a.id));});
    var outstandingRows=rows.filter(function(row){return row.outstanding>0;});
    return {rows:rows,today:end,cutoff:cutoff,days:days,onlyOutstanding:onlyOutstanding,outstandingCount:outstandingRows.length,outstanding:money(outstandingRows.reduce(function(n,row){return n+row.outstanding;},0)),pendingApproval:rows.filter(function(row){return row.waiting&&row.status==='Pending approval';}).length,proofRequired:rows.filter(function(row){return row.waiting&&row.status==='Approved – proof required';}).length};
  }
  return {build:build};
});
