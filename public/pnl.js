(function(){
  'use strict';
  const el=id=>document.getElementById(id);
  const escape=value=>String(value??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const fmt=value=>value==null?'Unavailable':'₹'+Number(value).toLocaleString('en-IN',{minimumFractionDigits:2,maximumFractionDigits:2});
  const pct=value=>value==null?'—':(value*100).toFixed(1)+'%';
  const tone=value=>value==null||value===0?'neutral':value>0?'positive':'negative';
  const num=value=>'<td class="num">'+fmt(value)+'</td>';
  let report=null,sequence=0,abort=null;
  const today=new Date(Date.now()+19800000).toISOString().slice(0,10);
  const start='2026-08-22';
  el('to').max=today;el('from').max=today;
  function period(key){
    if(el('from').disabled)document.querySelector('[aria-label="Date filter mode"] button')?.click();
    let from=today.slice(0,7)+'-01',to=today;
    if(key==='today')from=today;
    if(key==='start')from=start;
    if(key==='last'){
      const date=new Date(today.slice(0,7)+'-01T00:00:00Z');date.setUTCDate(0);to=date.toISOString().slice(0,10);from=to.slice(0,7)+'-01';
    }
    el('from').value=from<start?start:from;el('to').value=to;load();
  }
  const query=()=>new URLSearchParams({from:el('from').disabled?start:el('from').value||start,to:el('to').value,channel:el('channel').value}).toString();
  function table(headers,rows){return '<div class="scroll"><table><thead><tr>'+headers.map(h=>'<th>'+escape(h)+'</th>').join('')+'</tr></thead><tbody>'+rows+'</tbody></table></div>';}
  function drill(label,filter){return '<button class="drill" data-filter="'+escape(filter)+'">'+escape(label)+'</button>';}
  function render(){
    const totals=report.totals,previous=report.previous?.totals;
    el('rangeNote').textContent=report.range.from+' to '+report.range.to+(report.range.partial?' · Partial period from accounting start':'')+(report.previous?' · Compared with '+report.previous.from+' to '+report.previous.to+(report.previous.partial?' (partial)':''):' · No earlier accounting period');
    el('cards').innerHTML=[['Net sales excluding GST',fmt(totals.netSales),''],['Paid operating expenses',fmt(totals.expenses),''],['Management profit / loss',fmt(totals.netProfit),tone(totals.netProfit)],['Profit margin',pct(totals.margin),tone(totals.netProfit)]].map(([label,value,color])=>'<div class="stat panel"><div class="label">'+label+'</div><div class="amount '+color+'">'+value+'</div></div>').join('');
    el('warnings').hidden=!report.warnings.length;
    el('warnings').innerHTML=report.warnings.length?'<strong>Result incomplete: review '+report.warnings.length+' item(s).</strong><p>Missing costs or tax details are not assumed to be zero.</p><details><summary>Show missing data and exceptions</summary><ul>'+report.warnings.map(w=>'<li><strong>'+escape(w.reference)+'</strong> '+escape(w.message)+'</li>').join('')+'</ul></details>':'';
    const rows=[];
    function statementRow(label,field,style='',filter=field){
      const current=totals[field],prior=previous?.[field]??null;
      const change=current!==null&&prior!==null?current-prior:null;
      const percent=current!==null&&totals.netSales>0?current/totals.netSales:null;
      rows.push('<tr class="'+style+'"><td>'+drill(label,filter)+'</td>'+num(current)+num(prior)+num(change)+'<td class="num">'+pct(percent)+'</td></tr>');
    }
    statementRow('Sales including GST before discounts','grossSales');
    statementRow('Less: discounts','discounts');statementRow('Less: returns / refunds','returns');statementRow('Less: GST, net of returns','gst','','sales');
    statementRow('Net sales excluding GST','netSales','total','sales');statementRow('Other business income','otherIncome');statementRow('Total income','totalIncome','total','income');
    statementRow('Cost of products sold (SKU FIFO)','cogs');statementRow('Gross profit on sales','grossProfit','total','sales');
    rows.push('<tr class="section"><td colspan="5">Paid operating expenses</td></tr>');
    for(const [group,label] of [['fixed','Fixed'],['running','Running'],['variable','Variable'],['marketing','Marketing']]){
      const categories=report.categories.filter(c=>c.group===group),amount=categories.reduce((s,c)=>s+c.amount,0);
      const controls=categories.map((c,index)=>'expense-category-'+group+'-'+index).join(' ');
      rows.push('<tr class="expense-head"><td><button class="group-toggle" data-group-toggle="'+group+'" aria-expanded="false"'+(controls?' aria-controls="'+controls+'"':' disabled')+'><span class="group-arrow" aria-hidden="true">▸</span>'+label+' expenses</button></td>'+num(amount)+'<td colspan="3"></td></tr>');
      categories.forEach((category,index)=>rows.push('<tr id="expense-category-'+group+'-'+index+'" data-expense-group="'+group+'" hidden><td class="indent">'+drill(category.category+' ('+category.count+')','category:'+category.id)+'</td>'+num(category.amount)+'<td colspan="3"></td></tr>'));
    }
    statementRow('Total paid operating expenses','expenses','total');statementRow('Management profit / loss','netProfit','total grand','all');
    el('statement').innerHTML='<p class="note muted">'+escape(report.policy.channels)+'</p>'+table(['Particulars','Selected period','Previous period','Change','% of net sales'],rows.join(''));
    el('tax').innerHTML='<p class="note muted">Per-piece taxable sale value ≤ ₹2,500: 5%. Above ₹2,500: 18%. Invoice differences remain visible; no invoices are changed by this report.</p>'+table(['Date / invoice','SKU','Quantity','GST-inclusive value','Recorded GST','Expected GST','Rate','Review'],report.taxes.map(r=>'<tr><td>'+escape(r.date)+'<br>'+drill(r.reference,'reference:'+r.reference)+'</td><td>'+escape(r.sku)+'</td><td class="num">'+r.qty+'</td>'+num(r.gross)+num(r.recorded)+num(r.calculated)+'<td class="num">'+pct(r.rate)+'</td><td>'+escape(r.status)+'</td></tr>').join('')||'<tr><td colspan="8">No tax lines in this period.</td></tr>');
    el('collections').innerHTML='<p class="note muted">One sale can have an advance and COD collection. Courier/bank settlement is a transfer, not another sale. Collection figures require successful payment evidence.</p>'+table(['Order / status','Order value','Collected','Cash refunded','Still collectible','Advance held'],report.collections.map(r=>'<tr><td>'+drill(r.reference,'collection:'+r.id)+'<br><span class="muted">'+escape(r.status)+'</span></td>'+num(r.orderValue)+num(r.collected)+num(r.cashRefunded)+num(r.balanceToCollect)+num(r.customerAdvance)+'</tr>').join('')||'<tr><td colspan="6">No orders in this period.</td></tr>');
    const max=Math.max(1,...report.trends.map(r=>r.netSales||0));
    el('trends').innerHTML=table(['Month','Net sales','SKU COGS','Paid expenses','Profit / loss'],report.trends.map(r=>'<tr><td>'+escape(r.month)+'</td><td class="num">'+fmt(r.netSales)+'<span class="bar" style="width:'+Math.max(0,(r.netSales||0)/max*100)+'%"></span></td>'+num(r.cogs)+num(r.expenses)+'<td class="num '+tone(r.netProfit)+'">'+fmt(r.netProfit)+'</td></tr>').join('')||'<tr><td colspan="5">No activity in this period.</td></tr>');
    el('policy').innerHTML='<div class="policies"><h2>Report rules</h2><dl>'+Object.entries(report.policy).map(([k,v])=>'<dt>'+escape({operatingExpenses:'Paid expenses',cogs:'Product costs',gst:'GST',sales:'Sales timing',channels:'Channel filter'}[k]||k)+'</dt><dd>'+escape(v)+'</dd>').join('')+'</dl><h2>Read-only sources</h2><ul>'+report.feeds.map(f=>'<li>'+escape(f.name)+' — '+escape(f.status)+(f.updatedAt?' · Last sync '+escape(f.updatedAt):'')+'</li>').join('')+'</ul><p>Accounting begins on 22 August 2026. Opening SKU quantities/costs must be verified separately. No April data or percentage COGS estimates are used.</p></div>';
    el('export').disabled=false;el('print').disabled=false;
  }
  async function load(){
    const request=++sequence;if(abort)abort.abort();abort=new AbortController();
    if(el('details').open)el('details').close();
    document.body.classList.add('loading');el('status').className='';el('status').textContent='Reading sales, SKU allocations and actual payments…';
    el('export').disabled=true;el('print').disabled=true;
    try{
      const response=await fetch('/api/pl/report?'+query(),{signal:abort.signal,cache:'no-store'});
      if(response.status===401){location.href='/login.html';return;}
      const result=await response.json();if(!response.ok||!result.success)throw Error(result.error||'Report could not be loaded.');
      if(request!==sequence)return;report=result;render();el('status').textContent=result.totals.complete?'All included sales and payments have complete report data.':'Gross activity is shown. Profit is unavailable until the listed exceptions are resolved.';
    }catch(error){if(error.name==='AbortError')return;if(request===sequence){report=null;el('cards').innerHTML='';for(const id of ['statement','tax','collections','trends','policy'])el(id).innerHTML='';el('warnings').hidden=true;el('status').className='error';el('status').textContent=error.message;}}
    finally{if(request===sequence)document.body.classList.remove('loading');}
  }
  function showDetails(filter){
    if(!report)return;
    if(filter.startsWith('collection:')){
      const entry=report.collections.find(r=>r.id===filter.slice(11));if(!entry)return;
      el('detailTitle').textContent='Payments for '+entry.reference;
      el('detailBody').innerHTML=table(['Date','Transaction','Method','Collected amount'],entry.payments.map(p=>'<tr><td>'+escape(p.date)+'</td><td>'+escape(p.id)+'</td><td>'+escape(p.method)+'</td>'+num(p.amount)+'</tr>').join('')||'<tr><td colspan="4">Successful payment details have not been received from the source.</td></tr>');
    }else{
      const entries=report.transactions.filter(r=>filter==='all'||filter==='income'&&['sale','return','income'].includes(r.kind)||filter==='sales'&&['sale','return'].includes(r.kind)||filter.startsWith('group:')&&r.group===filter.slice(6)||filter.startsWith('category:')&&r.group+'/'+r.category===filter.slice(9)||filter.startsWith('reference:')&&r.reference===filter.slice(10)||!filter.includes(':')&&Number(r[filter]||0)!==0);
      el('detailTitle').textContent='Underlying transactions ('+entries.length+')';
      el('detailBody').innerHTML=table(['Date / reference','Particulars / category','Account / method','Net sales / income','COGS','Paid expense'],entries.map(r=>'<tr><td>'+escape(r.date)+'<br>'+escape(r.reference)+(r.expenseDate?'<br><span class="muted">Expense: '+escape(r.expenseDate)+'</span>':'')+'</td><td>'+escape(r.particulars)+'<br><span class="muted">'+escape(r.category)+'</span>'+skuDetails(r)+(safeProof(r.proof)?'<br><a href="'+escape(r.proof)+'" target="_blank" rel="noopener">Open proof</a>':'')+'</td><td>'+escape(r.account)+'<br>'+escape(r.method)+'</td>'+num(r.netSales===null?null:r.netSales+r.otherIncome)+num(r.costComplete?r.cogs:null)+num(r.expenses)+'</tr>').join('')||'<tr><td colspan="6">No underlying transactions for this total.</td></tr>');
    }
    el('details').showModal();
  }
  function safeProof(value){return /^\/(?!\/)/.test(value)||/^https:\/\//.test(value);}
  function skuDetails(entry){
    return entry.details?.some(d=>d.sku)?'<details><summary>SKU / purchase allocations</summary>'+entry.details.filter(d=>d.sku).map(d=>'<div class="allocation">'+escape(d.sku)+' · '+escape(d.qty)+' unit(s)'+(d.allocations||[]).map(a=>'<br>'+escape(a.purchaseId)+' · '+a.qty+' × '+fmt(a.unitCost)+' = '+fmt(a.amount)).join('')+'</div>').join('')+'</details>':'';
  }
  document.addEventListener('click',event=>{
    const head=event.target.closest('[data-group-toggle]');
    if(head){
      const expanded=head.getAttribute('aria-expanded')!=='true';
      head.setAttribute('aria-expanded',String(expanded));
      document.querySelectorAll('[data-expense-group="'+head.dataset.groupToggle+'"]').forEach(row=>row.hidden=!expanded);
      return;
    }
    const button=event.target.closest('[data-filter]');if(button)showDetails(button.dataset.filter);
  });
  el('filters').addEventListener('submit',event=>{event.preventDefault();load();});
  document.querySelectorAll('[data-period]').forEach(button=>button.addEventListener('click',()=>period(button.dataset.period)));
  document.querySelectorAll('[data-tab]').forEach(button=>button.addEventListener('click',()=>{document.querySelectorAll('[data-tab]').forEach(b=>b.setAttribute('aria-pressed',String(b===button)));document.querySelectorAll('.view').forEach(view=>view.hidden=view.id!==button.dataset.tab);}));
  el('closeDetails').addEventListener('click',()=>el('details').close());
  el('export').addEventListener('click',()=>{if(report)location.href='/api/pl/report/export?'+new URLSearchParams({from:report.range.from,to:report.range.to,channel:report.channel});});
  el('print').addEventListener('click',()=>window.print());
  period('month');
})();
