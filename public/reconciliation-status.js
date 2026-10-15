(function(root,factory){if(typeof module==='object'&&module.exports)module.exports=factory();else root.ReconciliationStatus=factory();})(typeof window!=='undefined'?window:this,function(){
  'use strict';
  var esc=function(v){return String(v==null?'':v).replace(/[&<>"']/g,function(c){return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c];});};
  var money=function(v){return '₹'+Number(v||0).toLocaleString('en-IN',{minimumFractionDigits:2,maximumFractionDigits:2});};
  function when(v){if(!v)return '';if(/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}$/.test(String(v)))return String(v)+' IST';var date=new Date(v);return Number.isFinite(date.getTime())?new Intl.DateTimeFormat('en-IN',{timeZone:'Asia/Kolkata',dateStyle:'short',timeStyle:'short',hour12:false}).format(date):String(v);}
  function refreshButton(){return '<button type="button" class="btn mini ghost" data-reconciliation-refresh>Refresh shared status</button>';}
  function notice(title,detail,tone){return '<section class="reconciliation-shared-status" role="status" aria-live="polite"><div class="msg '+(tone||'')+'"><b>'+esc(title)+'</b>'+(detail?'<div>'+detail+'</div>':'')+'</div>'+refreshButton()+'</section>';}
  function paytm(data){
    var state=data.reconciliationStatus;if(!state)return notice('Paytm status unavailable','Refresh to load the shared saved state.','err');
    var c=state.counts,attempt=state.lastAttempt;
    var stats=[['Total settlement batches',c.total],['Linked / posted to Axis',c.posted],['Paytm finalized · bank link pending',c.finalized],['Ready to finalize',c.ready],['Incomplete settlements',c.incomplete]];
    var html='<section id="paytmSharedStatus" class="reconciliation-shared-status" role="status" aria-live="polite"><h3>Shared Paytm reconciliation status</h3><p class="hint">Owner and Admin see the same saved status. Paytm review and linking the Axis bank credit are separate steps. Refreshes never finalize, classify or post anything.</p><div class="stats">'+stats.map(function(x){return '<div class="stat"><div class="k">'+esc(x[0])+'</div><div class="v">'+Number(x[1]||0)+'</div></div>';}).join('')+'</div>'+refreshButton();
    if(attempt)html+='<p class="hint"><b>Last bulk attempt:</b> '+esc(when(attempt.at))+' by '+esc(attempt.by)+' · '+Number(attempt.newlyFinalized||0)+' newly finalized in that attempt · '+Number(attempt.incomplete||0)+' did not pass. This is not the total previously finalized.</p>';
    var incomplete=state.incompleteSettlements||[];
    html+=incomplete.length?'<h4>Why '+incomplete.length+' settlement(s) remain incomplete</h4><div style="overflow:auto"><table class="resp"><thead><tr><th>Settled date / UTR</th><th>Bank net</th><th>Exact finalization reason</th><th>Bank-link status</th></tr></thead><tbody>'+incomplete.map(function(s){return '<tr><td>'+esc(s.settledDate||'Not supplied')+'<div>'+esc(s.utr||s.payoutId)+'</div></td><td>'+money(s.net)+'</td><td>'+s.reasons.map(esc).join('<br>')+(s.blockedTransactions.length?'<details><summary>Payment details ('+s.blockedTransactions.length+')</summary><ul>'+s.blockedTransactions.map(function(t){return '<li>'+esc(t.transactionId)+' — '+esc(t.reason)+'</li>';}).join('')+'</ul></details>':'')+'</td><td>'+(s.bankLinkReady?'Exact Axis credit verified; use its existing bank-link workflow.':s.bankReasons.map(esc).join('<br>'))+'</td></tr>';}).join('')+'</tbody></table></div>':'<p class="msg ok">No incomplete Paytm settlements remain.</p>';
    return html+'</section>';
  }
  function paytmRow(s){if(!s)return '';return '<div class="msg '+(s.code==='incomplete'?'err':'ok')+'"><b>'+esc(s.label)+'</b>'+(s.reasons||[]).map(function(r){return '<div>'+esc(r)+'</div>';}).join('')+(s.bankReasons||[]).map(function(r){return '<div class="muted">Bank link: '+esc(r)+'</div>';}).join('')+'</div>';}
  function bank(d){
    var approval=d.finalizationApproval,pending=approval&&approval.status==='pending',conflicts=d.reconciliationConflicts||[],title,tone='';
    if(conflicts.length){title='Review blocked — reconciliation conflicts';tone='err';}
    else if(d.statementSummary&&d.statementSummary.validated===false){title='Review blocked — statement totals are not validated';tone='err';}
    else if(d.unresolved){title='Review required — '+d.unresolved+' unresolved item(s)';tone='err';}
    else if(pending){title='Awaiting Owner approval';}
    else if(!d.balanceResolved){title='Transactions reviewed — closing balance pending';tone='err';}
    else{title='Ready for final reconciliation approval';tone='ok';}
    var detail='Shared statement '+esc(d.draftId)+' · Ledger closing '+money(d.ledgerClosing)+' · Bank closing '+money(d.bankClosing)+' · Difference '+money(d.balanceDifference)+'. No status refresh changes balances.';
    if(pending)detail+='<div>Final reconciliation requested by '+esc(approval.requestedBy)+' on '+esc(when(approval.requestedAt))+'. Awaiting Owner approval; nothing has been posted by this request.</div>';
    if(approval&&approval.status==='rejected')detail+='<div>Previous request rejected: '+esc(approval.reason||approval.decisionReason||'Review the approval record.')+'</div>';
    return notice(title,detail,tone);
  }
  function bankSaved(d){var last=d.lastReconciliation;if(!last)return notice('Statement or register required','No finalized reconciliation has been saved for '+esc(d.account)+'.','err');return notice(last.balanceReconciled===false?'Transactions finalized — balance pending':'Finalized reconciliation saved','Reconciled through '+esc(d.updatedThrough||last.to||'')+' · Finalized by '+esc(last.by||last.finalizedBy||'')+' on '+esc(when(last.at||last.finalizedAt))+'.',last.balanceReconciled===false?'err':'ok');}
  function cash(d){var pending=d.pending||[],latest=d.latest;return notice(pending.length?'Awaiting Owner approval — '+pending.length+' cash verification request(s)':latest?'Physical cash verification approved':'Physical cash verification required',latest?'Last approved through '+esc(latest.verifiedThrough)+' · Physical cash '+money(latest.countedBalance)+' · Ledger '+money(latest.ledgerBalance)+' · Difference '+money(latest.difference)+' · Approved by '+esc(latest.approvedBy||latest.requestedBy)+'.':'No approved physical count yet. A count or approval never adjusts the ledger automatically.',latest&&!pending.length?'ok':'');}
  function card(st){return notice(st.status==='finalized'?'Credit-card statement finalized':'Credit-card statement review pending',esc(st.id)+' · '+(st.rows||[]).length+' transaction(s) · Statement/review movement difference '+money(st.movementDifference)+(st.status==='finalized'?' · Finalized by '+esc(st.finalizedBy||'')+' on '+esc(when(st.finalizedAt)):'. Classifications and all posting checks still need to pass.'),st.status==='finalized'?'ok':'');}
  function createRefreshController(options){
    var running=false;
    return {run:async function(){if(running||options.isHidden&&options.isHidden()||options.isBusy&&options.isBusy())return false;running=true;try{await options.refresh();return true;}catch(error){if(options.onError)options.onError(error);return false;}finally{running=false;}},isRunning:function(){return running;}};
  }
  return {paytm:paytm,paytmRow:paytmRow,bank:bank,bankSaved:bankSaved,cash:cash,card:card,when:when,createRefreshController:createRefreshController};
});
