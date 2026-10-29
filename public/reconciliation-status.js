(function(root,factory){if(typeof module==='object'&&module.exports)module.exports=factory();else root.ReconciliationStatus=factory();})(typeof window!=='undefined'?window:this,function(){
  'use strict';
  var esc=function(v){return String(v==null?'':v).replace(/[&<>"']/g,function(c){return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c];});};
  var money=function(v){return '₹'+Number(v||0).toLocaleString('en-IN',{minimumFractionDigits:2,maximumFractionDigits:2});};
  function when(v){if(!v)return '';if(/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}$/.test(String(v)))return String(v)+' IST';var date=new Date(v);return Number.isFinite(date.getTime())?new Intl.DateTimeFormat('en-IN',{timeZone:'Asia/Kolkata',dateStyle:'short',timeStyle:'short',hour12:false}).format(date):String(v);}
  function refreshButton(){return '<button type="button" class="btn mini ghost" data-reconciliation-refresh>Refresh shared status</button>';}
  function notice(title,detail,tone){return '<section class="reconciliation-shared-status" role="status" aria-live="polite"><div class="msg '+(tone||'')+'"><b>'+esc(title)+'</b>'+(detail?'<div>'+detail+'</div>':'')+'</div>'+refreshButton()+'</section>';}
  function paytmCoverage(c){
    if(!c)return '<section class="paytm-evidence-coverage" role="status"><div class="msg">Paytm evidence coverage is unavailable. Refresh the shared status; a bank-credit date alone does not confirm the payment report.</div></section>';
    var pendingBank=Number(c.pendingBankOnlySettlements)||0,pendingReceipts=Number(c.pendingReceipts)||0,pendingSettlements=Number(c.pendingSettlements)||0;
    var html='<section class="paytm-evidence-coverage" role="status"><h4>Paytm evidence coverage</h4><div class="claim-meta"><span>Imported payment report through <b>'+esc(c.importedPaymentsThrough||'Not yet')+'</b></span><span>Imported report settlements through <b>'+esc(c.importedSettlementsThrough||'Not yet')+'</b></span></div><div class="claim-meta"><span>Payments reconciled through <b>'+esc(c.reportPaymentsThrough||'Not yet')+'</b></span><span>Report settlements verified and bank-posted through <b>'+esc(c.reportSettlementsThrough||'Not yet')+'</b></span></div><div class="hint">These report dates are separate from the bank credits below. Imported evidence is not automatically a completed reconciliation.</div>';
    html+='<div>Bank settlements seen through <b>'+esc(c.bankSettlementsThrough||'Not yet')+'</b> — bank receipt only; this does not confirm missing Paytm report details.</div>';
    if(Object.prototype.hasOwnProperty.call(c,'overallReconciledThrough'))html+='<div><b>Overall clearing reconciliation through '+esc(c.overallReconciledThrough||'Not yet')+'</b> — includes receipt allocations and historical bank-only gaps.</div>';
    if(c.firstPendingReceiptDate)html+='<div class="hint">Receipt allocation review starts '+esc(c.firstPendingReceiptDate)+'. A completed report batch does not automatically resolve a different Shopify receipt amount.</div>';
    if(c.firstPendingBankOnlyDate)html+='<div class="hint">Bank-only history awaiting report details starts '+esc(c.firstPendingBankOnlyDate)+'. It is separate from completed imported report batches.</div>';
    if(pendingBank||pendingReceipts||pendingSettlements)html+='<div class="msg err">Pending: '+pendingBank+' bank settlement(s) awaiting Paytm report details · '+pendingReceipts+' receipt(s) awaiting verification · '+pendingSettlements+' settlement(s) not fully completed.'+(c.firstPendingSettlementDate?' Earliest pending settlement: '+esc(c.firstPendingSettlementDate)+'.':'')+'</div>';
    if(c.lastVerified)html+='<div class="hint">Last verified report evidence'+(c.lastVerified.by?' by '+esc(c.lastVerified.by):'')+(c.lastVerified.at?' on '+esc(when(c.lastVerified.at)):'')+'.</div>';
    if(c.asOf)html+='<div class="hint">Evidence as of '+esc(c.asOf)+'.</div>';
    return html+'</section>';
  }
  function paytmLedgerRow(s){
    if(!s)return '';
    var tone=s.code==='posted'?'pos':s.code==='incomplete'?'neg':'muted',html='<div class="paytm-ledger-status '+tone+'" data-paytm-status="'+esc(s.code||'report_pending')+'"><b>'+esc(s.label||'Paytm evidence review pending')+'</b>';
    (s.reasons||[]).forEach(function(reason){html+='<div>'+esc(reason)+'</div>';});
    (s.bankReasons||[]).forEach(function(reason){html+='<div>Bank link: '+esc(reason)+'</div>';});
    if(s.settledDate)html+='<div class="muted">Settlement date: '+esc(s.settledDate)+'</div>';
    if((s.settlementIds||[]).length)html+='<div class="muted">Settlement evidence: '+s.settlementIds.map(esc).join(', ')+'</div>';
    if((s.transactionIds||[]).length)html+='<div class="muted">Paytm transaction: '+s.transactionIds.map(esc).join(', ')+'</div>';
    if(s.finalizedAt||s.finalizedBy)html+='<div class="muted">Evidence verified'+(s.finalizedBy?' by '+esc(s.finalizedBy):'')+(s.finalizedAt?' on '+esc(when(s.finalizedAt)):'')+'</div>';
    return html+'</div>';
  }
  function paytm(data){
    var state=data.reconciliationStatus;if(!state)return notice('Paytm status unavailable','Refresh to load the shared saved state.','err');
    var c=state.counts,attempt=state.lastAttempt;
    var stats=[['Imported report settlement batches',c.total],['Linked / posted to Axis',c.posted],['Paytm finalized · bank link pending',c.finalized],['Ready to finalize',c.ready],['Incomplete report settlements',c.incomplete]];
    var html='<section id="paytmSharedStatus" class="reconciliation-shared-status" role="status" aria-live="polite"><h3>Shared Paytm reconciliation status</h3><p class="hint">Owner and Admin see the same saved status. Paytm review and linking the Axis bank credit are separate steps. Refreshes never finalize, classify or post anything.</p><div class="stats">'+stats.map(function(x){return '<div class="stat"><div class="k">'+esc(x[0])+'</div><div class="v">'+Number(x[1]||0)+'</div></div>';}).join('')+'</div>'+refreshButton();
    if(attempt)html+='<p class="hint"><b>Last bulk attempt:</b> '+esc(when(attempt.at))+' by '+esc(attempt.by)+' · '+Number(attempt.newlyFinalized||0)+' newly finalized in that attempt · '+Number(attempt.incomplete||0)+' did not pass. This is not the total previously finalized.</p>';
    html+=paytmCoverage(data.paytmCoverage)+paytmPreviews(data.reportPreviews||[]);
    var incomplete=state.incompleteSettlements||[];
    html+=incomplete.length?'<h4>Why '+incomplete.length+' settlement(s) remain incomplete</h4><div style="overflow:auto"><table class="resp"><thead><tr><th>Settled date / UTR</th><th>Bank net</th><th>Exact finalization reason</th><th>Bank-link status</th></tr></thead><tbody>'+incomplete.map(function(s){return '<tr><td>'+esc(s.settledDate||'Not supplied')+'<div>'+esc(s.utr||s.payoutId)+'</div></td><td>'+money(s.net)+'</td><td>'+s.reasons.map(esc).join('<br>')+(s.blockedTransactions.length?'<details><summary>Payment details ('+s.blockedTransactions.length+')</summary><ul>'+s.blockedTransactions.map(function(t){return '<li>'+esc(t.transactionId)+' — '+esc(t.reason)+'</li>';}).join('')+'</ul></details>':'')+'</td><td>'+(s.bankLinkReady?'Exact Axis credit verified; use its existing bank-link workflow.':s.bankReasons.map(esc).join('<br>'))+'</td></tr>';}).join('')+'</tbody></table></div>':'<p class="msg ok">No incomplete settlements remain within the imported Paytm report. Bank-only settlements awaiting report details are shown separately above.</p>';
    return html+'</section>';
  }
  function paytmPreviews(previews){
    if(!previews.length)return '';
    var active=previews.filter(function(p){return p.actionable;}),covered=previews.filter(function(p){return !p.actionable;});
    function row(p){return '<li><b>'+esc(p.id)+'</b> · '+esc(when(p.at))+' · '+esc(p.by||'')+'<div>'+esc((p.sources||[]).map(function(s){return s.name;}).join(', '))+'</div><div>'+Number(p.newCount||0)+' new · '+Number(p.enrichmentCount||0)+' missing-detail updates · '+Number(p.conflictCount||0)+' conflicts</div>'+(p.reasons||[]).map(function(r){return '<div>'+esc(r)+'</div>';}).join('')+(p.actionable?'<button type="button" class="btn mini ghost" data-pt-resume-preview="'+esc(p.id)+'">Review saved upload preview</button>':'')+'</li>';}
    return '<section id="paytmUploadPreviewStatus" style="overflow-wrap:anywhere"><h4>Upload previews — separate from settlement reconciliation</h4>'+(active.length?'<p class="msg err">'+active.length+' upload preview(s) need evidence review. No upload preview posts or finalizes money.</p><ul>'+active.map(row).join('')+'</ul>':'<p>No upload evidence review is pending.</p>')+(covered.length?'<details><summary>'+covered.length+' already-covered / no-new-row preview(s) — not pending settlements</summary><p>Retained for reference, not deleted or posted automatically.</p><ul>'+covered.map(row).join('')+'</ul></details>':'')+'</section>';
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
  function bankSaved(d){var last=d.lastReconciliation;if(!last&&d.account==='Counter Cash')return notice('Optional cash-register file history','No file-based cash-register reconciliation is saved. This is separate from physical cash verification; see its approved and pending counts below.','');if(!last)return notice('Statement or register required','No finalized reconciliation has been saved for '+esc(d.account)+'.','err');return notice(last.balanceReconciled===false?'Transactions finalized — balance pending':'Finalized reconciliation saved','Reconciled through '+esc(d.updatedThrough||last.to||'')+' · Finalized by '+esc(last.by||last.finalizedBy||'')+' on '+esc(when(last.at||last.finalizedAt))+'.',last.balanceReconciled===false?'err':'ok');}
  function cash(d){
    var pending=d.pending||[],latest=d.latest,review=pending.filter(function(p){return p.reviewRequired;}),clean=pending.length-review.length;
    var title=review.length?'Cash verification review required — '+review.length+' count(s) need review':pending.length?'Awaiting Owner approval — '+pending.length+' cash verification request(s)':latest?'Physical cash verification approved':'Physical cash verification required';
    var detail=latest?'Last approved snapshot through '+esc(latest.verifiedThrough)+' · Physical cash '+money(latest.countedBalance)+' · Ledger '+money(latest.ledgerBalance)+' · Difference '+money(latest.difference)+' · Approved by '+esc(latest.approvedBy||latest.requestedBy)+'.':'No approved physical count yet.';
    if(pending.length)detail+='<div>'+clean+' submitted count(s) agree with the current ledger and await Owner approval; '+review.length+' need review against the current ledger.</div>';
    detail+='<div>A submission is not Owner approval. No status refresh changes cash, approvals or the ledger.</div>';
    return notice(title,detail,review.length?'err':latest&&!pending.length?'ok':'');
  }
  function cashRequest(x){
    var current=x.currentLedgerBalance!=null?money(x.currentLedgerBalance):'Unavailable',difference=x.currentDifference!=null?money(x.currentDifference):'Unavailable';
    return '<b>'+esc(x.id)+' · Physical cash '+money(x.countedBalance)+'</b><div class="muted">Verify through '+esc(x.verifiedThrough)+' · Submitted by '+esc(x.requestedBy)+(x.note?' · '+esc(x.note):'')+'</div><div>Submitted snapshot: ledger '+money(x.ledgerBalance)+' · difference '+money(x.difference)+'</div><div><b>Current check:</b> ledger '+current+' · difference '+difference+'</div><div class="msg '+(x.reviewRequired?'err':'')+'">'+(x.reviewRequired?'<b>Review required before Owner decision.</b> '+(x.reviewReasons||[]).map(esc).join(' '):'Count agrees with the current ledger — awaiting Owner approval.')+' No ledger adjustment has been made.</div>';
  }
  function card(st){if(st.status==='archived')return notice('Duplicate credit-card review archived',esc(st.id)+' · '+(st.rows||[]).length+' retained transaction(s). Excluded from pending reviews; uploaded evidence, saved choices and original posted transactions are unchanged.','');return notice(st.status==='finalized'?'Credit-card statement finalized':'Credit-card statement review pending',esc(st.id)+' · '+(st.rows||[]).length+' transaction(s) · Statement/review movement difference '+money(st.movementDifference)+(st.status==='finalized'?' · Finalized by '+esc(st.finalizedBy||'')+' on '+esc(when(st.finalizedAt)):'. Classifications and all posting checks still need to pass.'),st.status==='finalized'?'ok':'');}
  function createRefreshController(options){
    var running=false;
    return {run:async function(){if(running||options.isHidden&&options.isHidden()||options.isBusy&&options.isBusy())return false;running=true;try{await options.refresh();return true;}catch(error){if(options.onError)options.onError(error);return false;}finally{running=false;}},isRunning:function(){return running;}};
  }
  return {paytm:paytm,paytmCoverage:paytmCoverage,paytmLedgerRow:paytmLedgerRow,paytmPreviews:paytmPreviews,paytmRow:paytmRow,bank:bank,bankSaved:bankSaved,cash:cash,cashRequest:cashRequest,card:card,when:when,createRefreshController:createRefreshController};
});
