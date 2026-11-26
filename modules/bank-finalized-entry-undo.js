'use strict';

const crypto=require('crypto');

const list=value=>Array.isArray(value)?value:Object.values(value||{});
const money=value=>Math.round(Number(value||0)*100)/100;
const clone=value=>JSON.parse(JSON.stringify(value));
const text=value=>String(value==null?'':value);

function rowFingerprint(record,row,strongSignature){
  return crypto.createHash('sha256').update(JSON.stringify({
    recordId:record.id,draftId:record.draftId,rowId:row.id,
    bank:strongSignature(row.bank),linked:(row.linkedRecordIds||[]).map(String).sort(),
    ledger:row.ledger&&row.ledger.id||'',decision:row.decision||'',reason:row.reason||''
  })).digest('hex');
}

function allRows(record){
  return [].concat(record.reconciliationRows||[],record.carriedReconciliationRows||[]);
}

function sourceCreatedByReconciliation(store,record,row){
  const ids=new Set((row.linkedRecordIds||[]).map(String)),draftIds=new Set([text(record.draftId),text(record.id)]),matches=source=>{
    if(!source)return false;
    const sourceDraft=text(source.reconciliationDraft||source.reconciliationSource&&source.reconciliationSource.draftId);
    if(!draftIds.has(sourceDraft))return false;
    if(text(source.bankRowId||source.reconciliationSource&&source.reconciliationSource.rowId)===text(row.id))return true;
    return [source.id,source.adjustmentId,source.settlementId,source.transferId].some(id=>ids.has(text(id)));
  };
  for(const name of ['adjustments','transfers','receipts','vendorAdvances','vendorOpeningPayables','bankTruthMovements','reconciliationExpenses','paytmSettlements'])if(list(store[name]).some(matches))return true;
  return Object.values(store.expenses||{}).some(expense=>{
    if(!expense)return false;
    if(matches(expense))return true;
    return (expense.payments||[]).some(payment=>draftIds.has(text(payment.bankReconciliationDraft))&&(text(payment.bankReconciliationRow)===text(row.id)||ids.has(text(expense.id+'/'+payment.id))));
  });
}

function eligibility(store,record,row){
  if(!record||!record.finalizedAt||!row||!row.bank)return{available:false,reason:'Finalized bank evidence is unavailable.'};
  if(row.reopenedAt)return{available:false,reason:'This entry is already reopened.'};
  const linked=(row.linkedRecordIds||[]).map(String);
  if(sourceCreatedByReconciliation(store,record,row))return{available:false,reason:'This reconciliation created an accounting posting. Correct or reverse that source entry first; a one-click unlink could duplicate money.'};
  return{available:true,reason:'Reopens this bank row only. The bank transaction and underlying ledger entry are retained.'};
}

function summaryFor(record,rows){
  if(record.statementSummary&&record.statementSummary.from&&record.statementSummary.to)return clone(record.statementSummary);
  const ordered=rows.slice().sort((a,b)=>text(a.date).localeCompare(text(b.date))),first=ordered[0]||{},last=ordered.at(-1)||{},opening=Number.isFinite(Number(first.balance))?money(Number(first.balance)-Number(first.credit||0)+Number(first.debit||0)):0,closing=Number.isFinite(Number(last.balance))?money(last.balance):money(opening+ordered.reduce((sum,row)=>sum+Number(row.credit||0)-Number(row.debit||0),0));
  return{format:'REOPENED',from:text(first.date),to:text(last.date),openingBalance:opening,closingBalance:closing,totalDebits:money(ordered.reduce((sum,row)=>sum+Number(row.debit||0),0)),totalCredits:money(ordered.reduce((sum,row)=>sum+Number(row.credit||0),0)),validated:true};
}

function create(deps){
  const find=(store,nature,account,recordId,rowId)=>{
    const book=(store.bankStatements||{})[deps.bookKey(nature,account)],record=book&&(book.imports||[]).find(item=>text(item.id)===text(recordId)),row=record&&allRows(record).find(item=>text(item.id)===text(rowId));
    return{book,record,row};
  };
  const requests=store=>store.bankReconciliationUndoRequests=store.bankReconciliationUndoRequests&&typeof store.bankReconciliationUndoRequests==='object'?store.bankReconciliationUndoRequests:{};
  const pendingFor=(store,record,row)=>Object.values(requests(store)).filter(item=>item.recordId===record.id&&item.rowId===row.id&&item.status==='pending').sort((a,b)=>text(b.requestedAt).localeCompare(text(a.requestedAt)))[0]||null;

  function request(req,res){
    if(!deps.isOwner(req)&&!deps.isPrashant(req))return res.status(403).json({success:false,error:'Only Prashant or the Owner can request undo of a finalized reconciliation entry.'});
    const store=deps.loadStore(),body=req.body||{},nature=deps.nature(body.nature),account=text(body.account).trim(),recordId=text(body.recordId),rowId=text(body.rowId),reason=text(body.reason).trim();
    if(!deps.canAccess(req,store,nature,account))return res.status(403).json({success:false,error:'You cannot access this account reconciliation.'});
    if(reason.length<10)return res.status(400).json({success:false,error:'Enter a clear reason of at least 10 characters.'});
    const {record,row}=find(store,nature,account,recordId,rowId);if(!record||!row)return res.status(404).json({success:false,error:'Finalized reconciliation entry not found.'});
    const allowed=eligibility(store,record,row);if(!allowed.available)return res.status(409).json({success:false,error:allowed.reason});
    const existing=pendingFor(store,record,row);if(existing)return res.status(409).json({success:false,error:'An undo request for this entry is already awaiting Owner approval.',request:existing});
    const now=new Date().toISOString(),item={id:'BRU-'+Date.now()+'-'+crypto.randomBytes(2).toString('hex'),nature,account,recordId:record.id,rowId:row.id,status:'pending',reason,requestedBy:req.user.username,requestedAt:now,fingerprint:rowFingerprint(record,row,deps.strongSignature),bank:{date:row.bank.date,debit:Number(row.bank.debit||0),credit:Number(row.bank.credit||0),reference:row.bank.reference||'',description:row.bank.description||''},linkedRecordIds:(row.linkedRecordIds||[]).slice()};
    requests(store)[item.id]=item;deps.audit(store,req,'FINALIZED_BANK_ENTRY_UNDO_REQUESTED','bank_reconciliation',record.id,{nature,account,rowId:row.id,after:item,note:reason});deps.saveStore(store);return res.status(202).json({success:true,approvalPending:true,request:item});
  }

  function decide(req,res){
    if(!deps.isOwner(req))return res.status(403).json({success:false,error:'Only the Owner can approve or reject a finalized-entry undo request.'});
    const store=deps.loadStore(),body=req.body||{},item=requests(store)[text(body.requestId)],decision=text(body.decision).toLowerCase(),decisionReason=text(body.reason).trim();
    if(!item||item.status!=='pending')return res.status(404).json({success:false,error:'Pending undo request not found.'});
    if(!['approve','reject'].includes(decision))return res.status(400).json({success:false,error:'Choose approve or reject.'});
    if(!deps.canAccess(req,store,item.nature,item.account))return res.status(403).json({success:false,error:'You cannot access this account reconciliation.'});
    if(decision==='reject'){
      item.status='rejected';item.rejectedBy=req.user.username;item.rejectedAt=new Date().toISOString();item.decisionReason=decisionReason;deps.audit(store,req,'FINALIZED_BANK_ENTRY_UNDO_REJECTED','bank_reconciliation',item.recordId,{nature:item.nature,account:item.account,rowId:item.rowId,before:{status:'pending'},after:item,note:decisionReason||'Owner rejected undo request'});deps.saveStore(store);return res.json({success:true,request:item});
    }
    const {book,record,row}=find(store,item.nature,item.account,item.recordId,item.rowId);if(!book||!record||!row)return res.status(409).json({success:false,error:'The finalized entry changed or is unavailable. Nothing was reopened.'});
    const allowed=eligibility(store,record,row);if(!allowed.available)return res.status(409).json({success:false,error:allowed.reason});
    if(rowFingerprint(record,row,deps.strongSignature)!==item.fingerprint)return res.status(409).json({success:false,error:'The finalized entry changed after the request. Review it again; nothing was reopened.'});
    if(Object.values(store.bankReconciliationDrafts||{}).some(draft=>draft.account===item.account&&deps.nature(draft.nature)===item.nature))return res.status(409).json({success:false,error:'Finish or discard the existing reconciliation preview for this account before approving this undo.'});
    const stored=deps.findStored(book,row);if(stored.error||!stored.transaction)return res.status(409).json({success:false,error:stored.error||'The exact stored bank transaction could not be identified. Nothing was reopened.'});
    const storedEntries=Object.entries(book.transactions||{}).filter(([,value])=>value===stored.transaction||text(value&&value.id)===text(stored.transaction.id));if(storedEntries.length!==1)return res.status(409).json({success:false,error:'The exact stored bank transaction is missing or ambiguous. Nothing was reopened.'});
    const sourceRows=allRows(record).filter(candidate=>candidate&&candidate.bank),seen=new Map(),rows=[];let targetIndex=-1;
    sourceRows.forEach(candidate=>{const key=deps.strongSignature(candidate.bank),occurrence=(seen.get(key)||0)+1;seen.set(key,occurrence);const copy=clone(candidate.bank);rows.push(copy);if(candidate===row)targetIndex=rows.length-1;});
    if(targetIndex<0||!rows.length)return res.status(409).json({success:false,error:'The retained statement rows could not be reconstructed safely. Nothing was reopened.'});
    const now=new Date().toISOString(),draftId='BRD-REOPEN-'+Date.now()+'-'+crypto.randomBytes(3).toString('hex'),before={linkedRecordIds:(row.linkedRecordIds||[]).slice(),ledger:row.ledger?clone(row.ledger):null,decision:row.decision||'',reason:row.reason||'',storedTransactionId:stored.transaction.id};
    delete book.transactions[storedEntries[0][0]];
    const linked=new Set(before.linkedRecordIds);for(const mapName of ['bankDateOverrides','bankReconciliationLinks'])for(const id of linked){const value=(store[mapName]||{})[id];if(value&&[text(record.draftId),text(record.id)].includes(text(value.reconciliationDraft||value.recordId)))delete store[mapName][id];}
    row.undoHistory=Array.isArray(row.undoHistory)?row.undoHistory:[];row.undoHistory.push({requestId:item.id,before:clone(before),reason:item.reason,approvedBy:req.user.username,approvedAt:now,reopenedDraftId:draftId});row.linkedRecordIds=[];row.ledger=null;row.decision='Reopened after Owner-approved undo';row.reason=item.reason;row.reopenedAt=now;row.reopenedBy=req.user.username;row.reopenedDraftId=draftId;row.undoRequestId=item.id;
    store.bankReconciliationDrafts=store.bankReconciliationDrafts||{};const tag=row.entityTag&&row.entityTag.nature?clone(row.entityTag):null;store.bankReconciliationDrafts[draftId]={id:draftId,account:item.account,nature:item.nature,transactions:rows,summary:summaryFor(record,rows),resolutions:{},matchingPolicy:'balanced_date_amount_v5',originalName:'Reopened finalized entry '+record.id+' · '+row.id,createdAt:now,createdBy:req.user.username,reopenedFromFinalized:{recordId:record.id,rowId:row.id,requestId:item.id,targetIndex,at:now},...(tag?{transactionOwnership:{['bank-'+targetIndex]:{nature:tag.nature,reason:'Preserved entity from finalized bank evidence while the ledger link is re-reviewed.',by:req.user.username,at:now,source:'reopened_finalized_entry'}}}:{})};
    item.status='approved';item.approvedBy=req.user.username;item.approvedAt=now;item.reopenedDraftId=draftId;item.before=before;
    deps.audit(store,req,'FINALIZED_BANK_ENTRY_UNDO_APPROVED','bank_reconciliation',record.id,{nature:item.nature,account:item.account,rowId:row.id,before,after:{requestId:item.id,reopenedDraftId:draftId,bankTransactionRetainedInHistory:true,ledgerSourceRetained:true},note:item.reason});deps.saveStore(store);return res.json({success:true,request:item,draftId,reopened:true});
  }

  function decorate(store,record){
    if(!record)return record;
    const copy=clone(record);allRows(copy).forEach(row=>{const original=allRows(record).find(candidate=>candidate.id===row.id),pending=original&&pendingFor(store,record,original),allowed=original&&eligibility(store,record,original);row.undoRequest=pending?clone(pending):null;row.undoAvailable=!!(allowed&&allowed.available);row.undoUnavailableReason=allowed&&!allowed.available?allowed.reason:'';});return copy;
  }

  function register(router){router.post('/api/expenses/bank-statements/finalized-entry-undo/request',request);router.post('/api/expenses/bank-statements/finalized-entry-undo/decision',decide);}
  return{register,request,decide,decorate,eligibility:(store,record,row)=>eligibility(store,record,row)};
}

module.exports={create,rowFingerprint,eligibility};
