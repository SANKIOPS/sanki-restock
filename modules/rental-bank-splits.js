'use strict';
// Explicit, exact-paise allocation of one bank credit; no automatic migration.
const { randomUUID, createHash } = require('node:crypto'), rental = require('./rental-register');
const list = x => Array.isArray(x) ? x : [], clone = x => JSON.parse(JSON.stringify(x));
const fail = (message, status = 409) => { const error = new Error(message); error.status = status; throw error; };
const active = x => x && !x.reversedAt && !x.voidedAt;
const canonical = x => Array.isArray(x) ? '[' + x.map(canonical).join(',') + ']' : x && typeof x === 'object' ? '{' + Object.keys(x).sort().filter(k => x[k] !== undefined).map(k => JSON.stringify(k) + ':' + canonical(x[k])).join(',') + '}' : JSON.stringify(x);
const hash = x => createHash('sha256').update(canonical(x)).digest('hex');
function cents(value, positive = false) {
  const n = Number(value), c = Math.round(n * 100);
  if (!['number','string'].includes(typeof value) || typeof value==='string'&&!/^\d+(?:\.\d{1,2})?$/.test(value) || !Number.isFinite(n) || n < 0 || positive && n <= 0 || !Number.isSafeInteger(c) || Math.abs(n * 100 - c) > .00001) fail('Enter valid rupees and paise; both rent and security must be positive.', 400);
  return c;
}
function bankIdentity(account, bank) { return hash(['rental-bank-split-v1', account, bank.date, String(bank.reference || ''), String(bank.description || ''), cents(bank.debit ?? 0), cents(bank.credit ?? 0), bank.balance == null ? null : Number(bank.balance)]); }
function sameBank(a,b) {
  if (!a || !b || a.date !== b.date) return false;
  try { if(cents(a.debit ?? 0)!==cents(b.debit ?? 0)||cents(a.credit ?? 0)!==cents(b.credit ?? 0))return false; } catch (_) { return true; }
  const ref=x=>String(x||'').toLowerCase().replace(/[^a-z0-9]/g,''), ar=ref(a.reference),br=ref(b.reference);if(ar&&br)return ar===br;
  const text=x=>String(x||'').toLowerCase().replace(/\s+/g,' ').trim(),ad=text(a.description),bd=text(b.description);
  return !ad||!bd||ad===bd||a.balance==null||b.balance==null||Math.abs(Number(a.balance)-Number(b.balance))<.005;
}
function foreignClaimError(store,draft,bank) {
  for(const other of Object.values(store.bankReconciliationDrafts||{})) {
    if(!other||other.id===draft.id||other.account!==draft.account)continue;
    for(const [rowId,r] of Object.entries(other.resolutions||{})) {
      if(!r||r.action==='unmatch_suggestion')continue;
      const ids=new Set([rowId,r.linkedRowId,r.primaryRowId,r.bankRowId].concat(list(r.bankRowIds),list(r.bankReferences).map(x=>x&&x.rowId)).filter(x=>/^bank-/.test(String(x||''))));
      for(const id of ids){const m=/^bank-(\d+)$/.exec(String(id)),original=m&&list(other.transactions)[Number(m[1])];if(original&&sameBank(original,bank)||!original&&list(other.transactions).some(x=>sameBank(x,bank)))return 'This bank transaction already has a saved review in another reconciliation, or its identity is ambiguous. Review that saved decision instead of creating another receipt.';}
    }
  }return '';
}
function assertWritable(store,draft,bank) {
  if(!draft||draft.originalStatementPeriod||draft.reconstructedFromFinalized||draft.finalizedAt||draft.status==='finalized')fail('This statement is locked or belongs to finalized history.');
  const date=String(bank&&bank.date||'');if(!/^\d{4}-\d{2}-\d{2}$/.test(date)||!Number.isFinite(Date.parse(date+'T00:00:00Z'))||new Date(date+'T00:00:00Z').toISOString().slice(0,10)!==date)fail('The statement transaction date is invalid.');
  if(Object.values(store.bankReconciliationApprovals||{}).some(x=>x&&x.status==='pending'&&(x.draftId===draft.id||x.account===draft.account&&(!x.from||date>=x.from)&&(!x.to||date<=x.to))))fail('This reconciliation is awaiting Owner approval. Withdraw and review that request first.');
  for(const [key,book] of Object.entries(store.bankStatements||{}))if(book&&(key===draft.account||key.endsWith('|'+draft.account)||book.account===draft.account)&&book.reconciledThrough&&date<=book.reconciledThrough)fail('This transaction belongs to a finalized bank period.');
}
function contains(value,ids){if(typeof value==='string')return ids.has(value);if(Array.isArray(value))return value.some(x=>contains(x,ids));return !!value&&typeof value==='object'&&Object.entries(value).some(([k,x])=>ids.has(k)||contains(x,ids));}
function record(store,{draft,row,body,register,nature,account,actor,now}) {
  if(store.receipts!=null&&!Array.isArray(store.receipts)||store.receiptSeq!=null&&(!Number.isSafeInteger(store.receiptSeq)||store.receiptSeq<0))fail('The receipt register or sequence cannot be verified. Nothing was recorded.');
  if(!register||typeof register!=='object'||Array.isArray(register)||!Array.isArray(register.tenancies)||!Array.isArray(register.invoices)||register.revision!=null&&(!Number.isSafeInteger(register.revision)||register.revision<0))fail('The rental register cannot be verified. Nothing was recorded.');
  if(!Number.isSafeInteger((register.revision??0)+1))fail('The rental revision is exhausted. Review the register before recording another receipt.');
  if(row&&row.resolution||(draft.resolutions||{})[body.rowId])fail('This bank transaction already has a saved decision. Review or undo it before creating another receipt.');
  if(Object.keys(body).some(k=>!['draftId','rowId','tenancyId','kind','note','rentAmount','securityAmount'].includes(k)))fail('Only the tenant, rent/security split and remark may be supplied; bank amount, date and account remain authoritative.',400);
  if(body.kind!=='rent_security'||!row||row.status!=='missing_in_app'||row.app||row.resolution||!row.bank)fail('Choose one unmatched incoming bank transaction without an existing decision.',400);
  const bank=row.bank,amount=cents(bank.credit??0,true),rent=cents(body.rentAmount,true),security=cents(body.securityAmount,true);
  if(cents(bank.debit??0)||rent+security!==amount)fail('Rent plus security must exactly equal the incoming bank credit, to the paise.',400);
  if(!['SAMAST','PERSONAL'].includes(nature)||!account||account!==draft.account)fail('Choose a valid receiving account and its authorized SAMAST or PERSONAL entity.',400);
  assertWritable(store,draft,bank);
  if(row.reversalReviewRequired||row.reversalPairCandidate||row.bankReversalPair||bank.reversal)fail('Review the bank reversal and its original payment instead of recording rent and security.');
  if(row.entityTag&&(row.entityTag.sourceConflict||['mixed_linked_entries','ambiguous_candidates'].includes(row.entityTag.source))||list(row.reconciliationConflicts).length)fail('Resolve conflicting source ownership before recording rent and security.');
  const tenants=list(register&&register.tenancies).filter(x=>x.id===String(body.tenancyId||''));if(tenants.length!==1)fail('Choose one existing rental tenant / floor.',400);
  const tenancy=tenants[0],note=String(body.note||'').trim();if(!note||note.length>2000)fail('Enter a reconciliation remark of no more than 2,000 characters.',400);
  if(tenancy.securityOpening!=null&&tenancy.securityOpeningDate&&bank.date<=tenancy.securityOpeningDate)fail('This tenant has a recorded opening security balance as of '+tenancy.securityOpeningDate+'. Review that opening balance before adding an earlier security deposit, so it is not counted twice.');
  const m=/^bank-(\d+)$/.exec(String(body.rowId||'')),original=m&&list(draft.transactions)[Number(m[1])],signature=bankIdentity(account,bank);
  if(!original||bankIdentity(account,original)!==signature)fail('The selected transaction no longer matches the retained bank evidence. Reload the statement.');
  const same=e=>e&&(e.draftId===draft.id&&e.rowId===body.rowId||e.bankSignature===signature);
  if((draft.resolutions||{})[body.rowId]||list(store.receipts).some(x=>same(x.bankReconciliationEvidence))||Object.values(store.expenses||{}).some(x=>same(x.reconciliationSource)||same(x.bankReconciliationEvidence)))fail('This bank transaction already has a recorded source. Link or review that source instead.');
  const conflict=foreignClaimError(store,draft,bank);if(conflict)fail(conflict);
  for(const key of ['allocations','credits','deposits','activity'])if(!Array.isArray(register[key]))fail('The rental register cannot be verified. Nothing was recorded.');
  if(register.bankReceiptSplits!=null&&!Array.isArray(register.bankReceiptSplits))fail('The saved rental receipt splits cannot be verified. Nothing was recorded.');
  const id='RBS-'+randomUUID(),evidence={draftId:draft.id,rowId:body.rowId,reference:String(bank.reference||''),description:String(bank.description||''),bankSignature:signature},source=[tenancy.tenant,tenancy.property,tenancy.floor].filter(Boolean).join(' · '),receipts=[],records=[];
  let seq=Number.isSafeInteger(store.receiptSeq)?store.receiptSeq:0;
  for(const [kind,value] of [['rent',rent],['security',security]]){let receiptId;do{if(!Number.isSafeInteger(seq+1))fail('The receipt sequence is exhausted. Nothing was recorded.');receiptId='REC-'+String(++seq).padStart(5,'0');}while(list(store.receipts).some(x=>x.id===receiptId)||receipts.some(x=>x.id===receiptId));receipts.push({id:receiptId,nature,account,amount:value/100,receiptType:kind==='rent'?'rental_income':'rental_security',category:kind==='rent'?'Rental Income':'Rental Security Deposit',source,date:bank.date,note,proof:'',proofs:[],proofException:'Verified directly against uploaded bank statement',createdBy:actor,createdAt:now,tenancyId:tenancy.id,rentalKind:kind,rentalBankSplitId:id,bankReconciliationEvidence:clone(evidence)});}
  let remaining=rent;const invoices=list(register.invoices).filter(x=>!x.voidedAt&&x.tenancyId===tenancy.id&&(!x.month||x.month<=bank.date.slice(0,7))&&rental.outstanding(register,x)>0).sort((a,b)=>String(a.dueDate||a.month||'').localeCompare(String(b.dueDate||b.month||'')));
  for(const invoice of invoices){if(!remaining)break;const applied=Math.min(remaining,cents(rental.outstanding(register,invoice),true));records.push({collection:'allocations',value:{id:'RA-'+randomUUID(),tenancyId:tenancy.id,invoiceId:invoice.id,kind:'receipt',receiptId:receipts[0].id,creditId:'',amount:applied/100,date:bank.date,reason:note,createdAt:now,createdBy:actor,rentalBankSplitId:id}});remaining-=applied;}
  if(remaining)records.push({collection:'credits',value:{id:'CREDIT-'+randomUUID(),tenancyId:tenancy.id,receiptId:receipts[0].id,amount:remaining/100,date:bank.date,reason:note,createdAt:now,createdBy:actor,rentalBankSplitId:id}});
  records.push({collection:'deposits',value:{id:'SEC-'+randomUUID(),tenancyId:tenancy.id,receiptId:receipts[1].id,amount:security/100,date:bank.date,reason:note,createdAt:now,createdBy:actor,rentalBankSplitId:id}});
  const group={id,tenancyId:tenancy.id,nature,account,date:bank.date,amount:amount/100,rentAmount:rent/100,securityAmount:security/100,receiptIds:receipts.map(x=>x.id),recordIds:records.map(x=>x.value.id),sourceFingerprints:receipts.map(hash),records:records.map(x=>({collection:x.collection,id:x.value.id,fingerprint:hash(x.value)})),bankReconciliationEvidence:clone(evidence),note,createdBy:actor,createdAt:now};
  // Validate and plan both parts before mutating either source.
  store.receiptSeq=seq;store.receipts=list(store.receipts).concat(receipts);records.forEach(x=>register[x.collection].push(x.value));register.bankReceiptSplits=list(register.bankReceiptSplits).concat(group);register.activity.push({at:now,by:actor,action:'bank_rent_security_receipt',tenancyId:tenancy.id,rentalBankSplitId:id,detail:note+' · Rent ₹'+rent/100+' · Security ₹'+security/100+' · '+group.receiptIds.join(' + ')});register.revision=(Number(register.revision)||0)+1;store.rentals=register;
  draft.transactionOwnership=draft.transactionOwnership||{};draft.transactionOwnership[body.rowId]={nature,reason:note,by:actor,at:now,source:'rental_workflow'};
  draft.resolutions=draft.resolutions||{};draft.resolutions[body.rowId]={action:'link_multiple_existing',reason:note,remark:note,description:source+' · Rent ₹'+rent/100+' + Security ₹'+security/100,appIds:group.receiptIds.slice(),receiptIds:group.receiptIds.slice(),rentalKind:'rent_security',rentalBankSplitId:id,tenancyId:tenancy.id,rentalRecordIds:group.recordIds.slice(),rentAmount:rent/100,securityAmount:security/100,postingNature:nature,by:actor,at:now};
  return {receipt:receipts[0],receipts,rentalSplit:group,tenancy:{id:tenancy.id,tenant:tenancy.tenant,floor:tenancy.floor,property:tenancy.property},rentalKind:'rent_security'};
}
function evidenceError(store,draft) {
  try{
    if(store.rentals&&store.rentals.bankReceiptSplits!=null&&!Array.isArray(store.rentals.bankReceiptSplits))return 'The saved rent/security split register cannot be verified.';
    const groups=list(store.rentals&&store.rentals.bankReceiptSplits).filter(x=>active(x)&&x.bankReconciliationEvidence&&x.bankReconciliationEvidence.draftId===draft.id),resolutions=Object.entries(draft.resolutions||{}).filter(([,x])=>x&&x.rentalBankSplitId);
    if(groups.length!==resolutions.length)return 'The rent/security sources and saved review disagree. Review both parts before finalizing.';
    for(const [rowId,r] of resolutions){const found=groups.filter(x=>x.id===r.rentalBankSplitId),m=/^bank-(\d+)$/.exec(rowId),bank=m&&list(draft.transactions)[Number(m[1])];if(found.length!==1||!bank)return 'The rent/security split or original bank transaction cannot be identified uniquely.';
      const g=found[0],e=g.bankReconciliationEvidence,sources=g.receiptIds.map(id=>list(store.receipts).filter(x=>x.id===id));
      const tenants=list(store.rentals&&store.rentals.tenancies).filter(x=>x.id===g.tenancyId);if(tenants.length!==1||tenants[0].securityOpening!=null&&tenants[0].securityOpeningDate&&bank.date<=tenants[0].securityOpeningDate)return 'The tenancy or opening security balance changed after review. Review that opening balance before finalizing to prevent duplicate security.';
      if(g.receiptIds.length!==2||new Set(g.receiptIds).size!==2||sources.some(x=>x.length!==1)||bankIdentity(draft.account,bank)!==e.bankSignature||e.rowId!==rowId||g.account!==draft.account||g.date!==bank.date||!['SAMAST','PERSONAL'].includes(g.nature)||r.action!=='link_multiple_existing'||r.rentalKind!=='rent_security'||r.tenancyId!==g.tenancyId||r.postingNature!==g.nature||hash(r.appIds)!==hash(g.receiptIds)||hash(r.receiptIds)!==hash(g.receiptIds)||hash(r.rentalRecordIds)!==hash(g.recordIds)||cents(r.rentAmount,true)!==cents(g.rentAmount,true)||cents(r.securityAmount,true)!==cents(g.securityAmount,true)||cents(g.rentAmount,true)+cents(g.securityAmount,true)!==cents(bank.credit,true)||cents(g.amount,true)!==cents(bank.credit,true)||cents(bank.debit??0))return 'The rent/security split no longer matches its retained bank evidence. Review both parts; nothing was posted.';
      for(let i=0;i<2;i++)if(hash(sources[i][0])!==g.sourceFingerprints[i]||sources[i][0].accountingExcluded||sources[i][0].rentalBankSplitId!==g.id||sources[i][0].receiptType!==(i===0?'rental_income':'rental_security')||cents(sources[i][0].amount,true)!==cents(i===0?g.rentAmount:g.securityAmount,true))return 'A linked rent/security receipt changed after review. Use its audited source correction workflow.';
      for(const record of g.records){const found=list(store.rentals&&store.rentals[record.collection]).filter(x=>x.id===record.id);if(found.length!==1||hash(found[0])!==record.fingerprint)return 'A rental allocation, advance or security deposit changed after review. Review its source before finalizing.';}
      for(const allocation of list(store.rentals.allocations).filter(x=>g.recordIds.includes(x.id))){const invoices=list(store.rentals.invoices).filter(x=>x.id===allocation.invoiceId&&x.tenancyId===g.tenancyId&&!x.voidedAt);if(invoices.length!==1||rental.outstanding(store.rentals,invoices[0])<0)return 'A rent invoice is missing, voided or over-allocated after review. Review the invoice before finalizing.';}
      for(const id of g.receiptIds)if(cents(rental.receiptUsage(store.rentals,id))!==cents(list(store.receipts).find(x=>x.id===id).amount,true))return 'The rental allocations no longer equal the classified bank amount.';
      const conflict=foreignClaimError(store,draft,bank);if(conflict)return conflict;
    }return '';
  }catch(_){return 'The saved rent/security split evidence cannot be verified. Review it before finalizing; nothing was posted.';}
}
function undo(store,{draft,rowId,actor,now,reason}) {
  if(!store.rentals||!Number.isSafeInteger(store.rentals.revision)||store.rentals.revision<0||!Number.isSafeInteger(store.rentals.revision+1))fail('The rental revision cannot be incremented safely. Nothing was removed.');
  const r=(draft.resolutions||{})[rowId];if(!r||!r.rentalBankSplitId)fail('Choose a saved rent and security review.',404);const m=/^bank-(\d+)$/.exec(rowId),bank=m&&list(draft.transactions)[Number(m[1])];assertWritable(store,draft,bank);
  const error=evidenceError(store,draft);if(error)fail(error);if(!String(reason||'').trim())fail('Enter a reason for undoing the rent and security receipt.',400);
  const register=store.rentals,g=register.bankReceiptSplits.find(x=>x.id===r.rentalBankSplitId&&active(x)),ids=new Set(g.recordIds),receiptIds=new Set(g.receiptIds),refs=new Set([g.id].concat(g.recordIds,g.receiptIds)),after={...register,allocations:register.allocations.filter(x=>!ids.has(x.id)),credits:register.credits.filter(x=>!ids.has(x.id)),deposits:register.deposits.filter(x=>!ids.has(x.id))};
  // Project and validate removal of both parts before touching either one.
  for(const id of g.receiptIds){const dependency=rental.receiptRemovalError(register,id,g.recordIds);if(dependency)fail(dependency);}
  if(['allocations','credits','deposits'].some(k=>contains(after[k],refs)))fail('This rent/security receipt has dependent rental settlements. Reverse those first.');
  const otherDrafts=Object.values(store.bankReconciliationDrafts||{}).map(x=>x.id===draft.id?{...x,resolutions:Object.fromEntries(Object.entries(x.resolutions||{}).filter(([id])=>id!==rowId)),decisionAudit:[]}:x),overrides=Object.fromEntries(Object.entries(store.bankDateOverrides||{}).filter(([id,v])=>!(receiptIds.has(id)&&v&&v.provisional&&v.reconciliationDraft===draft.id)));
  if([store.bankStatements,otherDrafts,overrides,store.bankReconciliationLinks,store.bankMovementAuthority,list(store.expenseRefunds).filter(x=>x.status!=='voided'),store.vendorAdvances,store.receivables,store.transfers,store.adjustments,store.reconciliationExpenses,store.loanRepayments,list(register.bankReceiptSplits).filter(x=>x.id!==g.id)].some(x=>contains(x,refs)))fail('This rent/security receipt is linked to another review, finalized entry or settlement. Resolve that dependency first.');
  const before={group:clone(g),receipts:list(store.receipts).filter(x=>receiptIds.has(x.id)).map(clone),resolution:clone(r)};
  register.allocations=after.allocations;register.credits=after.credits;register.deposits=after.deposits;store.receipts=store.receipts.filter(x=>!receiptIds.has(x.id));Object.assign(g,{reversedAt:now,reversedBy:actor,reversalReason:String(reason).trim()});register.activity.push({at:now,by:actor,action:'undo_bank_rent_security_receipt',tenancyId:g.tenancyId,rentalBankSplitId:g.id,detail:String(reason).trim()});register.revision++;
  return {before,rentalSplit:clone(g)};
}
module.exports={record,undo,evidenceError,bankIdentity};
