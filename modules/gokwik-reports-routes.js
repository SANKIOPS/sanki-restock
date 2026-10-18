'use strict';
const g = require('./gokwik-accounting');

function registerGoKwikReports(router,deps) {
  const {loadStore,saveStore,audit,upload,orders,isOwner,canAccess,registerVendor} = deps;
  const deny=(req,res)=>{if(canAccess(req))return false;res.status(403).json({success:false,error:'Owner/Admin with access to SANKI and IndusInd 0437 only.'});return true;};
  const event=(s,req,action,id,details)=>audit(s,req,action,'gokwik_settlement',id,{nature:'SANKI',account:g.CLEARING,...details});
  const respond=(res,s,extra)=>res.json({success:true,...extra,view:g.view(s,orders())});
  const run=handler=>(req,res)=>{if(deny(req,res))return;try{return handler(req,res);}catch(e){res.status(409).json({success:false,error:e.message});}};
  const base='/api/expenses/gokwik-reports';
  router.get(base,run((req,res)=>{const s=loadStore();res.json({success:true,...g.view(s,orders()),apiAccounting:s.gokwikApiAccounting||null});}));
  router.get(base+'/previews/:id',run((req,res)=>{const p=(loadStore().gokwikPreviews || {})[req.params.id];if(!p)throw Error('Preview not found.');res.json({success:true,preview:{id:p.id,name:p.name,transactions:p.transactions,settlements:g.batches(p.transactions)}});}));
  router.get(base+'/source/:id',run((req,res)=>{
    const source=(loadStore().gokwikImports || {})[req.params.id];
    if(!source)return res.status(404).json({success:false,error:'Report source not found.'});
    const isApi=source.sourceType==='gokwik_api_v2';res.set('Content-Type',isApi?'application/json; charset=utf-8':'text/csv; charset=utf-8');res.set('Content-Disposition','attachment; filename="gokwik-settlement.'+(isApi?'json':'csv')+'"');res.send(Buffer.from(source.source,'base64'));
  }));
  router.get(base+'/proof/:id',run((req,res)=>{
    const s=loadStore(),batch=g.getBatch(s,req.params.id),escape=value=>String(value).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&apos;'}[c])),lines=[
      'GoKwik settlement report summary - NOT a tax invoice',
      'UTR: '+batch.utr+' | Settlement date: '+batch.settledDate,
      'Gross: INR '+batch.gross.toFixed(2)+' | Net to IndusInd 0437: INR '+batch.net.toFixed(2),
      'Fee: '+batch.fee.toFixed(2)+' | Tax: '+batch.tax.toFixed(2)+' | Additional fees/tax: '+(batch.additionalFee+batch.additionalTax).toFixed(2),
      'Separate deduction: INR '+batch.deduction.toFixed(2)+' - provisional Subscription Expense, vendor GoKwik',
      'Original provider evidence is retained in universal reconciliation. No tax-credit eligibility is asserted.',
      ...batch.transactions.slice(0,100).map(tx=>tx.date+' | #'+tx.orderNumber+' | '+tx.paymentId+' | INR '+tx.gross.toFixed(2)),
      ...(batch.transactions.length>100?['Additional payments: see complete original CSV.']:[])
    ];
    res.set('Content-Type','image/svg+xml');res.set('Content-Security-Policy',"default-src 'none'; style-src 'unsafe-inline'; sandbox");res.send('<svg xmlns="http://www.w3.org/2000/svg" width="1200" height="'+(55+lines.length*28)+'"><rect width="100%" height="100%" fill="white"/><g font-family="Arial,sans-serif" font-size="15" fill="#17354b">'+lines.map((line,i)=>'<text x="20" y="'+(32+i*28)+'">'+escape(line)+'</text>').join('')+'</g></svg>');
  }));
  router.post(base+'/preview', (req,res,next)=>{if(deny(req,res))return;upload.single('report')(req,res,error=>error?res.status(400).json({success:false,error:error.message}):next());},run((req,res)=>{
    if(!req.file || !/\.csv$/i.test(req.file.originalname))throw Error('Choose the original GoKwik settlement CSV.');
    const transactions=g.parseReport(req.file.buffer),s=loadStore(),id='GKW-PREVIEW-'+require('crypto').randomUUID();
    const preview={id,name:String(req.file.originalname).slice(0,180),at:new Date().toISOString(),by:req.user.username,hash:require('crypto').createHash('sha256').update(req.file.buffer).digest('hex'),transactions,source:req.file.buffer.toString('base64')};
    // Validate the complete combined evidence before saving even a preview.
    mergeTransactions(s,transactions,false);
    s.gokwikPreviews=s.gokwikPreviews || {};s.gokwikPreviews[id]=preview;saveStore(s);
    res.json({success:true,preview:{id,name:preview.name,transactions,settlements:g.batches(transactions)}});
  }));
  router.post(base+'/confirm',run((req,res)=>{
    const s=loadStore(),id=String(req.body.previewId || ''),preview=(s.gokwikPreviews || {})[id];if(!preview)throw Error('Upload preview not found.');
    const next=mergeTransactions(s,preview.transactions,true),importId='GKW-IMPORT-'+preview.hash.slice(0,24),at=new Date().toISOString();
    s.gokwikImports=s.gokwikImports || {};
    if(!s.gokwikImports[importId])s.gokwikImports[importId]={...preview,id:importId,source:preview.source,count:preview.transactions.length,at,by:req.user.username};
    s.gokwikTransactions=next.transactions;
    for(const tx of Object.values(s.gokwikTransactions))if(!tx.importId)tx.importId=importId;
    s.gokwikOrderLinks=s.gokwikOrderLinks || {};const orderList=orders();let linked=0;
    for(const tx of preview.transactions) {
      if(s.gokwikOrderLinks[tx.paymentId])continue;
      try {const link=g.validateLink(s,s.gokwikTransactions[tx.paymentId],orderList);s.gokwikOrderLinks[tx.paymentId]={...link,at,by:req.user.username,reason:'Exact merchant Shopify ID and order number; original GoKwik capture evidence'};linked++;}catch{} // visibly unresolved, never guessed by amount
    }
    delete s.gokwikPreviews[id];event(s,req,'GOKWIK_REPORT_IMPORTED',importId,{after:{added:next.added,linked,hash:preview.hash},note:'Report evidence only. No payout, expense, opening balance or bank finalization posted.'});saveStore(s);respond(res,s,{added:next.added,linked});
  }));
  router.post(base+'/discard-preview',run((req,res)=>{const s=loadStore(),id=String(req.body.previewId || '');if(!s.gokwikPreviews?.[id])throw Error('Preview not found.');delete s.gokwikPreviews[id];event(s,req,'GOKWIK_PREVIEW_DISCARDED',id,{});saveStore(s);respond(res,s);}));
  router.post(base+'/link-order',run((req,res)=>{
    const s=loadStore(),id=String(req.body.paymentId || ''),tx=(s.gokwikTransactions || {})[id],reason=String(req.body.reason || '').trim();if(!tx)throw Error('Payment not found.');if(reason.length<10)throw Error('Enter a review reason of at least 10 characters.');
    assertNotPosted(s,tx);const at=new Date().toISOString(),link=g.validateLink(s,tx,orders(),{confirmedOrderId:req.body.orderId});s.gokwikOrderLinks=s.gokwikOrderLinks || {};
    const identityReview=req.body.orderId?{sourceOrderId:tx.orderId,orderId:link.orderId,reason,at,by:req.user.username}:(s.gokwikOrderLinks[id]||{}).identityReview;
    s.gokwikOrderLinks[id]={...link,...(identityReview?{identityReview}:{}),reason,at,by:req.user.username};invalidateApproval(s,tx);event(s,req,'GOKWIK_ORDER_LINKED',id,{after:s.gokwikOrderLinks[id]});saveStore(s);respond(res,s);
  }));
  router.post(base+'/unmatch',run((req,res)=>{
    const s=loadStore(),id=String(req.body.paymentId || ''),tx=(s.gokwikTransactions || {})[id],reason=String(req.body.reason || '').trim();if(!tx)throw Error('Payment not found.');if(reason.length<10)throw Error('Enter an unmatch reason of at least 10 characters.');assertNotPosted(s,tx);const before=s.gokwikOrderLinks?.[id];if(s.gokwikOrderLinks)delete s.gokwikOrderLinks[id];invalidateApproval(s,tx);event(s,req,'GOKWIK_ORDER_UNMATCHED',id,{before,note:reason});saveStore(s);respond(res,s);
  }));
  router.post(base+'/submit',run((req,res)=>{
    const s=loadStore(),id=String(req.body.settlementId || ''),reason=String(req.body.reason || '').trim();if(reason.length<10)throw Error('Enter a reconciliation remark of at least 10 characters.');const checked=g.review(s,id,orders());
    const approval={fingerprint:checked.fingerprint,requestedBy:req.user.username,requestedAt:new Date().toISOString(),reason,deductionCategory:g.SUBSCRIPTION,deductionVendor:'GoKwik',deductionTreatment:'User-directed provisional subscription expense; reclassify against actual invoice later'};
    s.gokwikApprovals=s.gokwikApprovals || {};s.gokwikApprovals[id]=approval;event(s,req,'GOKWIK_APPROVAL_REQUESTED',id,{after:approval,note:'No posting until Owner approval. Bank account opening and statement summary untouched.'});saveStore(s);respond(res,s);
  }));
  router.post(base+'/withdraw',run((req,res)=>{const s=loadStore(),id=String(req.body.settlementId || ''),reason=String(req.body.reason || '').trim();if(reason.length<10)throw Error('Enter a withdrawal/rejection reason of at least 10 characters.');if(!s.gokwikApprovals?.[id])throw Error('Pending approval not found.');const before=s.gokwikApprovals[id];delete s.gokwikApprovals[id];event(s,req,'GOKWIK_APPROVAL_WITHDRAWN',id,{before,note:reason});saveStore(s);respond(res,s);}));
  router.post(base+'/approve',run((req,res)=>{
    if(!isOwner(req))return res.status(403).json({success:false,error:'Only the Owner can approve and post GoKwik settlements.'});
    const s=loadStore(),id=String(req.body.settlementId || ''),approval=(s.gokwikApprovals || {})[id];if(!approval)throw Error('Submit this settlement for approval first.');
    const checked=g.review(s,id,orders());if(checked.fingerprint!==approval.fingerprint)throw Error('Report, order funding or bank evidence changed after submission. Review and submit again; nothing was posted.');
    const {batch,bank}=checked,now=new Date().toISOString(),sourceIds=[...new Set(batch.transactions.map(t=>t.importId))],proof=base+'/proof/'+encodeURIComponent(id);
    s.expenses=s.expenses || {};s.transfers=s.transfers || [];s.gokwikPostings=s.gokwikPostings || {};s.customLedgers=s.customLedgers || {};
    const vendor=registerVendor(s,'SANKI','GoKwik',{}),expenses=[];
    if(s.sankiCategoryCatalog?.categories&&!s.sankiCategoryCatalog.categories.some(c=>String(c.name).toLowerCase()===g.SUBSCRIPTION.toLowerCase()))s.sankiCategoryCatalog.categories.push({name:g.SUBSCRIPTION,group:'Subscriptions',type:'fixed',createdBy:req.user.username,createdAt:now});
    const createExpense=(total,ledger,particulars,type)=>{
      if(g.money(total)<=0)return;
      do{s.seq=Number(s.seq || 0)+1;}while(s.expenses['EX-'+String(s.seq).padStart(5,'0')]);
      const expenseId='EX-'+String(s.seq).padStart(5,'0');
      const payment={id:'PAY-001',date:bank.date,amount:total,account:g.CLEARING,paymentType:'Settlement deduction',proof,proofs:[proof],bankReference:batch.utr,paidBy:req.user.username,paidAt:now,personalFunds:false,gokwikPostingId:id};
      const expense={id:expenseId,nature:'SANKI',channel:'Website',date:bank.date,amount:total,requestedAmount:total,paidAmount:total,status:'paid',approvedAt:now,approvedBy:req.user.username,createdAt:now,createdBy:req.user.username,claimant:req.user.username,vendor,ledger,type,particulars,account:g.CLEARING,fundedBy:'company',paidAlready:false,paymentType:payment.paymentType,payments:[payment],paymentProof:proof,billPhoto:proof,billPhotos:[proof],bill:'statement',exceptionEvidence:'Original GoKwik settlement CSV and exact 0437 bank credit',exceptionReason:'Owner approved report-backed settlement deduction',gokwikPostingId:id,gokwikSourceIds:sourceIds,paidAt:now,paidBy:req.user.username};
      s.expenses[expenseId]=expense;s.customLedgers[ledger]=s.customLedgers[ledger] || {name:ledger,type};expenses.push(expenseId);event(s,req,'GOKWIK_EXPENSE_POSTED',expenseId,{after:expense});
    };
    createExpense(Number((batch.fee+batch.tax+batch.additionalFee+batch.additionalTax).toFixed(2)),'BANK CHARGES','GoKwik processing fee and tax · UTR '+batch.utr+' · fee '+batch.fee+', tax '+batch.tax+', additional fee '+batch.additionalFee+', additional tax '+batch.additionalTax,'running');
    createExpense(batch.deduction,g.SUBSCRIPTION,'GoKwik subscription expense (provisional classification per user instruction) · UTR '+batch.utr,'fixed');
    do{s.transferSeq=Number(s.transferSeq || 0)+1;}while(s.transfers.some(t=>t.id==='TR-'+String(s.transferSeq).padStart(5,'0')));
    const transfer={id:'TR-'+String(s.transferSeq).padStart(5,'0'),nature:'SANKI',fromNature:'SANKI',toNature:'SANKI',classification:'internal_transfer',fromAccount:g.CLEARING,toAccount:g.BANK,amount:batch.net,date:bank.date,bankReference:batch.utr,reference:batch.utr,proof,proofs:[proof],note:'GoKwik net settlement · '+batch.transactions.map(t=>'#'+t.orderNumber).join(', '),gokwikPostingId:id,createdAt:now,createdBy:req.user.username};
    s.transfers.push(transfer);
    const posting={id,utr:batch.utr,gross:batch.gross,net:batch.net,fee:batch.fee,tax:batch.tax,additionalFee:batch.additionalFee,additionalTax:batch.additionalTax,deduction:batch.deduction,date:bank.date,bankSignature:bank.signature,bankLocator:bank.locator,transferId:transfer.id,expenseIds:expenses,sourceIds,approval,postedBy:req.user.username,postedAt:now};
    s.gokwikPostings[id]=posting;delete s.gokwikApprovals[id];event(s,req,'GOKWIK_SETTLEMENT_POSTED',id,{after:posting,note:'One net internal transfer and paid GoKwik charges. No duplicate sale, bank opening adjustment or statement finalization.'});saveStore(s);respond(res,s,{posting});
  }));
}
function financial(tx){const {importId,...rest}=tx;return rest;}
function mergeTransactions(store,incoming) {
  const transactions={...(store.gokwikTransactions || {})};let added=0;
  for(const tx of incoming){const prior=transactions[tx.paymentId];if(prior&&g.hash(financial(prior))!==g.hash(financial(tx)))throw Error('Saved GoKwik payment conflicts with this report: '+tx.paymentId);if(!prior){if(Object.values(store.gokwikPostings || {}).some(p=>p.utr===tx.utr))throw Error('This report adds a payment to an already posted settlement UTR. Review its original evidence; no changes were saved.');transactions[tx.paymentId]={...tx};added++;}}
  g.batches(Object.values(transactions));return {transactions,added};
}
function assertNotPosted(store,tx){if(Object.values(store.gokwikPostings || {}).some(p=>p.utr===tx.utr))throw Error('This settlement is posted. Its financial evidence is locked; do not unmatch or post it again.');}
function invalidateApproval(store,tx){if(store.gokwikApprovals)for(const batch of g.batches(Object.values(store.gokwikTransactions || {})))if(batch.utr===tx.utr)delete store.gokwikApprovals[batch.id];}
module.exports={registerGoKwikReports,mergeTransactions};
