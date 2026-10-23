'use strict';
const express=require('express');
const fs=require('fs');
const path=require('path');
const crypto=require('crypto');
const multer=require('multer');
const expenseModule=require('./expenses');
const {parseBankStatementUpload}=expenseModule;
const router=express.Router();
const cardAccounting=require('./credit-card-accounting');
const expenseRefunds=require('./expense-refunds');
const bankAuthorityTransaction=require('./bank-authority-transaction');
const settlements=require('./credit-card-settlements');
const DATA_DIR=process.env.DATA_PATH?path.dirname(process.env.DATA_PATH):path.join(__dirname,'..');
const STORE_PATH=path.join(DATA_DIR,'credit-cards.json');
const EXP_PATH=path.join(DATA_DIR,'expenses.json');
const FILE_DIR=path.join(DATA_DIR,'credit-card-statements');
try{fs.mkdirSync(FILE_DIR,{recursive:true});}catch{}
const upload=multer({storage:multer.diskStorage({destination:(req,file,cb)=>cb(null,FILE_DIR),filename:(req,file,cb)=>cb(null,Date.now()+'-'+crypto.randomBytes(6).toString('hex')+path.extname(file.originalname||'.pdf').toLowerCase())}),limits:{fileSize:15*1024*1024}});
const NATURES=['SANKI','SAMAST','PERSONAL'],CHANNELS=['POS','Website','Both'];
const CLASSIFICATIONS=['expense','refund','card_payment','unclassified_credit','emi_principal','emi_interest','fee'];
function num(v){const n=parseFloat(v);return isNaN(n)?0:n;}
function round2(v){return Math.round(num(v)*100)/100;}
function normalizeAllocations(items,amount,fallbackNature){
  const sums={};(Array.isArray(items)?items:[]).forEach(x=>{const nature=NATURES.includes(x&&x.nature)?x.nature:'';const value=round2(x&&x.amount);if(nature&&value>0)sums[nature]=round2((sums[nature]||0)+value);});
  const out=NATURES.filter(n=>sums[n]>0).map(n=>({nature:n,amount:sums[n]}));
  if(!out.length&&NATURES.includes(fallbackNature)&&num(amount)>0)return[{nature:fallbackNature,amount:round2(amount)}];
  return out;
}
function allocationTotal(items){return round2((items||[]).reduce((n,x)=>n+num(x.amount),0));}
function sourceMovement(st){return round2((st.rows||[]).reduce((n,r)=>n+num(r.debit)-num(r.credit),0));}
function reviewedMovement(st){return round2((st.rows||[]).reduce((n,r)=>{if(['duplicate','link','merge','replace'].includes(r.duplicateResolution))return n+num(r.debit)-num(r.credit);if(['expense','fee','emi_principal','emi_interest'].includes(r.classification))return n+num(r.amount);if(['refund','card_payment','unclassified_credit'].includes(r.classification))return n-num(r.amount);return n;},0));}
function blank(){return{cards:{},statements:{},merchantRules:{
  'meta ads':{pattern:'meta ads',category:'MARKETING EXPENSE',source:'default'},
  shopify:{pattern:'shopify',category:'Subscriptions',source:'default'},
  swiggy:{pattern:'swiggy',category:'FOOD EXPENSE',source:'default'},
  zomato:{pattern:'zomato',category:'FOOD EXPENSE',source:'default'},
  'fuel station':{pattern:'fuel station',category:'Fuel Expenses',source:'default'},
  'late fee':{pattern:'late fee',category:'Fee & Charges',source:'default'},
  interest:{pattern:'interest',category:'Fee & Charges',source:'default'}
},payments:[],audit:[],cardSeq:0,statementSeq:0,paymentSeq:0,auditSeq:0};}
function readObject(file,fallback){try{const value=JSON.parse(fs.readFileSync(file,'utf8'));if(!value||typeof value!=='object'||Array.isArray(value))throw new Error('Invalid accounting store.');return value;}catch(error){if(error.code==='ENOENT')return fallback;throw new Error('The accounting store is unreadable. No records were changed.',{cause:error});}}
function load(){bankAuthorityTransaction.recover(DATA_DIR);return Object.assign(blank(),readObject(STORE_PATH,{}));}
function save(s){const t=STORE_PATH+'.tmp-'+process.pid+'-'+Date.now();fs.writeFileSync(t,JSON.stringify(s));fs.renameSync(t,STORE_PATH);}
function loadExpenses(){return readObject(EXP_PATH,{});}
function rawSnapshot(file){try{return fs.readFileSync(file,'utf8');}catch(error){if(error.code==='ENOENT')return null;throw error;}}
function storeSnapshots(){return{cards:rawSnapshot(STORE_PATH),expenses:rawSnapshot(EXP_PATH)};}
function commitStores(s,exp,before){return bankAuthorityTransaction.commit(DATA_DIR,[{fileName:'expenses.json',before:before.expenses,after:JSON.stringify(exp)},{fileName:'credit-cards.json',before:before.cards,after:JSON.stringify(s)}]);}
function validDate(value){const text=String(value||'');if(!/^\d{4}-\d{2}-\d{2}$/.test(text))return false;const date=new Date(text+'T00:00:00Z');return Number.isFinite(date.getTime())&&date.toISOString().slice(0,10)===text;}
function reviewRevisionMatches(st,body){return Number.isSafeInteger(body&&body.reviewRevision)&&body.reviewRevision===num(st.reviewRevision);}
function settlementSourceVisible(req,nature,account){const policy=expenseModule.creditCardSettlementPolicy;return !!(policy&&req&&NATURES.includes(nature)&&(nature!=='PERSONAL'||policy.canManagePersonalAccounting(req))&&policy.accountVisibleToReq(req,account)&&policy.allowedPayingAccount(req,nature,account));}
function privateSettlement(req,row){const policy=expenseModule.creditCardSettlementPolicy;return row.classification==='card_payment'&&!!row.payingAccount&&!!policy&&((row.payingNature==='PERSONAL'&&!policy.canManagePersonalAccounting(req))||!policy.accountVisibleToReq(req,row.payingAccount));}
function canReviewPersonal(req){const policy=expenseModule.creditCardSettlementPolicy;return !!(policy&&policy.canManagePersonalAccounting(req));}
function visibleDuplicateWarnings(s,st,row,req,exp){return(row.duplicateWarnings||[]).filter(w=>{
  if(w.kind==='card_payment'){const payment=(s.payments||[]).find(p=>p.id===w.id&&p.cardId===st.cardId&&p.active!==false);return !!payment&&settlementSourceVisible(req,payment.nature,payment.account);}
  if(w.kind==='statement'){const other=s.statements[String(w.id).split('/')[0]];return !!other&&other.cardId===st.cardId;}
  if(w.kind==='expense'){const expense=(exp.expenses||{})[w.id];return !!expense&&(!(expense.ownerOnly||expense.nature==='PERSONAL')||canReviewPersonal(req))&&(expense.creditCardId===st.cardId||(expense.payments||[]).some(p=>p.creditCardId===st.cardId));}
  return false;
});}
function externalSourceError(req,row){const policy=expenseModule.creditCardSettlementPolicy;if(row.settlementMode!=='external')return'';if(row.payingNature==='PERSONAL'&&!(policy&&policy.canManagePersonalAccounting(req)))return'The Owner must review a private external card-payment source.';if(policy&&policy.isTrackedAccount(row.payingAccount))return'This is a tracked paying account. Link its existing payment instead of treating it as an external source.';return'';}
function roles(req){return req.user&&(req.user.roles||(req.user.role?[req.user.role]:[]))||[];}
function allowed(req){return roles(req).some(x=>['admin','owner','accounting'].includes(x));}
function owner(req){return roles(req).some(x=>['admin','owner'].includes(x));}
function guard(req,res,next){return allowed(req)?next():res.status(403).json({success:false,error:'Credit cards are admin/accounting only.'});}
function visibleCard(req,c){return !c.ownerOnly||owner(req);}
function audit(s,req,action,subjectId,details){s.auditSeq=(s.auditSeq||0)+1;s.audit.push({id:'CCA-'+String(s.auditSeq).padStart(6,'0'),at:new Date().toISOString(),by:req.user&&req.user.username||'admin',action,subjectId,details:details||{}});}
function cardName(c){return c.name+' '+c.last4;}
function merchantText(v){return String(v||'').toLowerCase().replace(/\b(?:upi|pos|txn|ref|payment|purchase|debit|credit)\b/g,' ').replace(/[^a-z0-9]+/g,' ').replace(/\s+/g,' ').trim();}
function merchantKey(v){const s=merchantText(v);return s.split(' ').filter(x=>x.length>2&&!/^\d+$/.test(x)).slice(0,3).join(' ');}
function suggestRule(s,narration){const text=merchantText(narration),rules=Object.values(s.merchantRules||{}).filter(r=>text.includes(r.pattern)).sort((a,b)=>b.pattern.length-a.pattern.length);return rules[0]||null;}
function inferClassification(row){const n=String(row.description||'').toLowerCase();if(row.credit){if(settlements.isSettlementNarration(n))return'card_payment';if(/refund|reversal|reversed|chargeback/.test(n))return'refund';return'unclassified_credit';}if(/emi/.test(n)&&/interest/.test(n))return'emi_interest';if(/emi/.test(n))return'emi_principal';if(/fee|charge|interest|gst/.test(n))return'fee';return'expense';}
function expenseType(name){const n=String(name||'').toLowerCase();if(/rent|salary/.test(n))return'fixed';if(/marketing|advertis|meta|social media|promotion|model|shoot/.test(n))return'marketing';if(/subscription|fee|charge|bank|internet|wifi|technology|telephone|courier|packing|freight/.test(n))return'running';return'variable';}
function statementView(s,st,req){const card=s.cards[st.cardId],exp=loadExpenses(),canView=(nature,account)=>settlementSourceVisible(req,nature,account),rows=(st.rows||[]).map(r=>Object.assign({},r,{merchant:cardAccounting.merchantName(r.merchant||r.narration,card&&(card.issuingBank||card.name)),suggested:!!r.suggestedCategory,duplicateWarnings:visibleDuplicateWarnings(s,st,r,req,exp),settlementCandidates:num(r.credit)>0?settlements.candidates(s,exp,st,r,canView):[]},privateSettlement(req,r)?{settlementProtected:true,settlementProtectedMessage:'Private settlement source — Owner review required',settlementMode:'',settlementSourceId:'',settlementSourceFingerprint:'',settlementReason:'',settlementReference:'',settlementLegacyConfirmed:false,payingNature:'',payingAccount:'',settlementCandidates:[]}:{})),available=expenseRefunds.manualCardRefunds(exp,st.cardId,card&&cardName(card)),linked=new Set(rows.map(r=>r.expenseRefundReceiptId).filter(Boolean));(exp.receipts||[]).filter(r=>!r.accountingExcluded&&linked.has(r.id)&&!available.some(a=>a.id===r.id)).forEach(r=>available.push(r));return Object.assign({},st,{reviewRevision:num(st.reviewRevision),refundReceipts:available.filter(r=>(!r.ownerOnly&&r.nature!=='PERSONAL')||canReviewPersonal(req)).map(r=>({id:r.id,date:r.date,amount:r.amount,nature:r.nature,reference:r.bankReference||'',source:r.source})),fileUrl:st.storedName?'/api/expenses/credit-cards/statements/'+encodeURIComponent(st.id)+'/file':'',cardName:card&&cardName(card),sourceMovement:sourceMovement(st),reviewedMovement:reviewedMovement(st),movementDifference:round2(reviewedMovement(st)-sourceMovement(st)),totals:cardAccounting.totals(rows),rows});}
function directCardPurchases(cardId){const exp=loadExpenses();return Object.values(exp.expenses||{}).flatMap(e=>{const posted=!!e.approvedAt||['approved','partially_paid','paid'].includes(String(e.status||''));if(!posted)return[];return(e.payments||[]).filter(p=>!p.accountingExcluded&&(p.creditCardId===cardId||e.creditCardId===cardId)).map(p=>({id:e.id+'/'+p.id,date:p.date,description:(e.vendor||e.particulars||e.id)+' · '+(e.particulars||'credit-card expense'),amount:num(p.amount),proof:p.proof||e.billPhoto||''}));});}
function liability(s,cardId){const c=s.cards[cardId];let v=num(c&&c.openingOutstanding);Object.values(s.statements||{}).filter(x=>x.cardId===cardId&&x.status==='finalized').forEach(st=>(st.rows||[]).forEach(r=>{if(!cardAccounting.activeRow(r))return;if(['expense','fee','emi_principal','emi_interest'].includes(r.classification))v+=num(r.amount);if(r.classification==='refund')v-=num(r.amount);if(r.classification==='card_payment')v-=num(r.amount);}));if(!Object.values(s.statements).some(st=>st.cardId===cardId&&st.status==='finalized'&&st.statementSourceOfTruth)){directCardPurchases(cardId).forEach(p=>v+=p.amount);(s.payments||[]).filter(p=>p.cardId===cardId&&p.active!==false).forEach(p=>v-=num(p.amount));}expenseRefunds.manualCardRefunds(loadExpenses(),cardId,c&&cardName(c)).forEach(r=>v-=num(r.amount));return round2(v);}
function duplicateWarnings(exp,cc,row){cc=cc||load();const amount=num(row.amount),date=row.date,n=merchantText(row.narration),found=[];Object.values(exp.expenses||{}).forEach(e=>{if(Math.abs(num(e.amount)-amount)>.01)return;const days=Math.abs((Date.parse(e.date)-Date.parse(date))/86400000);if(days<=2&&(merchantText(e.particulars+' '+e.vendor).includes(n)||n.includes(merchantText(e.particulars+' '+e.vendor))))found.push({kind:'expense',id:e.id,date:e.date,amount:e.amount,label:e.vendor||e.particulars});});(cc.payments||[]).filter(p=>p.active!==false&&Math.abs(num(p.amount)-amount)<.01&&Math.abs((Date.parse(p.date)-Date.parse(date))/86400000)<=2).forEach(p=>found.push({kind:'card_payment',id:p.id,date:p.date,amount:p.amount,label:p.account}));Object.values(cc.statements||{}).filter(st=>st.status==='finalized').forEach(st=>(st.rows||[]).filter(r=>Math.abs(num(r.amount)-amount)<.01&&Math.abs((Date.parse(r.date)-Date.parse(date))/86400000)<=2&&merchantText(r.narration)===n).forEach(r=>found.push({kind:'statement',id:st.id+'/'+r.id,date:r.date,amount:r.amount,label:r.narration})));return found.slice(0,5);}
function rebuildRules(s){const defaults=blank().merchantRules;s.merchantRules=defaults;Object.values(s.statements||{}).filter(st=>st.status==='finalized').sort((a,b)=>String(a.finalizedAt).localeCompare(String(b.finalizedAt))).forEach(st=>st.rows.forEach(r=>{if(!['expense','fee','emi_interest','refund'].includes(r.classification)||!r.category)return;const key=merchantKey(r.narration);if(!key)return;const old=s.merchantRules[key]||{};s.merchantRules[key]={pattern:key,category:r.category,source:'confirmed statement',confirmations:num(old.confirmations)+1,lastConfirmedAt:st.finalizedAt,lastConfirmedBy:st.finalizedBy};}));}
function validateRefundLinks(s,st,exp){
  const c=s.cards[st.cardId],used=new Set();
  for(const r of st.rows||[]){
    if(r.replaces){const id='CCE-'+r.replaces.statementId+'-'+r.replaces.rowId;if(expenseRefunds.hasSource(exp,id)||expenseRefunds.protectsMovement(exp,id))return 'An overlapping source transaction has linked refunds. Correct those refunds before replacing the source statement.';}
    if(!cardAccounting.activeRow(r)||r.classification!=='refund')continue;
    const receipt=(exp.receipts||[]).find(x=>x.id===r.expenseRefundReceiptId&&!x.accountingExcluded&&x.creditCardRefund);
    if(r.expenseRefundReceiptId){
      if(!receipt||receipt.account!==cardName(c)||receipt.nature!==r.nature||receipt.date!==r.date||round2(receipt.amount)!==round2(r.amount))return 'The linked expense refund must have the same card, entity, date and exact amount.';
      if(used.has(receipt.id)||Object.values(s.statements||{}).some(other=>other.id!==st.id&&other.status==='finalized'&&(other.rows||[]).some(x=>cardAccounting.activeRow(x)&&x.expenseRefundReceiptId===receipt.id&&!(r.replaces&&r.replaces.statementId===other.id&&r.replaces.rowId===x.id))))return 'This expense refund is already linked to another card-statement credit.';
      used.add(receipt.id);
    }else{
      const candidates=expenseRefunds.manualCardRefunds(exp,c.id,cardName(c)).filter(x=>x.date===r.date&&round2(x.amount)===round2(r.amount));
      if(candidates.length&&!(r.refundLinkDecision==='separate'&&String(r.refundLinkReason||'').trim().length>=10))return 'A card refund is already logged for this date and amount. Select its refund reference in the review, or explain a genuinely separate transaction (at least 10 characters).';
    }
  }
  return '';
}
router.use('/api/expenses/credit-cards',guard);
require('./credit-card-payment-corrections').create({policy:()=>expenseModule.sourceCorrectionPolicy}).register(router);
router.get('/api/expenses/credit-cards',(req,res)=>{const s=load(),visibleIds=new Set(Object.values(s.cards).filter(c=>visibleCard(req,c)).map(c=>c.id)),cards=Object.values(s.cards).filter(c=>visibleCard(req,c)).map(c=>Object.assign({},c,{displayName:cardName(c),outstanding:liability(s,c.id),statementCount:Object.values(s.statements).filter(x=>x.cardId===c.id).length})),exp=loadExpenses(),settlementAllocations=(exp.creditCardSettlementAllocations||[]).filter(x=>visibleIds.has(x.cardId)&&((x.payerNature!=='PERSONAL'&&x.beneficiaryNature!=='PERSONAL')||canReviewPersonal(req))&&(!x.payingAccount||settlementSourceVisible(req,x.payerNature,x.payingAccount)));res.json({success:true,cards,rules:Object.values(s.merchantRules),settlementAllocations,classifications:CLASSIFICATIONS,natures:NATURES.filter(n=>n!=='PERSONAL'||canReviewPersonal(req)),channels:CHANNELS,isOwner:owner(req)});});
router.post('/api/expenses/credit-cards',(req,res)=>{if(!owner(req))return res.status(403).json({success:false,error:'Owner/Admin only.'});const s=load(),b=req.body||{},last4=String(b.last4||'').replace(/\D/g,'').slice(-4),name=String(b.name||'').trim();if(!name||last4.length!==4)return res.status(400).json({success:false,error:'Enter card name and last four digits.'});let c=b.id&&s.cards[b.id];if(!c){s.cardSeq=(s.cardSeq||0)+1;c={id:'CC-'+String(s.cardSeq).padStart(4,'0'),createdAt:new Date().toISOString()};}Object.assign(c,{name,last4,cardholder:String(b.cardholder||'').trim(),issuingBank:String(b.issuingBank||'').trim(),cycleDay:Math.max(1,Math.min(31,num(b.cycleDay)||1)),dueDay:Math.max(1,Math.min(31,num(b.dueDay)||1)),creditLimit:round2(b.creditLimit),openingOutstanding:round2(b.openingOutstanding),ownerOnly:!!b.ownerOnly,active:b.active!==false});s.cards[c.id]=c;audit(s,req,b.id?'CARD_UPDATED':'CARD_CREATED',c.id,c);save(s);res.json({success:true,card:Object.assign({},c,{displayName:cardName(c),outstanding:liability(s,c.id)})});});
// Parsing may yield to another financial request. Reload after it completes so
// a linked bank correction or newly imported statement is never overwritten.
router.post('/api/expenses/credit-cards/statements/import',upload.single('statement'),async(req,res)=>{let s=load(),card=s.cards[req.body.cardId];if(!req.file||!card||!visibleCard(req,card)){if(req.file)try{fs.unlinkSync(req.file.path);}catch{}return res.status(400).json({success:false,error:'Choose an accessible card and statement file.'});}try{const parsed=await parseBankStatementUpload(req.file.path,req.file.originalname,String(req.body.password||''),{unbilled:req.body.kind==='unbilled'});if(!parsed.length)throw new Error('No dated transactions were found.');s=load();card=s.cards[req.body.cardId];if(!card||!visibleCard(req,card))throw new Error('This card is no longer accessible. No statement was imported.');const fileHash=crypto.createHash('sha256').update(fs.readFileSync(req.file.path)).digest('hex'),existing=Object.values(s.statements).find(st=>st.cardId===card.id&&st.fileHash===fileHash);if(existing){fs.unlinkSync(req.file.path);return res.json({success:true,alreadyImported:true,statement:statementView(s,existing,req)});}s.statementSeq=(s.statementSeq||0)+1;const id='CCS-'+String(s.statementSeq).padStart(5,'0'),exp=loadExpenses(),seen={};const rows=parsed.map((x,i)=>{const narration=String(x.description||'Transaction').trim(),classification=inferClassification(x),rule=suggestRule(s,narration),amount=round2(x.debit||x.credit),base=[x.date,amount,narration].join('|'),occ=seen[base]=(seen[base]||0)+1,rowId='CCR-'+crypto.createHash('sha1').update(base+'|'+occ).digest('hex').slice(0,12);const row={id:rowId,date:x.date,postingDate:x.valueDate||x.date,narration,merchant:cardAccounting.merchantName(narration,card.issuingBank||card.name),reference:x.reference||'',amount,debit:round2(x.debit),credit:round2(x.credit),balance:round2(x.balance),classification,inferredStatementRow:!!x.statementGenerated,category:rule&&['expense','fee','emi_interest'].includes(classification)?rule.category:'',suggestedCategory:rule&&rule.category||'',suggestedRule:rule&&rule.pattern||'',nature:'',channel:'',claimant:'',bill:'',confirmed:false,duplicateResolution:'',linkedExpenseId:''};row.duplicateWarnings=duplicateWarnings(exp,null,row);return row;});const dates=rows.map(x=>x.date).sort();s.statements[id]={id,cardId:card.id,kind:req.body.kind==='unbilled'?'unbilled':'billed',status:'review',originalName:req.file.originalname,storedName:path.basename(req.file.path),mimeType:req.file.mimetype,fileHash:crypto.createHash('sha256').update(fs.readFileSync(req.file.path)).digest('hex'),uploadedAt:new Date().toISOString(),uploadedBy:req.user.username,periodFrom:dates[0],periodTo:dates.at(-1),summary:parsed.statementSummary||{},rows};cardAccounting.matchRows(s,s.statements[id]);audit(s,req,'STATEMENT_UPLOADED',id,{cardId:card.id,file:req.file.originalname,rows:rows.length});save(s);res.json({success:true,statement:statementView(s,s.statements[id],req)});}catch(e){try{fs.unlinkSync(req.file.path);}catch{}res.status(400).json({success:false,error:e.message||'Could not read statement.'});}});
router.post('/api/expenses/credit-cards/statements/manual',(req,res)=>{const s=load(),b=req.body||{},c=s.cards[b.cardId];if(!c||!visibleCard(req,c)||!/^\d{4}-\d{2}-\d{2}$/.test(String(b.date||''))||!(num(b.amount)>0)||!String(b.narration||'').trim())return res.status(400).json({success:false,error:'Choose card, date, narration and amount.'});s.statementSeq=(s.statementSeq||0)+1;const id='CCS-'+String(s.statementSeq).padStart(5,'0'),narration=String(b.narration).trim(),classification=CLASSIFICATIONS.includes(b.classification)?b.classification:'expense',rule=suggestRule(s,narration),isCredit=['refund','card_payment','unclassified_credit'].includes(classification),row={id:'CCR-'+crypto.randomBytes(6).toString('hex'),date:b.date,postingDate:b.date,narration,merchant:cardAccounting.merchantName(narration,c.issuingBank||c.name),reference:String(b.reference||''),amount:round2(b.amount),debit:isCredit?0:round2(b.amount),credit:isCredit?round2(b.amount):0,balance:0,classification,category:rule&&rule.category||'',suggestedCategory:rule&&rule.category||'',suggestedRule:rule&&rule.pattern||'',nature:'',channel:'',claimant:'',bill:'',payingNature:'',payingAccount:'',confirmed:false,duplicateResolution:'',linkedExpenseId:'',duplicateWarnings:duplicateWarnings(loadExpenses(),null,{amount:b.amount,date:b.date,narration})};s.statements[id]={id,cardId:c.id,kind:b.kind==='unbilled'?'unbilled':'billed',status:'review',originalName:'Manual entry',storedName:'',mimeType:'',fileHash:'',uploadedAt:new Date().toISOString(),uploadedBy:req.user.username,periodFrom:b.date,periodTo:b.date,summary:{format:'Manual'},rows:[row]};cardAccounting.matchRows(s,s.statements[id]);audit(s,req,'MANUAL_STATEMENT_CREATED',id,{cardId:c.id});save(s);res.json({success:true,statement:statementView(s,s.statements[id],req)});});
router.get('/api/expenses/credit-cards/statements',(req,res)=>{const s=load(),list=Object.values(s.statements).filter(st=>{const c=s.cards[st.cardId];return c&&visibleCard(req,c)&&(!req.query.cardId||st.cardId===req.query.cardId);}).sort((a,b)=>b.uploadedAt.localeCompare(a.uploadedAt)).map(st=>statementView(s,st,req));res.json({success:true,statements:list});});
router.get('/api/expenses/credit-cards/statements/:id/file',(req,res)=>{const s=load(),st=s.statements[req.params.id],c=st&&s.cards[st.cardId];if(!st||!st.storedName||!c||!visibleCard(req,c))return res.status(404).end();res.type(st.mimeType||path.extname(st.storedName));res.sendFile(path.join(FILE_DIR,st.storedName));});
router.post('/api/expenses/credit-cards/statements/:id/review',(req,res)=>{
  const s=load(),st=s.statements[req.params.id],c=st&&s.cards[st.cardId],body=req.body||{};
  if(!st||!c||!visibleCard(req,c)||st.status==='finalized')return res.status(400).json({success:false,error:'Statement is unavailable or finalized.'});
  if(!reviewRevisionMatches(st,body))return res.status(409).json({success:false,error:'This statement review changed or its revision is missing. Refresh it before saving; no review was overwritten.'});
  const items=Array.isArray(body.rows)?body.rows:[],ids=new Set((st.rows||[]).map(r=>r.id)),byId=new Map();
  if(!items.length)return res.status(400).json({success:false,error:'Choose the statement rows to review.'});
  for(const item of items){
    if(!item||!ids.has(item.id)||byId.has(item.id))return res.status(400).json({success:false,error:'The review contains an unknown or repeated statement row.'});
    byId.set(item.id,item);
  }
  // Work on a detached proposal. A rejected row never partially saves a batch.
  const proposed=JSON.parse(JSON.stringify(st)),exp=loadExpenses(),canView=(nature,account)=>settlementSourceVisible(req,nature,account);
  s.statements[st.id]=proposed;
  for(const row of proposed.rows){
    const input=byId.get(row.id);if(!input)continue;
    if(privateSettlement(req,row))return res.status(403).json({success:false,error:'A private settlement source requires Owner review. Its saved link and classifications were preserved.'});
    const credit=num(row.credit)>0,classes=credit?['refund','card_payment','unclassified_credit']:['expense','emi_principal','emi_interest','fee'];
    if(!classes.includes(input.classification))return res.status(400).json({success:false,error:'Classification must match the statement debit/credit direction for '+row.narration+'.'});
    if(input.classification==='refund'&&settlements.isSettlementNarration(row.narration))return res.status(409).json({success:false,error:'This narration identifies a card payment, not an expense refund. Keep it as Card payment or pending review; it must not offset expenses.'});
    const nonExpense=['card_payment','unclassified_credit'].includes(input.classification);
    if(!nonExpense&&input.nature==='PERSONAL'&&!canReviewPersonal(req))return res.status(403).json({success:false,error:'The Owner must review private card-expense or refund allocations.'});
    if(!nonExpense&&input.type&&!['running','variable','marketing'].includes(input.type))return res.status(400).json({success:false,error:'Choose Running, Variable or Marketing.'});
    Object.assign(row,{merchant:cardAccounting.merchantName(input.merchant||row.merchant||row.narration,c.issuingBank||c.name),classification:input.classification,
      type:nonExpense?'':input.type||'',category:nonExpense?'':String(input.category||'').trim(),nature:nonExpense?'':NATURES.includes(input.nature)?input.nature:'',
      channel:nonExpense?'':CHANNELS.includes(input.channel)?input.channel:'',confirmed:input.classification!=='unclassified_credit',
      duplicateResolution:'',linkedExpenseId:'',duplicateWarnings:[],allocations:[]});
    if(row.classification==='card_payment'){
      const checked=settlements.reviewSettlement(s,exp,proposed,row,{...row,...input,settlementLegacyConfirmed:input.settlementLegacyConfirmed===true},canView);
      if(checked.error)return res.status(409).json({success:false,error:checked.error});
      Object.assign(row,checked.fields,{expenseRefundReceiptId:'',refundLinkDecision:'',refundLinkReason:''});
      const externalError=externalSourceError(req,row);if(externalError)return res.status(409).json({success:false,error:externalError});
      if(row.settlementMode==='pending')row.confirmed=false;
    }else{
      Object.assign(row,{settlementMode:'',settlementSourceId:'',settlementSourceFingerprint:'',settlementReason:'',settlementReference:'',settlementLegacyConfirmed:false,payingNature:'',payingAccount:''});
      Object.assign(row,{expenseRefundReceiptId:row.classification==='refund'?String(input.expenseRefundReceiptId||''):'',
        refundLinkDecision:row.classification==='refund'?String(input.refundLinkDecision||''):'',
        refundLinkReason:row.classification==='refund'?String(input.refundLinkReason||'').trim():''});
    }
  }
  proposed.statementSourceOfTruth=true;proposed.reviewRevision=num(st.reviewRevision)+1;
  audit(s,req,'STATEMENT_REVIEWED',st.id,{confirmed:proposed.rows.filter(x=>x.confirmed).length,reviewRevision:proposed.reviewRevision,
    settlements:proposed.rows.filter(x=>x.classification==='card_payment').map(x=>({rowId:x.id,mode:x.settlementMode,sourceId:x.settlementSourceId,reason:x.settlementReason,...(x.settlementLegacyConfirmed===true?{legacyRepaymentConfirmed:true}:{})}))});
  save(s);res.json({success:true,statement:statementView(s,proposed,req)});
});
router.post('/api/expenses/credit-cards/statements/:id/finalize',(req,res)=>{
  const s=load(),st=s.statements[req.params.id],c=st&&s.cards[st.cardId];
  if(!st||!c||!visibleCard(req,c)||st.status==='finalized')return res.status(400).json({success:false,error:'Statement is unavailable or already finalized.'});
  if(!reviewRevisionMatches(st,req.body))return res.status(409).json({success:false,error:'This statement review changed or its revision is missing. Refresh and review it before finalizing.'});
  const exp=loadExpenses(),before=storeSnapshots(),canView=(nature,account)=>settlementSourceVisible(req,nature,account);
  for(const row of st.rows){
    if(num(row.credit)>0&&row.classification==='refund'&&settlements.isSettlementNarration(row.narration))return res.status(409).json({success:false,error:'A card-payment credit cannot be posted as a negative expense refund.'});
    if(row.classification==='unclassified_credit')return res.status(409).json({success:false,error:'An unidentified credit is still pending review. Confirm whether it is a card payment or a genuine merchant/fee refund before finalizing.'});
  }
  st.rows.forEach(r=>{delete r.replaces;delete r.duplicateOf;});cardAccounting.matchRows(s,st);
  const unresolved=st.rows.filter(r=>{if(r.duplicateOf)return false;const classes=num(r.credit)>0?['refund','card_payment']:['expense','emi_principal','emi_interest','fee'];return !classes.includes(r.classification)||(['expense','fee','emi_interest','refund'].includes(r.classification)&&(!r.category||!r.nature||(!r.type&&st.statementSourceOfTruth)||(r.nature!=='PERSONAL'&&!r.channel)));});
  if(unresolved.length)return res.status(409).json({success:false,error:unresolved.length+' transaction(s) still need a valid classification, type, category, entity or channel.'});
  const settlementError=settlements.validateStatement(s,exp,st,canView);
  if(settlementError)return res.status(409).json({success:false,error:settlementError});
  for(const row of st.rows.filter(cardAccounting.activeRow)){const problem=externalSourceError(req,row);if(problem)return res.status(409).json({success:false,error:problem});}
  const difference=round2(reviewedMovement(st)-sourceMovement(st));if(Math.abs(difference)>.01)return res.status(409).json({success:false,error:'Statement movement and reviewed card movement differ by ₹'+Math.abs(difference)+'. Correct the classifications before finalizing.'});
  const refundError=validateRefundLinks(s,st,exp);if(refundError)return res.status(409).json({success:false,error:refundError});
  for(const r of st.rows){if(r.replaces){const previous=s.statements[r.replaces.statementId],old=previous&&(previous.rows||[]).find(x=>x.id===r.replaces.rowId);if(old)old.supersededBy={statementId:st.id,rowId:r.id};}}
  const now=new Date().toISOString();st.status='finalized';st.statementSourceOfTruth=true;st.finalizedAt=now;st.finalizedBy=req.user.username;st.reviewRevision=num(st.reviewRevision)+1;
  cardAccounting.syncPostings(s,exp);
  for(const r of st.rows){if(!['expense','fee','emi_interest','refund'].includes(r.classification)||!r.category)continue;const key=merchantKey(r.narration);if(key){const old=s.merchantRules[key]||{};s.merchantRules[key]={pattern:key,category:r.category,source:'confirmed statement',confirmations:num(old.confirmations)+1,lastConfirmedAt:now,lastConfirmedBy:req.user.username};}}
  audit(s,req,'STATEMENT_FINALIZED',st.id,{rows:st.rows.length,cardId:c.id,settlements:st.rows.filter(r=>r.classification==='card_payment').map(r=>({rowId:r.id,mode:r.settlementMode,sourceId:r.settlementSourceId}))});
  try{commitStores(s,exp,before);}catch(error){return res.status(409).json({success:false,error:'Accounting changes could not be committed safely. '+error.message});}
  res.json({success:true,statement:statementView(s,st,req),outstanding:liability(s,c.id)});
});
router.post('/api/expenses/credit-cards/statements/:id/reopen',(req,res)=>{
  if(!owner(req))return res.status(403).json({success:false,error:'Owner/Admin only.'});
  const s=load(),st=s.statements[req.params.id],reason=String(req.body&&req.body.reason||'').trim();
  if(!st||st.status!=='finalized'||!reason)return res.status(400).json({success:false,error:'Choose a finalized statement and enter a correction reason.'});
  const exp=loadExpenses(),snapshots=storeSnapshots();
  if((exp.reconciliationExpenses||[]).some(e=>e.creditCardStatementId===st.id&&(expenseRefunds.hasSource(exp,e.id)||expenseRefunds.protectsMovement(exp,e.id))))return res.status(409).json({success:false,error:'This statement has linked expense refunds. Reverse them in Refunds & Returns before reopening their source statement.'});
  const previous={status:st.status,finalizedAt:st.finalizedAt,finalizedBy:st.finalizedBy};
  exp.reconciliationExpenses=(exp.reconciliationExpenses||[]).filter(x=>x.creditCardStatementId!==st.id);
  exp.transfers=(exp.transfers||[]).filter(x=>!String(x.note||'').includes('Credit card statement payment · '+st.id));
  exp.creditCardSettlementAllocations=(exp.creditCardSettlementAllocations||[]).filter(x=>x.statementId!==st.id);
  Object.values(s.statements).forEach(other=>(other.rows||[]).forEach(row=>{if(row.supersededBy&&row.supersededBy.statementId===st.id)delete row.supersededBy;}));
  st.status='review';st.reviewRevision=num(st.reviewRevision)+1;cardAccounting.syncPostings(s,exp);
  st.reopenedAt=new Date().toISOString();st.reopenedBy=req.user.username;st.reopenReason=reason;delete st.finalizedAt;delete st.finalizedBy;rebuildRules(s);
  audit(s,req,'STATEMENT_REOPENED',st.id,{reason,before:previous,after:{status:'review'}});
  try{commitStores(s,exp,snapshots);}catch(error){return res.status(409).json({success:false,error:'Accounting changes could not be committed safely. '+error.message});}
  res.json({success:true,statement:statementView(s,st,req),outstanding:liability(s,st.cardId)});
});
router.post('/api/expenses/credit-cards/payments',(req,res)=>{
  const s=load(),b=req.body||{},c=s.cards[b.cardId],value=Number(b.amount),amount=round2(value),nature=NATURES.includes(b.nature)?b.nature:'',allocations=normalizeAllocations(b.allocations,amount,nature);
  if(!c||!visibleCard(req,c)||!Number.isFinite(value)||!(amount>0)||Math.abs(value*100-Math.round(value*100))>0.000001||!validDate(b.date)||!nature)return res.status(400).json({success:false,error:'Choose card, exact amount, paying entity, accessible account and valid date.'});
  const policy=expenseModule.creditCardSettlementPolicy,account=policy&&policy.allowedPayingAccount(req,nature,b.account);
  if(!account||!settlementSourceVisible(req,nature,account))return res.status(403).json({success:false,error:'This paying account or entity is not accessible. No payment was recorded.'});
  if(allocations.some(a=>a.nature==='PERSONAL')&&!canReviewPersonal(req))return res.status(403).json({success:false,error:'The Owner must review the private ownership allocation for this card payment. No payment was recorded.'});
  if(Math.abs(allocationTotal(allocations)-amount)>.01)return res.status(400).json({success:false,error:'The SANKI, SAMAST and PERSONAL allocation must equal the card payment amount.'});
  const exp=loadExpenses(),duplicate=settlements.duplicatePayment(s,exp,{...b,amount,nature,account});
  if(duplicate)return res.status(409).json({success:false,error:duplicate});
  const before=storeSnapshots();s.paymentSeq=(s.paymentSeq||0)+1;
  const p={id:'CCP-'+String(s.paymentSeq).padStart(5,'0'),cardId:c.id,amount,date:b.date,nature,account,allocations,paymentKind:['full','minimum','partial','excess'].includes(b.paymentKind)?b.paymentKind:'partial',proof:String(b.proof||''),reference:String(b.reference||'').trim(),createdAt:new Date().toISOString(),createdBy:req.user.username,active:true};
  s.payments.push(p);exp.transfers=Array.isArray(exp.transfers)?exp.transfers:[];exp.creditCardSettlementAllocations=Array.isArray(exp.creditCardSettlementAllocations)?exp.creditCardSettlementAllocations:[];exp.transferSeq=num(exp.transferSeq)+1;
  exp.transfers.push({id:'TR-'+String(exp.transferSeq).padStart(5,'0'),nature,fromNature:nature,toNature:nature,classification:'credit_card_payment',fromAccount:p.account,toAccount:cardName(c),amount,date:p.date,proof:p.proof,reference:p.reference,note:'Credit card payment '+p.id+(p.reference?' · '+p.reference:''),creditCardId:c.id,creditCardPaymentId:p.id,createdBy:req.user.username,createdAt:p.createdAt});
  allocations.forEach(a=>exp.creditCardSettlementAllocations.push({id:'CCA-'+p.id+'-'+a.nature,cardId:c.id,cardName:cardName(c),creditCardPaymentId:p.id,date:p.date,amount:a.amount,payerNature:nature,beneficiaryNature:a.nature,payingAccount:p.account,status:a.nature===nature?'self':'due',note:a.nature===nature?'Own share of card payment':a.nature+' owes '+nature+' for mixed-use card payment',createdBy:req.user.username,createdAt:p.createdAt}));
  audit(s,req,'CARD_PAYMENT_RECORDED',p.id,p);
  try{commitStores(s,exp,before);}catch(error){return res.status(409).json({success:false,error:'Payment could not be committed safely. '+error.message});}
  res.json({success:true,payment:p,outstanding:liability(s,c.id)});
});
router.get('/api/expenses/credit-cards/:id/ledger',(req,res)=>{const s=load(),c=s.cards[req.params.id];if(!c||!visibleCard(req,c))return res.status(404).json({success:false,error:'Card not found.'});const entries=[{id:'OPENING',date:'',description:'Opening outstanding',debit:num(c.openingOutstanding),credit:0}];Object.values(s.statements).filter(st=>st.cardId===c.id&&st.status==='finalized').forEach(st=>st.rows.forEach(r=>{if(!cardAccounting.activeRow(r))return;if(['expense','fee','emi_principal','emi_interest'].includes(r.classification))entries.push({id:r.id,date:r.date,description:r.narration,debit:r.amount,credit:0,statementId:st.id});if(['refund','card_payment'].includes(r.classification))entries.push({id:r.id,date:r.date,description:r.narration,debit:0,credit:r.amount,statementId:st.id});}));if(!Object.values(s.statements).some(st=>st.cardId===c.id&&st.status==='finalized'&&st.statementSourceOfTruth)){directCardPurchases(c.id).forEach(p=>entries.push({id:p.id,date:p.date,description:p.description,debit:p.amount,credit:0,proof:p.proof}));s.payments.filter(p=>p.cardId===c.id&&p.active!==false).forEach(p=>entries.push({id:p.id,date:p.date,description:p.paymentKind+' card payment from '+p.account,debit:0,credit:p.amount,proof:p.proof}));}expenseRefunds.manualCardRefunds(loadExpenses(),c.id,cardName(c)).forEach(r=>entries.push({id:r.id,date:r.date,description:r.source,debit:0,credit:r.amount}));entries.sort((a,b)=>String(a.date+a.id).localeCompare(String(b.date+b.id)));let balance=0;entries.forEach(e=>{balance+=num(e.debit)-num(e.credit);e.balance=round2(balance);});res.json({success:true,card:Object.assign({},c,{displayName:cardName(c)}),outstanding:round2(balance),entries:entries.slice().reverse()});});
module.exports={router,merchantKey,inferClassification};
