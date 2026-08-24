// ═══════════════════════════════════════════════════════════════
// Expenses — the money-OUT side of in-app accounting, built around SANKI's
// real "runner" process and made leakage-proof by PROOF at every gate.
//
// Real-world flow this mirrors (founder, 2026-08-16):
//   1. A RUNNER goes and does the task (buys food, fetches goods from a vendor…)
//   2. They LOG the expense with a printed/handwritten bill. If the claimant
//      paid personally and no bill exists, seller-payment proof + alternative
//      evidence + a reason form an admin-only exception.
//   3. ACCOUNTS APPROVES normal bills; only ADMIN approves no-bill exceptions.
//   4. Company payment (or reimbursement to the claimant) requires its own proof.
//   5. Only APPROVED business (A3) expenses hit the P&L; cash balances drop only
//      when actually PAID. Home (C3) / personal (PSNL) / Hotel are excluded.
//
// Vendor "books" are kept in each vendor's name: billed vs paid vs outstanding.
// Running cash balances are kept per paying account (opening ± adjustments − paid).
// ═══════════════════════════════════════════════════════════════
const express = require('express');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const multer = require('multer');

const router = express.Router();

const DATA_DIR = process.env.DATA_PATH
  ? path.dirname(process.env.DATA_PATH)
  : path.join(__dirname, '..');
const EXP_PATH = path.join(DATA_DIR, 'expenses.json');
const PROC_PATH = process.env.PROCUREMENT_PATH || path.join(DATA_DIR, 'procurement.json');
const SALES_PATH = process.env.SALES_PATH || path.join(DATA_DIR, 'sales.json');
const ORDERS_PATH = process.env.ORDERS_PATH || path.join(DATA_DIR, 'orders.json');
const DEFAULT_SALES_BANK = 'Axis Bank 3448';
const DEFAULT_COUNTER_CASH = 'Counter Cash';
const SALES_LEDGER_FROM = '2026-08-21';

// Proof images (bill + payment screenshot) live on the /data volume, same
// pattern as procurement photos.
const PROOF_DIR = path.join(DATA_DIR, 'expense-proofs');
try { fs.mkdirSync(PROOF_DIR, { recursive: true }); } catch { /* exists */ }
const proofUpload = multer({
  storage: multer.diskStorage({
    destination: (req, file, cb) => cb(null, PROOF_DIR),
    filename: (req, file, cb) => {
      const ext = (path.extname(file.originalname || '') || '.jpg').toLowerCase().replace(/[^.a-z0-9]/g, '');
      const privacy = normalizedNature(req.body && req.body.nature) === 'PERSONAL' ? 'personal-' : '';
      cb(null, privacy + Date.now() + '-' + crypto.randomBytes(6).toString('hex') + (ext || '.jpg'));
    }
  }),
  limits: { fileSize: 15 * 1024 * 1024 },
  fileFilter: (req, file, cb) => cb(null, /^image\//.test(file.mimetype))
});

function num(v) { const n = parseFloat(v); return isNaN(n) ? 0 : n; }
function round0(n) { return Math.round(n); }
function vendorKey(v) { return String(v || '').trim().toLowerCase().replace(/[^a-z0-9]+/g, ' ').replace(/\s+/g, ' ').trim(); }
function fuzzyIncludes(text, query) {
  const a=String(text||'').toLowerCase(),q=String(query||'').toLowerCase();
  if(!q||a.includes(q))return true;
  let i=0;for(const ch of a){if(ch===q[i])i++;if(i===q.length)return true;}return false;
}

// ── Chart of accounts: the founder's 137 real Tally ledgers ──────
const LEDGERS = [
  'A3 Suspense','Acrylic Expense A3','Camerawork Expense-A3','Carpenting Expense-A3','Civil Work Expense-A3',
  'Cleaning Expense-A3','Electrician Expense-A3','Flex Board Expense-A3','Food & Drinks-A3','General Expenses-A3',
  'Glass Work Expense-A3','Interior Expense-A3','Labor Expense-A3','Paint Expense-A3','Plumbing Expense-A3',
  'Porter Expense A3','Rapido-A3','Tiles Expense-A3','Water Tank Security-A3','Welding Work-A3','Wifi Expense-A3',
  'Banner Expenses-Hotel','Deepa Hotel Staff','Electrician-Hotel','Electricity Expense-Hotel','Food & Drink-Hotel',
  'General Expenses-Hotel','Rapido-Hotel','Sachin Hotel Staff','Stationary-Hotel','Travelling-Hotel',
  'A3 Expenses','ALTERATION','BANK CHARGES','Barcode & Stickers Rolls','BSES','C3 Expenses','CLEANING EXP',
  'COD COURIER CHARGES','COMPUTER REPAIR','CUSTOMER CHANGE','Customer Refund','DONATION','DRY CLEAN','DUMMIES',
  'ELECTRICITY BILL','ELECTRICITY EXPENSE','FOOD EXPENSE','Gagan Sir Fuel','Gagan Sir PSNL Expenses','Go Kwik',
  'Incentive Expense','Internet Expense','JJ','KN Expenses','Labor Charge','LED Rent','MANDIR ELECTRICITY EXPENSE',
  'MARKETING EXPENSE','MCA EXP','Medical Expenses','Meta Wallet Recharge','Mis Exp','MIS OFFICE EXP','Models Payment',
  'OFFICE EXP','PACKING MATERIAL','PARKING','PETROL','POOJA MANDIR','PORTER','REPAIR & MAINTANCE','Salary Expense',
  'SANKI New Store Security Deposit','SCOTTY REPAIR','Sendvise Wallet Recharge','Shopify Payments','Social Media Expenses',
  'STAFF WELFARE','STATIONERY','Subscriptions','Tags and Labels','Technology Expense','TELEPHONE EXP','Threads',
  'TRAVELING','Warehouse Rent Expense','Water & Soda','Workindia Payment','Zipper Bags','Velocity Recharge','U M',
  'SUSPENSE','Furniture Expense-A3','Flooring Expense-A3','Changing Room Expense-A3','Putty Expense-A3',
  'Cash Counter Expense-A3','Fee & Charges','Pradeep IMP','Mirror Expense-A3','Electrician Expense','Wallpaper Expense-A3',
  'Paint Items','Fuel Expenses','Operating Expense','Miscellaneous Expense','Abdul AC','Jitender Painter','Aman Glass Work',
  'Rahul CCTV','Conveyance Expenses','Office Consumables','General Expense','Housekeeping and Consumable Expense',
  'Business Promotion Expenses','VC','Freight Charges','Housekeeping Expense','Office Equipment Expense','Rapido Expense',
  'Vehicle Repair & Maintenance Expense','Printing & Stationery Expense','Electronic Items Expense',
  'AI Software Subscription Expense','Advertisement Expense','Staff Uniform Expense','Religious Expenses','Electrical Expense',
  'Office Consumable Expense','Guest Refreshment Expenses','Store Supplies','Parking Expense','Installation Expense',
  'Consumable Expense','Shoot Expense','Dry Cleaning Expense'
];

// NATURE was A3(business)/C3/PSNL/Hotel. The founder collapsed it (2026-08-16):
// this module is for SANKI BUSINESS expenses only — everything here is 'SANKI'
// and counts in the P&L. Home/personal/hotel spend simply isn't logged here.
function defaultNature() { return 'SANKI'; }

// Only BUSINESS ledgers are offered in the category picker. The 137-ledger
// Tally list still carries home/personal/hotel headers from the old sheet —
// these are filtered OUT so the founder never sees or picks a non-business one.
function isBusinessLedger(name) {
  const n = String(name).toLowerCase();
  if (n.includes('hotel')) return false;                 // separate venture
  if (n.includes('psnl')) return false;                  // personal
  if (n.includes('c3')) return false;                    // home
  if (n.includes('donation') || n.includes('pooja') || n.includes('mandir') ||
      n.includes('religious') || n.includes('medical')) return false;
  return true;
}
// TYPE routes the expense to a P&L row.
function defaultType(name) {
  const n = name.toLowerCase();
  if (n.includes('rent') || n.includes('salary')) return 'fixed';
  if (n.includes('marketing') || n.includes('advertis') || n.includes('meta wallet') ||
      n.includes('social media') || n.includes('business promotion') || n.includes('models payment') ||
      n.includes('shoot') || n.includes('camerawork') || n.includes('banner')) return 'marketing';
  if (n.includes('courier') || n.includes('go kwik') || n.includes('gokwik') || n.includes('shopify payments') ||
      n.includes('packing') || n.includes('freight') || n.includes('barcode') || n.includes('zipper') ||
      n.includes('tags') || n.includes('bank charges') || n.includes('internet') || n.includes('wifi') ||
      n.includes('subscription') || n.includes('sendvise') || n.includes('velocity recharge') ||
      n.includes('fee & charges') || n.includes('technology') || n.includes('telephone')) return 'running';
  return 'variable';
}

const TYPES = ['fixed', 'running', 'variable', 'marketing'];
const NATURES = ['SANKI', 'SAMAST', 'PERSONAL'];
const BUSINESS_NATURES = ['SANKI', 'SAMAST', 'A3'];  // PERSONAL never enters a business P&L
const SANKI_PL_NATURES = ['SANKI', 'A3'];
const CHANNELS = ['POS', 'Website', 'Shared'];
const BILLS = ['printed', 'handwritten', 'none'];
const PAID_BY = ['company', 'claimant'];
const PAYMENT_TYPES = ['UPI', 'Cash', 'Credit'];
const PERSONAL_CATEGORIES = ['Food & Dining','Household Staff','Children & Education','Medical & Healthcare','Travel & Transport','Home & Utilities','Shopping','Subscriptions','Personal Care','Gifts & Charity','Entertainment','Financial Charges','Miscellaneous Personal'];
const ENTITY_ACCOUNTS = {
  SANKI: ['Axis Bank 3448','Tiana 0425','Prashant Axis 3645','Counter Cash','Gagan Sir Cash','Prashant Cash'],
  SAMAST: ['IndusInd Bank 7883','ICICI Bank 0993','ICICI Bank 0992','Kirti Nagar Cash'],
  PERSONAL: ['IndusInd Bank 7883','ICICI Bank 0993','ICICI Bank 0992','Gagan Personal Cash']
};
const CLAIMANT_ACCOUNTS = {
  arshpreet: ['Arshpreet 1919'],
  shivam: ['Shivam 4807'],
  pradeep: ['Pradeep 8606']
};
const ACCOUNT_RENAMES = { 'Axis Bank 3645':'Prashant Axis 3645', 'Cash':'Counter Cash' };

// Accounts the founder actually pays from are added in-app (with approval) —
// start minimal instead of the old guessed list.
const DEFAULT_ACCOUNTS = ['Paytm', 'Cash'];
const DEFAULT_PEOPLE = ['Pradeep', 'Prashant'];   // claimants (people who run the errand)

// ── Store ────────────────────────────────────────────────────────
function blankStore() {
  return {
    expenses: {},
    receivables: {},
    vendors: {},
    vendorsByNature: { SAMAST: {}, PERSONAL: {} },
    accounts: DEFAULT_ACCOUNTS.slice(),
    people: DEFAULT_PEOPLE.slice(),      // claimants
    openingBalances: {},                 // { [account]: opening ₹ }
    openingBalancesByNature: { SAMAST: {}, PERSONAL: {} }, // non-SANKI books stay separate
    adjustments: [],                     // [{ id, account, amount(+/-), note, date }] top-ups/corrections
    transfers: [],                       // [{ id, nature, fromAccount, toAccount, amount, date, proof, note }]
    receipts: [],                        // money received other than sales/receivables
    auditLog: [],                        // immutable accounting activity trail
    ledgerOverrides: {},
    customLedgers: {},                   // { [name]: { name, type } } admin-approved new categories
    requests: [],                        // [{ id, kind:'ledger'|'account', name, meta, status, by, at, decidedBy, decidedAt }]
    openingInvestment: 0,
    procurementAccounting: {
      mediator: 'Logistics Mediator',
      trackPostedFrom: process.env.PROCUREMENT_ACCOUNTING_FROM || '2026-08-21T00:00:00+05:30',
      paymentsByPo: {}
    },
    odConfig: { 'Tiana 0425': { limit: 0, ratePct: 0 } },
    seq: 0, receivableSeq: 0, adjSeq: 0, transferSeq: 0, receiptSeq: 0, reqSeq: 0, auditSeq: 0
  };
}
function loadStore() {
  try {
    const s = Object.assign(blankStore(), JSON.parse(fs.readFileSync(EXP_PATH, 'utf8')));
    // Repair records finalized under the old instalment rule, which marked the
    // whole agreement paid when only "payment required now" had been paid.
    const rename = value => ACCOUNT_RENAMES[String(value || '')] || value;
    s.accounts = Array.from(new Set([].concat(...Object.values(ENTITY_ACCOUNTS), s.accounts || []).map(rename))).filter(a => a !== 'Federal Bank 7328');
    Object.values(s.expenses || {}).forEach(e => {
      e.account = rename(e.account);
      (e.payments || []).forEach(p => { p.account = rename(p.account); });
      (e.reimbursementPayments || []).forEach(p => { p.account = rename(p.account); });
      if (!e.approvedAt || e.status === 'rejected') return;
      const recorded = (e.payments || []).reduce((n, p) => n + num(p.amount), 0);
      e.paidAmount = Math.max(num(e.paidAmount), recorded);
      e.status = e.paidAmount >= num(e.amount) ? 'paid' : (e.paidAmount > 0 ? 'partially_paid' : 'approved');
      if (e.paidAlready) e.vendorPaymentCompleted = e.paidAmount >= num(e.amount);
    });
    (s.adjustments || []).forEach(x => { x.account = rename(x.account); });
    (s.transfers || []).forEach(x => { x.fromAccount = rename(x.fromAccount); x.toAccount = rename(x.toAccount); });
    Object.values(s.receivables || {}).forEach(x => (x.collections || []).forEach(c => { c.account = rename(c.account); }));
    Object.values((s.procurementAccounting || {}).paymentsByPo || {}).forEach(x => (x.payments || []).forEach(p => { p.account = rename(p.account); }));
    const renameBalanceKeys = map => Object.entries(ACCOUNT_RENAMES).forEach(([oldName, newName]) => {
      if (map && map[oldName] != null) { map[newName] = num(map[newName]) + num(map[oldName]); delete map[oldName]; }
    });
    renameBalanceKeys(s.openingBalances);
    Object.values(s.openingBalancesByNature || {}).forEach(renameBalanceKeys);
    // Owner-authorized one-time correction: remove the single legacy ₹5,000
    // source entry for Prashant Axis 3645 through 22 Aug 2026. A transfer is
    // removed atomically, so its debit and credit legs cannot become unequal.
    s.oneTimeMigrations=s.oneTimeMigrations||{};
    const correctionKey='delete-prashant-axis-3645-5000-through-2026-08-22';
    if(!s.oneTimeMigrations[correctionKey]){
      const account='Prashant Axis 3645',cutoff='2026-08-22',amount=5000,candidates=[];
      (s.transfers||[]).forEach((x,i)=>{if(num(x.amount)===amount&&String(x.date||'')<=cutoff&&(x.fromAccount===account||x.toAccount===account))candidates.push({kind:'transfer',index:i,entry:x});});
      (s.adjustments||[]).forEach((x,i)=>{if(num(x.amount)===amount&&String(x.date||'')<=cutoff&&x.account===account)candidates.push({kind:'adjustment',index:i,entry:x});});
      if(num((s.openingBalances||{})[account])===amount)candidates.push({kind:'opening',entry:{account,amount,date:'',nature:'SANKI'}});
      let result='not_found';
      if(candidates.length===1){
        const match=candidates[0];
        if(match.kind==='transfer')s.transfers.splice(match.index,1);
        else if(match.kind==='adjustment')s.adjustments.splice(match.index,1);
        else delete s.openingBalances[account];
        audit(s,null,'LEGACY_ENTRY_DELETED','account',account,{user:'gaganlambasanki',device:'System migration',nature:'SANKI',account,before:match.entry,after:null,note:'Owner authorized deletion of the ₹5,000 legacy entry through 22 Aug 2026'});
        result='deleted_'+match.kind;
      }else if(candidates.length>1)result='ambiguous_'+candidates.length;
      s.oneTimeMigrations[correctionKey]={appliedAt:new Date().toISOString(),account,nature:'SANKI',cutoff,amount,result};
      saveStore(s);
    }
    return s;
  }
  catch { return blankStore(); }
}
function saveStore(s) {
  const tmp = EXP_PATH + '.tmp-' + process.pid + '-' + Date.now();
  fs.writeFileSync(tmp, JSON.stringify(s));
  fs.renameSync(tmp, EXP_PATH);
}
function audit(s, req, action, subjectType, subjectId, data) {
  s.auditLog=Array.isArray(s.auditLog)?s.auditLog:[];s.auditSeq=(s.auditSeq||0)+1;
  const details=data||{},headers=req&&req.headers||{},header=name=>req&&typeof req.get==='function'?req.get(name):headers[String(name).toLowerCase()],forwarded=String(header('x-forwarded-for')||'').split(',')[0].trim(),ip=forwarded||String(req&&req.ip||req&&req.socket&&req.socket.remoteAddress||details.ip||''),userAgent=String(header('user-agent')||details.userAgent||''),device=details.device||(userAgent?/mobile|android|iphone/i.test(userAgent)?'Mobile':/ipad|tablet/i.test(userAgent)?'Tablet':'Desktop':(details.user?'Telegram':'Unknown'));
  const entry={id:'AUD-'+String(s.auditSeq).padStart(6,'0'),at:new Date().toISOString(),user:(req&&req.user&&req.user.username)||details.user||'system',action,subjectType,subjectId:String(subjectId||''),nature:normalizedNature(details.nature||'SANKI'),account:String(details.account||''),paymentId:String(details.paymentId||''),ip,device,userAgent:userAgent.slice(0,500),before:details.before==null?null:details.before,after:details.after==null?null:details.after,note:String(details.note||'')};
  s.auditLog.push(entry);if(s.auditLog.length>10000)s.auditLog=s.auditLog.slice(-10000);return entry;
}

// Owner-only Telegram quick capture. The Telegram webhook has already verified
// the linked owner chat before calling this function. A clear narration plus a
// payment screenshot becomes a fully paid PERSONAL entry in one step.
function createTelegramPersonalExpense(input) {
  const b=input||{},s=loadStore(),amount=num(b.amount),proof=String(b.proof||'').trim();
  const username=String(b.username||'').trim(),particulars=String(b.particulars||'').trim(),sourceKey=String(b.sourceKey||'').trim();
  if(!(amount>0)) return {success:false,error:'Amount must be greater than 0.'};
  if(!proof) return {success:false,error:'Payment screenshot is required.'};
  if(!particulars) return {success:false,error:'Narration is required.'};
  const requested=String(b.account||'').trim().toLowerCase(),accounts=companyAccountsForNature('PERSONAL');
  const suffixMatches=accounts.filter(a=>{const last=(a.match(/\d{4}$/)||[])[0];return last&&requested.length>=2&&last.endsWith(requested.replace(/\D/g,''));});
  const account=(/cash/i.test(requested)&&accounts.find(a=>/cash/i.test(a)))||accounts.find(a=>a.toLowerCase()===requested)||accounts.find(a=>requested&&a.toLowerCase().includes(requested))||accounts.find(a=>{const last=(a.match(/\d{4}$/)||[])[0];return last&&requested.includes(last);})||(suffixMatches.length===1?suffixMatches[0]:'');
  if(!account) return {success:false,error:'Personal account was not recognized.'};
  if(sourceKey){const duplicate=Object.values(s.expenses||{}).find(e=>e.telegramSourceKey===sourceKey);if(duplicate)return {success:true,duplicate:true,expense:duplicate};}
  const now=new Date().toISOString(),date=String(b.date||now.slice(0,10)).slice(0,10),paymentType=/cash/i.test(account)?'Cash':'UPI';
  s.seq=(s.seq||0)+1;const id='EX-'+String(s.seq).padStart(5,'0');
  const vendor=String(b.vendor||particulars).trim(),ledger=PERSONAL_CATEGORIES.includes(b.ledger)?b.ledger:'Miscellaneous Personal';
  s.expenses[id]={id,date,particulars,amount,isInstallment:false,requestedAmount:amount,nature:'PERSONAL',type:'variable',ledger,vendor,claimant:username,account,channel:'Shared',bill:'printed',fundedBy:'claimant',paymentType,qrPhoto:'',billPhoto:proof,purchasePaymentProof:proof,exceptionEvidence:'',exceptionReason:'',billNote:'Captured from owner Telegram payment screenshot',paidAlready:true,personalPaidAmount:amount,reimbursementStatus:'not_applicable',reimbursementAmount:0,reimbursementPayments:[],paymentProof:proof,status:'paid',paidAmount:amount,payments:[{id:'PAY-001',amount,date,account,paymentType,proof,note:'Owner payment captured from Telegram',paidBy:username,paidAt:now,personalFunds:true}],createdAt:now,createdBy:username,approvedAt:now,approvedBy:username,paidAt:now,paidBy:username,telegramSourceKey:sourceKey,telegramNeedsReview:b.needsReview!==false,telegramNarration:String(b.rawNarration||particulars).trim(),telegramOcrText:String(b.ocrText||'').slice(0,4000)};
  s.vendorsByNature=s.vendorsByNature||{};s.vendorsByNature.PERSONAL=s.vendorsByNature.PERSONAL||{};if(!s.vendorsByNature.PERSONAL[vendor.toLowerCase()])s.vendorsByNature.PERSONAL[vendor.toLowerCase()]={name:vendor,notes:'Added from Owner Telegram capture'};
  audit(s,null,'CREATED','expense',id,{nature:'PERSONAL',user:username,after:s.expenses[id],note:'Telegram capture'});saveStore(s);return {success:true,expense:s.expenses[id]};
}
function createTelegramPersonalReceipt(input) {
  const b=input||{},s=loadStore(),amount=num(b.amount),proof=String(b.proof||'').trim(),source=String(b.source||'').trim(),sourceKey=String(b.sourceKey||'').trim(),requested=String(b.account||'').trim().toLowerCase(),accounts=companyAccountsForNature('PERSONAL');
  const digits=requested.replace(/\D/g,''),matches=accounts.filter(a=>{const last=(a.match(/\d{4}$/)||[])[0];return last&&digits.length>=2&&last.endsWith(digits);});
  const account=(/cash/i.test(requested)&&accounts.find(a=>/cash/i.test(a)))||accounts.find(a=>a.toLowerCase()===requested)||accounts.find(a=>requested&&a.toLowerCase().includes(requested))||(matches.length===1?matches[0]:'');
  if(!(amount>0))return{success:false,error:'Receipt amount must be greater than 0.'};if(!proof)return{success:false,error:'Receipt proof is required.'};if(!source)return{success:false,error:'Source / narration is required.'};if(!account)return{success:false,error:'Personal receiving account was not recognized.'};
  s.receipts=Array.isArray(s.receipts)?s.receipts:[];if(sourceKey){const duplicate=s.receipts.find(x=>x.telegramSourceKey===sourceKey);if(duplicate)return{success:true,duplicate:true,receipt:duplicate};}
  const now=new Date().toISOString(),lower=source.toLowerCase(),receiptType=/refund/.test(lower)?'refund':(/sale|sold/.test(lower)?'asset_sale':(/contribution|capital/.test(lower)?'owner_contribution':'other_income'));
  s.receiptSeq=(s.receiptSeq||0)+1;const receipt={id:'REC-'+String(s.receiptSeq).padStart(5,'0'),nature:'PERSONAL',account,amount,receiptType,source,date:String(b.date||now.slice(0,10)).slice(0,10),note:'Captured from Owner Telegram receipt screenshot',proof,createdBy:String(b.username||'owner'),createdAt:now,telegramSourceKey:sourceKey,telegramOcrText:String(b.ocrText||'').slice(0,4000)};
  s.receipts.push(receipt);audit(s,null,'RECEIPT_RECORDED','receipt',receipt.id,{nature:'PERSONAL',account,user:receipt.createdBy,after:receipt,note:'Telegram capture'});saveStore(s);return{success:true,receipt};
}
function telegramExpense(id){const e=loadStore().expenses[id];return e?JSON.parse(JSON.stringify(e)):null;}
function telegramResolveAccount(nature,requested){const q=String(requested||'').toLowerCase(),accounts=companyAccountsForNature(normalizedNature(nature)),digits=q.replace(/\D/g,'');return(/cash/.test(q)&&accounts.find(a=>/cash/i.test(a)))||accounts.find(a=>a.toLowerCase()===q)||accounts.find(a=>digits&&a.replace(/\D/g,'').endsWith(digits))||'';}
function telegramApproveExpense(id,actor,changes){const s=loadStore(),e=s.expenses[id];if(!e)return{success:false,error:'Expense not found.'};if(e.status!=='pending')return{success:false,error:'This expense is already '+e.status+'.',expense:e};const before=JSON.parse(JSON.stringify(e)),c=changes||{};['particulars','vendor','ledger','type','paymentType'].forEach(k=>{if(c[k]!=null&&String(c[k]).trim())e[k]=String(c[k]).trim();});if(c.amount!=null&&num(c.amount)>0){e.amount=num(c.amount);e.requestedAmount=e.isInstallment?Math.min(num(e.requestedAmount)||e.amount,e.amount):e.amount;}if(c.nature)e.nature=normalizedNature(c.nature);if(e.ledger&&!pickableLedgers(s).some(x=>x.name.toLowerCase()===e.ledger.toLowerCase())){s.customLedgers[e.ledger]={name:e.ledger,type:TYPES.includes(e.type)?e.type:'variable'};}const changed=['nature','particulars','vendor','ledger','type','paymentType','amount','requestedAmount'].some(k=>JSON.stringify(before[k])!==JSON.stringify(e[k]));if(changed)audit(s,null,'EDITED','expense',id,{user:actor,device:'Telegram',nature:e.nature,before,after:e,note:'Edited during Telegram approval'});if(e.bill==='none'||!e.billPhoto)return{success:false,error:'This expense needs bill-exception review in the app before approval.',appRequired:true,expense:e};if(!e.vendor)return{success:false,error:'Vendor is required.',expense:e};if(!e.ledger)return{success:false,error:'Add a category before approving.',needsCategory:true,expense:e};const n=normalizedNature(e.nature);s.vendors=s.vendors||{};s.vendorsByNature=s.vendorsByNature||{};if(n==='SANKI'){s.vendors[e.vendor.toLowerCase()]=s.vendors[e.vendor.toLowerCase()]||{name:e.vendor,notes:''};}else{s.vendorsByNature[n]=s.vendorsByNature[n]||{};s.vendorsByNature[n][e.vendor.toLowerCase()]=s.vendorsByNature[n][e.vendor.toLowerCase()]||{name:e.vendor,notes:''};}e.status=num(e.paidAmount)>=num(e.amount)?'paid':num(e.paidAmount)>0?'partially_paid':'approved';if(e.paidAlready)e.reimbursementStatus='pending';e.approvedAt=new Date().toISOString();e.approvedBy=actor;audit(s,null,'APPROVED','expense',id,{user:actor,device:'Telegram',nature:e.nature,after:{status:e.status,approvedBy:actor,amount:e.amount}});saveStore(s);notifyExpenseUser(e,'approved');return{success:true,expense:e};}
function telegramRejectExpense(id,actor,reason){const s=loadStore(),e=s.expenses[id];if(!e)return{success:false,error:'Expense not found.'};if(e.status!=='pending')return{success:false,error:'Only a pending expense can be rejected.'};e.status='rejected';e.rejectReason=String(reason||'Rejected from Telegram');e.rejectedAt=new Date().toISOString();e.rejectedBy=actor;audit(s,null,'REJECTED','expense',id,{user:actor,device:'Telegram',nature:e.nature,after:{status:e.status,reason:e.rejectReason}});saveStore(s);notifyExpenseUser(e,'rejected');return{success:true,expense:e};}
function telegramRecordPayment(id,actor,b){const s=loadStore(),e=s.expenses[id],body=b||{};if(!e)return{success:false,error:'Expense not found.'};if(!['approved','partially_paid'].includes(e.status)||e.paidAlready)return{success:false,error:'This expense is not awaiting a vendor payment.'};const proof=String(body.proof||'');if(!proof)return{success:false,error:'Payment screenshot is required.'};const account=telegramResolveAccount(e.nature,body.account);if(!account)return{success:false,error:'Paying account was not recognized.',needsAccount:true};const issues=reconciliationIssues(s,normalizedNature(e.nature),account);if(issues.length)return{success:false,error:'This account has a reconciliation warning. Complete this payment in the app.',appRequired:true};const outstanding=Math.max(0,num(e.amount)-num(e.paidAmount)),amount=body.amount!=null?num(body.amount):outstanding;if(!(amount>0)||amount>outstanding)return{success:false,error:'Payment must be between ₹0 and '+outstanding+'.'};e.account=account;e.paidAmount=num(e.paidAmount)+amount;e.paymentProof=proof;e.payments=Array.isArray(e.payments)?e.payments:[];e.payments.push({id:'PAY-'+String(e.payments.length+1).padStart(3,'0'),amount,date:String(body.date||new Date().toISOString().slice(0,10)).slice(0,10),account,paymentType:'UPI',proof,note:'Recorded through Telegram',paidBy:actor,paidAt:new Date().toISOString()});e.status=e.paidAmount>=num(e.amount)?'paid':'partially_paid';e.paidAt=new Date().toISOString();e.paidBy=actor;audit(s,null,'PAYMENT_RECORDED','expense',id,{user:actor,device:'Telegram',nature:e.nature,account,paymentId:e.payments.at(-1).id,after:e.payments.at(-1)});saveStore(s);notifyExpenseUser(e,e.status==='paid'?'paid':'partially_paid',amount);return{success:true,expense:e,payment:e.payments.at(-1)};}
function procurementAccounting(s) {
  s.procurementAccounting = Object.assign({ mediator: 'Logistics Mediator', trackPostedFrom: '2026-08-21T00:00:00+05:30', paymentsByPo: {} }, s.procurementAccounting || {});
  s.procurementAccounting.paymentsByPo = s.procurementAccounting.paymentsByPo || {};
  return s.procurementAccounting;
}
function loadProcurementStore() {
  try { return JSON.parse(fs.readFileSync(PROC_PATH, 'utf8')); } catch { return { pos: {} }; }
}
function salesLedgerEntries() {
  const rows = [];
  try {
    const local = JSON.parse(fs.readFileSync(SALES_PATH, 'utf8'));
    (local.sales || []).filter(x => !x.voided).forEach(x => rows.push({ id:'SALE/'+x.id, date:x.day||String(x.ts||'').slice(0,10), account:String(x.paymentMode||'').toLowerCase()==='cash'?DEFAULT_COUNTER_CASH:DEFAULT_SALES_BANK, amount:num(x.total), description:'Sale · '+(x.channel||'POS')+' · '+(x.staff||'') }));
  } catch { /* no local-sales store yet */ }
  try {
    const shop = JSON.parse(fs.readFileSync(ORDERS_PATH, 'utf8'));
    Object.values(shop.orders || {}).filter(x => !x.cancelledAt && String(x.financialStatus||'').toLowerCase()==='paid').forEach(x => {
      const gateways=(x.paymentGateways||[]).join(' ').toLowerCase(), cash=gateways.includes('cash');
      rows.push({ id:'SHOPIFY/'+x.id, date:String(x.processedAt||x.createdAt||'').slice(0,10), account:cash?DEFAULT_COUNTER_CASH:DEFAULT_SALES_BANK, amount:num(x.total)-num(x.refundAmount), description:'Shopify sale · '+(x.name||x.id)+' · '+(x.channel||'') });
    });
  } catch { /* orders have not synced yet */ }
  const seen = new Set();
  return rows.filter(x=>x.date&&x.amount>0).filter(x => {
    const key = String(x.id || '') + '|' + x.date + '|' + x.amount;
    if (seen.has(key)) return false;
    seen.add(key); return true;
  });
}
function includeAutomaticSale(x) {
  return x.account !== DEFAULT_SALES_BANK || String(x.date || '') >= SALES_LEDGER_FROM;
}

function reconciliationIssues(s, nature, account) {
  const issues = [], seen = new Set();
  (s.transfers || []).filter(x => {
    const fromNature=normalizedNature(x.fromNature||x.nature),toNature=normalizedNature(x.toNature||x.nature);
    return (fromNature===nature&&(!account||x.fromAccount===account))||(toNature===nature&&(!account||x.toAccount===account));
  }).forEach(x => {
    if (!x.id || seen.has(x.id)) issues.push({ code:'duplicate_transfer', reference:x.id || '(missing)', message:'Duplicate or missing transfer reference.' });
    if (x.id) seen.add(x.id);
    if (!x.fromAccount || !x.toAccount || !(num(x.amount) > 0)) issues.push({ code:'incomplete_transfer', reference:x.id || '(missing)', message:'Transfer is missing its source, destination, or amount.' });
    if (x.fromAccount === x.toAccount) issues.push({ code:'same_account_transfer', reference:x.id || '(missing)', message:'Transfer source and destination are the same.' });
    if (!x.proof) issues.push({ code:'missing_transfer_proof', reference:x.id || '(missing)', message:'Transfer proof is missing.' });
  });
  return issues;
}
function poLandedTotal(po) {
  let total = 0;
  (po.newProducts || []).forEach(p => (p.variants || []).forEach(v => { total += num(v.landed) * num(v.qty); }));
  (po.existingAdds || []).forEach(v => { total += num(v.landed) * num(v.qty); });
  return round0(total);
}
function procurementPayables(s, includePaid) {
  const cfg = procurementAccounting(s), proc = loadProcurementStore();
  return Object.values(proc.pos || {}).filter(po => po.status === 'posted' && String(po.postedAt || '') >= String(cfg.trackPostedFrom || ''))
    .map(po => {
      const state = cfg.paymentsByPo[po.id] || {}, payments = Array.isArray(state.payments) ? state.payments : [];
      const amount = poLandedTotal(po), paidAmount = round0(payments.reduce((n, p) => n + num(p.amount), 0));
      return { id: po.id, source: 'procurement', nature: 'SANKI', vendor: state.mediator || cfg.mediator,
        supplier: po.vendor || '', billNo: po.billNo || '', date: po.dateReceive || po.datePurchase || String(po.postedAt || '').slice(0, 10),
        postedAt: po.postedAt || '', particulars: 'Advanced purchase · goods and China-to-store transport', amount, paidAmount,
        balanceDue: Math.max(0, amount - paidAmount), status: paidAmount >= amount ? 'paid' : (paidAmount > 0 ? 'partially_paid' : 'approved'), payments };
    }).filter(x => includePaid || x.balanceDue > 0).sort((a, b) => String(b.date).localeCompare(String(a.date)));
}
function ledgerMeta(s, name) {
  const ov = (s.ledgerOverrides || {})[name] || {};
  const custom = (s.customLedgers || {})[name] || {};
  return { name, nature: 'SANKI', type: ov.type || custom.type || defaultType(name) };
}
// The category picker = built-in BUSINESS ledgers ∪ admin-approved custom ones.
function pickableLedgers(s) {
  const names = LEDGERS.filter(isBusinessLedger).concat(PERSONAL_CATEGORIES,Object.keys(s.customLedgers || {}));
  const seen = {};
  return names.filter(n => (seen[n] ? false : (seen[n] = true)))
    .map(n => ledgerMeta(s, n))
    .sort((a, b) => a.name.localeCompare(b.name));
}
function storedAccountNames(s) {
  const names = new Set([].concat(s.accounts || [], [DEFAULT_SALES_BANK, DEFAULT_COUNTER_CASH], Object.keys(s.openingBalances || {}), Object.keys(s.odConfig || {})));
  Object.values(s.openingBalancesByNature || {}).forEach(map => Object.keys(map || {}).forEach(name => names.add(name)));
  (s.adjustments || []).forEach(x => { if (x.account) names.add(String(x.account)); });
  (s.transfers || []).forEach(x => { if (x.fromAccount) names.add(String(x.fromAccount)); if (x.toAccount) names.add(String(x.toAccount)); });
  Object.values(s.expenses || {}).forEach(e => {
    if (e.account) names.add(String(e.account));
    (e.payments || []).forEach(p => { if (p.account) names.add(String(p.account)); });
    (e.reimbursementPayments || []).forEach(p => { if (p.account) names.add(String(p.account)); });
  });
  return Array.from(names).map(x => String(x).trim()).filter(x => x && x !== '(unspecified)').sort((a, b) => a.localeCompare(b));
}
function companyAccountsForNature(nature) {
  return (ENTITY_ACCOUNTS[normalizedNature(nature)] || []).slice();
}
function personalAccountsForReq(req) {
  const username = String((req.user && req.user.username) || '').trim().toLowerCase();
  const roles = rolesOfReq(req);
  if (roles.includes('owner')) return Array.from(new Set([].concat(...Object.values(ENTITY_ACCOUNTS), ...Object.values(CLAIMANT_ACCOUNTS))));
  if (roles.includes('admin')) return Array.from(new Set([].concat(...Object.values(ENTITY_ACCOUNTS))));
  return (CLAIMANT_ACCOUNTS[username] || []).slice();
}
function ledgerAccountsForNature(s, nature) {
  const n = normalizedNature(nature), names = new Set(companyAccountsForNature(n));
  Object.values(s.expenses || {}).filter(e => normalizedNature(e.nature) === n).forEach(e => {
    if (e.account) names.add(String(e.account));
    (e.payments || []).forEach(p => { if (p.account) names.add(String(p.account)); });
    (e.reimbursementPayments || []).forEach(p => { if (p.account) names.add(String(p.account)); });
  });
  (s.adjustments || []).filter(x => normalizedNature(x.nature) === n).forEach(x => names.add(String(x.account)));
  (s.receipts || []).filter(x => normalizedNature(x.nature) === n).forEach(x => names.add(String(x.account)));
  (s.transfers || []).forEach(x => {
    if (normalizedNature(x.fromNature || x.nature) === n) names.add(String(x.fromAccount));
    if (normalizedNature(x.toNature || x.nature) === n) names.add(String(x.toAccount));
  });
  return Array.from(names).filter(Boolean).sort((a,b)=>a.localeCompare(b));
}
function allowedCompanyAccount(s, nature, account) {
  const candidate = ACCOUNT_RENAMES[String(account || '')] || String(account || '');
  return companyAccountsForNature(nature).find(name => name.toLowerCase() === candidate.toLowerCase());
}
function rolesOfReq(req) {
  return (req.user && (req.user.roles || (req.user.role ? [req.user.role] : []))) || [];
}
function isAdmin(req) { const r = rolesOfReq(req); return r.includes('admin') || r.includes('owner'); }
// Who may APPROVE & PAY: admin or accounting. A pure claimant may only LOG.
function canApprove(req) { const r = rolesOfReq(req); return r.includes('admin') || r.includes('accounting') || r.includes('samast_accounting') || r.includes('owner'); }
function normalizedNature(v) { const n = String(v || '').toUpperCase(); return n === 'SAMAST' || n === 'PERSONAL' ? n : 'SANKI'; }
function approvalNatures(req) {
  const r = rolesOfReq(req);
  if (r.includes('owner')) return NATURES.slice();
  const out = [];
  if (r.includes('admin')) out.push('SANKI', 'SAMAST', 'PERSONAL');
  if (r.includes('accounting')) out.push('SANKI');
  if (r.includes('samast_accounting')) out.push('SAMAST');
  return Array.from(new Set(out));
}
function submissionNatures(req) {
  const r = rolesOfReq(req);
  if (r.includes('owner')) return NATURES.slice();
  const out = approvalNatures(req);
  if (r.includes('admin') || r.includes('claimant')) out.push('SANKI', 'SAMAST', 'PERSONAL');
  if (r.includes('personal_claimant')) out.push('PERSONAL');
  return Array.from(new Set(out));
}
function canApproveExpenseNature(req, e) { return approvalNatures(req).includes(normalizedNature(e && e.nature)); }
function canViewExpense(req, e) {
  if (canApproveExpenseNature(req, e)) return true;
  return e && e.createdBy === (req.user && req.user.username);
}

// Notifications must never block an accounting action. Telegram is optional in
// local/test environments, so these helpers deliberately degrade to a no-op.
function notifyApproversNewExpense(e) {
  try {
    const telegram = require('./telegram');
    if(typeof telegram.notifyExpenseForApproval==='function'){telegram.notifyExpenseForApproval(e);return;}
    const esc = telegram.esc || (v => String(v));
    const send = typeof telegram.notifyApprovers === 'function' ? telegram.notifyApprovers : ((nature, text, opts) => telegram.notify(text, opts));
    send(normalizedNature(e.nature),
      `🧾 <b>New ${esc(normalizedNature(e.nature))} expense ${esc(e.id)}</b>\n${esc(e.createdBy)} · ${esc(e.vendor)} · ₹${round0(e.requestedAmount || e.amount)}`,
      { button: { text: 'Review expense', url: `/expenses.html?focus=${encodeURIComponent(e.id)}` } }
    );
  } catch { /* Telegram is optional */ }
}
function notifyExpenseUser(e, event, amount) {
  try {
    const telegram = require('./telegram');
    if (typeof telegram.notifyUser !== 'function') return;
    const esc = telegram.esc || (v => String(v));
    const labels = {
      approved: e.paidAlready ? `Approved — reimbursement of ₹${round0(e.personalPaidAmount)} is pending` : 'Approved',
      rejected: `Rejected${e.rejectReason ? ': ' + e.rejectReason : ''}`,
      partially_paid: `Part payment of ₹${round0(amount)} recorded; ₹${round0(e.amount - e.paidAmount)} remains`,
      paid: 'Vendor payment completed',
      partially_reimbursed: `Reimbursement of ₹${round0(amount)} recorded; ₹${round0(e.personalPaidAmount - e.reimbursementAmount)} remains`,
      reimbursed: 'Your reimbursement has been completed'
    };
    const paymentEvent=['paid','partially_paid'].includes(event),reimbursementEvent=['reimbursed','partially_reimbursed'].includes(event);
    const record=paymentEvent&&Array.isArray(e.payments)&&e.payments.length?e.payments.at(-1):(reimbursementEvent&&Array.isArray(e.reimbursementPayments)&&e.reimbursementPayments.length?e.reimbursementPayments.at(-1):null);
    const proof=record&&record.proof||'';
    const paidAmount=round0(amount!=null?amount:(record&&record.amount)||0);
    const details=[
      `💸 <b>${esc(normalizedNature(e.nature))} · ${esc(e.id)}</b> — ${esc(labels[event] || event)}`,
      `Vendor: <b>${esc(e.vendor||'—')}</b>`,
      `Particulars: ${esc(e.particulars||'—')}`,
      paymentEvent||reimbursementEvent?`Amount paid: <b>₹${paidAmount}</b>`:`Expense amount: <b>₹${round0(e.amount)}</b>`,
      record&&record.date?`Payment date: ${esc(record.date)}`:'',
      record&&record.account?`Paid from: ${esc(record.account)}`:'',
      record&&record.paymentType?`Payment type: ${esc(record.paymentType)}`:'',
      proof?'Payment proof is attached below.':''
    ].filter(Boolean).join('\n');
    const username=e.createdBy||e.claimant,button={button:{text:'View expense',url:`/expenses.html?focus=${encodeURIComponent(e.id)}`}};
    if(proof&&typeof telegram.notifyUserWithPhoto==='function') telegram.notifyUserWithPhoto(username,proof,details,button);
    else telegram.notifyUser(username,details,button);
  } catch { /* Telegram is optional */ }
}

// ── Proof image upload / serve ───────────────────────────────────
router.post('/api/expenses/upload', proofUpload.single('photo'), (req, res) => {
  if (!req.file) return res.status(400).json({ success: false, error: 'No image received.' });
  res.json({ success: true, url: '/api/expenses/photo/' + req.file.filename });
});
router.get('/api/expenses/photo/:file', (req, res) => {
  const name = path.basename(String(req.params.file || ''));
  if (name.startsWith('personal-')) {
    const s=loadStore(),url='/api/expenses/photo/'+name,linked=Object.values(s.expenses||{}).find(e=>normalizedNature(e.nature)==='PERSONAL'&&[e.billPhoto,e.qrPhoto,e.purchasePaymentProof,e.paymentProof].concat((e.payments||[]).map(p=>p.proof)).includes(url));
    if (!linked || !canViewExpense(req,linked)) return res.status(403).end();
  }
  const fp = path.join(PROOF_DIR, name);
  if (!fp.startsWith(PROOF_DIR) || !fs.existsSync(fp)) return res.status(404).end();
  res.sendFile(fp);
});

// ── Config for the entry form ────────────────────────────────────
router.get('/api/expenses/config', (req, res) => {
  const s = loadStore();
  const allowed = submissionNatures(req);
  const pendingReqs = (s.requests || []).filter(r => r.status === 'pending' && approvalNatures(req).includes(normalizedNature(r.nature)));
  const visiblePendingReqs = isAdmin(req) ? pendingReqs : (canApprove(req) ? pendingReqs.filter(r => r.kind === 'vendor') : []);
  const vendorsByNature = { SANKI: Object.values(s.vendors).map(v => v.name), SAMAST: Object.values(((s.vendorsByNature || {}).SAMAST) || {}).map(v => v.name), PERSONAL: Object.values(((s.vendorsByNature || {}).PERSONAL) || {}).map(v => v.name) };
  // Only approved master vendors are reusable. A name typed by a claimant is
  // promoted into this list only when the related expense is approved.
  Object.keys(vendorsByNature).forEach(n => vendorsByNature[n].sort((a, b) => a.localeCompare(b)));
  res.json({
    success: true,
    ledgers: pickableLedgers(s),
    vendors: vendorsByNature.SANKI,
    vendorsByNature,
    accounts: Array.from(new Set([].concat(...Object.values(ENTITY_ACCOUNTS)))), accountsByNature: ENTITY_ACCOUNTS, personalAccounts: personalAccountsForReq(req), people: Array.from(new Set([].concat(s.people||[],Object.values(s.expenses||{}).map(e=>e.createdBy||e.claimant).filter(Boolean)))).sort((a,b)=>a.localeCompare(b)),
    types: TYPES, natures: allowed, channels: CHANNELS,
    approvalNatures: approvalNatures(req),
    bills: BILLS.filter(b => b !== 'none'), paymentTypes: PAYMENT_TYPES,
    isAdmin: isAdmin(req),
    isOwner: rolesOfReq(req).includes('owner'),
    canApprove: canApprove(req),
    me: (req.user && req.user.username) || '',
    pendingCount: visiblePendingReqs.length
  });
});

// ── Create (starts PENDING, logged by a runner) ──────────────────
router.post('/api/expenses', (req, res) => {
  const b = req.body || {};
  const s = loadStore();
  // Only Admin/Owner classify expenses. Every other submitter gets the simple
  // claimant form and Admin/Owner assigns the category during review.
  const ledger = isAdmin(req) ? String(b.ledger || '').trim() : '';
  const amount = num(b.amount);
  if (isAdmin(req) && !ledger) return res.status(400).json({ success: false, error: 'Pick a category (ledger).' });
  if (ledger && !pickableLedgers(s).some(l => l.name.toLowerCase() === ledger.toLowerCase())) {
    return res.status(400).json({ success: false, error: 'Select an approved category.' });
  }
  if (!(amount > 0)) return res.status(400).json({ success: false, error: 'Amount must be greater than 0.' });

  const nature = normalizedNature(b.nature);
  if (!submissionNatures(req).includes(nature)) return res.status(403).json({ success: false, error: 'You do not have access to this accounting entity.' });
  // Claimants never classify accounting type: every submission starts as
  // Variable. An approver may reclassify it while reviewing the pending row.
  const type = isAdmin(req) && TYPES.includes(b.type) ? b.type : 'variable';
  const channel = isAdmin(req) && CHANNELS.includes(b.channel) ? b.channel : 'Shared';
  const bill = ['printed', 'handwritten'].includes(b.bill) ? b.bill : 'printed';
  const paymentType = PAYMENT_TYPES.includes(b.paymentType) ? b.paymentType : 'UPI';
  const billPhoto = String(b.billPhoto || '').trim();
  const qrPhoto = String(b.qrPhoto || '').trim();
  const paidAlready = b.paidAlready === true || b.paidAlready === 'true';
  const personalPaymentProof = String(b.personalPaymentProof || '').trim();
  if (!billPhoto) {
    return res.status(400).json({ success: false, error: 'Bill photo is required before an expense can be submitted.' });
  }
  if (paidAlready && !personalPaymentProof) {
    return res.status(400).json({ success: false, error: 'Upload proof of the payment you already made.' });
  }
  if (!paidAlready && paymentType === 'UPI' && !qrPhoto) {
    return res.status(400).json({ success: false, error: 'Vendor QR-code photo is required for UPI payment.' });
  }
  const claimant = (req.user && req.user.username) || 'system';
  const personalAccount = paymentType === 'Cash' ? claimant + ' Cash' : String(b.personalAccount || '').trim();
  if (paidAlready && paymentType !== 'Cash' && !personalAccount) {
    return res.status(400).json({ success: false, error: 'Enter the account used for your personal payment.' });
  }
  if (paidAlready && paymentType !== 'Cash' && !isAdmin(req) && !personalAccountsForReq(req).some(a => a.toLowerCase() === personalAccount.toLowerCase())) {
    return res.status(400).json({ success:false, error:'Select your assigned personal payment account.' });
  }

  const vendor = String(b.vendor || '').trim();
  if (!vendor) return res.status(400).json({ success: false, error: 'Vendor name is required.' });
  const isInstallment = !!b.isInstallment;
  const requestedAmount = isInstallment ? num(b.requestedAmount) : amount;
  if (isInstallment && (!(requestedAmount > 0) || requestedAmount > amount)) {
    return res.status(400).json({ success: false, error: 'Initial requested payment must be greater than 0 and cannot exceed the total agreed amount.' });
  }

  s.seq = (s.seq || 0) + 1;
  const id = 'EX-' + String(s.seq).padStart(5, '0');
  const now = new Date().toISOString();
  s.expenses[id] = {
    id,
    date: (b.date || now.slice(0, 10)).toString().slice(0, 10),
    particulars: String(b.particulars || '').trim(),
    amount,
    isInstallment,
    requestedAmount,
    nature, type, ledger, vendor,
    claimant,                                     // who did the errand (was "runner")
    account: '',                                  // selected by approver when payment is made
    channel, bill, fundedBy: paidAlready ? 'claimant' : 'company', paymentType,
    qrPhoto: !paidAlready && (paymentType === 'UPI' || paymentType === 'Credit') ? qrPhoto : '',
    billPhoto,                                    // normal printed/handwritten bill
    purchasePaymentProof: paidAlready ? personalPaymentProof : '', exceptionEvidence: '', exceptionReason: '', billNote: '',
    paidAlready,
    personalPaidAmount: paidAlready ? requestedAmount : 0,
    reimbursementStatus: paidAlready ? 'awaiting_approval' : 'not_applicable',
    reimbursementAmount: 0,
    reimbursementPayments: [],
    paymentProof: '',                             // company payment/reimbursement proof
    status: 'pending',                            // pending → approved → paid
    paidAmount: paidAlready ? requestedAmount : 0,
    payments: paidAlready ? [{
      id: 'PAY-001', amount: requestedAmount, date: String(b.date || now.slice(0, 10)).slice(0, 10),
      account: personalAccount, paymentType, proof: personalPaymentProof,
      note: String(b.paymentNote || 'Paid personally by submitter').trim(),
      paidBy: claimant, paidAt: now, personalFunds: true
    }] : [],
    createdAt: now,
    createdBy: (req.user && req.user.username) || 'system',
    approvedAt: null, approvedBy: null,
    paidAt: null, paidBy: null
  };
  audit(s,req,'CREATED','expense',id,{nature,after:s.expenses[id]});
  saveStore(s);
  notifyApproversNewExpense(s.expenses[id]);
  res.json({ success: true, expense: s.expenses[id] });
});

// ── Edit ─────────────────────────────────────────────────────────
// Single-segment POST paths that have their OWN handlers registered after this
// param route — the ':id' pattern would otherwise swallow them. Fall through.
const RESERVED_POST = new Set(['requests', 'accounts', 'settings', 'balances', 'transfers', 'receipts', 'receivables', 'vendors', 'custom-ledgers', 'upload']);
router.post('/api/expenses/:id', (req, res, next) => {
  if (RESERVED_POST.has(req.params.id)) return next();
  const s = loadStore();
  const e = s.expenses[req.params.id];
  if (!e) return res.status(404).json({ success: false, error: 'Not found.' });
  if (!canViewExpense(req, e)) return res.status(403).json({ success: false, error: 'You do not have access to this accounting entity.' });
  if (!canApprove(req) && (e.createdBy !== (req.user && req.user.username) || e.status !== 'pending')) {
    return res.status(403).json({ success: false, error: 'You can only edit your own pending expenses.' });
  }
  const b = req.body || {};
  const finalized = e.status !== 'pending';
  if (finalized && !isAdmin(req)) return res.status(403).json({ success:false, error:'Only Owner or Admin can edit an approved or paid expense.' });
  const editReason = String(b.editReason || '').trim();
  if (finalized && !editReason) return res.status(400).json({ success:false, error:'Reason for editing an approved or paid expense is required.' });
  const beforeEdit = JSON.parse(JSON.stringify(e));
  if (b.date != null) e.date = String(b.date).slice(0, 10);
  if (b.particulars != null) e.particulars = String(b.particulars).trim();
  if (b.amount != null) {
    const nextAmount = num(b.amount);
    if (!(nextAmount > 0)) return res.status(400).json({ success:false, error:'Expense amount must be greater than 0.' });
    if (nextAmount < num(e.paidAmount)) return res.status(400).json({ success:false, error:'Expense amount cannot be lower than ₹'+round0(e.paidAmount)+' already paid.' });
    e.amount = nextAmount;
  }
  if (b.isInstallment != null) e.isInstallment = b.isInstallment === true || b.isInstallment === 'true';
  if (b.requestedAmount != null) {
    const requested = num(b.requestedAmount);
    if (!(requested > 0) || requested > e.amount) return res.status(400).json({ success: false, error: 'Requested payment must be greater than 0 and cannot exceed the total amount.' });
    if (e.paidAlready && requested < num(e.personalPaidAmount)) return res.status(400).json({ success: false, error: 'Requested payment cannot be less than the amount already paid personally.' });
    e.requestedAmount = requested;
  }
  if (b.nature != null) {
    const requestedNature = String(b.nature || '').toUpperCase();
    if (!NATURES.includes(requestedNature)) return res.status(400).json({ success:false, error:'Select a valid accounting entity.' });
    const nextNature = normalizedNature(requestedNature);
    if (nextNature !== normalizedNature(e.nature)) {
      if (!isAdmin(req)) return res.status(403).json({ success:false, error:'Only Owner or Admin can change the accounting entity.' });
      if (!approvalNatures(req).includes(nextNature)) return res.status(403).json({ success:false, error:'You cannot move this expense to that accounting entity.' });
      const hasCompanyPayments = (e.payments || []).some(p => !p.personalFunds);
      const hasReimbursements = (e.reimbursementPayments || []).length > 0 || num(e.reimbursementAmount) > 0;
      if (hasCompanyPayments || hasReimbursements) return res.status(400).json({ success:false, error:'Entity cannot be changed after a company payment or reimbursement has been recorded.' });
      e.nature = nextNature;
    }
  }
  if (TYPES.includes(b.type)) e.type = b.type;
  if (b.ledger != null && String(b.ledger).trim()) {
    if (!isAdmin(req)) return res.status(403).json({ success: false, error: 'Only Admin or Owner can assign or change the category.' });
    const ledger = String(b.ledger).trim();
    if (!pickableLedgers(s).some(l => l.name.toLowerCase() === ledger.toLowerCase())) {
      if (!isAdmin(req)) return res.status(400).json({ success: false, error: 'Only Admin or Owner can add a missing category during approval.' });
      s.customLedgers = s.customLedgers || {};
      s.customLedgers[ledger] = { name: ledger, type: TYPES.includes(b.type) ? b.type : (e.type || 'variable') };
    }
    e.ledger = ledger;
  }
  if (canApprove(req) && CHANNELS.includes(b.channel)) e.channel = b.channel;
  if (PAYMENT_TYPES.includes(b.paymentType)) e.paymentType = b.paymentType;
  if (b.qrPhoto != null) e.qrPhoto = String(b.qrPhoto).trim();
  if (BILLS.includes(b.bill)) e.bill = b.bill;
  // Claimant identity is immutable: it always comes from the authenticated creator.
  if (b.account != null) e.account = String(b.account).trim();
  if (b.billPhoto != null) e.billPhoto = String(b.billPhoto).trim();
  if (b.billNote != null) e.billNote = String(b.billNote).trim();
  if (PAID_BY.includes(b.fundedBy)) e.fundedBy = b.fundedBy;
  if (b.purchasePaymentProof != null) e.purchasePaymentProof = String(b.purchasePaymentProof).trim();
  if (b.exceptionEvidence != null) e.exceptionEvidence = String(b.exceptionEvidence).trim();
  if (b.exceptionReason != null) { e.exceptionReason = String(b.exceptionReason).trim(); e.billNote = e.exceptionReason; }
  if (b.vendor != null) {
    const vendor = String(b.vendor).trim();
    if (!vendor) return res.status(400).json({ success: false, error: 'Vendor name is required.' });
    e.vendor = vendor;
  }
  const wasPaidAlready = !!e.paidAlready;
  if (b.paidAlready != null && canApprove(req)) {
    const nextPaidAlready = b.paidAlready === true || b.paidAlready === 'true';
    const hasCompanyPayments = (e.payments || []).some(p => !p.personalFunds);
    const hasReimbursements = (e.reimbursementPayments || []).length > 0 || num(e.reimbursementAmount) > 0;
    if (nextPaidAlready !== wasPaidAlready && (hasCompanyPayments || hasReimbursements)) {
      return res.status(400).json({ success:false, error:'Payment source cannot be changed after a company payment or reimbursement has been recorded.' });
    }
    e.paidAlready = nextPaidAlready;
  }
  if (e.paidAlready) {
    const personalAccount = e.paymentType === 'Cash' ? (e.claimant || e.createdBy) + ' Cash' : String(b.personalAccount != null ? b.personalAccount : (((e.payments || [])[0] || {}).account || '')).trim();
    const personalProof = String(b.personalPaymentProof != null ? b.personalPaymentProof : (e.purchasePaymentProof || '')).trim();
    if (!personalProof) return res.status(400).json({ success:false, error:'Personal payment proof is required.' });
    if (e.paymentType !== 'Cash' && !personalAccount) return res.status(400).json({ success:false, error:'Enter the account used for the personal payment.' });
    e.purchasePaymentProof = personalProof;
    e.payments = Array.isArray(e.payments) ? e.payments : [];
    if (!e.payments.some(p => p.personalFunds)) e.payments.unshift({ id:'PAY-001', amount:num(e.requestedAmount || e.amount), date:e.date, paidBy:e.claimant || e.createdBy, paidAt:new Date().toISOString(), personalFunds:true });
    const personalPayment = e.payments.find(p => p.personalFunds);
    if (personalPayment && (personalPayment.personalFunds || e.status === 'pending')) {
      personalPayment.account = personalAccount; personalPayment.paymentType = e.paymentType; personalPayment.proof = personalProof; personalPayment.amount = num(e.requestedAmount || e.amount);
    }
    e.fundedBy='claimant';e.personalPaidAmount=num(e.requestedAmount || e.amount);e.paidAmount=e.personalPaidAmount;e.reimbursementStatus=normalizedNature(e.nature)==='PERSONAL'?'not_applicable':(e.status==='pending'?'awaiting_approval':'pending');
  } else if (wasPaidAlready) {
    e.payments=(e.payments||[]).filter(p=>!p.personalFunds);e.fundedBy='company';e.personalPaidAmount=0;e.paidAmount=0;e.reimbursementStatus='not_applicable';e.purchasePaymentProof='';
    e.reimbursementAmount=0;e.reimbursementPayments=[];e.vendorPaymentCompleted=false;
    if (e.status !== 'pending') e.status='approved';
  }
  if (!e.billPhoto) return res.status(400).json({ success:false, error:'Bill photo is required.' });
  if (!e.paidAlready && e.paymentType === 'UPI' && !e.qrPhoto) {
    return res.status(400).json({ success: false, error: 'Vendor QR-code photo is required for UPI payment.' });
  }
  if (e.paymentType === 'Cash') e.qrPhoto = '';
  if (!e.paidAlready && finalized && e.status !== 'rejected') e.status = num(e.paidAmount) >= num(e.amount) ? 'paid' : (num(e.paidAmount) > 0 ? 'partially_paid' : 'approved');
  const tracked = ['nature','date','particulars','amount','isInstallment','requestedAmount','type','ledger','channel','paymentType','qrPhoto','bill','account','billPhoto','billNote','fundedBy','purchasePaymentProof','vendor','paidAlready','personalPaidAmount','paidAmount','reimbursementStatus'];
  const changes = tracked.filter(k => JSON.stringify(beforeEdit[k]) !== JSON.stringify(e[k])).map(k => ({ field:k, before:beforeEdit[k] == null ? '' : beforeEdit[k], after:e[k] == null ? '' : e[k] }));
  if (changes.length) {
    e.auditHistory = Array.isArray(e.auditHistory) ? e.auditHistory : [];
    e.auditHistory.push({ id:'EDIT-'+String(e.auditHistory.length+1).padStart(3,'0'), reason:editReason || 'Pending expense correction', changes, editedBy:(req.user&&req.user.username)||'system', editedAt:new Date().toISOString() });
    if(e.telegramNeedsReview)e.telegramNeedsReview=false;
    audit(s,req,'EDITED','expense',e.id,{nature:e.nature,before:beforeEdit,after:e,note:editReason||'Pending expense correction'});
  }
  saveStore(s);
  res.json({ success: true, expense: e });
});

// ── Approve (GATE 1: bill, or admin-only claimant-paid exception) ─
router.post('/api/expenses/:id/approve', (req, res) => {
  if (!canApprove(req)) return res.status(403).json({ success: false, error: 'Only accounting/admin can approve.' });
  const s = loadStore();
  const e = s.expenses[req.params.id];
  if (!e) return res.status(404).json({ success: false, error: 'Not found.' });
  if (!canApproveExpenseNature(req, e)) return res.status(403).json({ success: false, error: 'You cannot approve this accounting entity.' });
  if (e.status === 'approved' || e.status === 'partially_paid' || e.status === 'paid') {
    // Un-approve (only if not yet paid).
    if (e.status === 'paid' || e.status === 'partially_paid') return res.status(400).json({ success: false, error: 'Payments already recorded — cannot un-approve.' });
    e.status = 'pending'; e.approvedAt = null; e.approvedBy = null;
    saveStore(s);
    return res.json({ success: true, expense: e });
  }
  const isNoBillException = e.bill === 'none';
  if (isNoBillException && !isAdmin(req)) {
    return res.status(403).json({ success: false, error: 'Only an admin can approve a no-bill exception.' });
  }
  if (isNoBillException && (e.fundedBy !== 'claimant' || !e.purchasePaymentProof || !e.exceptionEvidence || !(e.exceptionReason || e.billNote))) {
    return res.status(400).json({ success: false, error: 'Complete seller-payment proof, alternative evidence, and exception reason before approval.' });
  }
  if (!isNoBillException && !e.billPhoto) {
    return res.status(400).json({ success: false, error: 'Bill photo required to approve.' });
  }
  if (!e.vendor) return res.status(400).json({ success: false, error: 'Vendor name required before approval.' });
  if (!e.ledger) return res.status(400).json({ success: false, error: 'Admin or Owner must assign a category before approval.' });
  // A claimant may type a new vendor directly. Approval confirms the corrected
  // name and promotes it into the reusable vendor list.
  if (normalizedNature(e.nature) === 'SANKI') {
    if (!s.vendors[e.vendor.toLowerCase()]) s.vendors[e.vendor.toLowerCase()] = { name: e.vendor, notes: '' };
  } else {
    const nature = normalizedNature(e.nature);
    s.vendorsByNature = s.vendorsByNature || {}; s.vendorsByNature[nature] = s.vendorsByNature[nature] || {};
    if (!s.vendorsByNature[nature][e.vendor.toLowerCase()]) s.vendorsByNature[nature][e.vendor.toLowerCase()] = { name: e.vendor, notes: '' };
  }
  const totalDue = num(e.amount);
  e.status = e.paidAmount >= totalDue ? 'paid' : (e.paidAmount > 0 ? 'partially_paid' : 'approved');
  if (e.paidAlready) { e.reimbursementStatus = 'pending'; e.vendorPaymentCompleted = e.paidAmount >= totalDue; }
  e.approvedAt = new Date().toISOString();
  e.approvedBy = (req.user && req.user.username) || 'admin';
  audit(s,req,'APPROVED','expense',e.id,{nature:e.nature,after:{status:e.status,approvedBy:e.approvedBy,amount:e.amount}});
  saveStore(s);
  notifyExpenseUser(e, 'approved');
  res.json({ success: true, expense: e });
});

// Other approved balances that can be settled with the same vendor payment.
router.get('/api/expenses/:id/payment-candidates', (req, res) => {
  if (!canApprove(req)) return res.status(403).json({ success:false, error:'Only accounting/admin can view payment candidates.' });
  const s = loadStore(), source = s.expenses[req.params.id];
  if (!source) return res.status(404).json({ success:false, error:'Not found.' });
  if (!canApproveExpenseNature(req, source)) return res.status(403).json({ success:false, error:'You cannot pay this accounting entity.' });
  const nature = normalizedNature(source.nature), vendor = vendorKey(source.vendor);
  const expenses = Object.values(s.expenses).filter(e => {
    if (e.id === source.id || e.paidAlready || !['approved','partially_paid'].includes(e.status)) return false;
    return normalizedNature(e.nature) === nature && vendorKey(e.vendor) === vendor && num(e.paidAmount) < num(e.amount);
  }).map(e => ({ id:e.id, date:e.date, vendor:e.vendor, particulars:e.particulars, amount:round0(e.amount), paidAmount:round0(e.paidAmount), balanceDue:round0(Math.max(0,num(e.amount)-num(e.paidAmount))) }))
    .sort((a,b) => String(b.date+b.id).localeCompare(String(a.date+a.id)));
  res.json({ success:true, source:{ id:source.id, nature, vendor:source.vendor }, expenses });
});

// One bank/cash transaction may settle several approved bills for the same
// entity and vendor. Each allocation is stored on its original expense so all
// existing ledgers and dashboards remain correct.
router.post('/api/expenses/batch-pay', (req, res) => {
  if (!canApprove(req)) return res.status(403).json({ success:false, error:'Only accounting/admin can pay.' });
  const s = loadStore(), b = req.body || {};
  const ids = [...new Set((Array.isArray(b.expenseIds) ? b.expenseIds : []).map(String))];
  if (!ids.length) return res.status(400).json({ success:false, error:'Select at least one expense.' });
  const expenses = ids.map(id => s.expenses[id]);
  if (expenses.some(e => !e)) return res.status(404).json({ success:false, error:'One or more selected expenses no longer exist.' });
  const first = expenses[0], nature = normalizedNature(first.nature), vendor = vendorKey(first.vendor);
  if (expenses.some(e => !canApproveExpenseNature(req,e))) return res.status(403).json({ success:false, error:'You cannot pay one of the selected accounting entities.' });
  if (expenses.some(e => normalizedNature(e.nature)!==nature || vendorKey(e.vendor)!==vendor)) return res.status(400).json({ success:false, error:'Combined payments must use the same entity and vendor.' });
  if (expenses.some(e => e.paidAlready || !['approved','partially_paid'].includes(e.status) || num(e.paidAmount)>=num(e.amount))) return res.status(400).json({ success:false, error:'Every selected expense must be an approved unpaid vendor balance.' });
  const proof = String(b.paymentProof || '').trim();
  if (!proof) return res.status(400).json({ success:false, error:'Payment screenshot required — no proof, no payment.' });
  const account = allowedCompanyAccount(s, nature, String(b.account || '').trim());
  if (!account) return res.status(400).json({ success:false, error:'Select a paying account assigned to this accounting entity.' });
  const reconIssues = reconciliationIssues(s, nature, account), overrideReason = String(b.reconciliationOverrideReason || '').trim();
  if (reconIssues.length && !overrideReason) return res.status(409).json({ success:false, requiresOverride:true, issues:reconIssues, error:'This account has an unresolved reconciliation warning. Enter an urgent-payment override reason to continue.' });
  const combinedOutstanding = round0(expenses.reduce((n,e)=>n+Math.max(0,num(e.amount)-num(e.paidAmount)),0));
  const requestedTotal = b.amount != null ? round0(num(b.amount)) : combinedOutstanding;
  if (!(requestedTotal > 0)) return res.status(400).json({ success:false, error:'Payment amount must be greater than 0.' });
  if (requestedTotal > combinedOutstanding) return res.status(400).json({ success:false, error:'Payment cannot exceed the combined outstanding amount of ₹'+combinedOutstanding+'.' });
  const date = String(b.date || new Date().toISOString().slice(0,10)).slice(0,10), paidBy = (req.user&&req.user.username)||'admin';
  const batchPaymentId = 'BPAY-' + Date.now().toString(36).toUpperCase();
  let remaining = requestedTotal; const allocations = [];
  // Allocate oldest bills first. This is predictable for recurring expenses and
  // leaves the newest selected bill partially paid when the payment is short.
  expenses.slice().sort((a,b)=>String((a.date||'')+a.id).localeCompare(String((b.date||'')+b.id))).forEach(e => {
    const outstanding = round0(Math.max(0,num(e.amount)-num(e.paidAmount))), amount = Math.min(outstanding,remaining);
    if (!(amount > 0)) return;
    remaining=round0(remaining-amount); allocations.push({ expense:e, amount });
    e.account=account; e.paymentProof=proof; e.payments=Array.isArray(e.payments)?e.payments:[];
    e.payments.push({ id:'PAY-'+String(e.payments.length+1).padStart(3,'0'), batchPaymentId, batchTotal:requestedTotal, amount, date, account,
      paymentType:PAYMENT_TYPES.includes(b.paymentType)?b.paymentType:(e.paymentType||''), proof, note:String(b.note||'').trim(),
      paidBy, paidAt:new Date().toISOString(), reconciliationOverrideReason:overrideReason, reconciliationIssuesAtPayment:reconIssues });
    e.paidAmount=round0(num(e.paidAmount)+amount); e.status=e.paidAmount>=num(e.amount)?'paid':'partially_paid'; e.paidAt=new Date().toISOString(); e.paidBy=paidBy;
  });
  const allocatedIds=allocations.map(x=>x.expense.id);
  allocations.forEach(x=>{x.expense.payments.at(-1).linkedExpenseIds=allocatedIds;});
  allocations.forEach(x=>audit(s,req,'PAYMENT_ALLOCATED','expense',x.expense.id,{nature:x.expense.nature,account,paymentId:x.expense.payments.at(-1).id,after:{batchPaymentId,amount:x.amount,linkedExpenseIds:allocatedIds,status:x.expense.status}}));
  saveStore(s);
  allocations.forEach(x => notifyExpenseUser(x.expense,x.expense.status==='paid'?'paid':'partially_paid',x.amount));
  res.json({ success:true, batchPaymentId, total:requestedTotal, combinedOutstanding, allocations:allocations.map(x=>({expenseId:x.expense.id,amount:x.amount,status:x.expense.status,balanceDue:round0(Math.max(0,num(x.expense.amount)-num(x.expense.paidAmount)))})), expenses });
});

// ── Pay (GATE 2: payment screenshot required) ────────────────────
router.post('/api/expenses/:id/pay', (req, res) => {
  if (!canApprove(req)) return res.status(403).json({ success: false, error: 'Only accounting/admin can pay.' });
  const s = loadStore();
  const e = s.expenses[req.params.id];
  if (!e) return res.status(404).json({ success: false, error: 'Not found.' });
  if (!canApproveExpenseNature(req, e)) return res.status(403).json({ success: false, error: 'You cannot pay this accounting entity.' });
  if (e.status === 'pending') return res.status(400).json({ success: false, error: 'Approve it before paying.' });
  if (e.status === 'paid') return res.status(400).json({ success: false, error: 'This expense is already fully paid.' });
  const b = req.body || {};
  const proof = String(b.paymentProof || e.paymentProof || '').trim();
  if (!proof) return res.status(400).json({ success: false, error: e.fundedBy === 'claimant' ? 'Reimbursement proof required — the claimant cannot be marked reimbursed without it.' : 'Payment screenshot required — no proof, no payment.' });
  const account = String(b.account || '').trim();
  if (!account) return res.status(400).json({ success: false, error: 'Select the account used for this payment.' });
  const allowedAccount = allowedCompanyAccount(s, e.nature, account);
  if (!allowedAccount) return res.status(400).json({ success: false, error: 'Select a paying account assigned to this accounting entity.' });
  const reconIssues = reconciliationIssues(s, normalizedNature(e.nature), allowedAccount);
  const overrideReason = String(b.reconciliationOverrideReason || '').trim();
  if (reconIssues.length && !overrideReason) return res.status(409).json({ success:false, requiresOverride:true, issues:reconIssues, error:'This account has an unresolved reconciliation warning. Enter an urgent-payment override reason to continue.' });
  e.account = allowedAccount;
  // A contract may be larger than the installment approved now. Payment is
  // capped at the current installment, while the contract balance remains
  // visible for reference.
  const approvedNow = num(e.amount);
  const outstanding = Math.max(0, approvedNow - num(e.paidAmount));
  const pay = b.amount != null ? num(b.amount) : outstanding;
  if (!(pay > 0)) return res.status(400).json({ success: false, error: 'Payment amount must be greater than 0.' });
  if (pay > outstanding) return res.status(400).json({ success: false, error: 'Payment cannot exceed the outstanding amount of ₹' + round0(outstanding) + '.' });
  e.paidAmount = num(e.paidAmount) + pay;
  e.paymentProof = proof;
  e.payments = Array.isArray(e.payments) ? e.payments : [];
  e.payments.push({
    id: 'PAY-' + String(e.payments.length + 1).padStart(3, '0'),
    amount: pay,
    date: String(b.date || new Date().toISOString().slice(0, 10)).slice(0, 10),
    account: e.account || '',
    paymentType: PAYMENT_TYPES.includes(b.paymentType) ? b.paymentType : (e.paymentType || ''),
    proof,
    note: String(b.note || '').trim(),
    paidBy: (req.user && req.user.username) || 'admin',
    paidAt: new Date().toISOString(), reconciliationOverrideReason: overrideReason,
    reconciliationIssuesAtPayment: reconIssues
  });
  e.status = e.paidAmount >= approvedNow ? 'paid' : 'partially_paid';
  e.paidAt = new Date().toISOString();
  e.paidBy = (req.user && req.user.username) || 'admin';
  audit(s,req,'PAYMENT_RECORDED','expense',e.id,{nature:e.nature,account:e.account,paymentId:e.payments.at(-1).id,after:e.payments.at(-1)});
  saveStore(s);
  notifyExpenseUser(e, e.status === 'paid' ? 'paid' : 'partially_paid', pay);
  res.json({ success: true, expense: e });
});

// Reimbursing the submitter clears a liability; it never increases vendor paid
// or P&L expense, so the original expense is not counted twice.
router.post('/api/expenses/:id/reimburse', (req, res) => {
  if (!canApprove(req)) return res.status(403).json({ success: false, error: 'Only accounting/admin can reimburse.' });
  const s = loadStore(); const e = s.expenses[req.params.id]; const b = req.body || {};
  if (!e) return res.status(404).json({ success: false, error: 'Not found.' });
  if (!canApproveExpenseNature(req, e)) return res.status(403).json({ success: false, error: 'You cannot reimburse this accounting entity.' });
  if (e.reimbursementStatus !== 'pending' && e.reimbursementStatus !== 'partially_reimbursed') {
    return res.status(400).json({ success: false, error: 'This expense has no approved reimbursement pending.' });
  }
  const proof = String(b.paymentProof || '').trim();
  if (!proof) return res.status(400).json({ success: false, error: 'Reimbursement payment proof is required.' });
  const due = Math.max(0, num(e.personalPaidAmount) - num(e.reimbursementAmount));
  const amount = b.amount != null ? num(b.amount) : due;
  if (!(amount > 0) || amount > due) return res.status(400).json({ success: false, error: 'Reimbursement must be greater than 0 and cannot exceed ₹' + round0(due) + '.' });
  const reimbursementAccount = allowedCompanyAccount(s, e.nature, b.account);
  if (!reimbursementAccount) return res.status(400).json({ success:false, error:'Select a reimbursement account assigned to this accounting entity.' });
  e.reimbursementAmount = num(e.reimbursementAmount) + amount;
  e.reimbursementPayments = Array.isArray(e.reimbursementPayments) ? e.reimbursementPayments : [];
  e.reimbursementPayments.push({
    id: 'REIM-' + String(e.reimbursementPayments.length + 1).padStart(3, '0'), amount,
    date: String(b.date || new Date().toISOString().slice(0, 10)).slice(0, 10),
    account: reimbursementAccount, paymentType: PAYMENT_TYPES.includes(b.paymentType) ? b.paymentType : 'UPI',
    proof, note: String(b.note || '').trim(), paidBy: (req.user && req.user.username) || 'admin', paidAt: new Date().toISOString()
  });
  e.reimbursementStatus = e.reimbursementAmount >= e.personalPaidAmount ? 'reimbursed' : 'partially_reimbursed';
  audit(s,req,'REIMBURSEMENT_RECORDED','expense',e.id,{nature:e.nature,account:reimbursementAccount,paymentId:e.reimbursementPayments.at(-1).id,after:e.reimbursementPayments.at(-1)});
  saveStore(s);
  notifyExpenseUser(e, e.reimbursementStatus, amount);
  res.json({ success: true, expense: e });
});

router.post('/api/expenses/:id/reject', (req, res) => {
  if (!canApprove(req)) return res.status(403).json({ success: false, error: 'Only accounting/admin can reject.' });
  const s = loadStore(); const e = s.expenses[req.params.id];
  if (!e) return res.status(404).json({ success: false, error: 'Not found.' });
  if (!canApproveExpenseNature(req, e)) return res.status(403).json({ success: false, error: 'You cannot reject this accounting entity.' });
  if (e.status !== 'pending') return res.status(400).json({ success: false, error: 'Only a pending expense can be rejected.' });
  e.status = 'rejected'; e.rejectReason = String((req.body || {}).reason || '').trim();
  audit(s,req,'REJECTED','expense',e.id,{nature:e.nature,after:{status:e.status,reason:e.rejectReason}});
  e.rejectedAt = new Date().toISOString(); e.rejectedBy = (req.user && req.user.username) || 'admin';
  if (e.paidAlready) e.reimbursementStatus = 'rejected';
  saveStore(s); notifyExpenseUser(e, 'rejected');
  res.json({ success: true, expense: e });
});

// ── Delete ───────────────────────────────────────────────────────
router.delete('/api/expenses/:id', (req, res) => {
  if (!canApprove(req)) return res.status(403).json({ success: false, error: 'Only accounting/admin can delete.' });
  const s = loadStore();
  if (!s.expenses[req.params.id]) return res.status(404).json({ success: false, error: 'Not found.' });
  if (!canApproveExpenseNature(req, s.expenses[req.params.id])) return res.status(403).json({ success: false, error: 'You cannot delete this accounting entity.' });
  const removed=JSON.parse(JSON.stringify(s.expenses[req.params.id]));
  delete s.expenses[req.params.id];
  audit(s,req,'DELETED','expense',req.params.id,{nature:removed.nature,before:removed});
  saveStore(s);
  res.json({ success: true });
});

// ── List with filters + totals ───────────────────────────────────
router.get('/api/expenses/list', (req, res) => {
  const s = loadStore();
  const id = (req.query.id || '').toString().trim();
  const from = (req.query.from || '').toString();
  const to = (req.query.to || '').toString();
  const status = (req.query.status || '').toString();
  const type = (req.query.type || '').toString();
  const vendor = (req.query.vendor || '').toString().toLowerCase();
  const claimant = (req.query.claimant || '').toString().toLowerCase();
  const paymentType = (req.query.paymentType || '').toString().toLowerCase();
  const missingBill = String(req.query.missingBill || '') === 'true';
  const nature = req.query.nature ? normalizedNature(req.query.nature) : '';
  if (nature && isAdmin(req) && !approvalNatures(req).includes(nature)) return res.status(403).json({ success:false, error:'You cannot view this accounting entity.' });
  let list = Object.values(s.expenses).filter(e => {
    if (!canViewExpense(req, e)) return false;
    if (id && e.id !== id) return false;
    if (nature && normalizedNature(e.nature) !== nature) return false;
    if (from && e.date < from) return false;
    if (to && e.date > to) return false;
    if (status && e.status !== status) return false;
    if (type && e.type !== type) return false;
    if (vendor && (e.vendor || '').toLowerCase() !== vendor) return false;
    if (claimant && String(e.createdBy || e.claimant || '').toLowerCase() !== claimant) return false;
    if (paymentType && String(e.paymentType || '').toLowerCase() !== paymentType) return false;
    if (missingBill && e.billPhoto) return false;
    return true;
  }).sort((a, b) => (b.date + b.id).localeCompare(a.date + a.id));

  const totals = { all: 0, pending: 0, approved: 0, paid: 0, noBill: 0, byType: {} };
  TYPES.forEach(t => { totals.byType[t] = 0; });
  list.forEach(e => {
    totals.all += e.amount;
    totals[e.status] = (totals[e.status] || 0) + e.amount;
    if (!e.billPhoto) totals.noBill += e.amount;
    if ((e.status === 'approved' || e.status === 'paid') && BUSINESS_NATURES.includes(normalizedNature(e.nature))) totals.byType[e.type] += e.amount;
  });
  const requestedLimit = Number(req.query.limit || 0);
  const limit = Number.isFinite(requestedLimit) && requestedLimit > 0 ? Math.min(200, Math.floor(requestedLimit)) : 0;
  const totalCount = list.length;
  if (limit) list = list.slice(0, limit);
  res.json({ success: true, expenses: list, totals, totalCount, hasMore: totalCount > list.length });
});

// Approved vendor bills that still require a full or partial company payment.
router.get('/api/expenses/pending-payments', (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ success: false, error: 'Owner/Admin only.' });
  const s = loadStore();
  const nature = req.query.nature ? normalizedNature(req.query.nature) : '';
  const vendor = String(req.query.vendor || '').trim().toLowerCase();
  const from = String(req.query.from || ''), to = String(req.query.to || ''), bucket = String(req.query.bucket || '');
  if (nature && !approvalNatures(req).includes(nature)) return res.status(403).json({ success: false, error: 'You cannot view this accounting entity.' });
  const today = new Date().toISOString().slice(0, 10);
  const expenses = Object.values(s.expenses || {}).filter(e => {
    if (!canApproveExpenseNature(req, e)) return false;
    if (nature && normalizedNature(e.nature) !== nature) return false;
    if (vendor && !String(e.vendor || '').toLowerCase().includes(vendor)) return false;
    if (from && String(e.date || e.approvedAt).slice(0, 10) < from) return false;
    if (to && String(e.date || e.approvedAt).slice(0, 10) > to) return false;
    const approvedNow = num(e.amount);
    if (!['approved', 'partially_paid'].includes(e.status) || num(e.paidAmount) >= approvedNow) return false;
    if (bucket === 'approved' && (num(e.paidAmount) > 0 || e.paymentType === 'Credit')) return false;
    if (bucket === 'partial' && num(e.paidAmount) <= 0) return false;
    if (bucket === 'credit' && e.paymentType !== 'Credit') return false;
    return true;
  }).map(e => ({
    ...e,
    balanceDue: round0(num(e.amount) - num(e.paidAmount)),
    contractBalance: round0(num(e.amount) - num(e.paidAmount)),
    daysPending: Math.max(0, Math.floor((Date.parse(today) - Date.parse(String(e.approvedAt || e.date).slice(0, 10))) / 86400000))
  })).sort((a, b) => b.daysPending - a.daysPending || String(a.approvedAt || '').localeCompare(String(b.approvedAt || '')));
  const purchases = (!nature || nature === 'SANKI') ? procurementPayables(s, false).filter(p => {
    if (vendor && !String(p.vendor).toLowerCase().includes(vendor) && !String(p.supplier).toLowerCase().includes(vendor)) return false;
    if (from && String(p.date || '') < from) return false;
    if (to && String(p.date || '') > to) return false;
    if (bucket === 'partial' && !(p.paidAmount > 0)) return false;
    if (bucket === 'credit') return false;
    if (bucket === 'approved' && p.paidAmount > 0) return false;
    return true;
  }) : [];
  res.json({ success: true, expenses, purchases, mediator: procurementAccounting(s).mediator, totalOutstanding: round0(expenses.reduce((n, e) => n + e.balanceDue, 0) + purchases.reduce((n, p) => n + p.balanceDue, 0)) });
});

// Advanced Purchases remains read-only here. Accounting owns only the mediator
// name and payment history keyed by PO, so inventory/Shopify data is untouched.
router.post('/api/expenses/procurement-payables/settings', (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ success: false, error: 'Admin/Owner only.' });
  const s = loadStore(), name = String((req.body || {}).mediator || '').trim();
  if (!name) return res.status(400).json({ success: false, error: 'Mediator name is required.' });
  const before=procurementAccounting(s).mediator;procurementAccounting(s).mediator = name;audit(s,req,'LEDGER_SETTING_CHANGED','procurement','mediator',{nature:'SANKI',before,after:name});saveStore(s);
  res.json({ success: true, mediator: name });
});
router.post('/api/expenses/procurement-payables/:id/pay', (req, res) => {
  if (!canApprove(req)) return res.status(403).json({ success: false, error: 'Only accounting/admin can pay.' });
  const s = loadStore(), b = req.body || {}, item = procurementPayables(s, true).find(x => x.id === req.params.id);
  if (!item) return res.status(404).json({ success: false, error: 'Purchase payable not found.' });
  const proof = String(b.paymentProof || '').trim(), account = String(b.account || '').trim(), pay = num(b.amount);
  if (!proof) return res.status(400).json({ success: false, error: 'Payment proof is required.' });
  const allowedAccount = allowedCompanyAccount(s, 'SANKI', account);
  if (!allowedAccount) return res.status(400).json({ success: false, error: 'Select a SANKI paying account.' });
  if (!(pay > 0) || pay > item.balanceDue) return res.status(400).json({ success: false, error: 'Payment must be greater than zero and cannot exceed ₹' + item.balanceDue + '.' });
  const cfg = procurementAccounting(s), state = cfg.paymentsByPo[item.id] || (cfg.paymentsByPo[item.id] = { payments: [] });
  state.payments = Array.isArray(state.payments) ? state.payments : [];
  state.payments.push({ id:'PPAY-'+String(state.payments.length+1).padStart(3,'0'), amount:pay, account:allowedAccount,
    date:String(b.date || new Date().toISOString().slice(0,10)).slice(0,10), paymentType:PAYMENT_TYPES.includes(b.paymentType)?b.paymentType:'UPI', proof,
    note:String(b.note || '').trim(), paidBy:(req.user&&req.user.username)||'admin', paidAt:new Date().toISOString() });
  audit(s,req,'PROCUREMENT_PAYMENT_RECORDED','procurement',item.id,{nature:'SANKI',account:allowedAccount,paymentId:state.payments.at(-1).id,after:state.payments.at(-1)});saveStore(s); res.json({ success:true, payable:procurementPayables(s, true).find(x=>x.id===item.id) });
});

// One-click spending: one row per actual payment, dated by the payment event.
router.get('/api/expenses/spending-dashboard', (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ success: false, error: 'Owner/Admin only.' });
  const s = loadStore(), from = String(req.query.from || ''), to = String(req.query.to || '');
  const nature = req.query.nature ? normalizedNature(req.query.nature) : '', accountFilter = String(req.query.account || '').trim().toLowerCase();
  if (nature && !approvalNatures(req).includes(nature)) return res.status(403).json({ success: false, error: 'You cannot view this accounting entity.' });
  const allowed = approvalNatures(req), inRange = d => (!from || d >= from) && (!to || d <= to);
  const payments = [];
  Object.values(s.expenses || {}).forEach(e => {
    const entity = normalizedNature(e.nature);
    if (!allowed.includes(entity) || (nature && entity !== nature)) return;
    (e.payments || []).filter(p => e.approvedAt && inRange(String(p.date || ''))).forEach(p => {
      const account = p.account || e.account;
      if (!accountFilter || String(account).toLowerCase() === accountFilter) payments.push({ id:e.id, paymentId:p.id||'', date:p.date||'', entity, kind:p.personalFunds?'Paid personally':'Vendor payment', vendor:e.vendor||'', claimant:e.claimant||e.createdBy||'', particulars:e.particulars||'', category:e.ledger||'', type:e.type||'', expenseAmount:round0(e.amount), amount:round0(p.amount), account, paymentType:p.paymentType||e.paymentType||'', proof:p.proof||e.paymentProof||'', billPhoto:e.billPhoto||'', qrPhoto:e.qrPhoto||'', approvedAt:e.approvedAt||'', approvedBy:e.approvedBy||'', paidBy:p.paidBy||'', contractTotal:e.isInstallment?round0(e.amount):0, contractBalance:e.isInstallment?round0(Math.max(0,num(e.amount)-num(e.paidAmount))):0 });
    });
  });
  payments.sort((a,b)=>String(b.date+b.id+b.paymentId).localeCompare(String(a.date+a.id+a.paymentId)));
  res.json({ success:true, range:{from,to}, totalPaid:round0(payments.reduce((n,p)=>n+num(p.amount),0)), count:payments.length, payments, accounts:storedAccountNames(s) });
});

router.get('/api/expenses/reimbursements', (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ success: false, error: 'Owner/Admin only.' });
  const s = loadStore();
  const status = String(req.query.status || '');
  const person = String(req.query.person || '').toLowerCase();
  const todayOnly = String(req.query.today || '') === 'true';
  const today = new Date().toISOString().slice(0, 10);
  const nature = req.query.nature ? normalizedNature(req.query.nature) : '';
  const from=String(req.query.from||''),to=String(req.query.to||'');
  let list = Object.values(s.expenses || {}).filter(e => {
    if (!canApproveExpenseNature(req, e)) return false;
    if (nature && normalizedNature(e.nature) !== nature) return false;
    if (!e.paidAlready || e.reimbursementStatus === 'awaiting_approval' || e.reimbursementStatus === 'rejected') return false;
    if (status && e.reimbursementStatus !== status) return false;
    if (person && !String(e.createdBy || e.claimant || '').toLowerCase().includes(person)) return false;
    if (todayOnly && e.date !== today) return false;
    if (from && String(e.date||'') < from) return false;
    if (to && String(e.date||'') > to) return false;
    return true;
  });
  list.sort((a, b) => String(b.approvedAt || b.createdAt).localeCompare(String(a.approvedAt || a.createdAt)));
  res.json({
    success: true,
    reimbursements: list,
    pendingTotal: round0(list.filter(e => e.reimbursementStatus !== 'reimbursed')
      .reduce((n, e) => n + Math.max(0, num(e.personalPaidAmount) - num(e.reimbursementAmount)), 0))
  });
});

router.post('/api/expenses/receivables', (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ success:false, error:'Owner/Admin only.' });
  const s=loadStore(), b=req.body||{}, nature=normalizedNature(b.nature), amount=num(b.amount);
  if(!approvalNatures(req).includes(nature)) return res.status(403).json({success:false,error:'You cannot create a receivable for this accounting entity.'});
  const party=String(b.party||'').trim(), reason=String(b.reason||'').trim();
  if(!party) return res.status(400).json({success:false,error:'Party name is required.'});
  if(!reason) return res.status(400).json({success:false,error:'Reason is required.'});
  if(!(amount>0)) return res.status(400).json({success:false,error:'Receivable amount must be greater than 0.'});
  s.receivableSeq=(s.receivableSeq||0)+1; const now=new Date().toISOString(), id='RCV-'+String(s.receivableSeq).padStart(5,'0');
  const item={id,nature,party,reason,amount,receivedAmount:0,status:'open',date:String(b.date||now.slice(0,10)).slice(0,10),dueDate:String(b.dueDate||'').slice(0,10),proof:String(b.proof||'').trim(),collections:[],createdBy:(req.user&&req.user.username)||'admin',createdAt:now};
  s.receivables=s.receivables||{};s.receivables[id]=item;audit(s,req,'CREATED','receivable',id,{nature,after:item});saveStore(s);res.json({success:true,receivable:item});
});

router.get('/api/expenses/receivables', (req,res) => {
  if(!isAdmin(req)) return res.status(403).json({success:false,error:'Owner/Admin only.'});
  const s=loadStore(), nature=req.query.nature?normalizedNature(req.query.nature):'', status=String(req.query.status||''), party=String(req.query.party||'').toLowerCase(),from=String(req.query.from||''),to=String(req.query.to||'');
  if(nature&&!approvalNatures(req).includes(nature)) return res.status(403).json({success:false,error:'You cannot view this accounting entity.'});
  const list=Object.values(s.receivables||{}).filter(x=>approvalNatures(req).includes(normalizedNature(x.nature))&&(!nature||normalizedNature(x.nature)===nature)&&(!status||x.status===status)&&(!party||String(x.party).toLowerCase().includes(party))&&(!from||String(x.date||'')>=from)&&(!to||String(x.date||'')<=to)).sort((a,b)=>String(b.date+b.id).localeCompare(String(a.date+a.id)));
  res.json({success:true,receivables:list,totalDue:round0(list.reduce((n,x)=>n+Math.max(0,num(x.amount)-num(x.receivedAmount)),0)),totalReceived:round0(list.reduce((n,x)=>n+num(x.receivedAmount),0))});
});

router.post('/api/expenses/receivables/:id/receive', (req,res) => {
  if(!isAdmin(req)) return res.status(403).json({success:false,error:'Owner/Admin only.'});
  const s=loadStore(), x=(s.receivables||{})[req.params.id], b=req.body||{};
  if(!x) return res.status(404).json({success:false,error:'Receivable not found.'});
  if(!approvalNatures(req).includes(normalizedNature(x.nature))) return res.status(403).json({success:false,error:'You cannot collect this receivable.'});
  const due=Math.max(0,num(x.amount)-num(x.receivedAmount)), amount=num(b.amount), account=String(b.account||'').trim(), proof=String(b.proof||'').trim();
  if(!(amount>0)||amount>due) return res.status(400).json({success:false,error:'Collection must be greater than 0 and cannot exceed ₹'+round0(due)+'.'});
  const receivingAccount=allowedCompanyAccount(s,x.nature,account);
  if(!receivingAccount) return res.status(400).json({success:false,error:'Select a receiving account assigned to this accounting entity.'});
  if(!proof) return res.status(400).json({success:false,error:'Collection proof is required.'});
  x.collections=Array.isArray(x.collections)?x.collections:[];x.collections.push({id:'COL-'+String(x.collections.length+1).padStart(3,'0'),amount,date:String(b.date||new Date().toISOString().slice(0,10)).slice(0,10),account:receivingAccount,proof,note:String(b.note||'').trim(),receivedBy:(req.user&&req.user.username)||'admin',receivedAt:new Date().toISOString()});
  x.receivedAmount=num(x.receivedAmount)+amount;x.status=x.receivedAmount>=x.amount?'received':'partially_received';audit(s,req,'COLLECTION_RECORDED','receivable',x.id,{nature:x.nature,account:receivingAccount,paymentId:x.collections.at(-1).id,after:x.collections.at(-1)});saveStore(s);res.json({success:true,receivable:x});
});

// ── Vendor books (accounts payable per vendor) ───────────────────
router.get('/api/expenses/vendors', (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ success: false, error: 'Owner/Admin only.' });
  const s = loadStore();
  const nature = req.query.nature ? normalizedNature(req.query.nature) : '';
  if (nature && !approvalNatures(req).includes(nature)) return res.status(403).json({ success: false, error: 'You cannot view this accounting entity.' });
  const search=String(req.query.search||'').trim().toLowerCase(), category=String(req.query.category||'').trim().toLowerCase(), source=String(req.query.source||'expense'), from=String(req.query.from||''), to=String(req.query.to||'');
  const books = {};
  const natures=nature?[nature]:approvalNatures(req);
  natures.forEach(n=>{const master=n==='SANKI'?s.vendors:(((s.vendorsByNature||{})[n])||{});Object.values(master).forEach(v=>{const key=n+'|'+v.name.toLowerCase();books[key]={name:v.name,nature:n,billed:0,paid:0,outstanding:0,count:0,notes:v.notes||'',entries:[]};});});
  Object.values(s.expenses).forEach(e => {
    const n=normalizedNature(e.nature),key=n+'|'+String(e.vendor||'').toLowerCase();
    if (!natures.includes(n)||!e.vendor||!books[key]||!['approved','partially_paid','paid'].includes(e.status)) return;
    if(category&&!String(e.ledger||'').toLowerCase().includes(category))return;
    if(from&&String(e.date||'')<from)return;if(to&&String(e.date||'')>to)return;
    const b=books[key];b.billed+=e.amount;b.paid+=num(e.paidAmount);b.count+=1;b.entries.push(e);
  });
  if(source==='sourcing'&&(!nature||nature==='SANKI')){Object.keys(books).forEach(k=>delete books[k]);procurementPayables(s,true).filter(p=>(!from||String(p.date||'')>=from)&&(!to||String(p.date||'')<=to)).forEach(p=>{const key='SANKI|'+p.vendor.toLowerCase(),b=books[key]||(books[key]={name:p.vendor,nature:'SANKI',billed:0,paid:0,outstanding:0,count:0,notes:'Advanced Purchases mediator',entries:[]});b.billed+=p.amount;b.paid+=p.paidAmount;b.count+=1;b.entries.push(p);});}
  const list = Object.values(books).map(b => ({
    name:b.name,nature:b.nature,count:b.count,billed:round0(b.billed),paid:round0(b.paid),outstanding:round0(b.billed-b.paid),notes:b.notes,entries:b.entries.sort((a,b)=>String(b.date+b.id).localeCompare(String(a.date+a.id)))
  })).filter(b=>(!category||b.count>0)&&(!search||fuzzyIncludes(b.name,search)||b.entries.some(e=>fuzzyIncludes(e.particulars,search)||fuzzyIncludes(e.ledger,search)))).sort((a,b)=>a.name.localeCompare(b.name));
  res.json({ success: true, vendors: list, totalOutstanding: list.reduce((n, b) => n + b.outstanding, 0) });
});
router.post('/api/expenses/vendors', (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ success: false, error: 'Owner/Admin only.' });
  const s = loadStore();
  const nature = normalizedNature((req.body || {}).nature);
  if (!approvalNatures(req).includes(nature)) return res.status(403).json({ success: false, error: 'You cannot edit this accounting entity.' });
  const name = String((req.body || {}).name || '').trim();
  if (!name) return res.status(400).json({ success: false, error: 'Vendor name required.' });
  if (nature === 'SANKI') {
    if (!s.vendors[name.toLowerCase()]) s.vendors[name.toLowerCase()] = { name, notes: '' };
  } else {
    s.vendorsByNature = s.vendorsByNature || {}; s.vendorsByNature[nature] = s.vendorsByNature[nature] || {};
    if (!s.vendorsByNature[nature][name.toLowerCase()]) s.vendorsByNature[nature][name.toLowerCase()] = { name, notes: '' };
  }
  audit(s,req,'VENDOR_ADDED','vendor',name,{nature,after:{name}});
  saveStore(s);
  res.json({ success: true });
});

// ── Running cash balances per account ────────────────────────────
// balance = opening + adjustments + transfers in − transfers out − payments.
router.get('/api/expenses/balances', (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ success: false, error: 'Owner/Admin only.' });
  const s = loadStore();
  const nature = req.query.nature ? normalizedNature(req.query.nature) : approvalNatures(req)[0];
  const from=String(req.query.from||''),to=String(req.query.to||'');
  if (!approvalNatures(req).includes(nature)) return res.status(403).json({ success: false, error: 'You cannot view this accounting entity.' });
  function totalsFor(inRange) {
    const paidOut = {}, collected = {}, adj = {}, transferIn = {}, transferOut = {};
    Object.values(s.expenses).forEach(e => {
      if (normalizedNature(e.nature) !== nature) return;
      const a = e.account || '(unspecified)';
      (e.payments || []).filter(p => e.approvedAt && inRange(p.date)).forEach(p => {
        const paymentAccount = p.account || a;
        paidOut[paymentAccount] = (paidOut[paymentAccount] || 0) + num(p.amount);
      });
      (e.reimbursementPayments || []).filter(p=>inRange(p.date)).forEach(p => {
        const ra = p.account || '(unspecified)';
        paidOut[ra] = (paidOut[ra] || 0) + num(p.amount);
      });
    });
    if (nature === 'SANKI') procurementPayables(s, true).forEach(p => (p.payments || []).filter(x=>inRange(x.date)).forEach(x => { paidOut[x.account] = (paidOut[x.account] || 0) + num(x.amount); }));
    (s.adjustments || []).filter(x => normalizedNature(x.nature) === nature && inRange(x.date)).forEach(x => { adj[x.account] = (adj[x.account] || 0) + num(x.amount); });
    Object.values(s.receivables||{}).filter(x=>normalizedNature(x.nature)===nature).forEach(x=>(x.collections||[]).filter(c=>inRange(c.date)).forEach(c=>{collected[c.account]=(collected[c.account]||0)+num(c.amount);}));
    (s.receipts || []).filter(x=>normalizedNature(x.nature)===nature&&inRange(x.date)).forEach(x=>{collected[x.account]=(collected[x.account]||0)+num(x.amount);});
    if(nature==='SANKI') salesLedgerEntries().filter(includeAutomaticSale).filter(x=>inRange(x.date)).forEach(x=>{collected[x.account]=(collected[x.account]||0)+num(x.amount);});
    (s.transfers || []).filter(x => inRange(x.date)).forEach(x => {
      if(normalizedNature(x.fromNature||x.nature)===nature) transferOut[x.fromAccount] = (transferOut[x.fromAccount] || 0) + num(x.amount);
      if(normalizedNature(x.toNature||x.nature)===nature) transferIn[x.toAccount] = (transferIn[x.toAccount] || 0) + num(x.amount);
    });
    return {paidOut,collected,adj,transferIn,transferOut};
  }
  const period=totalsFor(d=>(!from||String(d||'')>=from)&&(!to||String(d||'')<=to));
  // Closing balance deliberately ignores the From date: money received before
  // the selected period still exists. It is calculated cumulatively as of To.
  const closing=totalsFor(d=>!to||String(d||'')<=to);
  const openingMap = nature === 'SANKI' ? (s.openingBalances || {}) : (((s.openingBalancesByNature || {})[nature]) || {});
  const accounts = ledgerAccountsForNature(s, nature).map(name => {
    const opening = num(openingMap[name]);
    const spent = round0(period.paidOut[name] || 0),topups=round0(period.adj[name]||0),transferredIn=round0(period.transferIn[name]||0),transferredOut=round0(period.transferOut[name]||0),received=round0(period.collected[name]||0);
    const closingBalance=opening+num(closing.adj[name])+num(closing.collected[name])+num(closing.transferIn[name])-num(closing.transferOut[name])-num(closing.paidOut[name]);
    const issues = reconciliationIssues(s, nature, name);
    return { name, opening: round0(opening), topups, received, transferredIn, transferredOut, spent, periodNet:round0(topups+received+transferredIn-transferredOut-spent), balance:round0(closingBalance), closingAsOf:to||'today', reconciled:issues.length===0, reconciliationIssues:issues };
  });
  res.json({ success: true, accounts });
});

// A transfer is one atomic event that produces a debit and matching credit.
router.post('/api/expenses/transfers', (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ success: false, error: 'Owner/Admin only.' });
  const s = loadStore(); const b = req.body || {};
  const fromNature = normalizedNature(b.fromNature || b.nature), toNature = normalizedNature(b.toNature || b.nature);
  if (!approvalNatures(req).includes(fromNature) || !approvalNatures(req).includes(toNature)) return res.status(403).json({ success: false, error: 'You cannot transfer funds for one of these accounting entities.' });
  const fromAccount = allowedCompanyAccount(s, fromNature, b.fromAccount), toAccount = allowedCompanyAccount(s, toNature, b.toAccount);
  const amount = num(b.amount), proof = String(b.proof || '').trim();
  const classification=String(b.classification||(fromNature===toNature?'internal_transfer':'')).trim();
  if (!fromAccount || !toAccount) return res.status(400).json({ success: false, error: 'Select both accounts.' });
  if (fromNature===toNature && fromAccount.toLowerCase() === toAccount.toLowerCase()) return res.status(400).json({ success: false, error: 'Source and destination accounts must be different.' });
  if (fromNature!==toNature && !['owner_withdrawal','owner_contribution','inter_entity_loan','reimbursement'].includes(classification)) return res.status(400).json({ success:false,error:'Choose why money is moving between these entities.' });
  if (!(amount > 0)) return res.status(400).json({ success: false, error: 'Transfer amount must be greater than 0.' });
  if (!proof) return res.status(400).json({ success: false, error: 'Transfer proof is required.' });
  s.transferSeq = (s.transferSeq || 0) + 1;
  const transfer = { id: 'TR-' + String(s.transferSeq).padStart(5, '0'), nature:fromNature, fromNature, toNature, classification, fromAccount, toAccount, amount,
    date: String(b.date || new Date().toISOString().slice(0, 10)).slice(0, 10), proof, note: String(b.note || '').trim(),
    createdBy: (req.user && req.user.username) || 'admin', createdAt: new Date().toISOString() };
  s.transfers = Array.isArray(s.transfers) ? s.transfers : []; s.transfers.push(transfer);audit(s,req,'TRANSFER_RECORDED','transfer',transfer.id,{nature:fromNature,account:fromAccount,after:transfer});saveStore(s);
  res.json({ success: true, transfer });
});

router.post('/api/expenses/receipts', (req,res) => {
  if(!isAdmin(req)) return res.status(403).json({success:false,error:'Owner/Admin only.'});
  const s=loadStore(),b=req.body||{},nature=normalizedNature(b.nature);
  if(!approvalNatures(req).includes(nature)) return res.status(403).json({success:false,error:'You cannot record money for this entity.'});
  const account=allowedCompanyAccount(s,nature,b.account),amount=num(b.amount),proof=String(b.proof||'').trim(),source=String(b.source||'').trim(),receiptType=String(b.receiptType||'other_income').trim(),note=String(b.note||'').trim(),ownerCashDeclaration=rolesOfReq(req).includes('owner')&&/cash/i.test(String(account||''))&&!proof;
  if(!account) return res.status(400).json({success:false,error:'Select the account that received the money.'});
  if(!(amount>0)) return res.status(400).json({success:false,error:'Receipt amount must be greater than 0.'});
  if(!source) return res.status(400).json({success:false,error:'Source / party is required.'});
  if(!proof&&!ownerCashDeclaration) return res.status(400).json({success:false,error:'Receipt proof is required. Only the Owner may declare a cash receipt without proof.'});
  if(ownerCashDeclaration&&!note) return res.status(400).json({success:false,error:'Explain why no proof is available for this cash receipt.'});
  if(!['asset_sale','other_income','refund','owner_contribution'].includes(receiptType)) return res.status(400).json({success:false,error:'Choose a valid receipt type.'});
  s.receiptSeq=(s.receiptSeq||0)+1;s.receipts=Array.isArray(s.receipts)?s.receipts:[];
  const receipt={id:'REC-'+String(s.receiptSeq).padStart(5,'0'),nature,account,amount,receiptType,source,date:String(b.date||new Date().toISOString().slice(0,10)).slice(0,10),note,proof,proofException:ownerCashDeclaration?'Owner cash declaration — no external proof available':'',createdBy:(req.user&&req.user.username)||'admin',createdAt:new Date().toISOString()};
  s.receipts.push(receipt);audit(s,req,'RECEIPT_RECORDED','receipt',receipt.id,{nature,account,after:receipt});saveStore(s);res.json({success:true,receipt});
});

router.get('/api/expenses/account-ledger', (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ success: false, error: 'Owner/Admin only.' });
  const s = loadStore(), nature = normalizedNature(req.query.nature), account = String(req.query.account || '').trim();
  if (!approvalNatures(req).includes(nature)) return res.status(403).json({ success: false, error: 'You cannot view this accounting entity.' });
  if (!account) return res.status(400).json({ success: false, error: 'Select an account.' });
  if (!ledgerAccountsForNature(s, nature).some(a => a.toLowerCase() === account.toLowerCase())) return res.status(403).json({ success:false, error:'This account does not belong to the selected entity.' });
  const from = String(req.query.from || ''), to = String(req.query.to || ''), entries = [];
  const openingMap = nature === 'SANKI' ? (s.openingBalances || {}) : (((s.openingBalancesByNature || {})[nature]) || {});
  entries.push({ id: 'OPENING', date: '', kind: 'opening', description: 'Opening balance', credit: num(openingMap[account]), debit: 0 });
  (s.adjustments || []).filter(x => normalizedNature(x.nature) === nature && x.account === account).forEach(x => entries.push({ id:x.id,date:x.date,kind:'adjustment',description:x.note||'Balance adjustment',credit:Math.max(0,num(x.amount)),debit:Math.max(0,-num(x.amount)),proof:x.proof||'',by:x.createdBy||'' }));
  (s.receipts || []).filter(x=>normalizedNature(x.nature)===nature&&x.account===account).forEach(x=>entries.push({id:x.id,date:x.date,kind:'receipt',description:(x.receiptType==='asset_sale'?'Asset sale':'Money received')+' · '+x.source,credit:num(x.amount),debit:0,proof:x.proof,note:x.note,by:x.createdBy}));
  (s.transfers || []).forEach(x => {
    const isOut=normalizedNature(x.fromNature||x.nature)===nature&&x.fromAccount===account,isIn=normalizedNature(x.toNature||x.nature)===nature&&x.toAccount===account;if(!isOut&&!isIn)return;
    const other=(isOut?(x.toNature||x.nature)+' · '+x.toAccount:(x.fromNature||x.nature)+' · '+x.fromAccount);
    entries.push({id:x.id,date:x.date,kind:'transfer',description:(isOut?'Transfer to ':'Transfer from ')+other+' · '+String(x.classification||'internal transfer').replaceAll('_',' '),credit:isIn?num(x.amount):0,debit:isOut?num(x.amount):0,proof:x.proof,note:x.note,by:x.createdBy});
  });
  Object.values(s.expenses || {}).filter(e => normalizedNature(e.nature) === nature).forEach(e => {
    (e.payments || []).filter(p => e.approvedAt && (p.account || e.account) === account).forEach(p => entries.push({id:e.id+'/'+p.id,date:p.date,kind:p.personalFunds?'personal_expense':'expense',description:(e.vendor||'Vendor')+' · '+(e.particulars||e.id)+(p.personalFunds?' · paid personally':''),credit:0,debit:num(p.amount),proof:p.proof,by:p.paidBy}));
    (e.reimbursementPayments || []).filter(p => p.account === account).forEach(p => entries.push({id:e.id+'/'+p.id,date:p.date,kind:'reimbursement',description:'Reimbursement to '+(e.claimant||e.createdBy||'claimant'),credit:0,debit:num(p.amount),proof:p.proof,by:p.paidBy}));
  });
  if (nature === 'SANKI') procurementPayables(s, true).forEach(p => (p.payments || []).filter(x => x.account === account).forEach(x => entries.push({id:p.id+'/'+x.id,date:x.date,kind:'purchase',description:(p.vendor||'Mediator')+' · '+p.id+' · goods and transport',credit:0,debit:num(x.amount),proof:x.proof,by:x.paidBy})));
  Object.values(s.receivables||{}).filter(x=>normalizedNature(x.nature)===nature).forEach(x=>(x.collections||[]).filter(c=>c.account===account).forEach(c=>entries.push({id:x.id+'/'+c.id,date:c.date,kind:'receivable',description:'Received from '+x.party+' · '+x.reason,credit:num(c.amount),debit:0,proof:c.proof,by:c.receivedBy})));
  if(nature==='SANKI') salesLedgerEntries().filter(includeAutomaticSale).filter(x=>x.account===account).forEach(x=>entries.push({id:x.id,date:x.date,kind:'sale',description:x.description,credit:num(x.amount),debit:0}));
  const ordered = entries.sort((a,b) => String(a.date).localeCompare(String(b.date)) || String(a.id).localeCompare(String(b.id)));
  let running = 0; ordered.forEach(x => { running += num(x.credit)-num(x.debit); x.balance = round0(running); });
  const visible = ordered.filter(x => x.kind === 'opening' || ((!from || x.date >= from) && (!to || x.date <= to)))
    .sort((a,b) => a.kind === 'opening' ? 1 : (b.kind === 'opening' ? -1 : (String(b.date).localeCompare(String(a.date)) || String(b.id).localeCompare(String(a.id)))));
  const issues = reconciliationIssues(s, nature, account);
  res.json({ success:true, account, nature, entries:visible, balance:round0(running), reconciled:issues.length===0, reconciliationIssues:issues });
});
router.post('/api/expenses/balances', (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ success: false, error: 'Owner/Admin only.' });
  const s = loadStore();
  const b = req.body || {};
  const nature = normalizedNature(b.nature);
  if (!approvalNatures(req).includes(nature)) return res.status(403).json({ success: false, error: 'You cannot edit this accounting entity.' });
  if (b.setOpening && b.setOpening.account) {
    const openingAccount = allowedCompanyAccount(s, nature, b.setOpening.account);
    if (!openingAccount) return res.status(400).json({ success:false, error:'Select an account assigned to this entity.' });
    if (nature !== 'SANKI') {
      s.openingBalancesByNature = s.openingBalancesByNature || {};
      s.openingBalancesByNature[nature] = s.openingBalancesByNature[nature] || {};
      s.openingBalancesByNature[nature][openingAccount] = num(b.setOpening.amount);
    } else s.openingBalances[openingAccount] = num(b.setOpening.amount);
    audit(s,req,'OPENING_BALANCE_CHANGED','account',openingAccount,{nature,account:openingAccount,after:num(b.setOpening.amount)});
  }
  if (b.adjust && b.adjust.account && b.adjust.amount != null) {
    const adjustmentAccount = allowedCompanyAccount(s, nature, b.adjust.account);
    if (!adjustmentAccount) return res.status(400).json({ success:false, error:'Select an account assigned to this entity.' });
    const rawAmount = Math.abs(num(b.adjust.amount));
    const direction = String(b.adjust.direction || (num(b.adjust.amount) < 0 ? 'deduct' : 'add'));
    const note = String(b.adjust.note || '').trim();
    if (!(rawAmount > 0)) return res.status(400).json({ success: false, error: 'Adjustment amount must be greater than 0.' });
    if (!['add', 'deduct'].includes(direction)) return res.status(400).json({ success: false, error: 'Choose Add money or Deduct money.' });
    if (!note) return res.status(400).json({ success: false, error: 'Adjustment reason is required.' });
    s.adjSeq = (s.adjSeq || 0) + 1;
    s.adjustments.push({
      id: 'ADJ-' + s.adjSeq, account: adjustmentAccount,
      amount: direction === 'deduct' ? -rawAmount : rawAmount, note,
      date: (b.adjust.date || new Date().toISOString().slice(0, 10)).toString().slice(0, 10), nature,
      proof: String(b.adjust.proof || '').trim(), createdBy: (req.user && req.user.username) || 'admin', createdAt: new Date().toISOString()
    });
    audit(s,req,'ADJUSTMENT_RECORDED','account',adjustmentAccount,{nature,account:adjustmentAccount,after:s.adjustments.at(-1),note});
  }
  saveStore(s);
  res.json({ success: true });
});

function recordedAccountBalance(s,nature,account,asOf){
  const on=d=>!asOf||!d||String(d).slice(0,10)<=asOf,openingMap=nature==='SANKI'?(s.openingBalances||{}):(((s.openingBalancesByNature||{})[nature])||{});let total=num(openingMap[account]);
  (s.adjustments||[]).filter(x=>normalizedNature(x.nature)===nature&&x.account===account&&on(x.date)).forEach(x=>total+=num(x.amount));
  (s.receipts||[]).filter(x=>normalizedNature(x.nature)===nature&&x.account===account&&on(x.date)).forEach(x=>total+=num(x.amount));
  (s.transfers||[]).filter(x=>on(x.date)).forEach(x=>{if(normalizedNature(x.fromNature||x.nature)===nature&&x.fromAccount===account)total-=num(x.amount);if(normalizedNature(x.toNature||x.nature)===nature&&x.toAccount===account)total+=num(x.amount);});
  Object.values(s.expenses||{}).filter(e=>normalizedNature(e.nature)===nature).forEach(e=>{(e.payments||[]).filter(p=>e.approvedAt&&(p.account||e.account)===account&&on(p.date)).forEach(p=>total-=num(p.amount));(e.reimbursementPayments||[]).filter(p=>p.account===account&&on(p.date)).forEach(p=>total-=num(p.amount));});
  Object.values(s.receivables||{}).filter(x=>normalizedNature(x.nature)===nature).forEach(x=>(x.collections||[]).filter(c=>c.account===account&&on(c.date)).forEach(c=>total+=num(c.amount)));
  if(nature==='SANKI'){salesLedgerEntries().filter(includeAutomaticSale).filter(x=>x.account===account&&on(x.date)).forEach(x=>total+=num(x.amount));procurementPayables(s,true).forEach(p=>(p.payments||[]).filter(x=>x.account===account&&on(x.date)).forEach(x=>total-=num(x.amount)));}
  return round0(total);
}
router.get('/api/expenses/balance-sheet',(req,res)=>{
  if(!isAdmin(req))return res.status(403).json({success:false,error:'Owner/Admin only.'});const s=loadStore(),asOf=String(req.query.asOf||new Date().toISOString().slice(0,10)).slice(0,10),selected=req.query.nature?normalizedNature(req.query.nature):'',natures=selected?[selected]:approvalNatures(req);
  if(selected&&!approvalNatures(req).includes(selected))return res.status(403).json({success:false,error:'You cannot view this accounting entity.'});const accounts=[];natures.forEach(n=>ledgerAccountsForNature(s,n).forEach(name=>accounts.push({nature:n,name,balance:recordedAccountBalance(s,n,name,asOf)})));
  const bankAndCash=accounts.filter(x=>x.balance>0).reduce((n,x)=>n+x.balance,0),overdrafts=Math.abs(accounts.filter(x=>x.balance<0).reduce((n,x)=>n+x.balance,0));let receivables=0,vendorPayables=0,reimbursements=0;
  Object.values(s.receivables||{}).filter(x=>natures.includes(normalizedNature(x.nature))&&String(x.date||'')<=asOf).forEach(x=>{const received=(x.collections||[]).filter(c=>String(c.date||'')<=asOf).reduce((n,c)=>n+num(c.amount),0);receivables+=Math.max(0,num(x.amount)-received);});
  Object.values(s.expenses||{}).filter(e=>natures.includes(normalizedNature(e.nature))&&e.approvedAt&&String(e.date||'')<=asOf).forEach(e=>{if(!e.paidAlready)vendorPayables+=Math.max(0,num(e.amount)-num(e.paidAmount));if(e.paidAlready&&normalizedNature(e.nature)!=='PERSONAL')reimbursements+=Math.max(0,num(e.personalPaidAmount)-num(e.reimbursementAmount));});
  const procurement=natures.includes('SANKI')?procurementPayables(s,false).filter(x=>String(x.date||'')<=asOf).reduce((n,x)=>n+num(x.balanceDue),0):0,totalAssets=round0(bankAndCash+receivables),totalLiabilities=round0(overdrafts+vendorPayables+procurement+reimbursements),equity=round0(totalAssets-totalLiabilities);
  res.json({success:true,asOf,natures,assets:{accounts:accounts.filter(x=>x.balance>0),bankAndCash:round0(bankAndCash),receivables:round0(receivables),total:totalAssets},liabilities:{overdrafts:round0(overdrafts),vendorPayables:round0(vendorPayables),procurementPayables:round0(procurement),reimbursements:round0(reimbursements),total:totalLiabilities},equity:{recordedNetPosition:equity},note:'Recorded-system position only. Inventory, fixed assets, taxes, loans and opening capital must be entered before this becomes a statutory balance sheet.'});
});
router.get('/api/expenses/audit-log',(req,res)=>{
  if(!isAdmin(req))return res.status(403).json({success:false,error:'Owner/Admin only.'});const s=loadStore(),nature=req.query.nature?normalizedNature(req.query.nature):'',action=String(req.query.action||'').toUpperCase(),user=String(req.query.user||'').toLowerCase(),from=String(req.query.from||''),to=String(req.query.to||''),subject=String(req.query.subject||'').toLowerCase();
  const all=(s.auditLog||[]),matches=x=>(!nature||x.nature===nature)&&(!action||x.action===action)&&(!user||String(x.user).toLowerCase().includes(user))&&(!from||String(x.at).slice(0,10)>=from)&&(!to||String(x.at).slice(0,10)<=to)&&(!subject||String(x.subjectId+' '+x.subjectType+' '+x.account+' '+x.paymentId).toLowerCase().includes(subject)),entries=all.filter(matches).slice().reverse().slice(0,1000),expenseIds=Array.from(new Set(entries.filter(x=>x.subjectType==='expense').map(x=>x.subjectId)));
  const records=expenseIds.map(id=>{const timeline=all.filter(x=>x.subjectType==='expense'&&x.subjectId===id).sort((a,b)=>String(a.at).localeCompare(String(b.at))),current=(s.expenses||{})[id],created=timeline.find(x=>x.action==='CREATED'),deleted=timeline.find(x=>x.action==='DELETED'),snapshot=current||(created&&created.after)||(deleted&&deleted.before)||{};if(!created){timeline.unshift({id:'ORIGIN-'+id,at:snapshot.createdAt||((snapshot.date||'')+'T00:00:00.000Z'),user:snapshot.createdBy||snapshot.claimant||'unknown',action:'CREATED',subjectType:'expense',subjectId:id,nature:normalizedNature(snapshot.nature),account:'',paymentId:'',ip:'',device:'Unknown',userAgent:'',before:null,after:{nature:snapshot.nature,date:snapshot.date,vendor:snapshot.vendor,particulars:snapshot.particulars,amount:snapshot.amount,paymentType:snapshot.paymentType},note:'Original submission reconstructed from the expense record'});}return{id,nature:normalizedNature(snapshot.nature||timeline.at(-1)&&timeline.at(-1).nature),date:snapshot.date||'',vendor:snapshot.vendor||'',particulars:snapshot.particulars||'',amount:num(snapshot.amount),status:snapshot.status||(deleted?'deleted':''),claimant:snapshot.claimant||snapshot.createdBy||'',latestAt:timeline.at(-1)&&timeline.at(-1).at||'',timeline};}).sort((a,b)=>String(b.latestAt).localeCompare(String(a.latestAt))).slice(0,500);
  res.json({success:true,count:entries.length,entries,records});
});

// ── Private Owner Dashboard / OD view ───────────────────────────
function ownerAccounts(s, from, to) {
  const paidOut = {}, adjustments = {};
  Object.values(s.expenses || {}).forEach(e => {
    if (normalizedNature(e.nature) !== 'SANKI') return;
    const d = String((e.paidAt || e.date || '')).slice(0, 10);
    if ((from && d < from) || (to && d > to)) return;
    (e.payments || []).filter(p => !p.personalFunds).forEach(p => {
      const account = p.account || e.account || '(unspecified)';
      paidOut[account] = (paidOut[account] || 0) + num(p.amount);
    });
    (e.reimbursementPayments || []).forEach(p => {
      const account = p.account || '(unspecified)';
      paidOut[account] = (paidOut[account] || 0) + num(p.amount);
    });
  });
  (s.adjustments || []).forEach(x => {
    const d = String(x.date || '').slice(0, 10);
    if ((from && d < from) || (to && d > to)) return;
    adjustments[x.account] = (adjustments[x.account] || 0) + num(x.amount);
  });
  const names = Array.from(new Set([].concat(s.accounts || [], Object.keys(s.openingBalances || {}), Object.keys(s.odConfig || {}), ['Tiana 0425'])));
  return names.map(name => {
    const opening = num((s.openingBalances || {})[name]);
    const topups = round0(adjustments[name] || 0);
    const spent = round0(paidOut[name] || 0);
    return { name, opening: round0(opening), topups, sales: 0, spent, balance: round0(opening + topups - spent) };
  });
}
function ownerCapital(s, from, to) {
  let total = num(s.openingInvestment); const moves = [];
  (s.adjustments || []).forEach(x => {
    if (x.kind !== 'investment' && x.kind !== 'drawing') return;
    total += num(x.amount);
    const d = String(x.date || '').slice(0, 10);
    if ((!from || d >= from) && (!to || d <= to)) moves.push({ id: x.id, date: d, account: x.account, amount: num(x.amount), note: x.note || '', kind: x.kind });
  });
  moves.sort((a, b) => String(b.date).localeCompare(String(a.date)));
  return { ownerCash: round0(total), openingInvestment: round0(num(s.openingInvestment)), moves };
}
function ownerOd(s, accounts, from, to) {
  const cfg = Object.assign({ 'Tiana 0425': { limit: 0, ratePct: 0 } }, s.odConfig || {});
  const start = Date.parse(from || new Date().toISOString().slice(0, 10));
  const end = Date.parse(to || new Date().toISOString().slice(0, 10));
  const days = Math.max(1, Math.round((end - start) / 86400000) + 1);
  return Object.keys(cfg).map(name => {
    const account = accounts.find(a => a.name === name) || { balance: 0 };
    const used = Math.max(0, -num(account.balance));
    const limit = Math.abs(num(cfg[name].limit));
    const ratePct = Math.abs(num(cfg[name].ratePct));
    return { account: name, used: round0(used), limit: round0(limit), available: round0(limit - used), ratePct, interest: round0(used * ratePct / 100 * days / 365) };
  });
}
router.get('/api/owner/summary', (req, res) => {
  const s = loadStore();
  const from = String(req.query.from || '2026-07-09').slice(0, 10);
  const to = String(req.query.to || '').slice(0, 10);
  const accounts = ownerAccounts(s, from, to);
  const od = ownerOd(s, accounts, from, to);
  const odNames = new Set(od.map(x => x.account));
  const accountsTotal = round0(accounts.filter(a => !odNames.has(a.name)).reduce((n, a) => n + a.balance, 0));
  const capital = ownerCapital(s, from, to);
  const odUsed = round0(od.reduce((n, x) => n + x.used, 0));
  res.json(Object.assign({ success: true, accounts, accountsTotal, range: { from, to }, ledgerStart: '2026-07-09', od, odUsed, ownerInvested: round0(odUsed + capital.ownerCash) }, capital));
});
router.post('/api/owner/od', (req, res) => {
  const s = loadStore(); const b = req.body || {}; const account = String(b.account || '').trim();
  if (!account) return res.status(400).json({ success: false, error: 'Account required.' });
  s.odConfig = s.odConfig || {};
  s.odConfig[account] = { limit: Math.abs(num(b.limit)), ratePct: Math.abs(num(b.ratePct)) };
  if (b.openingUsed != null) s.openingBalances[account] = -Math.abs(num(b.openingUsed));
  if (!(s.accounts || []).includes(account)) s.accounts.push(account);
  saveStore(s); res.json({ success: true, odConfig: s.odConfig[account] });
});
router.post('/api/owner/invest', (req, res) => {
  const s = loadStore(); const b = req.body || {}; const amount = Math.abs(num(b.amount));
  if (!b.account || !amount) return res.status(400).json({ success: false, error: 'Account and amount required.' });
  const kind = b.kind === 'drawing' ? 'drawing' : 'investment';
  s.adjSeq = (s.adjSeq || 0) + 1;
  s.adjustments.push({ id: 'ADJ-' + s.adjSeq, account: String(b.account), amount: kind === 'drawing' ? -amount : amount, note: String(b.note || ''), date: String(b.date || new Date().toISOString().slice(0, 10)).slice(0, 10), kind });
  saveStore(s); res.json({ success: true });
});
router.post('/api/owner/invest/delete', (req, res) => {
  const s = loadStore(); const id = String((req.body || {}).id || '');
  s.adjustments = (s.adjustments || []).filter(x => !(x.id === id && (x.kind === 'investment' || x.kind === 'drawing')));
  saveStore(s); res.json({ success: true });
});
router.post('/api/owner/opening', (req, res) => {
  const s = loadStore(); s.openingInvestment = num((req.body || {}).amount); saveStore(s);
  res.json({ success: true, openingInvestment: s.openingInvestment });
});

// ── Settings: accounts / people / ledger classification overrides ─
router.post('/api/expenses/settings', (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ success: false, error: 'Admin only.' });
  const s = loadStore();
  const b = req.body || {};
  if (Array.isArray(b.accounts)) s.accounts = b.accounts.map(x => String(x).trim()).filter(Boolean);
  if (Array.isArray(b.people)) s.people = b.people.map(x => String(x).trim()).filter(Boolean);
  if (b.ledgerOverride && b.ledgerOverride.name) {
    const nm = String(b.ledgerOverride.name);
    const cur = s.ledgerOverrides[nm] || {};
    if (NATURES.includes(b.ledgerOverride.nature)) cur.nature = b.ledgerOverride.nature;
    if (TYPES.includes(b.ledgerOverride.type)) cur.type = b.ledgerOverride.type;
    s.ledgerOverrides[nm] = cur;
  }
  audit(s,req,'LEDGER_SETTING_CHANGED','settings','expense-settings',{after:b});saveStore(s);
  res.json({ success: true });
});

// ── Approval queue: request a NEW category, paying account, or vendor ─
// Anyone in the accounting area can REQUEST; only an admin can APPROVE. This is
// what stops the ledger list from turning into a free-for-all again.
router.post('/api/expenses/requests', (req, res) => {
  const s = loadStore();
  const b = req.body || {};
  const kind = ['account', 'vendor'].includes(b.kind) ? b.kind : 'ledger';
  const name = String(b.name || '').trim();
  if (!name) return res.status(400).json({ success: false, error: 'Name required.' });
  // Already exists? Then no request needed.
  const exists = kind === 'ledger'
    ? pickableLedgers(s).some(l => l.name.toLowerCase() === name.toLowerCase())
    : kind === 'vendor'
      ? !!s.vendors[name.toLowerCase()]
      : (s.accounts || []).some(a => a.toLowerCase() === name.toLowerCase());
  if (exists) return res.json({ success: true, already: true });
  // De-dupe pending requests.
  const dup = (s.requests || []).find(r => r.status === 'pending' && r.kind === kind && r.name.toLowerCase() === name.toLowerCase());
  if (dup) return res.json({ success: true, request: dup, already: true });
  s.reqSeq = (s.reqSeq || 0) + 1;
  const request = {
    id: 'RQ-' + String(s.reqSeq).padStart(4, '0'),
    kind, name,
    meta: kind === 'ledger' ? { type: TYPES.includes(b.type) ? b.type : 'variable' } : { details: String(b.details || '').trim() },
    status: 'pending',
    by: (req.user && req.user.username) || 'user',
    at: new Date().toISOString(),
    decidedBy: null, decidedAt: null
  };
  s.requests.push(request);
  audit(s,req,'REQUEST_CREATED','request',request.id,{after:request});saveStore(s);
  res.json({ success: true, request });
});
router.get('/api/expenses/requests', (req, res) => {
  const s = loadStore();
  const status = (req.query.status || 'pending').toString();
  const list = (s.requests || []).filter(r => (!status || r.status === status) &&
    (isAdmin(req) || (canApprove(req) && r.kind === 'vendor') || r.by === (req.user && req.user.username)))
    .sort((a, b) => (b.at || '').localeCompare(a.at || ''));
  res.json({ success: true, requests: list, isAdmin: isAdmin(req) });
});
router.post('/api/expenses/requests/:id/decide', (req, res) => {
  const s = loadStore();
  const r = (s.requests || []).find(x => x.id === req.params.id);
  if (!r) return res.status(404).json({ success: false, error: 'Request not found.' });
  if (!(isAdmin(req) || (r.kind === 'vendor' && canApprove(req)))) {
    return res.status(403).json({ success: false, error: r.kind === 'vendor' ? 'Only an approver/admin can decide vendor requests.' : 'Admin approval only.' });
  }
  if (r.status !== 'pending') return res.json({ success: true, request: r });
  const approve = !!(req.body || {}).approve;
  if (approve) {
    if (r.kind === 'ledger') {
      s.customLedgers = s.customLedgers || {};
      s.customLedgers[r.name] = { name: r.name, type: (r.meta && r.meta.type) || 'variable' };
    } else if (r.kind === 'vendor') {
      const editedName = String((req.body || {}).name || r.name).trim();
      if (!editedName) return res.status(400).json({ success: false, error: 'Vendor name required.' });
      s.vendors[editedName.toLowerCase()] = { name: editedName, notes: String((r.meta && r.meta.details) || '') };
      r.name = editedName;
    } else {
      if (!(s.accounts || []).some(a => a.toLowerCase() === r.name.toLowerCase())) s.accounts.push(r.name);
    }
    r.status = 'approved';
  } else {
    r.status = 'rejected';
  }
  r.decidedBy = (req.user && req.user.username) || 'admin';
  r.decidedAt = new Date().toISOString();
  audit(s,req,approve?'REQUEST_APPROVED':'REQUEST_REJECTED','request',r.id,{after:r});saveStore(s);
  res.json({ success: true, request: r });
});
// Admin: remove a paying account (fix the wrong ones) — keeps history intact.
router.post('/api/expenses/accounts', (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ success: false, error: 'Admin or Owner only.' });
  const s = loadStore();
  const name = String((req.body || {}).name || '').trim();
  if (!name) return res.status(400).json({ success: false, error: 'Account name required.' });
  const existing = storedAccountNames(s).find(a => a.toLowerCase() === name.toLowerCase());
  if (!existing) s.accounts.push(name);
  audit(s,req,'ACCOUNT_ADDED','account',existing||name,{after:{name:existing||name}});saveStore(s);
  res.json({ success: true, account: existing || name, accounts: storedAccountNames(s) });
});
router.post('/api/expenses/accounts/remove', (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ success: false, error: 'Admin only.' });
  const s = loadStore();
  const name = String((req.body || {}).name || '').trim();
  s.accounts = (s.accounts || []).filter(a => a !== name);
  audit(s,req,'ACCOUNT_REMOVED','account',name,{before:{name}});saveStore(s);
  res.json({ success: true, accounts: s.accounts });
});
// Admin: list / remove admin-approved CUSTOM categories (built-in ones stay).
router.get('/api/expenses/custom-ledgers', (req, res) => {
  const s = loadStore();
  res.json({ success: true, ledgers: Object.values(s.customLedgers || {}), isAdmin: isAdmin(req) });
});
router.post('/api/expenses/custom-ledgers/remove', (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ success: false, error: 'Admin only.' });
  const s = loadStore();
  const name = String((req.body || {}).name || '').trim();
  if (s.customLedgers && s.customLedgers[name]) delete s.customLedgers[name];
  audit(s,req,'CATEGORY_REMOVED','category',name,{before:{name}});saveStore(s);
  res.json({ success: true });
});

// ── Summary for the P&L: approved+ BUSINESS expenses by type ─
function summaryForPL(from, to) {
  const s = loadStore();
  const out = {
    POS: { fixed: 0, running: 0, variable: 0, marketing: 0 },
    Website: { fixed: 0, running: 0, variable: 0, marketing: 0 },
    Shared: { fixed: 0, running: 0, variable: 0, marketing: 0 },
    excluded: 0
  };
  Object.values(s.expenses).forEach(e => {
    if (!['approved', 'partially_paid', 'paid'].includes(e.status)) return; // approved-only gate
    if (from && e.date < from) return;
    if (to && e.date > to) return;
    const expenseNature = normalizedNature(e.nature);
    if (expenseNature === 'SAMAST' || expenseNature === 'PERSONAL') return; // separate books
    if (!SANKI_PL_NATURES.includes(e.nature || expenseNature)) { out.excluded += e.amount; return; }  // legacy drawings only
    const ch = CHANNELS.includes(e.channel) ? e.channel : 'Shared';
    out[ch][e.type] = (out[ch][e.type] || 0) + e.amount;
  });
  return out;
}

// Run idempotent store repairs/corrections when the service starts, rather
// than waiting for the first user to open an Expenses screen.
loadStore();

module.exports = { router, summaryForPL, createTelegramPersonalExpense, createTelegramPersonalReceipt, telegramExpense, telegramApproveExpense, telegramRejectExpense, telegramRecordPayment, telegramResolveAccount };
