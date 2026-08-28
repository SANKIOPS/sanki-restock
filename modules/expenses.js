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
const XLSX = require('xlsx');
const pdfParse = require('pdf-parse');
const { createWorker } = require('tesseract.js');
const tesseractEnglish = require('@tesseract.js-data/eng');

const router = express.Router();

const DATA_DIR = process.env.DATA_PATH
  ? path.dirname(process.env.DATA_PATH)
  : path.join(__dirname, '..');
const EXP_PATH = path.join(DATA_DIR, 'expenses.json');
const PROC_PATH = process.env.PROCUREMENT_PATH || path.join(DATA_DIR, 'procurement.json');
const SALES_PATH = process.env.SALES_PATH || path.join(DATA_DIR, 'sales.json');
const ORDERS_PATH = process.env.ORDERS_PATH || path.join(DATA_DIR, 'orders.json');
const STATEMENT_DIR = path.join(DATA_DIR, 'bank-statements');
const STATEMENT_DRAFT_DIR = path.join(DATA_DIR, 'bank-statement-drafts');
try { fs.mkdirSync(STATEMENT_DIR, { recursive:true }); } catch {}
try { fs.mkdirSync(STATEMENT_DRAFT_DIR, { recursive:true }); } catch {}
const statementUpload=multer({storage:multer.diskStorage({destination:(req,file,cb)=>cb(null,STATEMENT_DRAFT_DIR),filename:(req,file,cb)=>cb(null,Date.now()+'-'+crypto.randomBytes(4).toString('hex')+path.extname(file.originalname||'.xlsx').toLowerCase())}),limits:{fileSize:15*1024*1024}});
const DEFAULT_SALES_BANK = 'Axis Bank 3448';
const DEFAULT_COUNTER_CASH = 'Counter Cash';
const PAYTM_CLEARING_ACCOUNT = 'Paytm Settlement Clearing';
const SALES_LEDGER_FROM = '2026-08-21';
const COUNTER_CASH_RESET_DATE = '2026-08-22';
const ACCOUNTING_BUILD = '2026-08-26-bank-charge-ledgers-v2';

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
function roundCashSale(n) { const amount=num(n);return amount>0?Math.ceil(amount/10)*10:amount; }
function cashEntryIsVisible(account,date) { return String(account||'')!==DEFAULT_COUNTER_CASH||String(date||'').slice(0,10)>=COUNTER_CASH_RESET_DATE; }
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
  PERSONAL: ['IndusInd Bank 7883','ICICI Bank 0993','ICICI Bank 0992','Gagan Personal Cash','Namita 5464','Namita Cash']
};
const CLAIMANT_ACCOUNTS = {
  arshpreet: ['Arshpreet 1919'],
  shivam: ['Shivam 4807'],
  pradeep: ['Pradeep 8606'],
  namita: ['Namita 5464','Namita Cash']
};
const USER_PAYMENT_ACCOUNTS = {
  prashant: ['Prashant Axis 3645', 'Prashant Cash', 'Counter Cash']
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
    openingBalanceDates: {},             // effective dates for reset openings
    openingBalancesByNature: { SAMAST: {}, PERSONAL: {} }, // non-SANKI books stay separate
    adjustments: [],                     // [{ id, account, amount(+/-), note, date }] top-ups/corrections
    transfers: [],                       // [{ id, nature, fromAccount, toAccount, amount, date, proof, note }]
    receipts: [],                        // money received other than sales/receivables
    bankStatements: {},                  // cumulative normalized statement rows by account
    paytmSettlements: [],                // finalized Paytm-to-bank settlement explanations
    reconciliationExpenses: [],          // P&L/category postings backed by official bank rows
    bankReconciliationDrafts: {},        // temporary previews; never part of the official ledger
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
    s.reconciliationExpenses=Array.isArray(s.reconciliationExpenses)?s.reconciliationExpenses:[];
    // Repair the two owner-identified Axis charges that were previously saved
    // only as balance adjustments. These postings affect spending/P&L only;
    // the official bank row remains the sole Axis balance movement.
    let bankChargeRepairAdded=false;
    (s.adjustments||[]).filter(x=>x.account===DEFAULT_SALES_BANK&&String(x.date||'')==='2026-08-22'&&num(x.amount)<0&&([250,45].includes(Math.abs(num(x.amount)))||/bank\s*charges?/i.test(String(x.note||'')))).forEach(x=>{
      if(!s.reconciliationExpenses.some(e=>e.adjustmentId===x.id)){s.reconciliationExpenses.push({id:'BRE-'+x.id,nature:normalizedNature(x.nature),date:x.date,amount:Math.abs(num(x.amount)),account:x.account,category:'BANK CHARGES',type:'running',vendor:'Axis Bank',particulars:String(x.note||'Bank charges').replace(/\s*\[Bank reconciliation[^\]]*\]/i,''),adjustmentId:x.id,reconciliationDraft:x.reconciliationDraft||'',createdAt:x.createdAt||new Date().toISOString(),createdBy:x.createdBy||'system'});bankChargeRepairAdded=true;}
    });
    (s.paytmSettlements||[]).forEach(x=>{if(!(num(x.chargeAmount)>0)){const saved=String(x.reason||'').match(/charges?\s*(?:₹|rs\.?|inr)?\s*([0-9,]+(?:\.\d{1,2})?)/i),charge=saved?num(saved[1].replace(/,/g,'')):0;if(charge>0){x.chargeAmount=charge;x.grossAmount=num(x.netAmount)+charge;bankChargeRepairAdded=true;}}});
    (s.paytmSettlements||[]).filter(x=>num(x.chargeAmount)>0).forEach(x=>{
      if(!s.reconciliationExpenses.some(e=>e.settlementId===x.id)){s.reconciliationExpenses.push({id:'BRE-'+x.id,nature:'SANKI',date:x.date,amount:num(x.chargeAmount),account:x.bankAccount||DEFAULT_SALES_BANK,category:'BANK CHARGES',type:'running',vendor:'Paytm',particulars:'Paytm settlement charges · connected sales '+((x.orderIds||[]).map(n=>'#'+String(n).replace(/^#/,'')).join(', ')||'not specified'),settlementId:x.id,bankTransactionId:x.bankTransactionId||'',reconciliationDraft:x.reconciliationDraft||'',createdAt:x.createdAt||new Date().toISOString(),createdBy:x.createdBy||'system'});bankChargeRepairAdded=true;}
    });
    (s.transfers || []).forEach(x => { x.fromAccount = rename(x.fromAccount); x.toAccount = rename(x.toAccount); });
    const removeTr9Key='owner-delete-tr-00009-both-ledger-sides';
    s.oneTimeMigrations=s.oneTimeMigrations||{};
    if(!s.oneTimeMigrations[removeTr9Key]){const i=(s.transfers||[]).findIndex(x=>x.id==='TR-00009'),before=i>=0?s.transfers[i]:null;if(i>=0)s.transfers.splice(i,1);audit(s,null,'TRANSFER_DELETED','transfer','TR-00009',{user:'gaganlambasanki',device:'System migration',nature:before&&before.fromNature||'SANKI',account:before&&before.fromAccount||'Axis Bank 3448',before,after:null,note:'Owner confirmed removal from both account ledgers; ADV-00001 remains the salary advance record'});s.oneTimeMigrations[removeTr9Key]={appliedAt:new Date().toISOString(),result:before?'deleted':'not_found',before};saveStore(s);}
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
    const counterCashResetKey='counter-cash-reset-2026-08-22-opening-5240';
    if(!s.oneTimeMigrations[counterCashResetKey]){
      const before=num((s.openingBalances||{})[DEFAULT_COUNTER_CASH]);s.openingBalances=s.openingBalances||{};s.openingBalanceDates=s.openingBalanceDates||{};s.openingBalances[DEFAULT_COUNTER_CASH]=5240;s.openingBalanceDates[DEFAULT_COUNTER_CASH]=COUNTER_CASH_RESET_DATE;
      s.oneTimeMigrations[counterCashResetKey]={appliedAt:new Date().toISOString(),account:DEFAULT_COUNTER_CASH,effectiveDate:COUNTER_CASH_RESET_DATE,before,after:5240,rule:'Exclude all earlier Counter Cash movements; round cash sales upward to ₹10'};
      audit(s,null,'COUNTER_CASH_RESET','account',DEFAULT_COUNTER_CASH,{user:'gaganlambasanki',device:'System migration',nature:'SANKI',account:DEFAULT_COUNTER_CASH,before:{opening:before},after:{opening:5240,effectiveDate:COUNTER_CASH_RESET_DATE},note:'Owner-authorized Counter Cash reset'});saveStore(s);
    }
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
    const telegramAmountKey='correct-ex-00044-telegram-ocr-35121-to-5121';
    if(!s.oneTimeMigrations[telegramAmountKey]){
      const e=s.expenses&&s.expenses['EX-00044'];let result='not_found_or_already_changed';
      if(e&&normalizedNature(e.nature)==='PERSONAL'&&num(e.amount)===35121&&e.paidAlready){
        const before=JSON.parse(JSON.stringify(e));
        e.amount=5121;e.requestedAmount=5121;e.personalPaidAmount=5121;e.paidAmount=5121;
        (e.payments||[]).filter(p=>p.personalFunds).forEach(p=>{p.amount=5121;});
        e.telegramNeedsReview=false;
        e.auditHistory=Array.isArray(e.auditHistory)?e.auditHistory:[];
        e.auditHistory.push({id:'EDIT-'+String(e.auditHistory.length+1).padStart(3,'0'),reason:'Correct Telegram OCR amount from ₹35,121 to ₹5,121',changes:[{field:'amount',before:35121,after:5121},{field:'requestedAmount',before:35121,after:5121},{field:'paidAmount',before:35121,after:5121},{field:'personalPaidAmount',before:35121,after:5121}],editedBy:'gaganlambasanki',editedAt:new Date().toISOString()});
        audit(s,null,'EDITED','expense',e.id,{user:'gaganlambasanki',device:'System migration',nature:'PERSONAL',before,after:e,note:'Owner confirmed Telegram OCR read ₹35,121 instead of ₹5,121'});
        result='corrected';
      }
      s.oneTimeMigrations[telegramAmountKey]={appliedAt:new Date().toISOString(),expenseId:'EX-00044',from:35121,to:5121,result};
      saveStore(s);
    }
    if(bankChargeRepairAdded)saveStore(s);
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
  const isNamita=String(b.username||'').trim().toLowerCase()==='namita',requested=String(isNamita?(/cash/i.test(String(b.account||''))?'Namita Cash':'Namita 5464'):b.account||'').trim().toLowerCase(),accounts=companyAccountsForNature('PERSONAL');
  const suffixMatches=accounts.filter(a=>{const last=(a.match(/\d{4}$/)||[])[0];return last&&requested.length>=2&&last.endsWith(requested.replace(/\D/g,''));});
  const account=(isNamita&&/cash/i.test(requested)?'Namita Cash':'')||(/cash/i.test(requested)&&accounts.find(a=>/cash/i.test(a)))||accounts.find(a=>a.toLowerCase()===requested)||accounts.find(a=>requested&&a.toLowerCase().includes(requested))||accounts.find(a=>{const last=(a.match(/\d{4}$/)||[])[0];return last&&requested.includes(last);})||(suffixMatches.length===1?suffixMatches[0]:'');
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
function telegramBusinessCategories() {
  const s=loadStore();
  return pickableLedgers(s).map(x=>x.name).filter(name=>!PERSONAL_CATEGORIES.includes(name));
}
function telegramSuggestBusinessCategory(text) {
  const q=String(text||'').toLowerCase(),rules=[
    ['PETROL',/petrol|diesel|fuel|cng/],['Rapido Expense',/rapido|uber|ola|auto|taxi|parking|travel/],
    ['FOOD EXPENSE',/food|lunch|dinner|breakfast|tea|coffee|restaurant|snack/],['REPAIR & MAINTANCE',/repair|service|electrician|plumber|carpenter|maintenance/],
    ['CLEANING EXP',/clean|housekeeping|flower|phool|mala/],['STATIONERY',/stationery|paper|pen|office supply/],
    ['MARKETING EXPENSE',/marketing|advertis|meta|facebook|instagram|shoot|model/],['COD COURIER CHARGES',/courier|freight|delivery|transport|logistic/]
  ],available=telegramBusinessCategories();
  for(const [wanted,re] of rules)if(re.test(q)){const exact=available.find(x=>x.toLowerCase()===wanted.toLowerCase())||available.find(x=>x.toLowerCase().includes(wanted.split(' ')[0].toLowerCase()));if(exact)return{ledger:exact,confidence:true};}
  const direct=available.find(x=>q.includes(x.toLowerCase()));return{ledger:direct||'',confidence:!!direct};
}
// Screenshot-first capture for a new SANKI/SAMAST expense that an authorised
// accounts user has already paid. It is created, approved and posted only after
// Telegram shows the final preview and the user confirms it.
function createTelegramBusinessPaidExpense(input) {
  const b=input||{},s=loadStore(),amount=num(b.amount),proof=String(b.proof||'').trim(),actor=String(b.username||'').trim(),nature=normalizedNature(b.nature||'SANKI');
  if(nature==='PERSONAL')return{success:false,error:'Use the private PERSONAL bot for PERSONAL expenses.'};
  if(!(amount>0))return{success:false,error:'Amount must be greater than 0.'};
  if(!proof)return{success:false,error:'Payment screenshot is required.'};
  const account=(USER_PAYMENT_ACCOUNTS[String(actor).toLowerCase()]||[]).find(x=>x.toLowerCase()===String(b.account||'').toLowerCase());
  if(!account)return{success:false,error:'Select one of your assigned paying accounts.'};
  const categories=telegramBusinessCategories(),ledger=categories.find(x=>x.toLowerCase()===String(b.ledger||'').trim().toLowerCase());
  if(!ledger)return{success:false,error:'Select a valid expense category.',needsCategory:true};
  const particulars=String(b.particulars||b.vendor||'').trim(),vendor=String(b.vendor||particulars).trim();
  if(!vendor)return{success:false,error:'Vendor / payee is required.'};
  const sourceKey=String(b.sourceKey||'').trim();if(sourceKey){const duplicate=Object.values(s.expenses||{}).find(e=>e.telegramSourceKey===sourceKey);if(duplicate)return{success:true,duplicate:true,expense:duplicate};}
  const now=new Date().toISOString(),date=String(b.date||now.slice(0,10)).slice(0,10),paymentType=/cash/i.test(account)?'Cash':'UPI';s.seq=(s.seq||0)+1;const id='EX-'+String(s.seq).padStart(5,'0');
  const payment={id:'PAY-001',amount,date,account,paymentType,proof,note:'New paid expense captured through Telegram',paidBy:actor,paidAt:now,personalFunds:false};
  const expense={id,date,particulars,amount,isInstallment:false,requestedAmount:amount,nature,type:defaultType(ledger),ledger,vendor,claimant:actor,account,channel:'Shared',bill:'printed',fundedBy:'company',paymentType,qrPhoto:'',billPhoto:proof,purchasePaymentProof:'',exceptionEvidence:'',exceptionReason:'',billNote:'Payment proof captured through Telegram',paidAlready:false,personalPaidAmount:0,reimbursementStatus:'not_applicable',reimbursementAmount:0,reimbursementPayments:[],paymentProof:proof,status:'paid',paidAmount:amount,payments:[payment],createdAt:now,createdBy:actor,approvedAt:now,approvedBy:actor,paidAt:now,paidBy:actor,telegramSourceKey:sourceKey,telegramOcrText:String(b.ocrText||'').slice(0,4000)};
  s.expenses[id]=expense;s.vendorsByNature=s.vendorsByNature||{};if(nature==='SANKI')s.vendors[vendor.toLowerCase()]=s.vendors[vendor.toLowerCase()]||{name:vendor,notes:'Added from Telegram capture'};else{s.vendorsByNature[nature]=s.vendorsByNature[nature]||{};s.vendorsByNature[nature][vendor.toLowerCase()]=s.vendorsByNature[nature][vendor.toLowerCase()]||{name:vendor,notes:'Added from Telegram capture'};}
  audit(s,null,'CREATED','expense',id,{nature,user:actor,device:'Telegram',after:expense,note:'Screenshot-first paid expense'});audit(s,null,'APPROVED','expense',id,{nature,user:actor,device:'Telegram',after:{status:'paid',approvedBy:actor,amount}});audit(s,null,'PAYMENT_RECORDED','expense',id,{nature,user:actor,device:'Telegram',account,paymentId:payment.id,after:payment});saveStore(s);return{success:true,expense};
}
function telegramExpense(id){const e=loadStore().expenses[id];return e?JSON.parse(JSON.stringify(e)):null;}
function telegramResolveAccount(nature,requested){const q=String(requested||'').toLowerCase(),accounts=companyAccountsForNature(normalizedNature(nature)),digits=q.replace(/\D/g,'');return(/cash/.test(q)&&accounts.find(a=>/cash/i.test(a)))||accounts.find(a=>a.toLowerCase()===q)||accounts.find(a=>digits&&a.replace(/\D/g,'').endsWith(digits))||'';}
function telegramResolveTransferAccount(requested,preferredNature){const raw=String(requested||'').trim(),tag=raw.match(/^(SANKI|SAMAST|PERSONAL)\s+(.+)$/i),explicit=tag&&normalizedNature(tag[1]),q=String(tag?tag[2]:raw).trim().toLowerCase(),digits=q.replace(/\D/g,''),matches=[];NATURES.forEach(nature=>transferAccountsForNature(nature).forEach(account=>{if(account.toLowerCase()===q||(digits&&account.replace(/\D/g,'').endsWith(digits)))matches.push({nature,account});}));const wanted=explicit||preferredNature&&normalizedNature(preferredNature),scoped=wanted?matches.filter(x=>x.nature===wanted):matches;return scoped.length===1?scoped[0]:null;}
function telegramRecordTransfer(actor,body){const b=body||{},s=loadStore();let from=telegramResolveTransferAccount(b.fromAccount),to=telegramResolveTransferAccount(b.toAccount);if(from&&!to)to=telegramResolveTransferAccount(b.toAccount,from.nature);if(to&&!from)from=telegramResolveTransferAccount(b.fromAccount,to.nature);const amount=num(b.amount),proof=String(b.proof||'').trim();if(!from||!to)return{success:false,error:'One account was not recognized or is ambiguous. Add SANKI, SAMAST or PERSONAL before a shared account when needed.'};if(from.nature===to.nature&&from.account.toLowerCase()===to.account.toLowerCase())return{success:false,error:'Source and destination accounts must be different.'};if(!(amount>0))return{success:false,error:'Transfer amount must be greater than 0.'};if(!proof)return{success:false,error:'Transfer proof is required.'};const classification=String(b.classification||(from.nature===to.nature?'internal_transfer':'inter_entity_loan'));s.transferSeq=(s.transferSeq||0)+1;const now=new Date().toISOString(),transfer={id:'TR-'+String(s.transferSeq).padStart(5,'0'),nature:from.nature,fromNature:from.nature,toNature:to.nature,classification,fromAccount:from.account,toAccount:to.account,amount,date:String(b.date||now.slice(0,10)).slice(0,10),proof,note:String(b.note||'').trim(),createdBy:String(actor||'admin'),createdAt:now,device:'Telegram'};s.transfers=Array.isArray(s.transfers)?s.transfers:[];s.transfers.push(transfer);audit(s,null,'TRANSFER_RECORDED','transfer',transfer.id,{user:transfer.createdBy,device:'Telegram',nature:from.nature,account:from.account,after:transfer});saveStore(s);return{success:true,transfer};}
function telegramRecordNamitaTransfer(actor,body){const b=body||{},s=loadStore(),q=String(b.fromAccount||''),digits=q.replace(/\D/g,''),personal=companyAccountsForNature('PERSONAL'),personalMatches=personal.filter(a=>digits&&a.replace(/\D/g,'').endsWith(digits)),fromPersonal=personal.find(a=>a.toLowerCase()===q.toLowerCase())||(personalMatches.length===1?personalMatches[0]:''),fallback=telegramResolveTransferAccount(q),from=fromPersonal?{nature:'PERSONAL',account:fromPersonal}:fallback,toAccount=/cash/i.test(String(b.toAccount||''))?'Namita Cash':'Namita 5464',amount=num(b.amount),proof=String(b.proof||'').trim();if(!from)return{success:false,error:'The source account was not recognized.'};if(!(amount>0))return{success:false,error:'Transfer amount must be greater than 0.'};if(!proof)return{success:false,error:'Transfer proof is required.'};s.transferSeq=(s.transferSeq||0)+1;const now=new Date().toISOString(),classification=from.nature==='PERSONAL'?'internal_transfer':'owner_withdrawal',transfer={id:'TR-'+String(s.transferSeq).padStart(5,'0'),nature:from.nature,fromNature:from.nature,toNature:'PERSONAL',classification,fromAccount:from.account,toAccount,amount,date:String(b.date||now.slice(0,10)).slice(0,10),proof,note:String(b.note||'Namita funds').trim(),createdBy:String(actor||'owner'),createdAt:now,device:'Telegram'};s.transfers=Array.isArray(s.transfers)?s.transfers:[];s.transfers.push(transfer);audit(s,null,'TRANSFER_RECORDED','transfer',transfer.id,{user:transfer.createdBy,device:'Telegram',nature:from.nature,account:from.account,after:transfer});saveStore(s);return{success:true,transfer};}
function telegramApproveExpense(id,actor,changes){const s=loadStore(),e=s.expenses[id];if(!e)return{success:false,error:'Expense not found.'};if(e.status!=='pending')return{success:false,error:'This expense is already '+e.status+'.',expense:e};const before=JSON.parse(JSON.stringify(e)),c=changes||{};['particulars','vendor','ledger','type','paymentType'].forEach(k=>{if(c[k]!=null&&String(c[k]).trim())e[k]=String(c[k]).trim();});if(c.amount!=null&&num(c.amount)>0){e.amount=num(c.amount);e.requestedAmount=e.isInstallment?Math.min(num(e.requestedAmount)||e.amount,e.amount):e.amount;}if(c.nature)e.nature=normalizedNature(c.nature);if(e.ledger&&!pickableLedgers(s).some(x=>x.name.toLowerCase()===e.ledger.toLowerCase())){s.customLedgers[e.ledger]={name:e.ledger,type:TYPES.includes(e.type)?e.type:'variable'};}const changed=['nature','particulars','vendor','ledger','type','paymentType','amount','requestedAmount'].some(k=>JSON.stringify(before[k])!==JSON.stringify(e[k]));if(changed)audit(s,null,'EDITED','expense',id,{user:actor,device:'Telegram',nature:e.nature,before,after:e,note:'Edited during Telegram approval'});if(e.bill==='none'||!e.billPhoto)return{success:false,error:'This expense needs bill-exception review in the app before approval.',appRequired:true,expense:e};if(!e.vendor)return{success:false,error:'Vendor is required.',expense:e};if(!e.ledger)return{success:false,error:'Add a category before approving.',needsCategory:true,expense:e};const n=normalizedNature(e.nature);s.vendors=s.vendors||{};s.vendorsByNature=s.vendorsByNature||{};if(n==='SANKI'){s.vendors[e.vendor.toLowerCase()]=s.vendors[e.vendor.toLowerCase()]||{name:e.vendor,notes:''};}else{s.vendorsByNature[n]=s.vendorsByNature[n]||{};s.vendorsByNature[n][e.vendor.toLowerCase()]=s.vendorsByNature[n][e.vendor.toLowerCase()]||{name:e.vendor,notes:''};}e.status=num(e.paidAmount)>=num(e.amount)?'paid':num(e.paidAmount)>0?'partially_paid':'approved';if(e.paidAlready)e.reimbursementStatus='pending';e.approvedAt=new Date().toISOString();e.approvedBy=actor;audit(s,null,'APPROVED','expense',id,{user:actor,device:'Telegram',nature:e.nature,after:{status:e.status,approvedBy:actor,amount:e.amount}});saveStore(s);notifyExpenseUser(e,'approved');return{success:true,expense:e};}
function telegramRejectExpense(id,actor,reason){const s=loadStore(),e=s.expenses[id];if(!e)return{success:false,error:'Expense not found.'};if(e.status!=='pending')return{success:false,error:'Only a pending expense can be rejected.'};e.status='rejected';e.rejectReason=String(reason||'Rejected from Telegram');e.rejectedAt=new Date().toISOString();e.rejectedBy=actor;audit(s,null,'REJECTED','expense',id,{user:actor,device:'Telegram',nature:e.nature,after:{status:e.status,reason:e.rejectReason}});saveStore(s);notifyExpenseUser(e,'rejected');return{success:true,expense:e};}
function telegramRecordPayment(id,actor,b){const s=loadStore(),e=s.expenses[id],body=b||{};if(!e)return{success:false,error:'Expense not found.'};if(!['approved','partially_paid'].includes(e.status)||e.paidAlready)return{success:false,error:'This expense is not awaiting a vendor payment.'};const proof=String(body.proof||'');if(!proof)return{success:false,error:'Payment screenshot is required.'};const account=telegramResolveAccount(e.nature,body.account);if(!account)return{success:false,error:'Paying account was not recognized.',needsAccount:true};const issues=reconciliationIssues(s,normalizedNature(e.nature),account);if(issues.length)return{success:false,error:'This account has a reconciliation warning. Complete this payment in the app.',appRequired:true};const outstanding=Math.max(0,num(e.amount)-num(e.paidAmount)),amount=body.amount!=null?num(body.amount):outstanding;if(!(amount>0)||amount>outstanding)return{success:false,error:'Payment must be between ₹0 and '+outstanding+'.'};e.account=account;e.paidAmount=num(e.paidAmount)+amount;e.paymentProof=proof;e.payments=Array.isArray(e.payments)?e.payments:[];e.payments.push({id:'PAY-'+String(e.payments.length+1).padStart(3,'0'),amount,date:String(body.date||new Date().toISOString().slice(0,10)).slice(0,10),account,paymentType:'UPI',proof,note:'Recorded through Telegram',paidBy:actor,paidAt:new Date().toISOString()});e.status=e.paidAmount>=num(e.amount)?'paid':'partially_paid';e.paidAt=new Date().toISOString();e.paidBy=actor;audit(s,null,'PAYMENT_RECORDED','expense',id,{user:actor,device:'Telegram',nature:e.nature,account,paymentId:e.payments.at(-1).id,after:e.payments.at(-1)});saveStore(s);notifyExpenseUser(e,e.status==='paid'?'paid':'partially_paid',amount);return{success:true,expense:e,payment:e.payments.at(-1)};}
// Runs an existing synchronous accounting route for a linked Telegram user.
// This keeps validation, permissions, ledger posting and audit behavior identical
// between Telegram and the web app instead of maintaining two accounting engines.
function telegramApi(method, routePath, target, input) {
  const layer=router.stack.find(item=>item.route&&item.route.path===routePath&&item.route.methods[String(method||'POST').toLowerCase()]);
  if(!layer)return{success:false,error:'Accounting action is unavailable.'};
  const o=input||{},req={body:o.body||{},params:o.params||{},query:o.query||{},headers:{'user-agent':'Telegram','x-forwarded-for':'Telegram'},ip:'Telegram',user:{username:String(target&&target.username||target||''),roles:(target&&target.roles)||[],role:((target&&target.roles)||[])[0]||''},get(name){return this.headers[String(name).toLowerCase()]||'';}};
  let status=200,result={success:false,error:'Accounting action did not return a result.'};
  const res={status(code){status=code;return this;},json(value){result=value;return this;},end(){return this;},sendFile(){return this;}};
  try{layer.route.stack[0].handle(req,res,()=>{});}catch(e){return{success:false,error:String(e.message||e),status:500};}
  return Object.assign({status},result||{});
}
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
    (local.sales || []).filter(x => !x.voided).forEach(x => {const cash=String(x.paymentMode||'').toLowerCase()==='cash';rows.push({ id:'SALE/'+x.id, date:x.day||String(x.ts||'').slice(0,10), account:cash?DEFAULT_COUNTER_CASH:DEFAULT_SALES_BANK, amount:cash?roundCashSale(x.total):num(x.total), originalAmount:num(x.total), description:'Sale · '+(x.channel||'POS')+' · '+(x.staff||'') });});
  } catch { /* no local-sales store yet */ }
  try {
    const shop = JSON.parse(fs.readFileSync(ORDERS_PATH, 'utf8'));
    Object.values(shop.orders || {}).filter(x => !x.cancelledAt && String(x.financialStatus||'').toLowerCase()==='paid').forEach(x => {
      const gateways=(x.paymentGateways||[]).join(' ').toLowerCase(),cash=gateways.includes('cash'),storeCredit=/store\s*credit|gift\s*card/.test(gateways),paytm=/paytm/.test(gateways),orderNo=String(x.orderNumber||x.name||x.id||'').replace(/\D/g,'').replace(/^0+/,''),gross=num(x.total)-num(x.refundAmount);
      // These are accounting exclusions only. This module only reads the cached
      // Shopify order and never changes the customer's Shopify store-credit balance.
      if(orderNo==='2720')return; // owner-confirmed test order: not genuine revenue
      if(orderNo==='2717')return; // owner-confirmed fully store-credit-funded order
      if(storeCredit&&!paytm&&!cash)return; // no fresh money entered a bank/gateway
      const freshPaytm=num(x.paytmAmount||x.paytmPaidAmount||x.freshPaymentAmount)||(paytm?gross:0);
      const amount=cash?roundCashSale(gross):freshPaytm;
      if(!(amount>0))return;
      rows.push({ id:'SHOPIFY/'+x.id, orderId:String(x.id), orderNumber:orderNo||String(x.name||x.id), date:String(x.processedAt||x.createdAt||'').slice(0,10), account:cash?DEFAULT_COUNTER_CASH:PAYTM_CLEARING_ACCOUNT, amount, gross, storeCreditUsed:Math.max(0,gross-amount), paymentGateways:x.paymentGateways||[], description:'Shopify sale · '+(x.name||x.id)+' · '+(x.channel||'') });
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
  (s.transfers || []).forEach(x => { if (x.fromAccount) names.add(String(x.fromAccount)); if (x.toAccount&&x.classification!=='credit_card_payment') names.add(String(x.toAccount)); });
  Object.values(s.expenses || {}).forEach(e => {
    if (e.account) names.add(String(e.account));
    (e.payments || []).forEach(p => { if (p.account) names.add(String(p.account)); });
    (e.reimbursementPayments || []).forEach(p => { if (p.account) names.add(String(p.account)); });
  });
  salaryAdvanceEntries().forEach(x => { if (x.account) names.add(x.account); });
  return Array.from(names).map(x => String(x).trim()).filter(x => x && x !== '(unspecified)').sort((a, b) => a.localeCompare(b));
}
function salaryAdvanceEntries() {
  try {
    const sal = JSON.parse(fs.readFileSync(path.join(DATA_DIR, 'salary.json'), 'utf8'));
    return Object.values(sal.advances || {}).filter(a => a.active !== false && num(a.amount) > 0).map(a => ({ id:a.id,date:a.date,account:a.account,amount:num(a.amount),employee:a.employeeName||((sal.employees||{})[a.empId]||{}).name||a.empId,proof:a.proof||'',note:a.note||'',by:a.createdBy||'' }));
  } catch (_) { return []; }
}
function salaryPaymentEntries(){try{const sal=JSON.parse(fs.readFileSync(path.join(DATA_DIR,'salary.json'),'utf8'));return (sal.salaryPayments||[]).filter(p=>p.active!==false&&num(p.amount)>0);}catch(_){return[];}}
function companyAccountsForNature(nature) {
  return (ENTITY_ACCOUNTS[normalizedNature(nature)] || []).slice();
}
function transferAccountsForNature(nature) {
  const n=normalizedNature(nature),accounts=companyAccountsForNature(n);
  if(n==='SANKI'||n==='SAMAST') ['arshpreet','shivam','pradeep'].forEach(username=>accounts.push(...(CLAIMANT_ACCOUNTS[username]||[])));
  return Array.from(new Set(accounts));
}
function personalAccountsForReq(req) {
  const username = String((req.user && req.user.username) || '').trim().toLowerCase();
  const roles = rolesOfReq(req);
  if (roles.includes('owner')) return Array.from(new Set([].concat(...Object.values(ENTITY_ACCOUNTS), ...Object.values(CLAIMANT_ACCOUNTS))));
  if (roles.includes('admin')) return (USER_PAYMENT_ACCOUNTS[username] || []).slice();
  return (CLAIMANT_ACCOUNTS[username] || []).slice();
}
function payingAccountsForReq(req, nature) {
  const username = String((req.user && req.user.username) || '').trim().toLowerCase();
  if (isOwner(req)) return companyAccountsForNature(nature);
  const assigned = USER_PAYMENT_ACCOUNTS[username] || [];
  return companyAccountsForNature(nature).filter(account => assigned.some(name => name.toLowerCase() === account.toLowerCase()));
}
function allowedPayingAccount(req, nature, account) {
  const candidate = ACCOUNT_RENAMES[String(account || '')] || String(account || '');
  const allowed=payingAccountsForReq(req,nature),exact=allowed.find(name=>name.toLowerCase()===candidate.toLowerCase()),digits=candidate.replace(/\D/g,''),matches=allowed.filter(name=>digits&&name.replace(/\D/g,'').endsWith(digits));
  return exact||(matches.length===1?matches[0]:undefined);
}
function ledgerAccountsForNature(s, nature) {
  const n = normalizedNature(nature), names = new Set(companyAccountsForNature(n));
  if(n==='SANKI')names.add(PAYTM_CLEARING_ACCOUNT);
  // Keep claimant sub-ledgers visible for both operating entities even when
  // they currently have no movement or a prior entry has been removed.
  if (n === 'SANKI' || n === 'SAMAST') {
    ['arshpreet','shivam','pradeep'].forEach(username =>
      (CLAIMANT_ACCOUNTS[username] || []).forEach(account => names.add(account)));
  }
  Object.values(s.expenses || {}).filter(e => normalizedNature(e.nature) === n).forEach(e => {
    if (e.account) names.add(String(e.account));
    (e.payments || []).forEach(p => { if (p.account) names.add(String(p.account)); });
    (e.reimbursementPayments || []).forEach(p => { if (p.account) names.add(String(p.account)); });
  });
  (s.adjustments || []).filter(x => normalizedNature(x.nature) === n).forEach(x => names.add(String(x.account)));
  (s.receipts || []).filter(x => normalizedNature(x.nature) === n).forEach(x => names.add(String(x.account)));
  (s.transfers || []).forEach(x => {
    if (normalizedNature(x.fromNature || x.nature) === n) names.add(String(x.fromAccount));
    if (x.classification!=='credit_card_payment'&&normalizedNature(x.toNature || x.nature) === n) names.add(String(x.toAccount));
  });
  return Array.from(names).filter(Boolean).sort((a,b)=>a.localeCompare(b));
}
function allowedCompanyAccount(s, nature, account) {
  const candidate = ACCOUNT_RENAMES[String(account || '')] || String(account || '');
  const allowed=companyAccountsForNature(nature),exact=allowed.find(name=>name.toLowerCase()===candidate.toLowerCase()),digits=candidate.replace(/\D/g,''),matches=allowed.filter(name=>digits&&name.replace(/\D/g,'').endsWith(digits));
  return exact||(matches.length===1?matches[0]:undefined);
}
function allowedTransferAccount(nature, account) {
  const candidate=ACCOUNT_RENAMES[String(account||'')]||String(account||''),allowed=transferAccountsForNature(nature),exact=allowed.find(name=>name.toLowerCase()===candidate.toLowerCase()),digits=candidate.replace(/\D/g,''),matches=allowed.filter(name=>digits&&name.replace(/\D/g,'').endsWith(digits));
  return exact||(matches.length===1?matches[0]:undefined);
}
function allowedReimbursementAccount(req, account) {
  const candidate = ACCOUNT_RENAMES[String(account || '')] || String(account || '');
  const allowed = Array.from(new Set([].concat(...approvalNatures(req).map(n => payingAccountsForReq(req, n)))));
  const exact=allowed.find(name=>name.toLowerCase()===candidate.toLowerCase()),digits=candidate.replace(/\D/g,''),matches=allowed.filter(name=>digits&&name.replace(/\D/g,'').endsWith(digits));
  return exact||(matches.length===1?matches[0]:undefined);
}
function rolesOfReq(req) {
  return (req.user && (req.user.roles || (req.user.role ? [req.user.role] : []))) || [];
}
function isOwner(req){return rolesOfReq(req).includes('owner');}
function isAdmin(req) { const r = rolesOfReq(req); return r.includes('admin') || r.includes('owner'); }
function bankStatementBookKey(nature,account){const n=normalizedNature(nature);return n==='PERSONAL'?'PERSONAL|'+String(account||''):String(account||'');}
function canAccessBankReconciliation(req,s,nature,account){const n=normalizedNature(nature),name=String(account||'');if(!isAdmin(req)||!approvalNatures(req).includes(n))return false;if(n==='PERSONAL'&&!isOwner(req))return false;return ledgerAccountsForNature(s,n).some(x=>x.toLowerCase()===name.toLowerCase())&&!/cash/i.test(name);}
function canAccessBankDraft(req,s,draft){return !!draft&&canAccessBankReconciliation(req,s,draft.nature,draft.account);}
// Who may APPROVE & PAY: admin or accounting. A pure claimant may only LOG.
function canApprove(req) { const r = rolesOfReq(req); return r.includes('admin') || r.includes('accounting') || r.includes('samast_accounting') || r.includes('owner'); }
function normalizedNature(v) { const n = String(v || '').toUpperCase(); return n === 'SAMAST' || n === 'PERSONAL' ? n : 'SANKI'; }
function approvalNatures(req) {
  const r = rolesOfReq(req);
  if (r.includes('owner')) return NATURES.slice();
  const out = [];
  // PERSONAL is a private owner book. Admins may operate the business books,
  // but must never receive PERSONAL rows through list/report/ledger APIs.
  if (r.includes('admin')) out.push('SANKI', 'SAMAST');
  if (r.includes('accounting')) out.push('SANKI');
  if (r.includes('samast_accounting')) out.push('SAMAST');
  return Array.from(new Set(out));
}
function submissionNatures(req) {
  const r = rolesOfReq(req);
  if (r.includes('owner')) return NATURES.slice();
  const out = approvalNatures(req);
  if (r.includes('admin')) out.push('SANKI', 'SAMAST');
  if (r.includes('claimant')) out.push('SANKI', 'SAMAST', 'PERSONAL');
  if (r.includes('personal_claimant')) out.push('PERSONAL');
  return Array.from(new Set(out));
}
function canApproveExpenseNature(req, e) { return approvalNatures(req).includes(normalizedNature(e && e.nature)); }
function canViewExpense(req, e) {
  if (normalizedNature(e && e.nature) === 'PERSONAL') return isOwner(req);
  if (canApproveExpenseNature(req, e)) return true;
  return e && e.createdBy === (req.user && req.user.username);
}
// Some legacy paid records have no approvedAt timestamp even though their
// finalized status and payment history prove that they were posted.
function paymentIsPosted(e){return!!(e&&e.approvedAt)||['approved','partially_paid','paid'].includes(String(e&&e.status||''));}

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

// Preserve a readable, immutable reconciliation report after the existing
// finalization workflow succeeds. This deliberately records the user-facing
// action (for example "Ledger created") instead of leaking internal posting
// terminology into the audit report.
router.use((req,res,next)=>{
  if(req.method!=='POST'||req.path!=='/api/expenses/bank-statements/finalize')return next();
  const before=loadStore(),draftId=String(req.body&&req.body.draftId||''),draft=(before.bankReconciliationDrafts||{})[draftId];
  if(!draft)return next();
  const snapshot=draftReconciliation(before,draft),originalJson=res.json.bind(res);
  res.json=payload=>{
    if(payload&&payload.success){
      try{
        const after=loadStore(),book=(after.bankStatements||{})[bankStatementBookKey(draft.nature,draft.account)],record=book&&(book.imports||[]).slice().sort((a,b)=>String(b.finalizedAt||'').localeCompare(String(a.finalizedAt||'')))[0];
        if(record&&!record.reconciliationRows){
          const statementRows=Object.values(book.transactions||{}).filter(x=>x.firstSeenImport===record.id),adjustments=(after.adjustments||[]).filter(x=>x.reconciliationDraft===draft.id),settlements=(after.paytmSettlements||[]).filter(x=>x.reconciliationDraft===draft.id);
          const bankTransaction=x=>x&&statementRows.find(t=>t.date===x.date&&Math.abs(num(t.debit)-num(x.debit))<.01&&Math.abs(num(t.credit)-num(x.credit))<.01&&String(t.reference||t.description||'')===String(x.reference||x.description||''));
          record.statementSummary=Object.assign({},draft.summary);record.reconciliationSummary=Object.assign({},snapshot.summary);record.ledgerClosingBalance=snapshot.ledgerClosing;record.openingResolution=draft.openingResolution||null;
          record.reconciliationRows=snapshot.rows.map(x=>{
            const r=x.resolution||{},tx=bankTransaction(x.bank),adjustment=x.bank&&adjustments.find(a=>a.date===x.bank.date&&Math.abs(num(a.amount)-(num(x.bank.credit)-num(x.bank.debit)))<.01),settlement=x.bank&&settlements.find(a=>a.date===x.bank.date&&Math.abs(num(a.netAmount)-num(x.bank.credit))<.01);
            let decision='Reviewed',linked=[];
            if(x.status==='matched')decision='Matched automatically';
            else if(r.action==='accept_match')decision='Matched with ledger entry';
            else if(r.action==='create_adjustment'){decision=r.category?'Ledger created':'Balance entry recorded';if(adjustment)linked.push(adjustment.id);}
            else if(r.action==='paytm_settlement'){decision='Paytm settlement recorded';if(settlement)linked.push(settlement.id);}
            else if(r.action==='timing_difference')decision='Timing difference';
            else if(r.action==='exclude')decision='Excluded';
            if(x.app&&x.app.id&&!linked.includes(x.app.id))linked.push(x.app.id);
            return{id:x.id,originalStatus:x.originalStatus||x.status,bank:x.bank?Object.assign({},x.bank,{transactionId:tx&&tx.id||''}):null,ledger:x.app?Object.assign({},x.app):null,decision,reason:r.reason||'',category:r.category||'',resolvedBy:r.by||record.finalizedBy,resolvedAt:r.at||record.finalizedAt,linkedRecordIds:linked,orderIds:r.orderIds||[]};
          });
          saveStore(after);
        }
      }catch{/* Finalization remains successful even if report enrichment fails. */}
    }
    return originalJson(payload);
  };
  next();
});

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
  const ownerView = isOwner(req);
  const pendingReqs = (s.requests || []).filter(r => r.status === 'pending' && approvalNatures(req).includes(normalizedNature(r.nature)));
  const visiblePendingReqs = isAdmin(req) ? pendingReqs : (canApprove(req) ? pendingReqs.filter(r => r.kind === 'vendor') : []);
  const vendorsByNature = { SANKI: Object.values(s.vendors).map(v => v.name), SAMAST: Object.values(((s.vendorsByNature || {}).SAMAST) || {}).map(v => v.name), PERSONAL: ownerView ? Object.values(((s.vendorsByNature || {}).PERSONAL) || {}).map(v => v.name) : [] };
  // Only approved master vendors are reusable. A name typed by a claimant is
  // promoted into this list only when the related expense is approved.
  Object.keys(vendorsByNature).forEach(n => vendorsByNature[n].sort((a, b) => a.localeCompare(b)));
  res.json({
    success: true,
    ledgers: pickableLedgers(s),
    vendors: vendorsByNature.SANKI,
    vendorsByNature,
    accounts: Array.from(new Set([].concat(...allowed.map(n => n === 'PERSONAL' && !ownerView ? personalAccountsForReq(req) : ENTITY_ACCOUNTS[n])))),
    accountsByNature: Object.fromEntries(NATURES.map(n => [n, n === 'PERSONAL' ? (allowed.includes(n) ? (ownerView ? ENTITY_ACCOUNTS[n] : personalAccountsForReq(req)) : []) : (allowed.includes(n) ? ENTITY_ACCOUNTS[n] : [])])),
    bankAccountsByNature: Object.fromEntries(NATURES.map(n => [n, approvalNatures(req).includes(n) && (n !== 'PERSONAL' || ownerView) ? ledgerAccountsForNature(s,n).filter(name => !/cash/i.test(name)) : []])),
    ledgerAccountsByNature: Object.fromEntries(NATURES.map(n => [n, allowed.includes(n) ? ledgerAccountsForNature(s,n) : []])),
    transferAccountsByNature: Object.fromEntries(NATURES.map(n => [n, approvalNatures(req).includes(n) ? transferAccountsForNature(n) : []])),
    payingAccountsByNature: Object.fromEntries(NATURES.map(n => [n, payingAccountsForReq(req,n)])),
    personalAccounts: personalAccountsForReq(req), people: Array.from(new Set([].concat(s.people||[],Object.values(s.expenses||{}).map(e=>e.createdBy||e.claimant).filter(Boolean)))).sort((a,b)=>a.localeCompare(b)),
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
  if (!canViewExpense(req, e)) return res.status(403).json({ success: false, error: normalizedNature(e.nature)==='PERSONAL'?'Only the Owner can view or edit PERSONAL accounting data.':'You do not have access to this accounting entity.' });
  if (!canApprove(req) && (e.createdBy !== (req.user && req.user.username) || e.status !== 'pending')) {
    return res.status(403).json({ success: false, error: 'You can only edit your own pending expenses.' });
  }
  const b = req.body || {};
  const finalized = e.status !== 'pending';
  if (finalized && !isAdmin(req)) return res.status(403).json({ success:false, error:'Only Owner or Admin can edit an approved or paid expense.' });
  const editReason = String(b.editReason || '').trim();
  if (finalized && !editReason) return res.status(400).json({ success:false, error:'Reason for editing an approved or paid expense is required.' });
  const beforeEdit = JSON.parse(JSON.stringify(e));
  const hasCompanyPayments=(e.payments||[]).some(p=>!p.personalFunds),hasReimbursements=(e.reimbursementPayments||[]).length>0||num(e.reimbursementAmount)>0;
  // A personally-paid amount is one accounting fact represented in the
  // expense and its linked personal payment. Owner/Admin may correct an OCR
  // mistake only while no company payment or reimbursement has followed; the
  // linked values are then updated together below and fully audited.
  const canCorrectPersonalPayment=isOwner(req)&&e.paidAlready&&!hasCompanyPayments&&!hasReimbursements;
  if (b.date != null) e.date = String(b.date).slice(0, 10);
  if (b.particulars != null) e.particulars = String(b.particulars).trim();
  if (b.amount != null) {
    const nextAmount = num(b.amount);
    if (!(nextAmount > 0)) return res.status(400).json({ success:false, error:'Expense amount must be greater than 0.' });
    if(nextAmount!==num(e.amount)&&!isOwner(req))return res.status(403).json({success:false,error:'Only the Owner can change an expense amount.'});
    if (nextAmount < num(e.paidAmount)&&!canCorrectPersonalPayment) return res.status(400).json({ success:false, error:'Expense amount cannot be lower than ₹'+round0(e.paidAmount)+' already paid.' });
    e.amount = nextAmount;
  }
  if (b.isInstallment != null) e.isInstallment = b.isInstallment === true || b.isInstallment === 'true';
  if (b.requestedAmount != null) {
    const requested = num(b.requestedAmount);
    if (!(requested > 0) || requested > e.amount) return res.status(400).json({ success: false, error: 'Requested payment must be greater than 0 and cannot exceed the total amount.' });
    if(requested!==num(e.requestedAmount||e.amount)&&!isOwner(req))return res.status(403).json({success:false,error:'Only the Owner can change the payment amount.'});
    if (e.paidAlready && requested < num(e.personalPaidAmount)&&!canCorrectPersonalPayment) return res.status(400).json({ success: false, error: 'Requested payment cannot be less than the amount already paid personally.' });
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
  if(b.paymentAccount!=null&&String(b.paymentAccount).trim()&&(e.payments||[]).some(p=>!p.personalFunds)){
    if(!isOwner(req))return res.status(403).json({success:false,error:'Only the Owner can change the account used for a recorded payment.'});
    const nextAccount=allowedCompanyAccount(s,e.nature,b.paymentAccount);
    if(!nextAccount)return res.status(400).json({success:false,error:'Select a paying account assigned to this accounting entity.'});
    const companyPayments=(e.payments||[]).filter(p=>!p.personalFunds),target=companyPayments.at(-1),oldAccount=target.account||e.account||'',batchId=target.batchPaymentId||'';
    if(nextAccount!==oldAccount){
      const affected=[];
      Object.values(s.expenses||{}).forEach(x=>(x.payments||[]).forEach(p=>{if(p.personalFunds)return;const same=p===target||(batchId&&p.batchPaymentId===batchId);if(!same)return;affected.push({expense:x,payment:p,before:p.account||x.account||''});p.account=nextAccount;if(x===e||((x.payments||[]).filter(q=>!q.personalFunds).at(-1)===p))x.account=nextAccount;}));
      affected.filter(x=>x.expense!==e).forEach(x=>audit(s,req,'PAYMENT_ACCOUNT_CHANGED','expense',x.expense.id,{nature:x.expense.nature,account:nextAccount,paymentId:x.payment.id,before:{account:x.before},after:{account:nextAccount},note:`Linked payment account corrected with ${e.id}${editReason?' · '+editReason:''}`}));
    }
  }
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
  const account = allowedPayingAccount(req, nature, String(b.account || '').trim());
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
  const allowedAccount = allowedPayingAccount(req, e.nature, account);
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
  const reimbursementAccount = allowedReimbursementAccount(req, b.account);
  if (!reimbursementAccount) return res.status(400).json({ success:false, error:'Select an authorised company or cash account for this reimbursement.' });
  const reimbursementAccountNatures=approvalNatures(req).filter(n=>companyAccountsForNature(n).some(a=>a.toLowerCase()===reimbursementAccount.toLowerCase()));
  e.reimbursementAmount = num(e.reimbursementAmount) + amount;
  e.reimbursementPayments = Array.isArray(e.reimbursementPayments) ? e.reimbursementPayments : [];
  e.reimbursementPayments.push({
    id: 'REIM-' + String(e.reimbursementPayments.length + 1).padStart(3, '0'), amount,
    date: String(b.date || new Date().toISOString().slice(0, 10)).slice(0, 10),
    account: reimbursementAccount, accountNatures:reimbursementAccountNatures, paymentType: PAYMENT_TYPES.includes(b.paymentType) ? b.paymentType : 'UPI',
    proof, note: String(b.note || '').trim(), paidBy: (req.user && req.user.username) || 'admin', paidAt: new Date().toISOString()
  });
  e.reimbursementStatus = e.reimbursementAmount >= e.personalPaidAmount ? 'reimbursed' : 'partially_reimbursed';
  audit(s,req,'REIMBURSEMENT_RECORDED','expense',e.id,{nature:e.nature,account:reimbursementAccount,paymentId:e.reimbursementPayments.at(-1).id,after:e.reimbursementPayments.at(-1)});
  saveStore(s);
  notifyExpenseUser(e, e.reimbursementStatus, amount);
  res.json({ success: true, expense: e });
});

// Reimburse several approved claimant expenses in one payment action. The
// common batch reference ties the individual ledger entries back to the same
// proof/payment without losing expense-level auditability.
router.post('/api/expenses/reimbursements/batch', (req, res) => {
  if (!canApprove(req)) return res.status(403).json({ success:false, error:'Only accounting/admin can reimburse.' });
  const s=loadStore(),b=req.body||{},ids=Array.from(new Set((Array.isArray(b.expenseIds)?b.expenseIds:[]).map(x=>String(x||'').trim()).filter(Boolean)));
  if (ids.length<2) return res.status(400).json({success:false,error:'Select at least two pending expenses to reimburse together.'});
  const proof=String(b.paymentProof||'').trim();
  if(!proof) return res.status(400).json({success:false,error:'Reimbursement payment proof is required.'});
  const reimbursementAccount=allowedReimbursementAccount(req,b.account);
  if(!reimbursementAccount) return res.status(400).json({success:false,error:'Select an authorised company or cash account for this reimbursement.'});
  const expenses=[];
  for(const id of ids){
    const e=s.expenses[id];
    if(!e) return res.status(404).json({success:false,error:'Expense '+id+' was not found.'});
    if(!canApproveExpenseNature(req,e)) return res.status(403).json({success:false,error:'You cannot reimburse '+id+' for this accounting entity.'});
    if(!['pending','partially_reimbursed'].includes(e.reimbursementStatus)) return res.status(400).json({success:false,error:id+' has no approved reimbursement pending.'});
    const due=Math.max(0,num(e.personalPaidAmount)-num(e.reimbursementAmount));
    if(!(due>0)) return res.status(400).json({success:false,error:id+' has no reimbursement amount due.'});
    expenses.push({e,due});
  }
  const date=String(b.date||new Date().toISOString().slice(0,10)).slice(0,10),paidAt=new Date().toISOString();
  const batchId='RB-'+paidAt.replace(/\D/g,'').slice(0,14)+'-'+Math.random().toString(36).slice(2,6).toUpperCase();
  const reimbursementAccountNatures=approvalNatures(req).filter(n=>companyAccountsForNature(n).some(a=>a.toLowerCase()===reimbursementAccount.toLowerCase()));
  expenses.forEach(({e,due})=>{
    e.reimbursementAmount=num(e.reimbursementAmount)+due;
    e.reimbursementPayments=Array.isArray(e.reimbursementPayments)?e.reimbursementPayments:[];
    const payment={id:'REIM-'+String(e.reimbursementPayments.length+1).padStart(3,'0'),batchId,amount:due,date,account:reimbursementAccount,accountNatures:reimbursementAccountNatures,paymentType:PAYMENT_TYPES.includes(b.paymentType)?b.paymentType:'UPI',proof,note:String(b.note||'').trim(),paidBy:(req.user&&req.user.username)||'admin',paidAt};
    e.reimbursementPayments.push(payment);e.reimbursementStatus='reimbursed';
    audit(s,req,'REIMBURSEMENT_RECORDED','expense',e.id,{nature:e.nature,account:reimbursementAccount,batchId,paymentId:payment.id,after:payment});
  });
  saveStore(s);
  expenses.forEach(({e,due})=>notifyExpenseUser(e,'reimbursed',due));
  res.json({success:true,batchId,expenseIds:ids,total:round0(expenses.reduce((n,x)=>n+x.due,0)),expenses:expenses.map(x=>x.e)});
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
  const payingAccount = (req.query.payingAccount || '').toString().trim().toLowerCase();
  const reference = (req.query.reference || '').toString().trim().toLowerCase();
  const missingBill = String(req.query.missingBill || '') === 'true';
  const nature = req.query.nature ? normalizedNature(req.query.nature) : '';
  if (nature && isAdmin(req) && !approvalNatures(req).includes(nature)) return res.status(403).json({ success:false, error:'You cannot view this accounting entity.' });
  let list = Object.values(s.expenses).filter(e => {
    if (!canViewExpense(req, e)) return false;
    if (id && e.id !== id) return false;
    if (reference) {
      const needle=reference.replace(/[^a-z0-9]/g,''),digits=reference.replace(/\D/g,''),hay=[e.id,e.billNo,e.billNumber].filter(Boolean).map(v=>String(v).toLowerCase().replace(/[^a-z0-9]/g,''));
      if(!hay.some(v=>(needle&&v.includes(needle))||(digits&&v.replace(/\D/g,'').includes(digits))))return false;
    }
    if (nature && normalizedNature(e.nature) !== nature) return false;
    // With a paying-account filter, reconcile on the actual payment movement
    // and its date (the same basis used by Account Ledgers), not the bill date.
    if (!payingAccount && !reference && from && e.date < from) return false;
    if (!payingAccount && !reference && to && e.date > to) return false;
    if (status && e.status !== status) return false;
    if (type && e.type !== type) return false;
    if (vendor && (e.vendor || '').toLowerCase() !== vendor) return false;
    if (claimant && String(e.createdBy || e.claimant || '').toLowerCase() !== claimant) return false;
    if (paymentType && String(e.paymentType || '').toLowerCase() !== paymentType) return false;
    if (payingAccount) {
      const movements=[];
      (e.payments||[]).filter(p=>paymentIsPosted(e)).forEach(p=>movements.push({account:p.account||e.account,date:p.date,amount:num(p.amount)}));
      (e.reimbursementPayments||[]).forEach(p=>movements.push({account:p.account,date:p.date,amount:num(p.amount)}));
      e.payingAccountAmount=round0(movements.filter(p=>String(p.account||'').toLowerCase()===payingAccount&&(!from||String(p.date||'')>=from)&&(!to||String(p.date||'')<=to)).reduce((n,p)=>n+p.amount,0));
      if(!(e.payingAccountAmount>0))return false;
    }
    if (missingBill && e.billPhoto) return false;
    return true;
  }).sort((a, b) => (b.date + b.id).localeCompare(a.date + a.id));

  const totals = { all: 0, pending: 0, approved: 0, paid: 0, noBill: 0, byType: {} };
  TYPES.forEach(t => { totals.byType[t] = 0; });
  list.forEach(e => {
    const countedAmount=payingAccount?num(e.payingAccountAmount):num(e.amount);
    totals.all += countedAmount;
    totals[e.status] = (totals[e.status] || 0) + countedAmount;
    if (!e.billPhoto) totals.noBill += countedAmount;
    if ((e.status === 'approved' || e.status === 'paid') && BUSINESS_NATURES.includes(normalizedNature(e.nature))) totals.byType[e.type] += countedAmount;
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
  const allowedAccount = allowedPayingAccount(req, 'SANKI', account);
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
  const nature = req.query.nature ? normalizedNature(req.query.nature) : '', accountFilter = String(req.query.account || '').trim().toLowerCase(), categoryFilter=String(req.query.category||'').trim().toLowerCase();
  if (nature && !approvalNatures(req).includes(nature)) return res.status(403).json({ success: false, error: 'You cannot view this accounting entity.' });
  const allowed = approvalNatures(req), inRange = d => (!from || d >= from) && (!to || d <= to);
  const payments = [];
  Object.values(s.expenses || {}).forEach(e => {
    const entity = normalizedNature(e.nature);
    if (!allowed.includes(entity) || (nature && entity !== nature)) return;
    if(categoryFilter&&String(e.ledger||'').trim().toLowerCase()!==categoryFilter)return;
    (e.payments || []).filter(p => paymentIsPosted(e) && inRange(String(p.date || ''))).forEach(p => {
      const account = p.account || e.account;
      if (!accountFilter || String(account).toLowerCase() === accountFilter) payments.push({ id:e.id, paymentId:p.id||'', date:p.date||'', entity, kind:p.personalFunds?'Paid personally':'Vendor payment', vendor:e.vendor||'', claimant:e.claimant||e.createdBy||'', particulars:e.particulars||'', category:e.ledger||'', type:e.type||'', expenseAmount:round0(e.amount), amount:round0(p.amount), account, paymentType:p.paymentType||e.paymentType||'', proof:p.proof||e.paymentProof||'', billPhoto:e.billPhoto||'', qrPhoto:e.qrPhoto||'', approvedAt:e.approvedAt||'', approvedBy:e.approvedBy||'', paidBy:p.paidBy||'', contractTotal:e.isInstallment?round0(e.amount):0, contractBalance:e.isInstallment?round0(Math.max(0,num(e.amount)-num(e.paidAmount))):0 });
    });
  });
  (s.reconciliationExpenses||[]).filter(e=>allowed.includes(normalizedNature(e.nature))&&(!nature||normalizedNature(e.nature)===nature)&&inRange(String(e.date||''))&&(!accountFilter||String(e.account||'').toLowerCase()===accountFilter)&&(!categoryFilter||String(e.category||'').toLowerCase()===categoryFilter)).forEach(e=>payments.push({id:e.id,paymentId:e.bankTransactionId||e.adjustmentId||'',date:e.date,entity:normalizedNature(e.nature),kind:'Bank-reconciled expense',vendor:e.vendor||'Bank',claimant:'',particulars:e.particulars||e.category,category:e.category,type:e.type||defaultType(e.category||''),expenseAmount:round0(e.amount),amount:round0(e.amount),account:e.account,paymentType:'Bank statement',proof:'',billPhoto:'',qrPhoto:'',approvedAt:e.createdAt||'',approvedBy:e.createdBy||'',paidBy:e.createdBy||''}));
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
  })).filter(b=>(!category||b.count>0)&&(!search||fuzzyIncludes(b.name,search)||b.entries.some(e=>fuzzyIncludes(e.particulars,search)||fuzzyIncludes(e.ledger,search)||String(e.id||'').toLowerCase().includes(search)||String(e.billNo||'').toLowerCase().includes(search)))).sort((a,b)=>a.name.localeCompare(b.name));
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

function vendorMasterForNature(s, nature) {
  const n = normalizedNature(nature);
  if (n === 'SANKI') return s.vendors = s.vendors || {};
  s.vendorsByNature = s.vendorsByNature || {};
  return s.vendorsByNature[n] = s.vendorsByNature[n] || {};
}
function vendorLinkedExpenses(s, nature, name) {
  const n = normalizedNature(nature), key = String(name || '').trim().toLowerCase();
  return Object.values(s.expenses || {}).filter(e => normalizedNature(e.nature) === n && String(e.vendor || '').trim().toLowerCase() === key);
}
router.post('/api/expenses/vendors/manage/edit', (req, res) => {
  if (!isOwner(req)) return res.status(403).json({ success:false, error:'Only the Owner can edit vendor ledgers.' });
  const s=loadStore(), b=req.body||{}, nature=normalizedNature(b.nature), oldName=String(b.name||'').trim(), newName=String(b.newName||'').trim();
  if(!oldName||!newName) return res.status(400).json({success:false,error:'Current and new vendor names are required.'});
  const master=vendorMasterForNature(s,nature),oldKey=oldName.toLowerCase(),newKey=newName.toLowerCase(),current=master[oldKey];
  if(!current) return res.status(404).json({success:false,error:'Vendor ledger not found.'});
  if(newKey!==oldKey&&master[newKey]) return res.status(409).json({success:false,error:'That vendor ledger already exists. Use Merge instead.'});
  const linked=vendorLinkedExpenses(s,nature,oldName),before={name:current.name,notes:current.notes||''};
  linked.forEach(e=>{e.vendor=newName;});delete master[oldKey];master[newKey]={...current,name:newName};
  audit(s,req,'VENDOR_RENAMED','vendor',oldName,{nature,before,after:{name:newName,linkedExpenses:linked.map(e=>e.id)}});
  saveStore(s);res.json({success:true,name:newName,updatedExpenses:linked.length});
});
router.post('/api/expenses/vendors/manage/merge', (req, res) => {
  if (!isOwner(req)) return res.status(403).json({ success:false, error:'Only the Owner can merge vendor ledgers.' });
  const s=loadStore(),b=req.body||{},nature=normalizedNature(b.nature),sourceName=String(b.sourceName||'').trim(),targetName=String(b.targetName||'').trim();
  if(!sourceName||!targetName||sourceName.toLowerCase()===targetName.toLowerCase()) return res.status(400).json({success:false,error:'Select two different vendor ledgers.'});
  const master=vendorMasterForNature(s,nature),sourceKey=sourceName.toLowerCase(),targetKey=targetName.toLowerCase(),source=master[sourceKey],target=master[targetKey];
  if(!source) return res.status(404).json({success:false,error:'Source vendor ledger not found.'});
  if(!target) return res.status(404).json({success:false,error:'Target vendor ledger not found.'});
  const linked=vendorLinkedExpenses(s,nature,sourceName);linked.forEach(e=>{e.vendor=target.name;});if(!target.notes&&source.notes)target.notes=source.notes;delete master[sourceKey];
  audit(s,req,'VENDOR_MERGED','vendor',sourceName,{nature,before:{source:source.name,target:target.name},after:{name:target.name,linkedExpenses:linked.map(e=>e.id)}});
  saveStore(s);res.json({success:true,name:target.name,updatedExpenses:linked.length});
});
router.post('/api/expenses/vendors/manage/delete', (req, res) => {
  if (!isOwner(req)) return res.status(403).json({ success:false, error:'Only the Owner can delete vendor ledgers.' });
  const s=loadStore(),b=req.body||{},nature=normalizedNature(b.nature),name=String(b.name||'').trim(),reason=String(b.reason||'').trim();
  if(!name) return res.status(400).json({success:false,error:'Vendor name is required.'});
  if(!reason) return res.status(400).json({success:false,error:'Reason for deletion is required.'});
  const master=vendorMasterForNature(s,nature),key=name.toLowerCase(),current=master[key];if(!current)return res.status(404).json({success:false,error:'Vendor ledger not found.'});
  const linked=vendorLinkedExpenses(s,nature,name);if(linked.length)return res.status(409).json({success:false,error:'This ledger has '+linked.length+' linked expense(s). Merge or rename it so accounting history is preserved.'});
  delete master[key];audit(s,req,'VENDOR_DELETED','vendor',name,{nature,before:current,note:reason});saveStore(s);res.json({success:true});
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
    const posted=(account,date)=>inRange(date)&&cashEntryIsVisible(account,date);
    Object.values(s.expenses).forEach(e => {
      const a = e.account || '(unspecified)';
      (e.payments || []).filter(p => paymentIsPosted(e) && posted(p.account||a,p.date)).forEach(p => {
        const paymentAccount = p.account || a;
        if(!ledgerAccountsForNature(s,nature).some(x=>x.toLowerCase()===String(paymentAccount).toLowerCase()))return;
        paidOut[paymentAccount] = (paidOut[paymentAccount] || 0) + num(p.amount);
      });
      (e.reimbursementPayments || []).filter(p=>posted(p.account,p.date)).forEach(p => {
        const ra = p.account || '(unspecified)';
        if(!ledgerAccountsForNature(s,nature).some(x=>x.toLowerCase()===String(ra).toLowerCase()))return;
        paidOut[ra] = (paidOut[ra] || 0) + num(p.amount);
      });
      // A reimbursement is an outflow from the company account and an equal
      // inflow back into the claimant account that originally funded the
      // expense. Without this credit, a fully reimbursed claimant incorrectly
      // remains negative forever in the Money Trail.
      const personalAccount=((e.payments||[]).find(p=>p.personalFunds&&p.account)||{}).account;
      if(personalAccount&&ledgerAccountsForNature(s,nature).some(x=>x.toLowerCase()===String(personalAccount).toLowerCase())){
        (e.reimbursementPayments||[]).filter(p=>posted(personalAccount,p.date)).forEach(p=>{
          collected[personalAccount]=(collected[personalAccount]||0)+num(p.amount);
        });
      }
    });
    if (nature === 'SANKI') procurementPayables(s, true).forEach(p => (p.payments || []).filter(x=>posted(x.account,x.date)).forEach(x => { paidOut[x.account] = (paidOut[x.account] || 0) + num(x.amount); }));
    if (nature === 'SANKI') salaryAdvanceEntries().filter(x=>posted(x.account,x.date)).forEach(x => { paidOut[x.account] = (paidOut[x.account] || 0) + num(x.amount); });
    if (nature === 'SANKI') salaryPaymentEntries().filter(x=>posted(x.account,x.date)).forEach(x => { paidOut[x.account] = (paidOut[x.account] || 0) + num(x.amount); });
    (s.adjustments || []).filter(x => normalizedNature(x.nature) === nature && posted(x.account,x.date)).forEach(x => { adj[x.account] = (adj[x.account] || 0) + num(x.amount); });
    Object.values(s.receivables||{}).filter(x=>normalizedNature(x.nature)===nature).forEach(x=>(x.collections||[]).filter(c=>posted(c.account,c.date)).forEach(c=>{collected[c.account]=(collected[c.account]||0)+num(c.amount);}));
    (s.receipts || []).filter(x=>normalizedNature(x.nature)===nature&&posted(x.account,x.date)).forEach(x=>{collected[x.account]=(collected[x.account]||0)+num(x.amount);});
    if(nature==='SANKI') salesLedgerEntries().filter(includeAutomaticSale).filter(x=>posted(x.account,x.date)).forEach(x=>{collected[x.account]=(collected[x.account]||0)+num(x.amount);});
    (s.transfers || []).filter(x => inRange(x.date)).forEach(x => {
      if(normalizedNature(x.fromNature||x.nature)===nature&&cashEntryIsVisible(x.fromAccount,x.date)) transferOut[x.fromAccount] = (transferOut[x.fromAccount] || 0) + num(x.amount);
      if(normalizedNature(x.toNature||x.nature)===nature&&cashEntryIsVisible(x.toAccount,x.date)) transferIn[x.toAccount] = (transferIn[x.toAccount] || 0) + num(x.amount);
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
  if (!isOwner(req)) return res.status(403).json({ success: false, error: 'Only the Owner can record general account transfers.' });
  const s = loadStore(); const b = req.body || {};
  const fromNature = normalizedNature(b.fromNature || b.nature), toNature = normalizedNature(b.toNature || b.nature);
  if (!approvalNatures(req).includes(fromNature) || !approvalNatures(req).includes(toNature)) return res.status(403).json({ success: false, error: 'You cannot transfer funds for one of these accounting entities.' });
  const fromAccount = allowedTransferAccount(fromNature, b.fromAccount), toAccount = allowedTransferAccount(toNature, b.toAccount);
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

router.post('/api/expenses/transfers/:id/delete', (req,res) => {
  if(!isOwner(req))return res.status(403).json({success:false,error:'Only the Owner can delete a recorded transfer.'});
  const s=loadStore(),id=String(req.params.id||''),index=(s.transfers||[]).findIndex(x=>x.id===id),reason=String((req.body||{}).reason||'').trim();
  if(index<0)return res.status(404).json({success:false,error:'Transfer not found.'});
  if(!reason)return res.status(400).json({success:false,error:'Reason for deleting the transfer is required.'});
  const transfer=s.transfers[index];
  audit(s,req,'TRANSFER_DELETED','transfer',id,{nature:transfer.fromNature||transfer.nature,account:transfer.fromAccount,before:transfer,note:reason});
  s.transfers.splice(index,1);saveStore(s);res.json({success:true,deleted:id});
});

router.post('/api/expenses/receipts', (req,res) => {
  if(!isOwner(req)) return res.status(403).json({success:false,error:'Only the Owner can record money received.'});
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
  const from = String(req.query.from || ''), to = String(req.query.to || ''), expenseNature = req.query.expenseNature ? normalizedNature(req.query.expenseNature) : '', entries = [];
  if(expenseNature&&!approvalNatures(req).includes(expenseNature))return res.status(403).json({success:false,error:'You cannot view expenses for this entity.'});
  const openingMap = nature === 'SANKI' ? (s.openingBalances || {}) : (((s.openingBalancesByNature || {})[nature]) || {});
  entries.push({ id: 'OPENING', date: account===DEFAULT_COUNTER_CASH?COUNTER_CASH_RESET_DATE:'', kind: 'opening', description: account===DEFAULT_COUNTER_CASH?'Opening balance effective 22 Aug 2026':'Opening balance', credit: num(openingMap[account]), debit: 0 });
  (s.adjustments || []).filter(x => normalizedNature(x.nature) === nature && x.account === account).forEach(x => entries.push({ id:x.id,date:x.date,kind:'adjustment',description:x.note||'Balance adjustment',credit:Math.max(0,num(x.amount)),debit:Math.max(0,-num(x.amount)),proof:x.proof||'',by:x.createdBy||'' }));
  (s.receipts || []).filter(x=>normalizedNature(x.nature)===nature&&x.account===account).forEach(x=>entries.push({id:x.id,date:x.date,kind:'receipt',description:(x.receiptType==='asset_sale'?'Asset sale':'Money received')+' · '+x.source,credit:num(x.amount),debit:0,proof:x.proof,note:x.note,by:x.createdBy}));
  (s.transfers || []).forEach(x => {
    const isOut=normalizedNature(x.fromNature||x.nature)===nature&&x.fromAccount===account,isIn=normalizedNature(x.toNature||x.nature)===nature&&x.toAccount===account;if(!isOut&&!isIn)return;
    const other=(isOut?(x.toNature||x.nature)+' · '+x.toAccount:(x.fromNature||x.nature)+' · '+x.fromAccount);
    entries.push({id:x.id,date:x.date,kind:'transfer',description:(isOut?'Transfer to ':'Transfer from ')+other+' · '+String(x.classification||'internal transfer').replaceAll('_',' '),credit:isIn?num(x.amount):0,debit:isOut?num(x.amount):0,proof:x.proof,note:x.note,by:x.createdBy});
  });
  // A bank ledger follows the account that moved, even when that account paid
  // an expense belonging to another entity (for example SANKI 3645 paying a
  // SAMAST bill). The entity remains visible on the ledger description.
  Object.values(s.expenses || {}).forEach(e => {
    const entryNature=normalizedNature(e.nature),entityLabel=' ['+entryNature+']';
    if (!approvalNatures(req).includes(entryNature)) return;
    const personalAccount=((e.payments||[]).find(p=>p.personalFunds&&p.account)||{}).account;
    (e.payments || []).filter(p => paymentIsPosted(e) && (p.account || e.account) === account).forEach(p => entries.push({id:e.id+'/'+p.id,date:p.date,kind:p.personalFunds?'personal_expense':'expense',entity:entryNature,description:(e.vendor||'Vendor')+' · '+(e.particulars||e.id)+entityLabel+(p.personalFunds?' · paid personally':''),credit:0,debit:num(p.amount),proof:p.proof,by:p.paidBy}));
    (e.reimbursementPayments || []).filter(p => p.account === account).forEach(p => entries.push({id:e.id+'/'+p.id,date:p.date,kind:'reimbursement',entity:entryNature,description:'Reimbursement to '+(e.claimant||e.createdBy||'claimant')+entityLabel,credit:0,debit:num(p.amount),proof:p.proof,by:p.paidBy}));
    (e.reimbursementPayments || []).filter(p => personalAccount === account).forEach(p => entries.push({id:e.id+'/'+p.id+'/RECEIVED',date:p.date,kind:'reimbursement_received',entity:entryNature,description:'Reimbursement received from '+(p.account||'company account')+entityLabel,credit:num(p.amount),debit:0,proof:p.proof,by:p.paidBy}));
  });
  if (nature === 'SANKI') procurementPayables(s, true).forEach(p => (p.payments || []).filter(x => x.account === account).forEach(x => entries.push({id:p.id+'/'+x.id,date:x.date,kind:'purchase',entity:'SANKI',description:(p.vendor||'Mediator')+' · '+p.id+' · goods and transport [SANKI]',credit:0,debit:num(x.amount),proof:x.proof,by:x.paidBy})));
  if (nature === 'SANKI') salaryAdvanceEntries().filter(x=>x.account===account).forEach(x=>entries.push({id:x.id,date:x.date,kind:'salary_advance',entity:'SANKI',description:'Salary advance · '+x.employee,credit:0,debit:num(x.amount),proof:x.proof,note:x.note,by:x.by}));
  if (nature === 'SANKI') salaryPaymentEntries().filter(x=>x.account===account).forEach(x=>entries.push({id:x.id,date:x.date,kind:'salary_payment',entity:'SANKI',description:'Salary payment · '+x.employeeName+' · '+x.ym,credit:0,debit:num(x.amount),proof:x.proof,note:x.note,by:x.createdBy}));
  Object.values(s.receivables||{}).filter(x=>normalizedNature(x.nature)===nature).forEach(x=>(x.collections||[]).filter(c=>c.account===account).forEach(c=>entries.push({id:x.id+'/'+c.id,date:c.date,kind:'receivable',description:'Received from '+x.party+' · '+x.reason,credit:num(c.amount),debit:0,proof:c.proof,by:c.receivedBy})));
  if(nature==='SANKI') salesLedgerEntries().filter(includeAutomaticSale).filter(x=>x.account===account).forEach(x=>entries.push({id:x.id,date:x.date,kind:'sale',description:x.description,credit:num(x.amount),debit:0}));
  if(nature==='SANKI'&&account===PAYTM_CLEARING_ACCOUNT)(s.paytmSettlements||[]).forEach(x=>entries.push({id:x.id,date:x.date,kind:'paytm_settlement',description:'Settled to '+x.bankAccount+' · Connected sales '+((x.orderIds||[]).map(n=>'#'+String(n).replace(/^#/,'')).join(', ')||'not specified')+(x.chargeAmount?' · Bank/Paytm charges ₹'+x.chargeAmount:''),credit:0,debit:num(x.grossAmount),settlement:x}));
  // Once a bank period is finalized, its imported rows are the authoritative
  // bank ledger. Manual/app movements remain visible only after that cutoff,
  // preventing Shopify gross sales or matched expenses from doubling the bank.
  const bankBook=(s.bankStatements||{})[account],coveredThrough=bankBook&&bankBook.reconciledThrough;
  if(bankBook&&coveredThrough){
    for(let i=entries.length-1;i>=0;i--)if(entries[i].kind!=='opening'&&entries[i].date<=coveredThrough)entries.splice(i,1);
    Object.values(bankBook.transactions||{}).forEach(tx=>{const settlement=(s.paytmSettlements||[]).find(x=>x.bankTransactionId===tx.id);entries.push({id:tx.id,date:tx.date,kind:'bank_statement',description:tx.description||'Bank transaction',credit:num(tx.credit),debit:num(tx.debit),reference:tx.reference||'',settlement:settlement||null});});
  }
  if(account===DEFAULT_COUNTER_CASH)for(let i=entries.length-1;i>=0;i--)if(entries[i].kind!=='opening'&&!cashEntryIsVisible(account,entries[i].date))entries.splice(i,1);
  const ordered = entries.sort((a,b) => String(a.date).localeCompare(String(b.date)) || String(a.id).localeCompare(String(b.id)));
  let running = 0; ordered.forEach(x => { running += num(x.credit)-num(x.debit); x.balance = round0(running); });
  const visible = ordered.filter(x => (x.kind === 'opening' || ((!from || x.date >= from) && (!to || x.date <= to))) && (!expenseNature || !x.entity || x.entity===expenseNature))
    .sort((a,b) => a.kind === 'opening' ? 1 : (b.kind === 'opening' ? -1 : (String(b.date).localeCompare(String(a.date)) || String(b.id).localeCompare(String(a.id)))));
  const issues = reconciliationIssues(s, nature, account);
  res.json({ success:true, account, nature, expenseNature, entries:visible, balance:round0(running), reconciled:issues.length===0, reconciliationIssues:issues });
});

function statementDate(v){if(v instanceof Date&&!isNaN(v))return v.toISOString().slice(0,10);if(typeof v==='number'){const d=XLSX.SSF.parse_date_code(v);if(d)return `${d.y}-${String(d.m).padStart(2,'0')}-${String(d.d).padStart(2,'0')}`;}const s=String(v||'').trim(),m=s.match(/(\d{1,2})[\/\-.](\d{1,2})[\/\-.](\d{2,4})/);if(m){const y=m[3].length===2?'20'+m[3]:m[3];return `${y}-${m[2].padStart(2,'0')}-${m[1].padStart(2,'0')}`;}return /^\d{4}-\d{2}-\d{2}/.test(s)?s.slice(0,10):'';}
function statementNum(v){return num(String(v==null?'':v).replace(/[₹,\s]/g,'').replace(/^\((.*)\)$/,'-$1'));}
function parseBankStatementFile(filePath){
  const wb=XLSX.readFile(filePath,{cellDates:true}),sheet=wb.Sheets[wb.SheetNames[0]],matrix=XLSX.utils.sheet_to_json(sheet,{header:1,defval:''}),headerRow=Math.max(0,matrix.findIndex(r=>{const h=r.map(x=>String(x).toLowerCase().replace(/[^a-z0-9]/g,''));return h.some(x=>['date','transactiondate','valuedate','txndate','postingdate'].includes(x))&&h.some(x=>/debit|credit|withdrawal|deposit|amount/.test(x));})),rows=XLSX.utils.sheet_to_json(sheet,{defval:'',range:headerRow});
  const out=[];rows.forEach((raw,index)=>{const normalized={};Object.keys(raw).forEach(k=>normalized[String(k).toLowerCase().replace(/[^a-z0-9]/g,'')]=raw[k]);const get=(...keys)=>{for(const k of keys)if(normalized[k]!=null&&normalized[k]!=='')return normalized[k];return '';};
    const date=statementDate(get('date','transactiondate','valuedate','txndate','postingdate')),description=String(get('narration','description','particulars','transactiondetails','remarks','details')).trim(),reference=String(get('reference','referenceno','transactionid','utr','utrno','chequeno','chqrefno')).trim();let debit=statementNum(get('debit','debitamount','withdrawal','withdrawalamount','dramount')),credit=statementNum(get('credit','creditamount','deposit','depositamount','cramount'));
    if(!debit&&!credit){const amount=statementNum(get('amount','transactionamount')),side=String(get('drcr','type','transactiontype')).toLowerCase();if(/dr|debit|withdraw/.test(side))debit=Math.abs(amount);else if(/cr|credit|deposit/.test(side))credit=Math.abs(amount);else if(amount<0)debit=Math.abs(amount);else credit=Math.abs(amount);}
    const balance=statementNum(get('balance','closingbalance','availablebalance'));if(!date||(!debit&&!credit))return;out.push({date,description,reference,debit:Math.abs(debit),credit:Math.abs(credit),balance,row:index+2});});return out;
}
function parseBankStatementText(raw){
  const text=String(raw||'').replace(/\r/g,'');
  if(/Axis Bank Account No/i.test(text)&&/ParticularsAmount\(INR\)Debit\/CreditBalance\(INR\)/i.test(text)){
    const section=(text.split(/S\.NOTransaction/i)[1]||'').split(/TRANSACTION TOTAL DR\/CR/i)[0]||'',out=[];
    const rowRe=/(?:^|\n)(\d{1,4})(\d{2}\/\d{2}\/\d{4})(\d{2}\/\d{2}\/\d{4})([\s\S]*?)([0-9][0-9,]*\.\d{2})(DR|CR)([0-9][0-9,]*\.\d{2})\s*(?:\([^\n]*\))?(?=\n\d{1,4}(?:\d{2}\/\d{2}\/\d{4}|$)|$)/gi;
    let match;while((match=rowRe.exec(section))){const amount=statementNum(match[5]),side=match[6].toUpperCase(),description=match[4].replace(/\s+/g,' ').trim(),reference=((description.match(/\b(?:IFT|UPI|IMPS|NEFT|RTGS|UTR)[\/\s:#-]*([A-Z0-9-]{5,})/i)||[])[1]||'');out.push({date:statementDate(match[2]),valueDate:statementDate(match[3]),description,reference,debit:side==='DR'?amount:0,credit:side==='CR'?amount:0,balance:statementNum(match[7]),row:Number(match[1])});}
    const opening=statementNum((text.match(/Opening Balance:\s*(?:INR|₹)?\s*([0-9,]+(?:\.\d{1,2})?)/i)||[])[1]);
    const closing=statementNum((text.match(/Closing Balance:\s*(?:INR|₹)?\s*([0-9,]+(?:\.\d{1,2})?)/i)||[])[1]);
    const period=text.match(/From\s*:\s*(\d{2}\/\d{2}\/\d{4})\s+To\s*:\s*(\d{2}\/\d{2}\/\d{4})/i),debits=out.reduce((n,x)=>n+x.debit,0),credits=out.reduce((n,x)=>n+x.credit,0),calculated=Math.round((opening+credits-debits)*100)/100;
    if(out.length&&Math.abs(calculated-closing)>0.01)throw new Error('Statement validation failed: '+out.length+' transaction(s) produced closing '+calculated+' instead of '+closing+'.');
    out.statementSummary={format:'Axis Bank PDF',from:period?statementDate(period[1]):out[0]&&out[0].date,to:period?statementDate(period[2]):out.at(-1)&&out.at(-1).date,openingBalance:opening,closingBalance:closing,totalDebits:Math.round(debits*100)/100,totalCredits:Math.round(credits*100)/100,validated:!!out.length&&Math.abs(calculated-closing)<=0.01};
    if(out.length)return out;
  }
  const out=[];String(raw||'').replace(/\r/g,'').split('\n').map(x=>x.replace(/\s+/g,' ').trim()).filter(Boolean).forEach((line,index)=>{
    const dm=line.match(/(?:^|\s)((?:\d{1,2}[\/\-.]\d{1,2}[\/\-.]\d{2,4})|(?:\d{4}[\/\-.]\d{1,2}[\/\-.]\d{1,2}))(?:\s|$)/);if(!dm)return;
    const date=statementDate(dm[1]);if(!date)return;const rest=line.slice((dm.index||0)+dm[0].length).trim(),tokens=Array.from(rest.matchAll(/(?:₹\s*)?\(?-?[0-9][0-9,]*(?:\.\d{1,2})?\)?(?:\s*(?:CR|DR))?/ig));if(tokens.length<2)return;
    const values=tokens.map(m=>statementNum(m[0].replace(/\b(?:CR|DR)\b/i,''))),balance=values.at(-1),amountTokens=tokens.slice(0,-1),amounts=values.slice(0,-1);let debit=0,credit=0;
    if(amounts.length>=2){debit=Math.abs(amounts.at(-2));credit=Math.abs(amounts.at(-1));}
    else{const token=amountTokens[0][0],amount=Math.abs(amounts[0]),context=(rest+' '+token).toLowerCase();if(/\bcr\b|credit|deposit|received/.test(context))credit=amount;else debit=amount;}
    if(!debit&&!credit)return;const firstAmount=amountTokens[0],description=rest.slice(0,firstAmount.index).trim().replace(/[|:-]+$/,'').trim(),reference=((description.match(/\b(?:utr|ref|txn|chq)[\s:#-]*([a-z0-9-]+)/i)||[])[1]||'');out.push({date,description,reference,debit,credit,balance:Math.abs(balance),row:index+1});
  });return out;
}
async function parseBankStatementUpload(filePath,originalName){
  const ext=path.extname(originalName||filePath).toLowerCase();if(['.xlsx','.xls','.csv'].includes(ext))return parseBankStatementFile(filePath);
  if(ext==='.pdf'){const parsed=await pdfParse(fs.readFileSync(filePath));const rows=parseBankStatementText(parsed.text);if(rows.length)return rows;throw new Error('No readable statement table was found in this PDF. Upload clear page images or the bank Excel/CSV export.');}
  if(['.png','.jpg','.jpeg','.webp','.bmp','.tif','.tiff'].includes(ext)){const worker=await createWorker(tesseractEnglish.code,1,{langPath:tesseractEnglish.langPath,gzip:tesseractEnglish.gzip,cacheMethod:'none'});try{const result=await worker.recognize(filePath);return parseBankStatementText(result&&result.data&&result.data.text||'');}finally{await worker.terminate();}}
  throw new Error('Use XLS, XLSX, CSV, PDF, PNG, JPG, WEBP, BMP or TIFF.');
}
function bankRowKey(account,row,occurrence){return crypto.createHash('sha256').update([account,row.date,row.debit,row.credit,row.reference||row.description,row.balance,occurrence].join('|')).digest('hex').slice(0,24);}
function appBankMovements(s,account,nature){const rows=[],n=normalizedNature(nature);
  (s.adjustments||[]).filter(x=>x.account===account&&normalizedNature(x.nature)===n).forEach(x=>rows.push({id:x.id,date:x.date,description:x.note||'Adjustment',debit:Math.max(0,-num(x.amount)),credit:Math.max(0,num(x.amount))}));
  (s.receipts||[]).filter(x=>x.account===account&&normalizedNature(x.nature)===n).forEach(x=>rows.push({id:x.id,date:x.date,description:x.source,credit:num(x.amount),debit:0}));
  (s.transfers||[]).forEach(x=>{if(x.fromAccount===account&&normalizedNature(x.fromNature||x.nature)===n)rows.push({id:x.id,date:x.date,description:'Transfer to '+x.toAccount,debit:num(x.amount),credit:0});if(x.toAccount===account&&normalizedNature(x.toNature||x.nature)===n)rows.push({id:x.id,date:x.date,description:'Transfer from '+x.fromAccount,debit:0,credit:num(x.amount)});});
  Object.values(s.expenses||{}).filter(e=>normalizedNature(e.nature)===n).forEach(e=>{(e.payments||[]).filter(p=>paymentIsPosted(e)&&(p.account||e.account)===account).forEach(p=>rows.push({id:e.id+'/'+p.id,date:p.date,description:(e.vendor||e.particulars||e.id),debit:num(p.amount),credit:0}));(e.reimbursementPayments||[]).filter(p=>p.account===account).forEach(p=>rows.push({id:e.id+'/'+p.id,date:p.date,description:'Reimbursement '+(e.claimant||e.createdBy||''),debit:num(p.amount),credit:0}));});
  Object.values(s.receivables||{}).filter(x=>normalizedNature(x.nature)===n).forEach(x=>(x.collections||[]).filter(c=>c.account===account).forEach(c=>rows.push({id:x.id+'/'+c.id,date:c.date,description:x.party,credit:num(c.amount),debit:0})));
  if(n==='SANKI'){salesLedgerEntries().filter(includeAutomaticSale).filter(x=>x.account===account).forEach(x=>rows.push({id:x.id,date:x.date,description:x.description,credit:num(x.amount),debit:0}));procurementPayables(s,true).forEach(p=>(p.payments||[]).filter(x=>x.account===account).forEach(x=>rows.push({id:p.id+'/'+x.id,date:x.date,description:p.vendor||p.id,debit:num(x.amount),credit:0})));salaryAdvanceEntries().filter(x=>x.account===account).forEach(x=>rows.push({id:x.id,date:x.date,description:'Salary advance · '+x.employee,debit:num(x.amount),credit:0}));salaryPaymentEntries().filter(x=>x.account===account).forEach(x=>rows.push({id:x.id,date:x.date,description:'Salary payment · '+x.employeeName,debit:num(x.amount),credit:0}));}return rows;}
function draftReconciliation(s,draft){
  const bank=draft.transactions||[],app=appBankMovements(s,draft.account,draft.nature).filter(x=>(!draft.summary.from||x.date>=draft.summary.from)&&(!draft.summary.to||x.date<=draft.summary.to)),used=new Set(),rows=[];
  bank.forEach((b,bi)=>{let best=-1,score=-1;app.forEach((a,i)=>{if(used.has(i)||Math.abs(num(a.debit)-num(b.debit))>.01||Math.abs(num(a.credit)-num(b.credit))>.01)return;const days=Math.abs((Date.parse(a.date)-Date.parse(b.date))/86400000);if(days>2)return;let next=days===0?3:(days===1?2:1);if(b.reference&&String(a.id+' '+a.description).toLowerCase().includes(String(b.reference).toLowerCase()))next=5;if(next>score){score=next;best=i;}});const id='bank-'+bi,resolution=draft.resolutions&&draft.resolutions[id];if(best>=0){used.add(best);rows.push({id,status:score>=3?'matched':'possible_match',confidence:score,bank:b,app:app[best],resolution});}else rows.push({id,status:'missing_in_app',bank:b,resolution});});
  app.forEach((a,i)=>{if(!used.has(i)){const id='app-'+i;rows.push({id,status:'missing_in_bank',app:a,resolution:draft.resolutions&&draft.resolutions[id]});}});
  rows.forEach(x=>{if(x.resolution)x.originalStatus=x.status,x.status='resolved';});const summary=rows.reduce((o,x)=>(o[x.status]=(o[x.status]||0)+1,o),{}),unresolved=rows.filter(x=>!['matched','resolved'].includes(x.status)).length,openingMap=draft.nature==='SANKI'?(s.openingBalances||{}):(((s.openingBalancesByNature||{})[draft.nature])||{}),currentOpening=num(openingMap[draft.account]),proposedOpening=draft.openingResolution?num(draft.openingResolution.amount):currentOpening,movements=appBankMovements(s,draft.account,draft.nature).filter(x=>!draft.summary.to||x.date<=draft.summary.to).reduce((n,x)=>n+num(x.credit)-num(x.debit),0),staged=rows.filter(x=>x.resolution&&['create_adjustment','paytm_settlement'].includes(x.resolution.action)&&x.bank).reduce((n,x)=>n+num(x.bank.credit)-num(x.bank.debit),0),ledgerClosing=Math.round((proposedOpening+movements+staged)*100)/100,bankClosing=num(draft.summary.closingBalance),balanceDifference=Math.round((bankClosing-ledgerClosing)*100)/100,balanceResolved=Math.abs(balanceDifference)<.01;
  return{success:true,draftId:draft.id,account:draft.account,statementSummary:draft.summary,summary,rows,unresolved,currentOpening,proposedOpening,ledgerClosing,bankClosing,balanceDifference,balanceResolved,openingResolution:draft.openingResolution||null,canFinalize:unresolved===0&&balanceResolved};
}
async function createBankReconciliationDraft(input){const b=input||{},parsed=await parseBankStatementUpload(b.filePath,b.originalName),s=loadStore(),account=String(b.account||''),nature=normalizedNature(b.nature);if(!ledgerAccountsForNature(s,nature).some(a=>a.toLowerCase()===account.toLowerCase()))return{success:false,error:'Select the correct account.'};if(!parsed.length)return{success:false,error:'No dated debit/credit transactions were found.'};const dates=parsed.map(x=>x.date).sort(),id='BRD-'+Date.now()+'-'+crypto.randomBytes(3).toString('hex'),summary=parsed.statementSummary||{format:path.extname(b.originalName||'').slice(1).toUpperCase(),from:dates[0],to:dates.at(-1),openingBalance:null,closingBalance:parsed.at(-1).balance,totalDebits:parsed.reduce((n,x)=>n+num(x.debit),0),totalCredits:parsed.reduce((n,x)=>n+num(x.credit),0),validated:true};s.bankReconciliationDrafts=s.bankReconciliationDrafts||{};s.bankReconciliationDrafts[id]={id,account,nature,transactions:parsed,summary,resolutions:{},temporaryFile:b.filePath,originalName:b.originalName||path.basename(b.filePath||''),fileHash:crypto.createHash('sha256').update(fs.readFileSync(b.filePath)).digest('hex'),createdAt:new Date().toISOString(),createdBy:b.username||'admin',expiresAt:new Date(Date.now()+7*86400000).toISOString()};saveStore(s);return Object.assign({success:true,draft:s.bankReconciliationDrafts[id]},draftReconciliation(s,s.bankReconciliationDrafts[id]));}
async function importBankStatementUpload(input){return createBankReconciliationDraft(input);}
router.post('/api/expenses/bank-statements/import',statementUpload.single('statement'),(req,res,next)=>{const s=loadStore();if(canAccessBankReconciliation(req,s,req.body&&req.body.nature,req.body&&req.body.account))return next();try{if(req.file&&req.file.path)fs.unlinkSync(req.file.path);}catch{}return res.status(403).json({success:false,error:'You cannot reconcile this bank account.'});},async(req,res)=>{if(!req.file)return res.status(400).json({success:false,error:'Choose an XLS, XLSX, CSV, PDF or statement image.'});try{const out=await createBankReconciliationDraft({filePath:req.file.path,originalName:req.file.originalname,account:req.body.account,nature:req.body.nature,username:req.user.username,device:'Web'});return res.status(out.success?200:400).json(out);}catch(e){try{if(req.file&&req.file.path)fs.unlinkSync(req.file.path);}catch{}return res.status(400).json({success:false,error:e.message||'Could not read this bank statement.'});}});
router.get('/api/expenses/bank-statements',(req,res)=>{const s=loadStore(),account=String(req.query.account||''),nature=normalizedNature(req.query.nature);if(!canAccessBankReconciliation(req,s,nature,account))return res.status(403).json({success:false,error:'You cannot view this bank reconciliation.'});const key=bankStatementBookKey(nature,account),book=((s.bankStatements||{})[key])||{transactions:{},imports:[]},transactions=Object.values(book.transactions||{}).sort((a,b)=>String(b.date+b.id).localeCompare(String(a.date+a.id))),lastAt=book.lastReconciliation&&book.lastReconciliation.at||'',draft=Object.values(s.bankReconciliationDrafts||{}).filter(x=>x.account===account&&normalizedNature(x.nature)===nature&&x.expiresAt>new Date().toISOString()&&(!lastAt||x.createdAt>lastAt)).sort((a,b)=>b.createdAt.localeCompare(a.createdAt))[0];res.json({success:true,account,nature,imports:(book.imports||[]).slice().reverse(),transactions,lastReconciliation:book.lastReconciliation||null,updatedThrough:book.reconciledThrough||'',closingBalance:book.lastReconciliation&&book.lastReconciliation.closingBalance||0,draft:draft?draftReconciliation(s,draft):null});});
router.get('/api/expenses/bank-statements/file/:id',(req,res)=>{const s=loadStore(),id=String(req.params.id||''),nature=normalizedNature(req.query.nature),account=String(req.query.account||'');if(!canAccessBankReconciliation(req,s,nature,account))return res.status(403).end();const book=(s.bankStatements||{})[bankStatementBookKey(nature,account)],record=book&&(book.imports||[]).find(x=>x.id===id),name=record&&path.basename(String(record.file||'')),fp=name&&path.join(STATEMENT_DIR,name);if(!record||!name||!fp.startsWith(STATEMENT_DIR)||!fs.existsSync(fp))return res.status(404).end();res.sendFile(fp);});
function reconcileBankStatementAccount(account,username,device,draftId,nature){const s=loadStore(),n=normalizedNature(nature),draft=(s.bankReconciliationDrafts||{})[draftId]||Object.values(s.bankReconciliationDrafts||{}).filter(x=>x.account===account&&normalizedNature(x.nature)===n).sort((a,b)=>b.createdAt.localeCompare(a.createdAt))[0];if(!draft)return{success:false,error:'Upload and preview a statement first.'};return draftReconciliation(s,draft);}
router.post('/api/expenses/bank-statements/reconcile',(req,res)=>{const b=req.body||{},s=loadStore(),draft=(s.bankReconciliationDrafts||{})[b.draftId];if(!canAccessBankDraft(req,s,draft))return res.status(403).json({success:false,error:'You cannot reconcile this bank account.'});const out=reconcileBankStatementAccount(String(b.account||''),req.user.username,'Web',b.draftId,draft.nature);res.status(out.success?200:400).json(out);});
router.post('/api/expenses/bank-statements/resolve',(req,res)=>{const b=req.body||{},s=loadStore(),draft=(s.bankReconciliationDrafts||{})[b.draftId];if(!draft)return res.status(404).json({success:false,error:'This reconciliation draft has expired.'});if(!canAccessBankDraft(req,s,draft))return res.status(403).json({success:false,error:'You cannot resolve this bank reconciliation.'});const view=draftReconciliation(s,draft),row=view.rows.find(x=>x.id===b.rowId),allowed=['accept_match','create_adjustment','paytm_settlement','timing_difference','exclude'];if(!row)return res.status(404).json({success:false,error:'Difference was not found.'});if(!allowed.includes(b.action))return res.status(400).json({success:false,error:'Choose a valid resolution.'});if(['timing_difference','exclude','create_adjustment','paytm_settlement'].includes(b.action)&&!String(b.reason||'').trim())return res.status(400).json({success:false,error:'Enter a reason for this resolution.'});if(b.action==='accept_match'&&!row.app)return res.status(400).json({success:false,error:'There is no suggested ledger entry to match.'});if(b.action==='paytm_settlement'){if(draft.account!==DEFAULT_SALES_BANK||!row.bank||!(num(row.bank.credit)>0))return res.status(400).json({success:false,error:'Only an Axis Bank credit can be classified as a Paytm settlement.'});const gross=num(b.grossAmount),charge=num(b.chargeAmount),net=num(row.bank.credit);if(!(gross>=net)||Math.abs(gross-net-charge)>.01)return res.status(400).json({success:false,error:'Gross settlement must equal bank credit plus Paytm charges.'});}const category=String(b.category||'').trim();if(b.action==='create_adjustment'&&category&&!pickableLedgers(s).some(x=>x.name.toLowerCase()===category.toLowerCase()))return res.status(400).json({success:false,error:'Choose a valid expense category.'});draft.resolutions=draft.resolutions||{};draft.resolutions[b.rowId]={action:b.action,reason:String(b.reason||'').trim(),category,appId:row.app&&row.app.id,grossAmount:num(b.grossAmount),chargeAmount:num(b.chargeAmount),orderIds:Array.isArray(b.orderIds)?b.orderIds.map(String).filter(Boolean):[],by:req.user.username,at:new Date().toISOString()};saveStore(s);res.json(draftReconciliation(s,draft));});
router.post('/api/expenses/bank-statements/resolve-balance',(req,res)=>{const b=req.body||{},s=loadStore(),draft=(s.bankReconciliationDrafts||{})[b.draftId],reason=String(b.reason||'').trim();if(!draft)return res.status(404).json({success:false,error:'This reconciliation draft has expired.'});if(!canAccessBankDraft(req,s,draft))return res.status(403).json({success:false,error:'You cannot resolve this bank reconciliation.'});if(!reason)return res.status(400).json({success:false,error:'Enter a reason for changing the opening balance.'});const amount=num(b.amount);draft.openingResolution={amount,previousAmount:draftReconciliation(s,draft).currentOpening,reason,by:req.user.username,at:new Date().toISOString()};saveStore(s);res.json(draftReconciliation(s,draft));});
router.post('/api/expenses/bank-statements/finalize',(req,res)=>{const b=req.body||{},s=loadStore(),draft=(s.bankReconciliationDrafts||{})[b.draftId];if(!draft)return res.status(404).json({success:false,error:'This reconciliation draft has expired.'});if(!canAccessBankDraft(req,s,draft))return res.status(403).json({success:false,error:'You cannot finalize this bank reconciliation.'});const view=draftReconciliation(s,draft);if(view.unresolved)return res.status(409).json({success:false,error:view.unresolved+' difference(s) still need a decision.',reconciliation:view});if(!view.balanceResolved)return res.status(409).json({success:false,error:'Ledger closing balance differs from the bank by ₹'+Math.abs(view.balanceDifference).toFixed(2)+'. Resolve the opening/carry-forward balance first.',reconciliation:view});s.bankStatements=s.bankStatements||{};const bookKey=bankStatementBookKey(draft.nature,draft.account),book=s.bankStatements[bookKey]||(s.bankStatements[bookKey]={transactions:{},imports:[]});Object.keys(book.transactions||{}).forEach(k=>{const x=book.transactions[k];if(x.date>=draft.summary.from&&x.date<=draft.summary.to)delete book.transactions[k];});const importId='BST-'+Date.now(),seen={},storedByRow=new Map();draft.transactions.forEach(row=>{const base=[row.date,row.debit,row.credit,row.reference||row.description,row.balance].join('|'),occurrence=seen[base]=(seen[base]||0)+1,key=bankRowKey(draft.account,row,occurrence),stored=Object.assign({id:'BTX-'+key,firstSeenImport:importId,lastSeenImport:importId},row);book.transactions[key]=stored;storedByRow.set(row,stored);});Object.entries(draft.resolutions||{}).forEach(([rowId,r])=>{const row=view.rows.find(x=>x.id===rowId),bank=row&&row.bank;if(!bank)return;const tx=storedByRow.get(bank);if(r.action==='create_adjustment'){s.adjSeq=num(s.adjSeq)+1;const adjustment={id:'ADJ-'+String(s.adjSeq).padStart(4,'0'),nature:draft.nature,account:draft.account,amount:num(bank.credit)-num(bank.debit),date:bank.date,note:r.reason+' [Bank reconciliation '+draft.id+']',reconciliationDraft:draft.id,createdBy:req.user.username,createdAt:new Date().toISOString()};s.adjustments.push(adjustment);if(r.category&&num(bank.debit)>0){s.reconciliationExpenses=Array.isArray(s.reconciliationExpenses)?s.reconciliationExpenses:[];s.reconciliationExpenses.push({id:'BRE-'+adjustment.id,nature:draft.nature,date:bank.date,amount:num(bank.debit),account:draft.account,category:r.category,type:defaultType(r.category),vendor:draft.account,particulars:r.reason,adjustmentId:adjustment.id,bankTransactionId:tx&&tx.id,reconciliationDraft:draft.id,createdBy:req.user.username,createdAt:new Date().toISOString()});}}if(r.action==='paytm_settlement'){s.paytmSettlements=Array.isArray(s.paytmSettlements)?s.paytmSettlements:[];const settlement={id:'PTM-'+Date.now()+'-'+rowId.replace(/\D/g,''),date:bank.date,bankAccount:draft.account,bankTransactionId:tx&&tx.id,netAmount:num(bank.credit),grossAmount:num(r.grossAmount),chargeAmount:num(r.chargeAmount),orderIds:r.orderIds||[],reason:r.reason,reconciliationDraft:draft.id,createdBy:req.user.username,createdAt:new Date().toISOString(),shopifyStoreCreditMutation:false};s.paytmSettlements.push(settlement);if(settlement.chargeAmount>0){s.reconciliationExpenses=Array.isArray(s.reconciliationExpenses)?s.reconciliationExpenses:[];s.reconciliationExpenses.push({id:'BRE-'+settlement.id,nature:'SANKI',date:settlement.date,amount:settlement.chargeAmount,account:settlement.bankAccount,category:'BANK CHARGES',type:'running',vendor:'Paytm',particulars:'Paytm settlement charges · connected sales '+(settlement.orderIds.map(n=>'#'+String(n).replace(/^#/,'')).join(', ')||'not specified'),settlementId:settlement.id,bankTransactionId:settlement.bankTransactionId,reconciliationDraft:draft.id,createdBy:req.user.username,createdAt:new Date().toISOString()});}}});if(draft.openingResolution){if(draft.nature==='SANKI')s.openingBalances[draft.account]=num(draft.openingResolution.amount);else{s.openingBalancesByNature=s.openingBalancesByNature||{};s.openingBalancesByNature[draft.nature]=s.openingBalancesByNature[draft.nature]||{};s.openingBalancesByNature[draft.nature][draft.account]=num(draft.openingResolution.amount);}}let storedFile='';try{const ext=path.extname(draft.originalName||draft.temporaryFile),name=importId+ext.toLowerCase();fs.renameSync(draft.temporaryFile,path.join(STATEMENT_DIR,name));storedFile=name;}catch{}const finalizedAt=new Date().toISOString(),record={id:importId,file:storedFile,originalName:draft.originalName,hash:draft.fileHash,from:draft.summary.from,to:draft.summary.to,rows:draft.transactions.length,uploadedAt:draft.createdAt,finalizedAt,finalizedBy:req.user.username};book.imports.push(record);book.reconciledThrough=draft.summary.to;book.lastReconciliation={at:finalizedAt,by:req.user.username,summary:view.summary,reconciled:true,closingBalance:draft.summary.closingBalance,ledgerClosingBalance:view.ledgerClosing,through:draft.summary.to,draftId:draft.id};audit(s,req,'BANK_RECONCILIATION_FINALIZED','account',draft.account,{nature:draft.nature,account:draft.account,after:book.lastReconciliation,resolutions:draft.resolutions,openingResolution:draft.openingResolution||null});Object.entries(s.bankReconciliationDrafts||{}).forEach(([id,x])=>{if(x.account===draft.account&&normalizedNature(x.nature)===normalizedNature(draft.nature)){if(id!==draft.id&&x.temporaryFile)try{fs.unlinkSync(x.temporaryFile);}catch{}delete s.bankReconciliationDrafts[id];}});saveStore(s);res.json({success:true,account:draft.account,reconciledThrough:draft.summary.to,reconciledAt:finalizedAt});});
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
  const on=d=>(!asOf||!d||String(d).slice(0,10)<=asOf)&&cashEntryIsVisible(account,d),openingMap=nature==='SANKI'?(s.openingBalances||{}):(((s.openingBalancesByNature||{})[nature])||{});let total=num(openingMap[account]);
  (s.adjustments||[]).filter(x=>normalizedNature(x.nature)===nature&&x.account===account&&on(x.date)).forEach(x=>total+=num(x.amount));
  (s.receipts||[]).filter(x=>normalizedNature(x.nature)===nature&&x.account===account&&on(x.date)).forEach(x=>total+=num(x.amount));
  (s.transfers||[]).filter(x=>on(x.date)).forEach(x=>{if(normalizedNature(x.fromNature||x.nature)===nature&&x.fromAccount===account)total-=num(x.amount);if(normalizedNature(x.toNature||x.nature)===nature&&x.toAccount===account)total+=num(x.amount);});
  Object.values(s.expenses||{}).forEach(e=>{(e.payments||[]).filter(p=>paymentIsPosted(e)&&(p.account||e.account)===account&&on(p.date)).forEach(p=>total-=num(p.amount));(e.reimbursementPayments||[]).filter(p=>p.account===account&&on(p.date)).forEach(p=>total-=num(p.amount));});
  Object.values(s.receivables||{}).filter(x=>normalizedNature(x.nature)===nature).forEach(x=>(x.collections||[]).filter(c=>c.account===account&&on(c.date)).forEach(c=>total+=num(c.amount)));
  if(nature==='SANKI'){salesLedgerEntries().filter(includeAutomaticSale).filter(x=>x.account===account&&on(x.date)).forEach(x=>total+=num(x.amount));procurementPayables(s,true).forEach(p=>(p.payments||[]).filter(x=>x.account===account&&on(x.date)).forEach(x=>total-=num(x.amount)));salaryAdvanceEntries().filter(x=>x.account===account&&on(x.date)).forEach(x=>total-=num(x.amount));salaryPaymentEntries().filter(x=>x.account===account&&on(x.date)).forEach(x=>total-=num(x.amount));}
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
  if(nature&&!approvalNatures(req).includes(nature))return res.status(403).json({success:false,error:'You cannot view this accounting entity.'});
  const allowed=approvalNatures(req),all=(s.auditLog||[]).filter(x=>allowed.includes(normalizedNature(x.nature))),matches=x=>(!nature||x.nature===nature)&&(!action||x.action===action)&&(!user||String(x.user).toLowerCase().includes(user))&&(!from||String(x.at).slice(0,10)>=from)&&(!to||String(x.at).slice(0,10)<=to)&&(!subject||String(x.subjectId+' '+x.subjectType+' '+x.account+' '+x.paymentId).toLowerCase().includes(subject)),entries=all.filter(matches).slice().reverse().slice(0,1000),expenseIds=Array.from(new Set(entries.filter(x=>x.subjectType==='expense').map(x=>x.subjectId)));
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
  (s.reconciliationExpenses||[]).filter(e=>normalizedNature(e.nature)==='SANKI'&&(!from||e.date>=from)&&(!to||e.date<=to)).forEach(e=>{const type=e.creditCardId?defaultType(e.category||''):(e.type||defaultType(e.category||'')),channel=CHANNELS.includes(e.channel)?e.channel:'Shared';out[channel][type]=(out[channel][type]||0)+num(e.amount);});
  return out;
}

// Run idempotent store repairs/corrections when the service starts, rather
// than waiting for the first user to open an Expenses screen.
loadStore();

module.exports = { router, summaryForPL, createTelegramPersonalExpense, createTelegramPersonalReceipt, createTelegramBusinessPaidExpense, telegramBusinessCategories, telegramSuggestBusinessCategory, telegramExpense, telegramApproveExpense, telegramRejectExpense, telegramRecordPayment, telegramResolveAccount, telegramRecordTransfer, telegramRecordNamitaTransfer, telegramApi, parseBankStatementFile, parseBankStatementText, parseBankStatementUpload, importBankStatementUpload, reconcileBankStatementAccount };
