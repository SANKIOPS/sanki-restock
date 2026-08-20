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

// Accounts the founder actually pays from are added in-app (with approval) —
// start minimal instead of the old guessed list.
const DEFAULT_ACCOUNTS = ['Paytm', 'Cash'];
const DEFAULT_PEOPLE = ['Pradeep', 'Prashant'];   // claimants (people who run the errand)

// ── Store ────────────────────────────────────────────────────────
function blankStore() {
  return {
    expenses: {},
    vendors: {},
    vendorsByNature: { SAMAST: {}, PERSONAL: {} },
    accounts: DEFAULT_ACCOUNTS.slice(),
    people: DEFAULT_PEOPLE.slice(),      // claimants
    openingBalances: {},                 // { [account]: opening ₹ }
    openingBalancesByNature: { SAMAST: {}, PERSONAL: {} }, // non-SANKI books stay separate
    adjustments: [],                     // [{ id, account, amount(+/-), note, date }] top-ups/corrections
    transfers: [],                       // [{ id, nature, fromAccount, toAccount, amount, date, proof, note }]
    ledgerOverrides: {},
    customLedgers: {},                   // { [name]: { name, type } } admin-approved new categories
    requests: [],                        // [{ id, kind:'ledger'|'account', name, meta, status, by, at, decidedBy, decidedAt }]
    openingInvestment: 0,
    odConfig: { 'Tiana 0425': { limit: 0, ratePct: 0 } },
    seq: 0, adjSeq: 0, transferSeq: 0, reqSeq: 0
  };
}
function loadStore() {
  try { return Object.assign(blankStore(), JSON.parse(fs.readFileSync(EXP_PATH, 'utf8'))); }
  catch { return blankStore(); }
}
function saveStore(s) {
  const tmp = EXP_PATH + '.tmp-' + process.pid + '-' + Date.now();
  fs.writeFileSync(tmp, JSON.stringify(s));
  fs.renameSync(tmp, EXP_PATH);
}
function ledgerMeta(s, name) {
  const ov = (s.ledgerOverrides || {})[name] || {};
  const custom = (s.customLedgers || {})[name] || {};
  return { name, nature: 'SANKI', type: ov.type || custom.type || defaultType(name) };
}
// The category picker = built-in BUSINESS ledgers ∪ admin-approved custom ones.
function pickableLedgers(s) {
  const names = LEDGERS.filter(isBusinessLedger).concat(Object.keys(s.customLedgers || {}));
  const seen = {};
  return names.filter(n => (seen[n] ? false : (seen[n] = true)))
    .map(n => ledgerMeta(s, n))
    .sort((a, b) => a.name.localeCompare(b.name));
}
function storedAccountNames(s) {
  const names = new Set([].concat(s.accounts || [], Object.keys(s.openingBalances || {}), Object.keys(s.odConfig || {})));
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
  if (r.includes('admin')) out.push('SANKI', 'SAMAST');
  if (r.includes('accounting')) out.push('SANKI');
  if (r.includes('samast_accounting')) out.push('SAMAST');
  return Array.from(new Set(out));
}
function submissionNatures(req) {
  const r = rolesOfReq(req);
  if (r.includes('owner')) return NATURES.slice();
  const out = approvalNatures(req);
  if (r.includes('admin') || r.includes('claimant')) out.push('SANKI', 'SAMAST');
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
    telegram.notifyUser(
      e.createdBy || e.claimant,
      `💸 <b>${esc(normalizedNature(e.nature))} · ${esc(e.id)}</b> — ${esc(labels[event] || event)}\n${esc(e.vendor)} · ₹${round0(e.amount)}`,
      { button: { text: 'View expense', url: `/expenses.html?focus=${encodeURIComponent(e.id)}` } }
    );
  } catch { /* Telegram is optional */ }
}

// ── Proof image upload / serve ───────────────────────────────────
router.post('/api/expenses/upload', proofUpload.single('photo'), (req, res) => {
  if (!req.file) return res.status(400).json({ success: false, error: 'No image received.' });
  res.json({ success: true, url: '/api/expenses/photo/' + req.file.filename });
});
router.get('/api/expenses/photo/:file', (req, res) => {
  const name = path.basename(String(req.params.file || ''));
  if (name.startsWith('personal-') && !rolesOfReq(req).includes('owner')) return res.status(403).end();
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
  Object.values(s.expenses || {}).forEach(e => {
    const nature = normalizedNature(e.nature);
    if (e.vendor && !vendorsByNature[nature].some(v => v.toLowerCase() === String(e.vendor).toLowerCase())) vendorsByNature[nature].push(e.vendor);
  });
  Object.keys(vendorsByNature).forEach(n => vendorsByNature[n].sort((a, b) => a.localeCompare(b)));
  res.json({
    success: true,
    ledgers: pickableLedgers(s),
    vendors: vendorsByNature.SANKI,
    vendorsByNature,
    accounts: storedAccountNames(s), people: s.people,
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
  // Claimants do not classify expenses. Admin/Owner assign the category while
  // reviewing; trusted accounting submitters may classify their own entries.
  const ledger = canApprove(req) ? String(b.ledger || '').trim() : '';
  const amount = num(b.amount);
  if (canApprove(req) && !ledger) return res.status(400).json({ success: false, error: 'Pick a category (ledger).' });
  if (ledger && !pickableLedgers(s).some(l => l.name.toLowerCase() === ledger.toLowerCase())) {
    return res.status(400).json({ success: false, error: 'Select an approved category.' });
  }
  if (!(amount > 0)) return res.status(400).json({ success: false, error: 'Amount must be greater than 0.' });

  const nature = normalizedNature(b.nature);
  if (!submissionNatures(req).includes(nature)) return res.status(403).json({ success: false, error: 'You do not have access to this accounting entity.' });
  // Claimants never classify accounting type: every submission starts as
  // Variable. An approver may reclassify it while reviewing the pending row.
  const type = canApprove(req) && TYPES.includes(b.type) ? b.type : 'variable';
  const channel = canApprove(req) && CHANNELS.includes(b.channel) ? b.channel : 'Shared';
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
    qrPhoto: !paidAlready && paymentType === 'UPI' ? qrPhoto : '',
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
      account: '', paymentType, proof: personalPaymentProof,
      note: String(b.paymentNote || 'Paid personally by submitter').trim(),
      paidBy: claimant, paidAt: now, personalFunds: true
    }] : [],
    createdAt: now,
    createdBy: (req.user && req.user.username) || 'system',
    approvedAt: null, approvedBy: null,
    paidAt: null, paidBy: null
  };
  saveStore(s);
  notifyApproversNewExpense(s.expenses[id]);
  res.json({ success: true, expense: s.expenses[id] });
});

// ── Edit ─────────────────────────────────────────────────────────
// Single-segment POST paths that have their OWN handlers registered after this
// param route — the ':id' pattern would otherwise swallow them. Fall through.
const RESERVED_POST = new Set(['requests', 'accounts', 'settings', 'balances', 'transfers', 'vendors', 'custom-ledgers', 'upload']);
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
  if (b.date != null) e.date = String(b.date).slice(0, 10);
  if (b.particulars != null) e.particulars = String(b.particulars).trim();
  if (b.amount != null && num(b.amount) > 0 && num(b.amount) >= num(e.paidAmount)) e.amount = num(b.amount);
  if (b.isInstallment != null) e.isInstallment = b.isInstallment === true || b.isInstallment === 'true';
  if (b.requestedAmount != null) {
    const requested = num(b.requestedAmount);
    if (!(requested > 0) || requested > e.amount) return res.status(400).json({ success: false, error: 'Requested payment must be greater than 0 and cannot exceed the total amount.' });
    if (e.paidAlready && requested < num(e.personalPaidAmount)) return res.status(400).json({ success: false, error: 'Requested payment cannot be less than the amount already paid personally.' });
    e.requestedAmount = requested;
  }
  // Entity is immutable after submission so an entry cannot be moved between
  // separate books accidentally. Owner can delete/re-enter a mistaken record.
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
  if (!e.paidAlready && e.paymentType === 'UPI' && !e.qrPhoto) {
    return res.status(400).json({ success: false, error: 'Vendor QR-code photo is required for UPI payment.' });
  }
  if (e.paymentType !== 'UPI') e.qrPhoto = '';
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
  e.status = e.paidAmount >= e.amount ? 'paid' : (e.paidAmount > 0 ? 'partially_paid' : 'approved');
  if (e.paidAlready) e.reimbursementStatus = 'pending';
  e.approvedAt = new Date().toISOString();
  e.approvedBy = (req.user && req.user.username) || 'admin';
  saveStore(s);
  notifyExpenseUser(e, 'approved');
  res.json({ success: true, expense: e });
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
  if (!storedAccountNames(s).some(name => name.toLowerCase() === account.toLowerCase())) return res.status(400).json({ success: false, error: 'Select a stored paying account or add it first.' });
  e.account = storedAccountNames(s).find(name => name.toLowerCase() === account.toLowerCase()) || account;
  const outstanding = Math.max(0, e.amount - num(e.paidAmount));
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
    paidAt: new Date().toISOString()
  });
  e.status = e.paidAmount >= e.amount ? 'paid' : 'partially_paid';
  e.paidAt = new Date().toISOString();
  e.paidBy = (req.user && req.user.username) || 'admin';
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
  e.reimbursementAmount = num(e.reimbursementAmount) + amount;
  e.reimbursementPayments = Array.isArray(e.reimbursementPayments) ? e.reimbursementPayments : [];
  e.reimbursementPayments.push({
    id: 'REIM-' + String(e.reimbursementPayments.length + 1).padStart(3, '0'), amount,
    date: String(b.date || new Date().toISOString().slice(0, 10)).slice(0, 10),
    account: String(b.account || '').trim(), paymentType: PAYMENT_TYPES.includes(b.paymentType) ? b.paymentType : 'UPI',
    proof, note: String(b.note || '').trim(), paidBy: (req.user && req.user.username) || 'admin', paidAt: new Date().toISOString()
  });
  e.reimbursementStatus = e.reimbursementAmount >= e.personalPaidAmount ? 'reimbursed' : 'partially_reimbursed';
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
  delete s.expenses[req.params.id];
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
  const nature = req.query.nature ? normalizedNature(req.query.nature) : '';
  let list = Object.values(s.expenses).filter(e => {
    if (!canViewExpense(req, e)) return false;
    if (id && e.id !== id) return false;
    if (nature && normalizedNature(e.nature) !== nature) return false;
    if (from && e.date < from) return false;
    if (to && e.date > to) return false;
    if (status && e.status !== status) return false;
    if (type && e.type !== type) return false;
    if (vendor && (e.vendor || '').toLowerCase() !== vendor) return false;
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
  if (!canApprove(req)) return res.status(403).json({ success: false, error: 'Only accounting/admin can view pending payments.' });
  const s = loadStore();
  const nature = req.query.nature ? normalizedNature(req.query.nature) : '';
  const vendor = String(req.query.vendor || '').trim().toLowerCase();
  if (nature && !approvalNatures(req).includes(nature)) return res.status(403).json({ success: false, error: 'You cannot view this accounting entity.' });
  const today = new Date().toISOString().slice(0, 10);
  const expenses = Object.values(s.expenses || {}).filter(e => {
    if (!canApproveExpenseNature(req, e)) return false;
    if (nature && normalizedNature(e.nature) !== nature) return false;
    if (vendor && !String(e.vendor || '').toLowerCase().includes(vendor)) return false;
    return ['approved', 'partially_paid'].includes(e.status) && num(e.paidAmount) < num(e.amount);
  }).map(e => ({
    ...e,
    balanceDue: round0(num(e.amount) - num(e.paidAmount)),
    daysPending: Math.max(0, Math.floor((Date.parse(today) - Date.parse(String(e.approvedAt || e.date).slice(0, 10))) / 86400000))
  })).sort((a, b) => b.daysPending - a.daysPending || String(a.approvedAt || '').localeCompare(String(b.approvedAt || '')));
  res.json({ success: true, expenses, totalOutstanding: round0(expenses.reduce((n, e) => n + e.balanceDue, 0)) });
});

// One-click spending analysis: incurred expense and actual company cash movement
// are deliberately separate so credit purchases do not look like paid cash.
router.get('/api/expenses/spending-dashboard', (req, res) => {
  if (!canApprove(req)) return res.status(403).json({ success: false, error: 'Only accounting/admin can view spending analytics.' });
  const s = loadStore(), from = String(req.query.from || ''), to = String(req.query.to || '');
  const nature = req.query.nature ? normalizedNature(req.query.nature) : '';
  if (nature && !approvalNatures(req).includes(nature)) return res.status(403).json({ success: false, error: 'You cannot view this accounting entity.' });
  const allowed = approvalNatures(req), inRange = d => (!from || d >= from) && (!to || d <= to);
  const groups = { entities: {}, vendors: {}, categories: {}, accounts: {}, daily: {} };
  const add = (bucket, key, field, amount) => {
    key = String(key || 'Unspecified'); bucket[key] = bucket[key] || { name:key, incurred:0, cashPaid:0, outstanding:0 };
    bucket[key][field] = round0(num(bucket[key][field]) + num(amount));
  };
  let incurred = 0, cashPaid = 0, outstanding = 0, reimbursementPaid = 0, count = 0;
  Object.values(s.expenses || {}).forEach(e => {
    const entity = normalizedNature(e.nature);
    if (!allowed.includes(entity) || (nature && entity !== nature)) return;
    const approved = ['approved','partially_paid','paid'].includes(e.status);
    if (approved && inRange(String(e.date || ''))) {
      incurred += num(e.amount); count += 1;
      add(groups.entities, entity, 'incurred', e.amount); add(groups.vendors, e.vendor, 'incurred', e.amount);
      add(groups.categories, e.ledger, 'incurred', e.amount); add(groups.daily, e.date, 'incurred', e.amount);
      const due = e.paidAlready ? Math.max(0, num(e.personalPaidAmount)-num(e.reimbursementAmount)) : Math.max(0,num(e.amount)-num(e.paidAmount));
      outstanding += due; add(groups.entities, entity, 'outstanding', due); add(groups.vendors, e.vendor, 'outstanding', due);
    }
    (e.payments || []).filter(p => !p.personalFunds && inRange(String(p.date || ''))).forEach(p => {
      cashPaid += num(p.amount); add(groups.entities, entity, 'cashPaid', p.amount); add(groups.vendors, e.vendor, 'cashPaid', p.amount);
      add(groups.categories, e.ledger, 'cashPaid', p.amount); add(groups.accounts, p.account || e.account, 'cashPaid', p.amount); add(groups.daily, p.date, 'cashPaid', p.amount);
    });
    (e.reimbursementPayments || []).filter(p => inRange(String(p.date || ''))).forEach(p => {
      cashPaid += num(p.amount); reimbursementPaid += num(p.amount); add(groups.entities, entity, 'cashPaid', p.amount);
      add(groups.accounts, p.account, 'cashPaid', p.amount); add(groups.daily, p.date, 'cashPaid', p.amount);
    });
  });
  const rows = bucket => Object.values(bucket).map(x => ({...x,incurred:round0(x.incurred),cashPaid:round0(x.cashPaid),outstanding:round0(x.outstanding)}))
    .sort((a,b) => (b.incurred+b.cashPaid+b.outstanding)-(a.incurred+a.cashPaid+a.outstanding) || a.name.localeCompare(b.name));
  res.json({ success:true, range:{from,to}, totals:{incurred:round0(incurred),cashPaid:round0(cashPaid),outstanding:round0(outstanding),reimbursementPaid:round0(reimbursementPaid),count},
    entities:rows(groups.entities),vendors:rows(groups.vendors),categories:rows(groups.categories),accounts:rows(groups.accounts),daily:rows(groups.daily).sort((a,b)=>a.name.localeCompare(b.name)) });
});

router.get('/api/expenses/reimbursements', (req, res) => {
  if (!canApprove(req)) return res.status(403).json({ success: false, error: 'Only accounting/admin can view reimbursements.' });
  const s = loadStore();
  const status = String(req.query.status || '');
  const person = String(req.query.person || '').toLowerCase();
  const todayOnly = String(req.query.today || '') === 'true';
  const today = new Date().toISOString().slice(0, 10);
  const nature = req.query.nature ? normalizedNature(req.query.nature) : '';
  let list = Object.values(s.expenses || {}).filter(e => {
    if (!canApproveExpenseNature(req, e)) return false;
    if (nature && normalizedNature(e.nature) !== nature) return false;
    if (!e.paidAlready || e.reimbursementStatus === 'awaiting_approval' || e.reimbursementStatus === 'rejected') return false;
    if (status && e.reimbursementStatus !== status) return false;
    if (person && !String(e.createdBy || e.claimant || '').toLowerCase().includes(person)) return false;
    if (todayOnly && e.date !== today) return false;
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

// ── Vendor books (accounts payable per vendor) ───────────────────
router.get('/api/expenses/vendors', (req, res) => {
  if (!canApprove(req)) return res.status(403).json({ success: false, error: 'Only accounting/admin can view vendor books.' });
  const s = loadStore();
  const nature = req.query.nature ? normalizedNature(req.query.nature) : approvalNatures(req)[0];
  if (!approvalNatures(req).includes(nature)) return res.status(403).json({ success: false, error: 'You cannot view this accounting entity.' });
  const books = {};
  const master = nature === 'SANKI' ? s.vendors : (((s.vendorsByNature || {})[nature]) || {});
  Object.values(master).forEach(v => { books[v.name] = { name: v.name, billed: 0, paid: 0, outstanding: 0, count: 0, notes: v.notes || '' }; });
  Object.values(s.expenses).forEach(e => {
    if (normalizedNature(e.nature) !== nature) return;
    if (!e.vendor) return;
    const b = books[e.vendor] || (books[e.vendor] = { name: e.vendor, billed: 0, paid: 0, outstanding: 0, count: 0, notes: '' });
    b.billed += e.amount; b.paid += num(e.paidAmount); b.count += 1;
  });
  const list = Object.values(books).map(b => ({
    name: b.name, count: b.count, billed: round0(b.billed), paid: round0(b.paid),
    outstanding: round0(b.billed - b.paid), notes: b.notes
  })).sort((a, b) => b.outstanding - a.outstanding || a.name.localeCompare(b.name));
  res.json({ success: true, vendors: list, totalOutstanding: list.reduce((n, b) => n + b.outstanding, 0) });
});
router.post('/api/expenses/vendors', (req, res) => {
  if (!canApprove(req)) return res.status(403).json({ success: false, error: 'Only accounting/admin can edit vendors.' });
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
  saveStore(s);
  res.json({ success: true });
});

// ── Running cash balances per account ────────────────────────────
// balance = opening + adjustments + transfers in − transfers out − payments.
router.get('/api/expenses/balances', (req, res) => {
  if (!canApprove(req)) return res.status(403).json({ success: false, error: 'Only accounting/admin can view balances.' });
  const s = loadStore();
  const nature = req.query.nature ? normalizedNature(req.query.nature) : approvalNatures(req)[0];
  if (!approvalNatures(req).includes(nature)) return res.status(403).json({ success: false, error: 'You cannot view this accounting entity.' });
  const paidOut = {}; const adj = {}; const transferIn = {}; const transferOut = {};
  Object.values(s.expenses).forEach(e => {
    if (normalizedNature(e.nature) !== nature) return;
    const a = e.account || '(unspecified)';
    // Personally funded payments did not leave a company account.
    const companyPaid = (e.payments || []).filter(p => !p.personalFunds)
      .reduce((n, p) => n + num(p.amount), 0);
    if (companyPaid > 0) paidOut[a] = (paidOut[a] || 0) + companyPaid;
    (e.reimbursementPayments || []).forEach(p => {
      const ra = p.account || '(unspecified)';
      paidOut[ra] = (paidOut[ra] || 0) + num(p.amount);
    });
  });
  (s.adjustments || []).filter(x => normalizedNature(x.nature) === nature).forEach(x => { adj[x.account] = (adj[x.account] || 0) + num(x.amount); });
  (s.transfers || []).filter(x => normalizedNature(x.nature) === nature).forEach(x => {
    transferOut[x.fromAccount] = (transferOut[x.fromAccount] || 0) + num(x.amount);
    transferIn[x.toAccount] = (transferIn[x.toAccount] || 0) + num(x.amount);
  });
  const openingMap = nature === 'SANKI' ? (s.openingBalances || {}) : (((s.openingBalancesByNature || {})[nature]) || {});
  const accounts = storedAccountNames(s).map(name => {
    const opening = num(openingMap[name]);
    const spent = round0(paidOut[name] || 0);
    const topups = round0(adj[name] || 0);
    const transferredIn = round0(transferIn[name] || 0), transferredOut = round0(transferOut[name] || 0);
    return { name, opening: round0(opening), topups, transferredIn, transferredOut, spent, balance: round0(opening + topups + transferredIn - transferredOut - spent) };
  });
  res.json({ success: true, accounts });
});

// A transfer is one atomic event that produces a debit and matching credit.
router.post('/api/expenses/transfers', (req, res) => {
  if (!canApprove(req)) return res.status(403).json({ success: false, error: 'Only accounting/admin can record transfers.' });
  const s = loadStore(); const b = req.body || {};
  const nature = normalizedNature(b.nature);
  if (!approvalNatures(req).includes(nature)) return res.status(403).json({ success: false, error: 'You cannot transfer funds for this accounting entity.' });
  const fromAccount = String(b.fromAccount || '').trim(), toAccount = String(b.toAccount || '').trim();
  const amount = num(b.amount), proof = String(b.proof || '').trim();
  const accounts = storedAccountNames(s);
  if (!fromAccount || !toAccount) return res.status(400).json({ success: false, error: 'Select both accounts.' });
  if (fromAccount.toLowerCase() === toAccount.toLowerCase()) return res.status(400).json({ success: false, error: 'Source and destination accounts must be different.' });
  if (!accounts.some(a => a.toLowerCase() === fromAccount.toLowerCase()) || !accounts.some(a => a.toLowerCase() === toAccount.toLowerCase())) return res.status(400).json({ success: false, error: 'Select stored accounts.' });
  if (!(amount > 0)) return res.status(400).json({ success: false, error: 'Transfer amount must be greater than 0.' });
  if (!proof) return res.status(400).json({ success: false, error: 'Transfer proof is required.' });
  s.transferSeq = (s.transferSeq || 0) + 1;
  const transfer = { id: 'TR-' + String(s.transferSeq).padStart(5, '0'), nature, fromAccount, toAccount, amount,
    date: String(b.date || new Date().toISOString().slice(0, 10)).slice(0, 10), proof, note: String(b.note || '').trim(),
    createdBy: (req.user && req.user.username) || 'admin', createdAt: new Date().toISOString() };
  s.transfers = Array.isArray(s.transfers) ? s.transfers : []; s.transfers.push(transfer); saveStore(s);
  res.json({ success: true, transfer });
});

router.get('/api/expenses/account-ledger', (req, res) => {
  if (!canApprove(req)) return res.status(403).json({ success: false, error: 'Only accounting/admin can view account ledgers.' });
  const s = loadStore(), nature = normalizedNature(req.query.nature), account = String(req.query.account || '').trim();
  if (!approvalNatures(req).includes(nature)) return res.status(403).json({ success: false, error: 'You cannot view this accounting entity.' });
  if (!account) return res.status(400).json({ success: false, error: 'Select an account.' });
  const from = String(req.query.from || ''), to = String(req.query.to || ''), entries = [];
  const openingMap = nature === 'SANKI' ? (s.openingBalances || {}) : (((s.openingBalancesByNature || {})[nature]) || {});
  entries.push({ id: 'OPENING', date: '', kind: 'opening', description: 'Opening balance', credit: num(openingMap[account]), debit: 0 });
  (s.adjustments || []).filter(x => normalizedNature(x.nature) === nature && x.account === account).forEach(x => entries.push({ id:x.id,date:x.date,kind:'adjustment',description:x.note||'Balance adjustment',credit:Math.max(0,num(x.amount)),debit:Math.max(0,-num(x.amount)) }));
  (s.transfers || []).filter(x => normalizedNature(x.nature) === nature && (x.fromAccount === account || x.toAccount === account)).forEach(x => entries.push({ id:x.id,date:x.date,kind:'transfer',description:x.fromAccount===account?'Transfer to '+x.toAccount:'Transfer from '+x.fromAccount,credit:x.toAccount===account?num(x.amount):0,debit:x.fromAccount===account?num(x.amount):0,proof:x.proof,note:x.note,by:x.createdBy }));
  Object.values(s.expenses || {}).filter(e => normalizedNature(e.nature) === nature).forEach(e => {
    (e.payments || []).filter(p => !p.personalFunds && (p.account || e.account) === account).forEach(p => entries.push({id:e.id+'/'+p.id,date:p.date,kind:'expense',description:(e.vendor||'Vendor')+' · '+(e.particulars||e.id),credit:0,debit:num(p.amount),proof:p.proof,by:p.paidBy}));
    (e.reimbursementPayments || []).filter(p => p.account === account).forEach(p => entries.push({id:e.id+'/'+p.id,date:p.date,kind:'reimbursement',description:'Reimbursement to '+(e.claimant||e.createdBy||'claimant'),credit:0,debit:num(p.amount),proof:p.proof,by:p.paidBy}));
  });
  const ordered = entries.sort((a,b) => String(a.date).localeCompare(String(b.date)) || String(a.id).localeCompare(String(b.id)));
  let running = 0; ordered.forEach(x => { running += num(x.credit)-num(x.debit); x.balance = round0(running); });
  const visible = ordered.filter(x => x.kind === 'opening' || ((!from || x.date >= from) && (!to || x.date <= to)));
  res.json({ success:true, account, nature, entries:visible, balance:round0(running) });
});
router.post('/api/expenses/balances', (req, res) => {
  if (!canApprove(req)) return res.status(403).json({ success: false, error: 'Only accounting/admin can edit balances.' });
  const s = loadStore();
  const b = req.body || {};
  const nature = normalizedNature(b.nature);
  if (!approvalNatures(req).includes(nature)) return res.status(403).json({ success: false, error: 'You cannot edit this accounting entity.' });
  if (b.setOpening && b.setOpening.account) {
    if (nature !== 'SANKI') {
      s.openingBalancesByNature = s.openingBalancesByNature || {};
      s.openingBalancesByNature[nature] = s.openingBalancesByNature[nature] || {};
      s.openingBalancesByNature[nature][String(b.setOpening.account)] = num(b.setOpening.amount);
    } else s.openingBalances[String(b.setOpening.account)] = num(b.setOpening.amount);
  }
  if (b.adjust && b.adjust.account && b.adjust.amount != null) {
    s.adjSeq = (s.adjSeq || 0) + 1;
    s.adjustments.push({
      id: 'ADJ-' + s.adjSeq, account: String(b.adjust.account),
      amount: num(b.adjust.amount), note: String(b.adjust.note || ''),
      date: (b.adjust.date || new Date().toISOString().slice(0, 10)).toString().slice(0, 10), nature
    });
  }
  saveStore(s);
  res.json({ success: true });
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
  saveStore(s);
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
  saveStore(s);
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
  saveStore(s);
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
  saveStore(s);
  res.json({ success: true, account: existing || name, accounts: storedAccountNames(s) });
});
router.post('/api/expenses/accounts/remove', (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ success: false, error: 'Admin only.' });
  const s = loadStore();
  const name = String((req.body || {}).name || '').trim();
  s.accounts = (s.accounts || []).filter(a => a !== name);
  saveStore(s);
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
  saveStore(s);
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

module.exports = { router, summaryForPL };
