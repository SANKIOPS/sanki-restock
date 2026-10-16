'use strict';

// Accounting roles remain additive. A claimant without a separately granted
// approval role is a submitter, never an expense operator.
function claimantOnly(req) {
  const user = req && req.user || {};
  const roles = Array.isArray(user.roles) ? user.roles : [user.role].filter(Boolean);
  return roles.some(role => ['claimant', 'personal_claimant'].includes(role)) &&
    !roles.some(role => ['owner', 'admin', 'accounting', 'samast_accounting'].includes(role));
}

function businessDate(at = new Date()) {
  return new Date(at).toLocaleDateString('sv-SE', { timeZone: 'Asia/Kolkata' });
}

function submissionDate(req, value, at = new Date()) {
  const today = businessDate(at);
  if (!claimantOnly(req)) return { date: String(value || today).slice(0, 10), businessDate: today };
  const date = value == null || value === '' ? today : String(value);
  if (date !== today) return {
    error: 'Claimants can log expenses only for today (' + today + ', India time). Backdated and future-dated expenses are not allowed.',
    businessDate: today
  };
  return { date: today, businessDate: today };
}

function allowedRequest(method, requestPath) {
  const path = String(requestPath || '').replace(/\/+$/, '');
  if (method === 'POST') return ['/api/expenses', '/api/expenses/upload'].includes(path);
  if (method !== 'GET') return false;
  return ['/api/expenses/config', '/api/expenses/list'].includes(path) ||
    /^\/api\/expenses\/EX-[A-Za-z0-9-]+$/.test(path) ||
    /^\/api\/expenses\/photo\/[^/]+$/.test(path);
}

function expenseProofs(expense) {
  const e = expense || {};
  return [].concat(e.billPhotos || [], e.billPhoto || [], e.qrPhoto || [],
    e.purchasePaymentProofs || [], e.purchasePaymentProof || [],
    e.paymentProofs || [], e.paymentProof || [],
    ...(e.payments || []).map(p => [].concat(p.proofs || [], p.proof || [])),
    ...(e.reimbursementPayments || []).map(p => [].concat(p.proofs || [], p.proof || []))).filter(Boolean);
}

module.exports = { claimantOnly, businessDate, submissionDate, allowedRequest, expenseProofs };
