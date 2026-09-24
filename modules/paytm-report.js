const XLSX = require('xlsx');

const START_DATE = '2026-08-22';

function money(value) {
  const n = Number(String(value == null ? '' : value).replace(/[,₹']/g, '').trim());
  return Number.isFinite(n) ? Math.round(n * 100) / 100 : NaN;
}
function clean(value) { return String(value == null ? '' : value).trim().replace(/^'|'$/g, ''); }
function date(value) {
  const text = clean(value);
  const iso = text.match(/^(\d{4})[-/](\d{2})[-/](\d{2})/);
  const indian = text.match(/^(\d{2})[-/](\d{2})[-/](\d{4})/);
  const result = iso ? `${iso[1]}-${iso[2]}-${iso[3]}` : indian ? `${indian[3]}-${indian[2]}-${indian[1]}` : '';
  if (!result) return '';
  const parsed = new Date(result + 'T00:00:00Z');
  return !Number.isNaN(parsed.getTime()) && parsed.toISOString().slice(0, 10) === result ? result : '';
}
function column(row, ...names) {
  for (const name of names) {
    const key = Object.keys(row).find(x => x.toLowerCase() === name.toLowerCase());
    if (key && row[key] !== undefined && row[key] !== '') return row[key];
  }
  return '';
}
function parsePaytmReport(buffer, fileName, throughDate) {
  const workbook = XLSX.read(buffer, { type: 'buffer', raw: true, cellDates: false });
  const sheet = workbook.Sheets[workbook.SheetNames[0]];
  const rows = XLSX.utils.sheet_to_json(sheet, { defval: '', raw: true });
  if (!rows.length) throw new Error('The Paytm report has no transaction rows.');
  const keys = Object.keys(rows[0]).map(x => x.toLowerCase());
  if (!keys.includes('status') || !keys.includes('amount') || !keys.includes('settled_amount')) throw new Error('This does not look like a Paytm transaction report. Expected Status, Amount and Settled_Amount columns.');
  const transactions = [], warnings = [], seen = new Map();
  let skipped = 0, outsidePeriod = 0, missingId = 0;
  rows.forEach((row, index) => {
    if (clean(column(row, 'Status')).toUpperCase() !== 'SUCCESS') { skipped++; return; }
    const transactionId = clean(column(row, 'Transaction_ID', 'transaction_id'));
    const transactionDate = date(column(row, 'Transaction_Date', 'transaction_date', 'Updated_Date', 'updated_date'));
    const amount = money(column(row, 'Amount', 'amount'));
    const commission = money(column(row, 'Commission', 'commission'));
    const gst = money(column(row, 'GST', 'gst'));
    const platformFee = money(column(row, 'Platform_Fee', 'platform_fee')) || 0;
    const settledAmount = money(column(row, 'Settled_Amount', 'settled_amount'));
    if (!transactionDate || [amount, commission, gst, platformFee, settledAmount].some(n => !Number.isFinite(n)) || amount <= 0 || commission < 0 || gst < 0 || platformFee < 0 || settledAmount < 0) throw new Error(`Row ${index + 2} has an invalid successful payment date or amount.`);
    if (transactionDate < START_DATE || transactionDate > throughDate) { outsidePeriod++; return; }
    if (Math.abs(amount - commission - gst - platformFee - settledAmount) > 0.02) throw new Error(`Row ${index + 2} does not balance: gross minus commission, platform fee and GST differs from settled amount.`);
    if (!transactionId) { missingId++; return; }
    const tx = {
      transactionId, date: transactionDate,
      time: clean(column(row, 'Transaction_Date', 'transaction_date', 'Updated_Date', 'updated_date')),
      amount, commission, gst, platformFee, settledAmount,
      payoutId: clean(column(row, 'Payout_ID', 'payout_id')),
      payoutDate: date(column(row, 'Payout_Date', 'payout_date')),
      utr: clean(column(row, 'UTR_No.', 'utr_no')),
      settledDate: date(column(row, 'Settled_Date', 'settled_date')),
      transactionType: clean(column(row, 'Transaction_Type', 'transaction_type')),
      comments: clean(column(row, 'Comments', 'comments')),
      paymentMode: clean(column(row, 'Payment_Mode', 'payment_mode')),
      posId: clean(column(row, 'POS_ID', 'pos_id')),
      merchantOrderId: clean(column(row, 'Merchant_Order_ID', 'merchant_order_id')),
      rrn: clean(column(row, 'RRN', 'rrncode')),
      source: fileName
    };
    tx.isCustomerPayment = !tx.transactionType || tx.transactionType.toUpperCase() === 'ACQUIRING';
    const previous = seen.get(transactionId);
    if (previous) {
      if (JSON.stringify({ ...previous, source: '' }) !== JSON.stringify({ ...tx, source: '' })) throw new Error(`Transaction ${transactionId} has conflicting values within this report.`);
      return;
    }
    seen.set(transactionId, tx);
    transactions.push(tx);
  });
  if (missingId) warnings.push(`${missingId} successful row(s) have no transaction ID and cannot be safely imported individually. Download the detailed Paytm report for those dates.`);
  if (outsidePeriod) warnings.push(`${outsidePeriod} successful row(s) fall outside ${START_DATE} to ${throughDate}.`);
  if (!transactions.length && !missingId) warnings.push('No successful transactions in the selected period.');
  return { transactions, warnings, skipped, missingId, outsidePeriod, sourceRows: rows.length };
}
function summarizePayouts(transactions) {
  const payouts = new Map();
  for (const tx of transactions) {
    const key = tx.utr || tx.payoutId;
    if (!key) continue;
    const settlementDate = tx.settledDate || tx.payoutDate;
    const payout = payouts.get(key) || { payoutId: key, settlementId: key, payoutDate: settlementDate, settledDate: settlementDate, utr: tx.utr, sourcePayoutIds: [], count: 0, customerPaymentCount: 0, nonCustomerCount: 0, gross: 0, customerGross: 0, nonCustomerAmount: 0, commission: 0, platformFee: 0, gst: 0, net: 0, transactionIds: [] };
    if (payout.payoutDate && settlementDate && payout.payoutDate !== settlementDate) throw new Error(`Settlement ${key} has conflicting settled dates.`);
    if (tx.payoutId && !payout.sourcePayoutIds.includes(tx.payoutId)) payout.sourcePayoutIds.push(tx.payoutId);
    payout.count++;
    if (tx.isCustomerPayment === false) { payout.nonCustomerCount++; payout.nonCustomerAmount = Math.round((payout.nonCustomerAmount + tx.amount) * 100) / 100; }
    else { payout.customerPaymentCount++; payout.customerGross = Math.round((payout.customerGross + tx.amount) * 100) / 100; }
    payout.gross = Math.round((payout.gross + tx.amount) * 100) / 100;
    payout.commission = Math.round((payout.commission + tx.commission) * 100) / 100;
    payout.platformFee = Math.round((payout.platformFee + Number(tx.platformFee || 0)) * 100) / 100;
    payout.gst = Math.round((payout.gst + tx.gst) * 100) / 100;
    payout.net = Math.round((payout.net + tx.settledAmount) * 100) / 100;
    payout.transactionIds.push(tx.transactionId);
    payouts.set(key, payout);
  }
  return [...payouts.values()].sort((a, b) => a.payoutDate.localeCompare(b.payoutDate) || a.payoutId.localeCompare(b.payoutId));
}

module.exports = { START_DATE, parsePaytmReport, summarizePayouts };
