'use strict';
const { verifiedReconciliationAnchor } = require('./reconciliation-opening');
const money = n => Math.round((Number(n) || 0) * 100) / 100;
const nextDay = d => new Date(Date.parse(d + 'T00:00:00Z') + 86400000).toISOString().slice(0,10);
const calendarDate = value => {
  const date=String(value||'');
  if(!/^\d{4}-\d{2}-\d{2}$/.test(date))return'';
  const parsed=new Date(date+'T00:00:00Z');
  return Number.isFinite(parsed.getTime())&&parsed.toISOString().slice(0,10)===date?date:'';
};

// A finalized statement can contain authoritative physical-bank evidence even
// while its ledger difference remains pending. That closing must never become
// an accounting opening anchor, but it is still the safest basis for the
// read-only bank-balance display. Otherwise a shared account falls back to one
// entity's ledger arithmetic and can show a wildly false balance.
function pendingStatementAnchor(book = {}, beforeValue) {
  const before=calendarDate(beforeValue),candidates=(book.imports||[]).flatMap(record=>{
    const summary=record&&record.statementSummary||{},date=calendarDate(record&&record.to||summary.to);
    const closing=summary.closingBalance;
    if(!before||!date||date>=before||summary.validated!==true||closing==null
      ||String(closing).trim()===''||!Number.isFinite(Number(closing)))return[];
    return[{record,date,closing:money(closing)}];
  });
  const cutoffDate=candidates.map(candidate=>candidate.date).sort().at(-1);
  if(!cutoffDate)return null;
  const latest=candidates.filter(candidate=>candidate.date===cutoffDate),amounts=new Set(latest.map(candidate=>Math.round(candidate.closing*100)));
  if(amounts.size!==1)return null;
  const selected=latest.slice().sort((a,b)=>String(a.record.id||'').localeCompare(String(b.record.id||'')))[0];
  return{cutoffDate,previousClosing:selected.closing,anchorSource:'pending_statement',anchorRecordId:String(selected.record.id||''),balancePending:true};
}
// A physical bank close covers ALL entities. Never add an entity opening or
// replay its old source payments on top of that same verified close.
function project({book = {}, movements = [], opening = 0, openingDate = '', to, allowPendingBankAnchor = false}) {
  let anchor = verifiedReconciliationAnchor(book, {before:nextDay(to), nature:'SAMAST',account:'IndusInd Bank 7883'});
  if(anchor.anchorConflict){
    const pending=allowPendingBankAnchor&&pendingStatementAnchor(book,nextDay(to));
    if(!pending)throw Object.assign(Error(anchor.anchorConflict.message), {status:409});
    anchor=pending;
  }
  const cutoff = anchor.cutoffDate || '', hasAnchor = anchor.previousClosing != null;
  const warnings = [], unique = new Map();
  for(const row of movements) {
    if(!row.id || !row.date || row.date > to) continue;
    const previous = unique.get(row.id);
    if(previous && (money(previous.credit)!==money(row.credit)||money(previous.debit)!==money(row.debit)||previous.date!==row.date)) throw Object.assign(Error('Conflicting physical bank movement: '+row.id),{status:409});
    unique.set(row.id,row);
  }
  const rows = [...unique.values()].filter(r=>hasAnchor?r.date>cutoff:(!openingDate||r.date>=openingDate)).sort((a,b)=>a.date.localeCompare(b.date)||a.id.localeCompare(b.id));
  let balance = hasAnchor?anchor.previousClosing:Number(opening)||0;
  const base = {id:'OPENING',kind:'opening',date:cutoff||openingDate,description:hasAnchor?'Verified bank closing brought forward · '+cutoff:'Physical bank opening',credit:Math.max(0,balance),debit:Math.max(0,-balance),balance,editable:false};
  for(const row of rows) { balance=money(balance+Number(row.credit||0)-Number(row.debit||0)); }
  if(!hasAnchor) warnings.push('No verified bank closing is available for this date. Balance uses the recorded physical opening and entries.');
  if(anchor.balancePending)warnings.push('Bank statement closing used through '+cutoff+'; its ledger difference is still pending review. Later recorded movements are included once.');
  else if(hasAnchor && to>cutoff) warnings.push('Bank verified through '+cutoff+'. Later recorded movements are included but are not a bank-verified closing.');
  const basis=anchor.balancePending?'Validated bank statement closing (ledger balance pending) plus later recorded movements':hasAnchor?'Verified bank closing plus later recorded movements':'Recorded physical opening plus movements';
  return {balance:money(balance),statementBalance:money(balance),openingEntry:base,rows,anchor:hasAnchor?anchor:null,reconciledThrough:cutoff,warnings,basis,verified:hasAnchor&&!anchor.balancePending&&to===cutoff};
}
module.exports={project,money};
