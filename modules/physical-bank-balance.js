'use strict';
const { verifiedReconciliationAnchor } = require('./reconciliation-opening');
const money = n => Math.round((Number(n) || 0) * 100) / 100;
const nextDay = d => new Date(Date.parse(d + 'T00:00:00Z') + 86400000).toISOString().slice(0,10);
// A physical bank close covers ALL entities. Never add an entity opening or
// replay its old source payments on top of that same verified close.
function project({book = {}, movements = [], opening = 0, openingDate = '', to}) {
  const anchor = verifiedReconciliationAnchor(book, {before:nextDay(to), nature:'SAMAST',account:'IndusInd Bank 7883'});
  if(anchor.anchorConflict) throw Object.assign(Error(anchor.anchorConflict.message), {status:409});
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
  if(hasAnchor && to>cutoff) warnings.push('Bank verified through '+cutoff+'. Later recorded movements are included but are not a bank-verified closing.');
  return {balance:money(balance),statementBalance:money(balance),openingEntry:base,rows,anchor:hasAnchor?anchor:null,reconciledThrough:cutoff,warnings,basis:hasAnchor?'Verified bank closing plus later recorded movements':'Recorded physical opening plus movements',verified:hasAnchor&&to===cutoff};
}
module.exports={project,money};
