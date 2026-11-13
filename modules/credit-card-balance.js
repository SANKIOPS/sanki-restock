'use strict';
// Pure liability projection. Issuer-validated finalized statements supersede
// the configured opening; they do not change bank payments or expense records.
const {activeRow}=require('./credit-card-accounting');
const text=v=>String(v||''),money=v=>Math.round((Number(v)||0)*100)/100;
const finite=v=>v!==null&&v!==undefined&&String(v).trim()!==''&&Number.isFinite(Number(v));
const date=v=>/^\d{4}-\d{2}-\d{2}$/.test(text(v))&&Number.isFinite(Date.parse(v+'T00:00:00Z'))&&new Date(v+'T00:00:00Z').toISOString().slice(0,10)===v;
function project(store,cardId,{directPurchases=[],manualRefunds=[]}={}){
 const card=store.cards?.[cardId]||{},statements=Object.values(store.statements||{}).filter(s=>s.cardId===cardId&&s.status==='finalized');
 const anchors=statements.filter(s=>s.statementSourceOfTruth&&s.summary?.validated===true&&date(s.summary.to)&&finite(s.summary.openingBalance)&&finite(s.summary.closingBalance)&&Math.abs(money(Number(s.summary.openingBalance)+(s.rows||[]).reduce((n,r)=>n+Number(r.debit||0)-Number(r.credit||0),0))-money(s.summary.closingBalance))<=.01).sort((a,b)=>a.summary.to.localeCompare(b.summary.to)||a.id.localeCompare(b.id));
 const anchor=anchors.at(-1),warnings=[],entries=[];
 if(anchor&&new Set(anchors.filter(s=>s.summary.to===anchor.summary.to).map(s=>money(s.summary.closingBalance))).size>1)throw Object.assign(Error('Finalized statements disagree on the card closing balance. Review those statements before using this liability.'),{status:409});
 const first=anchors[0],opening=first?money(first.summary.openingBalance):money(card.openingOutstanding);
 entries.push({id:'OPENING',date:'',description:first?'Opening outstanding from validated statement '+first.id:'Opening outstanding',debit:Math.max(0,opening),credit:Math.max(0,-opening),balance:opening});
 const movements=[];
 for(const st of statements)for(const r of st.rows||[]){if(!activeRow(r))continue;const debit=['expense','fee','emi_principal','emi_interest'].includes(r.classification)?money(r.amount):0,credit=['refund','card_payment'].includes(r.classification)?money(r.amount):0;if(debit||credit)movements.push({id:r.id,date:r.date,description:r.narration,debit,credit,statementId:st.id});}
 if(!statements.some(s=>s.statementSourceOfTruth)){
  directPurchases.forEach(p=>movements.push({id:p.id,date:p.date,description:p.description,debit:money(p.amount),credit:0,proof:p.proof}));
  (store.payments||[]).filter(p=>p.cardId===cardId&&p.active!==false).forEach(p=>movements.push({id:p.id,date:p.date,description:p.paymentKind+' card payment from '+p.account,debit:0,credit:money(p.amount),proof:p.proof}));
 }
 for(const r of manualRefunds){if(anchor&&r.date<=anchor.summary.to){warnings.push(r.id+': Credit predates the verified statement close; it is not deducted again.');continue;}movements.push({id:r.id,date:r.date,description:r.source,debit:0,credit:money(r.amount)});}
 movements.sort((a,b)=>text(a.date).localeCompare(text(b.date))||text(a.id).localeCompare(text(b.id)));
 let running=opening,anchored=false;
 const setAnchor=()=>{if(!anchor||anchored)return;if(Math.abs(running-money(anchor.summary.closingBalance))>.01)warnings.push('Earlier row totals do not reproduce the latest issuer closing. The displayed liability uses the validated closing, not an invented adjustment.');running=money(anchor.summary.closingBalance);entries.push({id:'STATEMENT-CLOSE/'+anchor.id,date:anchor.summary.to,description:'Verified issuer closing · '+anchor.id,debit:0,credit:0,balance:running,kind:'statement_closing',statementId:anchor.id});anchored=true;};
 for(const row of movements){if(anchor&&row.date>anchor.summary.to)setAnchor();running=money(running+row.debit-row.credit);entries.push({...row,balance:running});}
 setAnchor();
 return{outstanding:running,entries:entries.reverse(),balanceBasis:anchor?'Validated issuer closing plus later finalized movements':'Configured opening plus recorded movements',verifiedThrough:anchor?.summary.to||'',statementId:anchor?.id||'',configuredOpening:money(card.openingOutstanding),effectiveOpening:opening,balanceWarnings:warnings};
}
module.exports={project};
