'use strict';

// A shared physical bank account is not itself proof that one entity funded
// another. This projection keeps each entity's opening and tagged movements
// separate, then proves the split only when both entity ledgers add back to the
// physical bank balance. It never writes or retags accounting records.
const NATURES=['SAMAST','PERSONAL'];
const money=value=>Math.round(Number(value||0)*100)/100;
const own=(object,key)=>Object.prototype.hasOwnProperty.call(object||{},key);
const previousDay=value=>new Date(Date.parse(value+'T00:00:00Z')-86400000).toISOString().slice(0,10);
const validDate=value=>{const text=String(value||''),parsed=new Date(text+'T00:00:00Z');return /^\d{4}-\d{2}-\d{2}$/.test(text)&&Number.isFinite(parsed.getTime())&&parsed.toISOString().slice(0,10)===text;};
const nature=value=>String(value||'').trim().toUpperCase();

function build(store,{account,from,to,movements=[],physicalBalance=null,conflicts=[],verificationThrough=''}){
  if(!account||!validDate(from)||!validDate(to)||from>to)throw Object.assign(Error('A valid shared account and date range are required.'),{status:400});
  const openings={},openingDates={},missingOpeningEvidence=[],invalidOpeningEvidence=[];
  for(const entity of NATURES){
    const amounts=(store.openingBalancesByNature||{})[entity]||{},dates=(store.openingBalanceDatesByNature||{})[entity]||{};
    if(!own(amounts,account)||!validDate(dates[account]))missingOpeningEvidence.push(entity);
    const amount=Number(amounts[account]);
    if(own(amounts,account)&&!Number.isFinite(amount))invalidOpeningEvidence.push(entity);
    openings[entity]=Number.isFinite(amount)?money(amount):0;openingDates[entity]=String(dates[account]||'');
  }

  const unique=new Map(),movementConflicts=[],invalidMovements=[];
  for(const original of movements||[]){
    if(!original||!original.id||!validDate(original.date)){
      invalidMovements.push({id:original&&original.id||'',date:original&&original.date||'',reason:'Movement identity and date must be present and valid'});
      continue;
    }
    if(original.date>to)continue;
    const debit=Number(original.debit||0),credit=Number(original.credit||0);
    if(!Number.isFinite(debit)||!Number.isFinite(credit)||debit<0||credit<0||debit>0&&credit>0){invalidMovements.push({id:original.id,date:original.date,reason:'Debit and credit must be finite, non-negative and one-sided'});continue;}
    const row={...original,debit:money(debit),credit:money(credit)},prior=unique.get(row.id);
    if(prior&&(prior.date!==row.date||prior.debit!==row.debit||prior.credit!==row.credit||nature(prior.entity)!==nature(row.entity))){
      movementConflicts.push({id:row.id,reason:'Conflicting date, amount or entity on the same bank movement'});continue;
    }
    unique.set(row.id,row);
  }
  const rows=[...unique.values()].sort((a,b)=>a.date.localeCompare(b.date)||String(a.id).localeCompare(String(b.id)));
  const unassigned=rows.filter(row=>{
    const net=money(row.credit-row.debit);
    return net&&(!NATURES.includes(nature(row.entity)));
  }).map(row=>({...row,net:money(row.credit-row.debit)}));
  const balanceAt=(entity,cutoff)=>{
    let balance=openings[entity];
    for(const row of rows){
      if(row.date>cutoff||openingDates[entity]&&row.date<openingDates[entity])continue;
      const movementEntity=nature(row.entity),net=money(row.credit-row.debit);
      if(!net)continue;
      if(!NATURES.includes(movementEntity))continue;
      if(movementEntity===entity)balance=money(balance+net);
    }
    return balance;
  };
  const before=previousDay(from),openingPosition=Object.fromEntries(NATURES.map(entity=>[entity,balanceAt(entity,before)])),closingPosition=Object.fromEntries(NATURES.map(entity=>[entity,balanceAt(entity,to)]));
  const personalDeficitOpening=money(Math.max(0,-openingPosition.PERSONAL)),personalDeficitClosing=money(Math.max(0,-closingPosition.PERSONAL));
  const physicalKnown=physicalBalance!=null&&Number.isFinite(Number(physicalBalance)),entityClosingTotal=money(NATURES.reduce((sum,entity)=>sum+closingPosition[entity],0)),physicalDifference=physicalKnown?money(entityClosingTotal-Number(physicalBalance)):null;
  const blockers=[];
  if(missingOpeningEvidence.length)blockers.push('Opening evidence is missing for '+missingOpeningEvidence.join(' and ')+'.');
  if(invalidOpeningEvidence.length)blockers.push('Opening amount is invalid for '+invalidOpeningEvidence.join(' and ')+'.');
  const validOpeningDates=NATURES.map(entity=>openingDates[entity]).filter(validDate);
  if(new Set(validOpeningDates).size>1)blockers.push('SAMAST and Personal opening evidence uses different dates. A common physical-account opening date is required.');
  if(validOpeningDates.some(date=>date>from))blockers.push('The shared-account opening evidence begins after the selected report starts.');
  if(invalidMovements.length)blockers.push(invalidMovements.length+' bank movement(s) have an invalid identity, date, debit or credit amount.');
  if(unassigned.length)blockers.push(unassigned.length+' bank movement(s) do not have a reliable entity tag.');
  if(movementConflicts.length)blockers.push(movementConflicts.length+' duplicate movement identity conflict(s) exist.');
  if((conflicts||[]).length)blockers.push(conflicts.length+' finalized bank authority conflict(s) exist.');
  if(!validDate(verificationThrough)||verificationThrough<to)blockers.push('The physical bank account is not verified through '+to+'.');
  if(!physicalKnown)blockers.push('The physical bank closing is unavailable.');
  else if(Math.abs(physicalDifference)>.009)blockers.push('Entity sub-ledgers differ from the physical bank balance by ₹'+Math.abs(physicalDifference).toFixed(2)+'.');
  const proven=!blockers.length;
  return{
    account,from,to,verificationThrough,proven,status:proven?'proven':'needs_review',basis:'Personal opening + Personal-tagged credits − Personal-tagged debits. A negative Personal position is money of the other entity used by Personal, but only after both entity sub-ledgers reconcile to the physical bank balance and that bank balance is verified through the report date.',
    openings,openingDates,openingPosition,closingPosition,entityClosingTotal,physicalBalance:physicalKnown?money(physicalBalance):null,physicalDifference,
    personalDeficitOpening,personalDeficitClosing,
    periodIncrease:money(Math.max(0,personalDeficitClosing-personalDeficitOpening)),
    periodRestored:money(Math.max(0,personalDeficitOpening-personalDeficitClosing)),
    missingOpeningEvidence,invalidOpeningEvidence,invalidMovements,unassignedRows:unassigned,movementConflicts,authorityConflicts:conflicts||[],blockers
  };
}

module.exports={build,NATURES};
