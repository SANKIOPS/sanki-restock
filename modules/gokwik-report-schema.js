'use strict';

// One interpretation of the provider's source fields for every GoKwik reader.
// Keep identifiers as text. Never turn a rounded scientific ID into an exact ID.
const text=value=>String(value==null?'':value).trim();
function date(value) {
  const raw=text(value),parts=raw.match(/^(?:(\d{1,2})([/-])(\d{1,2})\2(\d{4})|(\d{4})-(\d{2})-(\d{2}))(?:[ T](\d{1,2}):(\d{2})(?::(\d{2}))?\s*(AM|PM)?)?$/i);
  if(!parts)throw Error('Invalid GoKwik date: '+raw+'. Use DD-MM-YYYY or DD/MM/YYYY, optionally with a valid time.');
  const year=Number(parts[4]||parts[5]),month=Number(parts[3]||parts[6]),day=Number(parts[1]||parts[7]);
  const hours=Number(parts[8]||0),minutes=Number(parts[9]||0),seconds=Number(parts[10]||0),ampm=parts[11];
  const parsed=new Date(Date.UTC(year,month-1,day));
  if(year<1900||year>9999||parsed.getUTCFullYear()!==year||parsed.getUTCMonth()!==month-1||parsed.getUTCDate()!==day||minutes>59||seconds>59||(ampm?(hours<1||hours>12):hours>23))throw Error('Invalid GoKwik date/time: '+raw);
  return `${year}-${String(month).padStart(2,'0')}-${String(day).padStart(2,'0')}`;
}
function sourceMoney(value,field) {
  const raw=text(value),valid=/^(?:\d+|\d{1,3}(?:,\d{3})+|\d{1,2}(?:,\d{2})*,\d{3})(?:\.\d{1,4})?$/.test(raw);
  const m=valid&&raw.replace(/,/g,'').match(/^(\d+)(?:\.(\d{1,4}))?$/);
  if(!m||/[1-9]/.test((m[2]||'').slice(2)))throw Error('Invalid paise amount in '+field+': '+raw);
  const result=Number(m[1])*100+Number(((m[2]||'')+'00').slice(0,2));
  if(!Number.isSafeInteger(result))throw Error('Amount too large in '+field);
  return result;
}
function amounts(row) {
  const values={};
  for(const field of ['Amount','Fee','Tax','Additional Fees','Additional Tax','gokwik Deduction','Debit','Credit'])values[field]=sourceMoney(row[field],field);
  const expected=values.Amount-values.Fee-values.Tax-values['Additional Fees']-values['Additional Tax'];
  if(values.Amount<=0||expected!==values.Credit)throw Error('GoKwik gross/fee/tax/credit totals do not balance: expected Credit '+(expected/100).toFixed(2)+', reported '+(values.Credit/100).toFixed(2)+'.');
  const net=values.Credit-values['gokwik Deduction'];
  if(net<0)throw Error('GoKwik deduction exceeds the settlement credit.');
  // Some exports repeat the net disbursal in Debit; others leave Debit at zero.
  // It is corroborating evidence, NOT a second deduction from the bank payout.
  if(values.Debit!==0&&values.Debit!==net)throw Error('GoKwik Debit does not equal the net payout: expected '+(net/100).toFixed(2)+' or zero, reported '+(values.Debit/100).toFixed(2)+'. Review this row; nothing was imported.');
  return {...values,net};
}
const scientificId=value=>/^\d+(?:\.\d+)?e\+?\d+$/i.test(text(value));
function roundedIdAgrees(source,exact) {
  const m=text(source).match(/^(\d+)(?:\.(\d+))?e\+?(\d+)$/i),id=text(exact);
  if(!m||!/^\d+$/.test(id)||id.length>25||m[1].length+(m[2]||'').length>25||Number(m[3])>24)return false;
  const shift=Number(m[3])-(m[2]||'').length;
  if(shift<0)return false;
  const quantum=10n**BigInt(shift),center=BigInt(m[1]+(m[2]||''))*quantum,value=BigInt(id);
  return value*2n>=center*2n-quantum&&value*2n<center*2n+quantum;
}
function headers(row) {
  const result={},seen=new Set();
  for(const [key,value]of Object.entries(row)){
    const name=text(key).replace(/\s+/g,' ').toLowerCase();
    if(seen.has(name))throw Error('Duplicate GoKwik CSV column: '+key);
    seen.add(name);result[name]=value;
  }
  return result;
}
module.exports={date,sourceMoney,amounts,scientificId,roundedIdAgrees,headers};
