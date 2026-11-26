'use strict';

// Read-only, permission-filtered finder for the global app search. It never
// changes an expense, transfer, account ledger or reconciliation.
const list=value=>Array.isArray(value)?value:Object.values(value||{});
const text=value=>String(value==null?'':value).trim();
const nature=value=>text(value).toUpperCase();
const money=value=>Math.round(Number(value||0)*100)/100;
const active=row=>row&&!row.accountingExcluded&&!row.deletedAt&&!row.voidedAt&&!row.reversedAt&&row.active!==false&&!['void','voided','deleted','reversed','rejected','cancelled','canceled'].includes(String(row.status||'').toLowerCase());
const safeDate=value=>/^\d{4}-\d{2}-\d{2}$/.test(text(value))?text(value):'';
const fold=value=>text(value).toLocaleLowerCase().replace(/[\u2010-\u2015\u2212]/g,'-').replace(/[,₹]/g,'').replace(/\s+/g,' ');
const suffix=value=>(text(value).match(/(\d{4})\D*$/)||[])[1]||'';
const MAX_RESULTS=60;
const groupManagement=require('./expense-category-groups');

function queryValue(value){
  if(typeof value!=='string'||value.length>160||/[\u0000-\u001f\u007f]/.test(value))throw Object.assign(Error('Enter a search of up to 160 characters.'),{status:400});
  const query=text(value);if(query&&query.length<2)throw Object.assign(Error('Enter at least two characters.'),{status:400});return query;
}
function pairFromQuery(query){
  const match=text(query).match(/(?:^|\s)(\d{4})\s*(?:to|->|→)\s*(\d{4})(?:\s|$)/i);
  return match?{from:match[1],to:match[2]}:null;
}
function searchRank(query,fields){
  const q=fold(query),values=[...new Set(fields.map(fold).filter(Boolean))],joined=values.join(' · ');
  if(!q)return 99;
  if(values.includes(q))return 0;
  if(values.some(value=>value.startsWith(q)))return 1;
  const tokens=q.split(' ').filter(Boolean);
  return tokens.every(token=>joined.includes(token))?2:99;
}
function dateAllowed(date,from,to){return(!from||date>=from)&&(!to||date<=to);}
function selectedGroup(value){
  if(value==null||value==='')return'';
  const group=groupManagement.cleanGroupName(value);if(!group)throw Object.assign(Error('Choose a valid expense group.'),{status:400});return group;
}
function categoryGroups(store){
  const rows=[];
  if(Array.isArray(store.sankiCategoryCatalog&&store.sankiCategoryCatalog.categories))rows.push(...store.sankiCategoryCatalog.categories);
  rows.push(...Object.values(store.customLedgers||{}));
  const byCategory=new Map();
  rows.forEach(row=>{const name=text(row&&row.name).toLocaleLowerCase(),group=groupManagement.resolveGroupName(store,row&&row.group);if(name&&group&&!byCategory.has(name))byCategory.set(name,group);});
  return byCategory;
}
function expenseGroup(store,expense,byCategory){
  const category=text(expense.ledger||expense.category).toLocaleLowerCase();
  return byCategory.get(category)||groupManagement.resolveGroupName(store,expense.categoryGroup||expense.group)||'';
}
function expenseFields(expense){
  const payments=list(expense.payments);
  return [expense.id,expense.date,expense.amount,expense.vendor,expense.particulars,expense.narration,expense.telegramNarration,expense.ledger,expense.category,expense.categoryGroup,expense.note,expense.billNo,expense.billNumber,expense.account,expense.status,...payments.flatMap(payment=>[payment&&payment.id,payment&&payment.reference,payment&&payment.bankReference,payment&&payment.account,payment&&payment.amount,payment&&payment.date,payment&&payment.note])];
}
function transferFields(transfer){return[transfer.id,transfer.date,transfer.amount,transfer.reference,transfer.bankReference,transfer.transactionReference,transfer.utr,transfer.fromAccount,transfer.toAccount,transfer.fromNature,transfer.toNature,transfer.nature,transfer.classification,transfer.note,transfer.intermediary];}

function create(deps){
  for(const key of ['read','canSearch','natures','canPersonal','accountVisible','canViewExpense'])if(typeof deps[key]!=='function')throw Error('Missing quick financial search dependency: '+key);
  const permittedNature=(req,value)=>{const n=nature(value);return!!n&&list(deps.natures(req)).includes(n)&&(n!=='PERSONAL'||deps.canPersonal(req)===true);};
  const permittedAccount=(req,value)=>!text(value)||deps.accountVisible(req,text(value))===true;
  function transferVisible(req,row){
    const from=nature(row.fromNature||row.nature),to=nature(row.toNature||row.nature);
    return permittedNature(req,from)&&permittedNature(req,to)&&permittedAccount(req,row.fromAccount)&&permittedAccount(req,row.toAccount);
  }
  function expenseVisible(req,row){
    if(!permittedNature(req,row.nature)||deps.canViewExpense(req,row)!==true||!permittedAccount(req,row.account))return false;
    return list(row.payments).every(payment=>payment&&permittedAccount(req,payment.account||row.account));
  }
  function search(req,input={}){
    if(deps.canSearch(req)!==true)throw Object.assign(Error('Accounting search access is required.'),{status:403});
    const query=queryValue(input.q==null?'':input.q),from=safeDate(input.from),to=safeDate(input.to),kind=['all','expense','transfer'].includes(input.kind)?input.kind:'all',group=selectedGroup(input.group);
    if(input.from&&!from||input.to&&!to||from&&to&&from>to)throw Object.assign(Error('Choose a valid date range.'),{status:400});
    const snapshot=deps.read(),store=snapshot&&snapshot.store||snapshot;
    if(!store||typeof store!=='object'||Array.isArray(store)||store.expenses!=null&&(typeof store.expenses!=='object'||Array.isArray(store.expenses))||store.transfers!=null&&!Array.isArray(store.transfers))throw Object.assign(Error('The accounting sources cannot be verified.'),{status:409});
    const pair=pairFromQuery(query),transfers=[],expenses=[],byCategory=categoryGroups(store),visibleExpenses=list(store.expenses).concat(list(store.reconciliationExpenses)).filter(row=>active(row)&&expenseVisible(req,row)),groups=Array.from(new Set(visibleExpenses.map(row=>expenseGroup(store,row,byCategory)).filter(Boolean))).sort((a,b)=>a.localeCompare(b));
    if(!query&&!group)return{success:true,query:'',pair:null,groups,summary:{transferCount:0,totalTransferred:0,expenseCount:0,totalExpenses:0},transfers:[],expenses:[],truncated:false};
    if(kind!=='expense')for(const row of list(store.transfers)){
      if(!active(row)||!transferVisible(req,row)||!dateAllowed(text(row.date),from,to))continue;
      const rank=pair?(suffix(row.fromAccount)===pair.from&&suffix(row.toAccount)===pair.to?0:99):searchRank(query,transferFields(row));if(rank===99)continue;
      transfers.push({rank,id:text(row.id),date:text(row.date),fromNature:nature(row.fromNature||row.nature),fromAccount:text(row.fromAccount),toNature:nature(row.toNature||row.nature),toAccount:text(row.toAccount),classification:text(row.classification||'internal_transfer'),amount:money(row.amount),note:text(row.note)});
    }
    if(kind!=='transfer'&&!pair)for(const row of visibleExpenses){
      if(!dateAllowed(text(row.date),from,to))continue;
      const rowGroup=expenseGroup(store,row,byCategory);if(group&&fold(rowGroup)!==fold(group))continue;
      const rank=query?searchRank(query,expenseFields(row).concat(rowGroup)):0;if(rank===99)continue;
      expenses.push({rank,id:text(row.id),date:text(row.date),nature:nature(row.nature),vendor:text(row.vendor),particulars:text(row.particulars||row.narration),group:rowGroup,category:text(row.ledger||row.category||'Unclassified'),amount:money(row.amount),status:text(row.status||'recorded'),account:text(row.account||list(row.payments)[0]&&list(row.payments)[0].account),stored:!!(store.expenses&&store.expenses[row.id]===row)});
    }
    const ordered=(a,b)=>a.rank-b.rank||String(b.date).localeCompare(String(a.date))||String(a.id).localeCompare(String(b.id));transfers.sort(ordered);expenses.sort(ordered);
    const summary={transferCount:transfers.length,totalTransferred:money(transfers.reduce((sum,row)=>sum+row.amount,0)),expenseCount:expenses.length,totalExpenses:money(expenses.reduce((sum,row)=>sum+row.amount,0))},total=transfers.length+expenses.length;
    return{success:true,query,pair,groups,summary,transfers:transfers.slice(0,MAX_RESULTS).map(({rank,...row})=>row),expenses:expenses.slice(0,MAX_RESULTS).map(({rank,...row})=>row),truncated:total>MAX_RESULTS};
  }
  function register(router){router.get('/api/expenses/quick-find',(req,res)=>{try{res.json(search(req,req.query||{}));}catch(error){res.status([400,403,409].includes(error.status)?error.status:409).json({success:false,error:[400,403].includes(error.status)?error.message:'The accounting sources could not be verified. No records were changed.'});}});}
  return{search,register};
}

module.exports={create,queryValue,pairFromQuery,searchRank,suffix};
