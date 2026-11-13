'use strict';

const UNIT='SEWAK_PARK_HOTEL';
const PRIMARY_ACCOUNT_LAST4='0992';

const text=value=>String(value||'').trim();
const token=value=>text(value).toUpperCase().replace(/[^A-Z0-9]+/g,'_').replace(/^_+|_+$/g,'');

function normalizeReportingUnit(value){
  return token(value)===UNIT?UNIT:'';
}

function isHotelAccount(account){
  return text(account).replace(/\D/g,'').slice(-4)===PRIMARY_ACCOUNT_LAST4;
}

// Older reconciled hotel receipts predate reportingUnit. These fields were
// selected deliberately by the reconciler, so they are safe compatibility
// evidence. Account 0992 by itself is intentionally not evidence of income.
function isHotelReceipt(record){
  if(!record)return false;
  if(normalizeReportingUnit(record.reportingUnit)===UNIT)return true;
  const receiptType=token(record.receiptType),category=token(record.category),source=token(record.incomeSource);
  return receiptType==='HOTEL_INCOME'||receiptType==='SEWAK_PARK_HOTEL_INCOME'||category==='SEWAK_PARK_HOTEL_INCOME'||source==='HOTEL_BUSINESS';
}

function isHotelExpense(record){
  return !!record&&normalizeReportingUnit(record.reportingUnit)===UNIT;
}

function isHotelPersonalUse(record){
  return !!record&&(token(record.reportingTreatment)==='PERSONAL_USE'||normalizeReportingUnit(record.sourceReportingUnit)===UNIT);
}

function transferUnits(record){
  return {
    from:normalizeReportingUnit(record&&record.fromReportingUnit),
    to:normalizeReportingUnit(record&&record.toReportingUnit)
  };
}

function isHotelInternalTransfer(record){
  const units=transferUnits(record);
  return units.from===UNIT&&units.to===UNIT;
}

function isHotelWithdrawal(record){
  const units=transferUnits(record);
  return units.from===UNIT&&units.to!==UNIT;
}

function isHotelFunding(record){
  const units=transferUnits(record);
  return units.from!==UNIT&&units.to===UNIT;
}

module.exports={
  UNIT,
  PRIMARY_ACCOUNT_LAST4,
  normalizeReportingUnit,
  isHotelAccount,
  isHotelReceipt,
  isHotelExpense,
  isHotelPersonalUse,
  transferUnits,
  isHotelInternalTransfer,
  isHotelWithdrawal,
  isHotelFunding
};
