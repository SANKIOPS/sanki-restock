'use strict';

const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const hotel=require('../modules/sewak-park-reporting');

const read=relative=>fs.readFileSync(path.join(__dirname,'..',relative),'utf8');

test('Sewak Park classification requires deliberate evidence and never treats every 0992 movement as hotel income',()=>{
  assert.equal(hotel.normalizeReportingUnit('Sewak Park Hotel'),'SEWAK_PARK_HOTEL');
  assert.equal(hotel.normalizeReportingUnit('sewak-park-hotel'),'SEWAK_PARK_HOTEL');
  assert.equal(hotel.normalizeReportingUnit('PERSONAL'),'');
  assert.equal(hotel.isHotelAccount('ICICI Bank 0992'),true);
  assert.equal(hotel.isHotelAccount('ICICI Bank 0993'),false);

  assert.equal(hotel.isHotelReceipt({account:'ICICI Bank 0992',receiptType:'other_income'}),false);
  assert.equal(hotel.isHotelReceipt({account:'ICICI Bank 0993',receiptType:'hotel_income'}),true);
  assert.equal(hotel.isHotelReceipt({category:'Sewak Park Hotel Income'}),true);
  assert.equal(hotel.isHotelReceipt({incomeSource:'hotel_business'}),true);
  assert.equal(hotel.isHotelReceipt({reportingUnit:'SEWAK_PARK_HOTEL'}),true);

  assert.equal(hotel.isHotelExpense({reportingUnit:'SEWAK_PARK_HOTEL'}),true);
  assert.equal(hotel.isHotelExpense({account:'ICICI Bank 0992'}),false);
  assert.equal(hotel.isHotelPersonalUse({reportingTreatment:'personal_use'}),true);
  assert.equal(hotel.isHotelPersonalUse({sourceReportingUnit:'SEWAK_PARK_HOTEL'}),true);
  assert.equal(hotel.isHotelPersonalUse({reportingUnit:'SEWAK_PARK_HOTEL'}),false);
});

test('Sewak Park transfer directions stay mutually exclusive',()=>{
  const internal={fromReportingUnit:'SEWAK_PARK_HOTEL',toReportingUnit:'SEWAK_PARK_HOTEL'};
  const withdrawal={fromReportingUnit:'SEWAK_PARK_HOTEL'};
  const funding={toReportingUnit:'SEWAK_PARK_HOTEL'};
  const unrelated={fromAccount:'ICICI Bank 0992',toAccount:'ICICI Bank 0993'};

  assert.deepEqual(hotel.transferUnits(internal),{from:'SEWAK_PARK_HOTEL',to:'SEWAK_PARK_HOTEL'});
  assert.equal(hotel.isHotelInternalTransfer(internal),true);
  assert.equal(hotel.isHotelWithdrawal(internal),false);
  assert.equal(hotel.isHotelFunding(internal),false);
  assert.equal(hotel.isHotelWithdrawal(withdrawal),true);
  assert.equal(hotel.isHotelFunding(withdrawal),false);
  assert.equal(hotel.isHotelFunding(funding),true);
  assert.equal(hotel.isHotelWithdrawal(funding),false);
  assert.equal(hotel.isHotelInternalTransfer(unrelated),false);
  assert.equal(hotel.isHotelWithdrawal(unrelated),false);
  assert.equal(hotel.isHotelFunding(unrelated),false);
});

test('0992 controls expose one report plus explicit income, operating-expense and personal-use choices',()=>{
  const reportHtml=read('public/compact-reports.html');
  const reportJs=read('public/compact-reports.js');
  const expensesHtml=read('public/expenses.html');

  assert.match(reportHtml,/value="SEWAK_PARK_HOTEL">Sewak Park Hotel \(0992\)/);
  assert.match(reportJs,/p\.set\('unit',HOTEL\)/);
  assert.match(reportJs,/entity:isHotel\?'PERSONAL':selectedReport/);

  assert.match(expensesHtml,/id="biReceiptType"[\s\S]*value="hotel_income">Sewak Park Hotel income/);
  assert.match(expensesHtml,/id="beHotelTreatment"[\s\S]*value="operating_expense">Sewak Park Hotel operating expense[\s\S]*value="personal_use">Personal expense using hotel money/);
  assert.match(expensesHtml,/id="editReportingTreatment"[\s\S]*value="operating_expense">Sewak Park Hotel operating expense[\s\S]*value="personal_use">Personal expense using hotel money/);
  assert.match(expensesHtml,/hotelTreatment:hotelTreatment/);
  assert.match(expensesHtml,/reportingUnit:'SEWAK_PARK_HOTEL'/);
  assert.match(expensesHtml,/sourceReportingUnit:'SEWAK_PARK_HOTEL'/);
});
