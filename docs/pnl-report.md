# Management P&L

Accounts → P&L is `/pnl.html`. It is separate from the legacy percentage-based
`/accounting.html` and its unit-economics assumptions. Only Owner, Admin and
SANKI Accounting can read the new report and export. Existing custom page
permissions remain respected.

## Rules

- Accounting boundary: 22 August 2026. No April data is imported or used.
- One completed POS sale or delivered order creates revenue, excluding GST.
  Website orders need an actual completion date; an edit timestamp is not a
  delivery timestamp. Orders has a delivery-date field for confirmed dates.
- Per-piece taxable garment value up to and including ₹2,500 uses 5%; above
  ₹2,500 uses 18%. GST-inclusive prices are divided by `(1 + rate)`. For an
  ambiguous inclusive price, missing price basis, invoice mismatch or missing
  shipping tax, profit remains unavailable until reviewed. Tax invoices are
  never altered by this report.
- Received inventory is allocated by SKU using FIFO. Vendor payment is not
  required. Purchases received later do not change earlier allocations.
  Purchase advances/payables are not additional operating expenses.
- Operating costs use actual approved payments/paid portions, capped to the
  bill amount. Excess vendor advances, reimbursements, card repayments and
  transfers are not counted again. Salary uses actual active payment records,
  not accrued salary or recoverable advances. Unresolved salary ledger links
  are flagged instead of counted twice.
- Active sales incentive payments are included on their actual payment date;
  unapproved/earned incentive balances are not paid costs. Received supplier
  refunds reverse only their verified original paid-expense allocations,
  including received vouchers. Pending refunds, unpaid-only credit notes and
  returned unallocated advances do not reduce paid operating costs. Linked
  card credits and refund receipts are not counted a second time.
- Dated credit notes reduce sales. Only physically restocked SKU quantities
  reverse the original cost. Refunds lacking item/tax/stock linkage remain
  explicit exceptions. Ledger-recorded refunds are not silently ignored.
- COD/advance collections are separate from revenue, with payment history
  cut off at the selected end date. Customer deposits and bank/courier
  settlements do not create extra sales.
- `All` includes Shared overhead; individual channels show direct costs only,
  with that exclusion disclosed. No arbitrary shared-cost allocation.
- This is a management hybrid-basis report, not a statutory/GST filing or
  cash-flow statement. No input-tax credit or depreciation is inferred.

## Read-only feeds and incomplete results

The loader directly reads orders.json, expenses.json, procurement.json and
salary.json alongside DATA_PATH, plus optional incentives.json. It also supports
the existing ORDERS_PATH, PROCUREMENT_PATH, INCENTIVES_PATH and SALES_PATH overrides. It never calls migration-bearing
expense/salary loaders or writes a report back to a financial store.

Optional sales.json supplies standalone manual sales. Verified opening stock
can be supplied in `pnl-opening-inventory.json`:

```json
{"lots":[{"id":"OPEN-EXAMPLE","sku":"EXAMPLE-SKU","date":"2026-08-22","qty":10,"unitCost":999,"verified":true}]}
```

This is a synthetic schema example, not business data. The opening balances
must be independently verified as of the start date; pre-boundary purchase
history is not reused as opening inventory. The current implementation reads
this feed; it does not create an opening-stock editor or import old figures.

Missing cost quantities, delivery dates, refund details or unreadable source
stores produce visible exceptions and an unavailable final profit, never an
assumed zero. Shopify metadata is refreshed by the existing sync mechanism
using importer version 2; the report itself makes no remote request.

The report is rebuilt from current verified records on each read. Source
invoice/cost corrections can restate history; it is not a frozen close-period
ledger. Source sync time is disclosed in Rules & sources.

## Views and export

Filters: date range, quick periods and sales channel. Views: P&L with previous
period, tax/deductions, customer collections/COD, monthly trends and policies.
Totals/categories open their underlying transactions and SKU purchase lots.
Excel export includes Summary, Transactions, SKU allocations, Tax,
Collections, Warnings and Policy. Print/Save PDF uses browser print.

GET `/api/pl/report?from=YYYY-MM-DD&to=YYYY-MM-DD&channel=All`

GET `/api/pl/report/export` with the same filters. Both use no-store caching,
validate dates and refuse future end dates. No report write endpoint exists.

## Verification / release

Run `node --test test/pnl-report.test.js test/pnl-ui.test.js
test/accounts-navigation.test.js test/auth-authorization.test.js`.
Fixtures are synthetic and API checks use disposable localhost servers.
`node tools/pnl-preview.js` serves synthetic figures only at
`http://127.0.0.1:32038/pnl.html`; never expose this unauthenticated preview
outside loopback.

Chrome verification used synthetic figures only: desktop and 390px mobile
layout, SKU drill-down, report tabs, channel/date filters and Excel download.
The wider suite completed with 966 passing tests and four existing failures
in the bank-transfer review, consolidated payment selector, inventory photo
search authorization assertion and owner-only category UI assertion. The
affected source assertions are unchanged from the main-branch baseline.
Focused P&L, navigation, authorization and refund/incentive checks pass;
this is not a claim of a fully green legacy suite. No accounting records
were edited to produce this report.
