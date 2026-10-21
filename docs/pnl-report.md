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
  When that date is absent, the report now joins the existing automatic
  Velocity shipment discovery and carrier-tracking evidence by Shopify order
  ID and AWB. All active AWBs must have delivered status and actual dates;
  split shipments use their latest delivery. No date is inferred from
  fulfilled status, settlement, sync time or last-updated time. Invalid,
  future and pre-order dates remain exceptions. POS and recorded delivery
  dates retain their existing precedence; cancelled/RTO orders are not
  revived by carrier fallback. No order or dispatch fields are rewritten.
  The existing ten-minute logistics poll supplies this cache without needing
  Orders-page access. The tracking reader accepts both Velocity's flat and
  nested response formats. Orders fulfilled outside Velocity with no AWB
  still require a verified manual completion/cancellation correction.
- Final per-piece garment selling value after discount up to and including ₹2,500 uses 5%; above
  ₹2,500 uses 18%. GST-inclusive prices are divided by `(1 + rate)`. For an
  missing price basis or missing shipping tax,
  profit remains unavailable until reviewed. Garment GST is calculated exclusively
  by this P&L policy: invoice rates, recorded GST differences and Shopify exempt
  flags never override it or block profit by themselves. Source invoice GST is
  retained for reference only. Tax invoices, Shopify, actual collections and all
  other accounting/ledger calculations are never altered by this report.
  Tax-exclusive garment amounts use taxable value plus calculated GST in the
  management report; source invoice/collection totals remain separate evidence.
  The slab is chosen from the final discounted selling value, not inferred by
  reversing candidate tax rates. This is the user's configured management
  report policy only and does not change statutory invoice calculations.
- Shopify Cost per item is the primary management COGS source, matched by
  unique normalized SKU from the existing persisted inventory snapshot.
  Reports return immediately using the synced copy. The first report read
  can start a coalesced background refresh; further refresh attempts are
  limited to once per 15 minutes (30 seconds while no saved snapshot exists).
  The snapshot timestamp is exposed as costSource.at and in the source list.
  Missing, zero, negative, non-INR and duplicate-SKU costs remain exceptions.
  Sale quantity multiplies unit cost regardless of supplier payment or today's
  available stock. No opening quantities or purchase lots are invented.
  Current synced cost changes restate management P&L history; this is not a
  frozen historical stock valuation. Refunds reverse the original sale's
  allocation. Purchase advances/payables are not extra operating expenses.
  The pure report builder retains legacy verified FIFO behavior only when
  callers explicitly omit the Shopify cost source; the app loader supplies it.
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
- Dated credit notes reduce sales and reverse the original P&L-calculated GST,
  not the source refund tax or a newly selected slab. Cumulative quantity/tax
  allocations prevent duplicate or excessive reversals; partial value credits
  follow their documented fraction of the original line. Only physically restocked SKU quantities
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
validate dates and refuse future end dates. These report reads never modify
source financial records.

## P&L-only missing-item corrections

The collapsed item-review panel can map an originally blank order-line SKU
to a unique positive-cost Shopify SKU, record a verified custom-item per-unit
COGS, or mark a shipping fee as requiring no inventory COGS. Shipping tax
retains the source invoice evidence; no garment slab or zero tax is guessed
when shipping evidence is missing. Refunds use the original annotated cost
and tax allocation; shipping fees never create or return inventory.

GET `/api/pl/report/line-corrections` lists exact source order/line identities
within the selected accounting period. Admin and Owner can preview selected
corrections via POST `/api/pl/report/line-corrections/preview` and then save
via POST `/api/pl/report/line-corrections`. Accounting remains read-only.
Existing authentication/page access gates remain in force. Preview and save
validate original price, quantity and a fingerprint covering SKU, title,
taxability, tax and discount. Source changes invalidate saved annotations
rather than silently overriding changed orders. Source-SKU lines, duplicate
targets, pre-boundary/undated orders, ambiguous Shopify SKUs, zero/invalid
custom costs and missing reasons are rejected.

Only `pnl-line-corrections.json` is written atomically alongside DATA_PATH.
Every revision retains the actor, reason, timestamp and prior value. No
Shopify order, invoice, stock, expense, payment or reconciliation store is
edited. Business-specific corrections are private runtime data, never
hardcoded into the public source repository.

### Confirmed cancelled orders (P&L only)

Admin and Owner can preview an exact order reference and save an audited
`cancelled` order-status annotation through the same private correction
store. The order identity is checked again at save and report time. No
business order reference is hardcoded into the source. The cancelled sale
is excluded from revenue, garment GST and COGS, and no missing-delivery
warning is emitted. Existing customer collections, cash refunds and any
unrefunded advance remain visible. No source cancellation date is invented.
`source_status` restores source-based recognition without deleting audit
history. Existing item annotations are preserved in both directions.
Shopify, orders, invoices, stock, payments and reconciliations are untouched.

### Confirmed delivery and manual-sale COGS

The same private audited store supports a verified actual delivery date for
in-house/manual shipments. Dates before order creation, future dates and
cancelled source orders are rejected. Report-only completion never changes
Velocity tracking, Shopify or Orders permissions.

Manual product-sale receipts may have user-confirmed quantity and per-unit
COGS. The receipt group identity, amount and dates are fingerprinted. Its
non-garment GST rate and inclusive/exclusive basis remain unresolved unless
both are explicitly supplied. No zero GST or garment classification is
assumed. Original receipts and their bank movements are not edited.

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
