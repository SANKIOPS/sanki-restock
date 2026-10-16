# GoKwik clearing and universal reconciliation

GoKwik is an account ledger, not a new tab or standalone reconciliation page.
Select **Tiana Traders IndusInd 0437** in the existing Account ledgers / universal
reconciliation controls. Its GoKwik report accordion is inside `ledgerRecon`.
The GoKwik clearing account also exposes this same embedded component.

## Workflow

1. Upload the 0437 bank statement using the existing universal statement uploader.
2. Expand GoKwik report and settlement reconciliation. Preview the original CSV,
   then explicitly confirm it as source evidence. Source files and hashes remain
   downloadable to authorized business-bank operators.
3. Customer payments match only an exact Shopify merchant order ID and consistent
   order number. Cash, store credit and Paytm funding are reserved independently.
   Missing/ambiguous orders, conflicts and bank-linked original receipts need review.
   Pre-posting order links can be unmatched with an audited reason.
4. Review each UTR batch. Gross less the report's actual fee, tax, additional
   fee/tax and separate GoKwik deduction must equal the exact 0437 bank credit.
   A bank draft supplies evidence without pretending that it is finalized.
5. Submit for Owner approval. No transfer or charge is posted on submission.
   Approval rechecks the report, order funding, order links and exact bank evidence
   fingerprint before one atomic save. Stale approvals return to review required.
6. Owner approval creates one standard net internal transfer and standard paid
   expenses. Match that transfer and finalize the bank statement through the same
   universal reconciliation menu. This workflow never sets a bank opening balance,
   rewrites a statement summary, or marks the whole bank account finalized.

## Accounting

- Website non-cash collections route to `GoKwik Settlement Clearing`, not directly
  to 0437. Explicit Paytm, cash, store-credit and COD components retain their own
  routes. Generic website UPI is not assumed to be Paytm.
- Imported, linked GoKwik capture amounts and collection dates are source evidence,
  not a second sale. They replace overlapping clearing receipt components. A known
  partial-COD advance is not also counted in the courier remainder.
- Processing fees and their report tax are a paid `BANK CHARGES` expense, vendor
  GoKwik. Individual fee/tax components remain on the immutable report and posting.
- The separate `gokwik Deduction` is a paid **Subscription Expense**, vendor
  **GoKwik**, as explicitly directed by the user on 16 October 2026. This is a
  provisional classification, not a claim that GoKwik's actual invoice was verified.
  It does not assert eligibility for any input tax credit.
- The expenses are Website-channel operating costs. The net transfer is not P&L
  revenue. Bank, clearing summary, detailed ledger, paid spending, vendor and P&L
  use the same standard transfer/expense records, including paise.
- Report-derived SVG summaries are clearly marked **not a tax invoice**. Original
  CSVs are retained separately. Posted payment amounts, dates, transfer and source
  evidence cannot be independently edited, unpaid, deleted or posted twice.

## Supported evidence and boundaries

Only INR Payment rows on/after 22 August 2026 are accepted. Slash/hyphen day-first
dates, ISO calendar dates and valid 12/24-hour times (including seconds) are
supported, with strict calendar validation. Zero-padded four-decimal money fields
are supported. CSV BOMs, CRLF, quoted fields, semicolon separators and UTF16LE are
handled without converting identifiers to numbers. Headers are trimmed and
case-insensitive; duplicate headers and malformed rows fail before any save.
Debit may be zero or exactly the net payout (Credit minus the separate deduction).
It is never deducted again. All GoKwik readers use the same monetary validation.
Unknown transaction types, refund/chargeback rows, nonzero fractional paise, inconsistent totals,
conflicting payment IDs and inconsistent UTR dates/providers fail closed. These
cases need additional source examples and a separately verified implementation.

Merchant IDs in scientific notation remain exactly as supplied. They never
auto-link. The UI requires an explicit full-ID confirmation and review reason,
and the server requires one unique cached Shopify order with the exact order
number and an ID within the source's rounding interval. The original ID, selected
exact ID, reviewer, time and reason remain auditable. Ambiguous or incompatible
identities stay blocked. Funding limits, source locks and Owner revalidation also
apply to these links; source CSVs are not rewritten.

Refunds/cancellations do not erase a genuine original capture. Refund money must
still be recorded separately through the existing refund workflow. Zero-net batches
do not generate an invented zero-value bank transfer.

Deploying this code does not import or finalize financial records. There is no
production-data migration, embedded customer report, automatic import or opening
adjustment. Existing finalized/manual bank
links are preserved and block silent reclassification. Subscription categorization
can later be corrected against an invoice through the descriptive expense editor,
without changing the locked payment amount/date.

## Release verification

The 16 October 2026 release was checked against main `dbbb158`: all existing files
being replaced match that baseline. It restores the previously tested connected
ledger integrations removed by a later payment merge, while retaining Admin bank
and cash claimant payments and Services access. No unconfirmed transfer amount is
silently corrected. The scoped financial/UI regression run passes 642 checks with
zero failures or skips; separate network-based claimant/refund suites cannot run
in a loopback-restricted sandbox and are not claimed as passing.

Content-version assertions normalize Windows CRLF to Git's LF before hashing.
P&L API tests exercise the real Express router in-process, including invalid-date
rejection, no-store headers, workbook decoding and unchanged financial sources.

### Import-format corrective release

The follow-up fix covers day-first hyphen timestamps, 12/24-hour times, strict
calendar validation, BOM/header normalization, UTF-16 and semicolon CSVs, and
preserves all identifiers as source text. Debit is validated as either zero or
the reported net payout, not deducted twice. Both GoKwik readers share the same
date and monetary field rules. Existing legacy evidence is projected with the
correct payout without rewriting its original source.

Rounded scientific-notation merchant IDs cannot auto-link. An explicit full-ID
review must pass order-number, uniqueness, rounding-interval and funding checks;
the original ID and reviewed ID remain auditable. A malformed row rejects the
whole upload before evidence is saved. The scoped financial/UI regression run
passes 689 checks, with zero failures, cancellations or skips. All new fixtures
are synthetic; the customer's settlement CSV is not part of the repository.
