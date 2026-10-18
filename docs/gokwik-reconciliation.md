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
5. The 0437 preview automatically includes exact report-backed net credits as
   preview-only matches. Admin submits the bank reconciliation for Owner approval;
   this saves no transfer, paid charge or settlement posting. There is no separate
   GoKwik settlement approval. Source/order identity exceptions still need review.
6. Owner finalization of the 0437 bank statement rechecks report, order funding,
   links, bank date/UTR/net and the submitted evidence fingerprint. In the same
   store save it creates standard net transfers and paid GoKwik expenses, links
   their real IDs in the immutable bank report and finalizes the bank statement.
   Ambiguous, conflicting, stale or already-finalized bank evidence fails closed.
   It never infers an opening balance or rewrites the statement summary. An
   existing unresolved carry-forward remains a separate bank reconciliation issue.

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
  final user-confirmed classification, not a claim that GoKwik's actual invoice was verified.
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

### Common statement and unposted outflows

The GoKwik clearing account now displays one date-ordered statement for the
selected range, including existing recorded movements and provider-reported
processing fees/tax, GoKwik service charges (Subscription Expense) and net payouts.
Unposted outflows are explicitly labeled review-only. The statement closing is
shown separately from the unchanged recorded account balance. Earlier movements
are carried into the range opening; future movements are excluded.

This is a read-only account-ledger response projection. It creates no transfers,
expenses, bank credits, approvals or finalizations and does not change balances
or P&L. Once 0437 Owner finalization posts a batch, real ledger rows replace projected
outflows without being counted twice. Settlement review remains inside universal
reconciliation, under one collapsed review section rather than separate primary
statement cards. No new tab or page is introduced.

### Display consistency and final service-charge classification

The 18 October instruction confirms the separate deduction as GoKwik service
charge, classified as Subscription Expense with GoKwik as vendor. It supersedes
the earlier provisional wording. New approval metadata and future expense
particulars use the confirmed treatment; existing source evidence is not rewritten.

Both the collapsed account header and expanded/detailed statement use the same
read-only providerStatement response. The primary value is statement closing,
including unposted provider outflows; recorded balance is explicitly secondary.
Card totals use paise precision and load without opening the card. Stale date,
entity or replaced-panel responses cannot overwrite a newer balance. Failed
requests display an unavailable state rather than a manufactured zero.

Transactions and settlement review groups display newest first. Running balances
are still calculated chronologically; only presentation order is reversed, with
the brought-forward opening at the bottom. Ready-for-review and blocked counts
are separate. None of these display reads confirms, submits, approves, posts or
finalizes anything.

### Unified 0437 finalization (18 October)

The report statement shows actual report captures even while order identity is
unresolved, without also counting estimated Shopify receipts. Recorded balances
remain separately labeled. Before bank approval, all report rows are informational.
Standalone submit/approve/withdraw provider routes fail without financial writes,
including old clients. Ready batches say "Awaiting 0437 reconciliation". Separate
GoKwik reminders cover exceptions and unconfirmed uploads only; ready batches
follow the bank's own Admin/Owner status. Deployment performs no migration,
financial submission, approval, posting or reconciliation finalization.
