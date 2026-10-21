# Results-only paid-expense confirmations

This is management-report evidence, not a financial-source correction. An
Admin or Owner with existing Results correction rights can preview and save a
confirmed paid amount/date for an existing approved business operating expense
whose source paid total is positive but whose usable payment evidence is
incomplete. The ordinary source payment, exclusion, account ledger, vendor
balance, reconciliation, cash register and Owner approval are not modified.

The existing private correction workflow accepts `expense_confirmation` with
an exact expense `reference`, a confirmed total `amount`, actual
`paymentDate`, and optional `adjustmentReference`. Preview validates the
existing source and returns a fingerprint. Save revalidates that fingerprint
and requires the actor and an audit reason. Entries and before/after history
remain in the private `pnl-line-corrections.json` store; no business references
or figures are hardcoded into source code.

The amount is capped by both the source bill amount and source paid total.
It must have at most two decimal places. The date must be real, on or after
the expense and accounting start, and not in the future. Pending, wholly
excluded, private, inventory, salary-advance and balance-sheet sources are
not eligible. An active reconciliation draft blocks this confirmation;
normal Owner approval cannot be bypassed through the report.

Source payments have priority across **all dates**, before report filtering.
The report-only amount is the confirmed total less already eligible source
paid costs. Once a genuine eligible payment covers the confirmation, the
private annotation becomes inactive automatically, even when the genuine
payment falls in a different daily or monthly report. This prevents counting
the same expense once on a confirmation date and again on a source-payment
date. An intervening source change that leaves an outstanding report-only
amount invalidates the fingerprint and requires another preview.

Report rows are explicitly sourced as **Results-only paid-expense
confirmation**, use their actual confirmed date, and expose actor, audit
time and reason. A related adjustment reference is **user-reported only**,
not verified bank or adjustment evidence. A closing-balance adjustment is
never automatically interpreted as this expense merely because it contains
enough money. The report row has no bank account debit or payment proof and
cannot authorize a supplier refund, salary link or reconciliation match.

The Excel transaction sheet appends report-basis and confirmation audit
columns. Existing source-invoice/refund columns keep their original positions.
The UI starts collapsed, requires a source-validated preview before save,
invalidates that preview after edits, rejects stale asynchronous responses,
and submits only to the existing private Results correction endpoints.

Regression coverage includes amount/date caps, source changes, role gates,
private and provisional restrictions, global payment deduplication, partial
source coverage, historical/channel ranges, category totals, audit
preservation, unchanged financial bytes, supplier-refund isolation, escaped
UI rendering, stale-response protection and duplicate-submit locking.
