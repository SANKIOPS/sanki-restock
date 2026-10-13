# Meta Ads records

Owners open **Marketing → Meta Ads** from the shared sidebar or dashboard.
Upload a CSV or XLSX **configuration export** using “Upload campaign setup”.
Preview validates the whole file without saving. Save imports its campaign,
ad-set and ad hierarchy in one atomic revision. Stable Meta IDs prevent repeated
imports creating duplicates. A uniquely named manual record under the matching
parent is adopted; ambiguous matches are rejected. Blank audience fields retain
existing settings and appear as warnings. Interests and behaviours inside
Flexible Inclusions and distinct Advantage audience age fields are preserved.
Preview links are retained; this does not download videos or import performance.
This is a manual marketing record, not a Meta API integration. Saving a record
does not publish an ad, adjust a live budget, or create an accounting expense.

The views keep one row per campaign, ad set, creative, dated result or field
change. Filters support campaign, collection, status, destination and search.
Each view exports its filtered records as CSV. Records can be archived and
restored; their links and history are retained.

## Results and budgets

The configured daily total includes all non-archived ad-set budgets, including
paused and unverified records. It is not actual spend. Unknown numbers remain
null; enter zero only when the report confirms zero.

Results require a date range and either whole-ad-set or individual-ad scope.
Duplicate rows for the same scope and dates are rejected. Overlapping date
ranges and ad/ad-set reports are not aggregated. Cost and rate columns are
calculated only when both inputs are known and the denominator is positive.

## Private persistent storage

The file meta-ads.json is stored beside DATA_PATH, or in the app root if
DATA_PATH is unset. Production DATA_PATH must point to the existing persistent
volume. Writes use a process lock, atomic replacement and a revision check;
field changes record actor, time, previous value, new value and reason.

The checked-in modules/meta-ads-seed.json is deliberately empty. Private
initial records may be supplied through the Railway environment variable
META_ADS_SEED_JSON. They use the same validation as edits and import only
when the persistent marketing file does not exist. Subsequent deployments
and environment changes never overwrite saved records.

Do not commit real campaign records, contact/account identifiers or business
performance to this public source repository. Keep the initial environment
value and runtime data within the private Railway service.

## Validation

Run the marketing tests for authorization, validation, persistence, private
initial seeding, audit history, revision conflicts, archive/restore and
null/zero behavior:

    node --test test/meta-ads.test.js

The UI is plain HTML/CSS/JavaScript and uses the shared module registry and
authentication.
