# Image Review

Owner-only navigation: Inventory → Image Review (`/image-review.html`).

The catalogue is read from Shopify, including inactive and sold-out products. Each review row groups sizes within a product colour. Filters use SANKI collection/gender/category tags; source labels are retained when no known category applies. All website gallery images remain visible. Initial filters are Casuals / Women.

A row starts Not reviewed. Explicitly mark all images correct or flag it for correction. No upload does not imply approval. Original front, back and detail photos are preserved byte-for-byte with immutable versions. Proposed corrections are staged separately. A complete proposal requires explicit approval; replacing an original invalidates that approval and current proposals. Changes to the Shopify gallery invalidate earlier review decisions. Concurrent edits use revision checks.

This release reads Shopify only. It does not generate images, replace media, delete images, change publication status, or execute approved uploads. Approval records identify a staged correction set for subsequent controlled publishing.

Persist `image-review.json` and `image-review-files/` alongside the directory containing DATA_PATH. Include both in backups. Never expose the directory as public static files. The authenticated API serves only files linked to review records. Corrupt review state fails closed instead of being overwritten. Exports include review history and photo links; photo bytes remain in authenticated storage.

Validation: node --test test/image-review.test.js test/catalogue-images.test.js test/auth-authorization.test.js test/manage-navigation.test.js

## Per-view workflow revision

The owner screen has only Front, Back and Detail reference slots. Each is optional. Uploading any one view makes the item ready for correction; unchanged views need no upload. Uploads save automatically. The former separate proposed correction upload and approval controls are hidden from this screen. Existing proposal records and audit history are retained. The backend accepts staged corrections only for referenced views and requires only those views before staged approval. Finished-image approval and Shopify publication remain subsequent controlled steps; uploading a reference never replaces live media.

## Queue and publishing connection

The queue supersedes the previous staging-only release: Create images saves an exact per-colour job, Codex downloads its original bytes through the authenticated operator screen, and returns inspected generated results to it. Ready to review shows originals and AI results side by side. Revise images requeues feedback. Approve & publish now performs the approved Shopify media replacement with byte backups, preserved variant assignments and retry receipts. See `docs/image-review-codex.md` for the operator workflow and limits. The app still cannot wake a Codex chat; generation starts when the owner asks Codex to process the queue.
