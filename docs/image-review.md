# Image Review

Owner-only navigation: Inventory → Image Review (`/image-review.html`). Initial filters are Casuals / Women. Each row groups sizes within a product colour and displays every current Shopify gallery image, including inactive and sold-out products. Source taxonomy labels are retained when no known category applies.

## Owner workflow

1. Review the numbered website images. If all are correct, explicitly mark Images correct. No upload alone does not imply approval.
2. Upload genuine Front, Back or Detail photos only for views needing correction. One reference is sufficient. Choose the numbered website photo to replace for each uploaded view, or Add as a new photo.
3. Create images saves the job. Tell Codex “Process the image queue” in the chat to start generation. The app cannot wake or run Codex itself.
4. Returned AI results are Ready to review, not live. Revise images sends feedback back to the queue. Approve & publish updates the requested photos. For complete five-view sets it also orders front → detail → 45° → back → product-only, assigns the model front to every size of this colour and updates the storefront card mapping (front thumbnail, detail hover). Other gallery photos are retained; sizes, stock, prices and product status remain unchanged. Published is shown only after Shopify readback and the live product-card rendering confirm the new front/detail selection. Failure is retryable without reuploading successful images.

The main theme is connected once using `node tools/deploy-image-review-card.js ENV_FILE BACKUP_DIRECTORY --deploy`. It installs the final approved-image selector in `snippets/product-card.liquid` and the `product.sanki-review-verify` rendering template. Both are inert for products without the `sanki.image_review_gallery` JSON metafield. The publisher merges only this colour's variant entries into that field. No per-product hardcoded theme updates are needed. A future replacement theme must retain this connection; otherwise publication fails verification rather than silently claiming success.

The authenticated Codex operator screen handles reference download and result return; the owner does not move generated files. See `docs/image-review-codex.md` for its workflow and publishing recovery details. No paid generation service is integrated.

Original bytes, historical versions, returned results and publish receipts are retained. Replacing a reference invalidates its queued job. Website gallery changes invalidate old decisions and block approval. Per-product publishing locks and optimistic record revisions prevent conflicting actions. Existing photos are backed up before removal, and retries retain successful uploads rather than duplicating them. Shared multi-colour photos cannot be selected for replacement.

## Operation

Persist `image-review.json` and `image-review-files/` alongside the directory containing DATA_PATH. Include both in backups. Do not expose this directory as static files: the owner-authenticated API serves only linked files. Corrupt review state fails closed without overwriting it. Exports include review history, jobs, photo links and publish receipts.

Viewer controls support repeated zoom up to 800%, mouse-wheel/pinch zoom, drag panning and Fit whole product.

Validation:

`node --test test/image-review-queue.test.js test/image-review.test.js test/catalogue-images.test.js test/auth-authorization.test.js test/manage-navigation.test.js`

Publishing integration tests use a mocked Shopify client. Production publishing is first exercised on an actual owner-approved result set, never dummy catalogue media.

## Image Reviewer responsibility

Users & Roles includes **Image Reviewer (review, upload and submit only)**. Assign this role alone for an image-review-only account. Its fixed page is Image Review; it may browse galleries, mark decisions, upload original references, create correction jobs and request revisions. It cannot publish, approve publishing, claim generation work, return generated results, download worker packets, export bulk review data, manage users, or access other departments. Additional independently assigned roles retain their own permissions. The Owner retains generation handoff and publishing controls. The page hides publishing controls for reviewers, and both the outer auth gate and module routes enforce the restrictions.

## Bulk approval

Owners can select complete Ready to review product-colour sets with the card checkboxes, select the ready sets on a page, and approve selected sets across pages. Filter changes clear the selection. “Approve all ready in filters” includes every matching page, not only visible cards; choosing Category changes its label to “Approve all ready in [category]”. Collection, gender, category and search filters all apply.

Both actions first show the exact product-colour list and count. Only the final “Approve & publish” starts Shopify publishing. Five current-job images are required for bulk eligibility. Publishing is sequential through the existing owner-only publish endpoint with the reviewed revision, fingerprint and job ID. Changed jobs, website conflicts and failures are reported per product, without silently approving them. Stop after current product ends the remaining run. Keep this browser tab open during a run. Successful sets are removed from the selection; failed sets can be inspected and retried from their individual review. Image Reviewers do not receive approval controls.
