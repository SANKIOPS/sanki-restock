# Image Review

Owner-only navigation: Inventory → Image Review (`/image-review.html`). Initial filters are Casuals / Women. Each row groups sizes within a product colour and displays every current Shopify gallery image, including inactive and sold-out products. Source taxonomy labels are retained when no known category applies.

## Owner workflow

1. Review the numbered website images. If all are correct, explicitly mark Images correct. No upload alone does not imply approval.
2. Upload genuine Front, Back or Detail photos only for views needing correction. One reference is sufficient. Choose the numbered website photo to replace for each uploaded view, or Add as a new photo.
3. Create images saves the job. Tell Codex “Process the image queue” in the chat to start generation. The app cannot wake or run Codex itself.
4. Returned AI results appear beside the originals. Revise images sends feedback back to the queue. Approve & publish replaces only the selected website photos and verifies the new gallery; other photos, product status, prices and stock remain unchanged.

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
