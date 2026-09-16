# Stock movements

Inventory Dashboard now includes Move Stock, Pending Approval, and History.
Keyboard/USB/Bluetooth scanners can enter an exact SKU. Scanner Enter never submits a transfer. Non-SKU barcodes need verified mapping before use.

Deployment is not activation. The old static inventory is NOT a baseline. Moves remain locked until an audited `stock_movements.json` has been provisioned on the persistent data volume by the reconciliation workflow. Do not hand-set `reconciled: true` before matching the new physical count to live Shopify and verifying opening quantities at both locations.

While reconciliation is pending, the movement form displays the nonempty rack labels from the 15 September physical-count sheet, separately for Display and Warehouse. These are choices for orientation only; they are not SKU-level position evidence. The source list is `modules/counted-rack-options.json`, and the approved baseline supersedes it after activation. A source rack becomes SKU-specific once an approved baseline is available.

Store schema:

```json
{"version":1,"baseline":{"reconciled":true,"reviewedBy":"owner","reconciledAt":"ISO timestamp","racks":{"Display":["1","T1","Accesorries"],"Warehouse":["5A","5B"]},"locations":{"Display":"gid://shopify/Location/ID","Warehouse":"gid://shopify/Location/ID"},"skus":{"EXACT_SKU":{"inventoryItemId":"gid://shopify/InventoryItem/ID"}}},"positions":[{"sku":"EXACT_SKU","location":"Display","rack":"1","quantity":1}],"movements":[]}
```

Use `STOCK_MOVEMENTS_PATH` to override the persistent path. Owner/admin can review; `STOCK_MOVEMENT_APPROVERS` is a comma-separated list of existing named manager usernames. Nobody may approve their own movement. No new role assignments are made automatically.

On submission, rack positions change immediately and status is pending. Review of a cross-location transfer sends a Shopify compare-and-set update to both available quantities with a persisted idempotency key. Same-location rack moves require approval but do not alter Shopify quantities. Earlier pending moves for the same SKU must be approved first. Failed Shopify confirmation remains sync-pending, never silently approved. Retries use the original request. Conflicting quantities require investigation, not an automatic overwrite.

Correction marks the reported physical position as needing investigation and blocks further SKU moves. It never silently returns stock to its previous rack. A manager must verify the physical correction and reconcile the store; a dedicated correction-resolution UI is still required before activation. After Shopify request dispatch, correction is blocked until its outcome is confirmed.

Remaining activation work: import verified physical baseline, integrate movement positions with all stock-search/product-detail views, integrate sales/receipts so rack balances remain current, add manager correction resolution, test live Shopify transfers and persistent-volume recovery. The feature is intentionally fail-closed until these checks are complete.
