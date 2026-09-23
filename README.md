# Log One Tool Army

Agent pipeline that fetches active Zendesk customs-clearance tickets, detects the requested data, logs draft/final responses to BigQuery, and optionally submits automatic Zendesk replies with PDF attachments when no human intervention is required.


## Zendesk ticket fetch and category classification

`ticket_fetcher.py` retrieves Zendesk mail tickets with API-level status filters for exactly `new`, `open`, and `pending`. The default Zendesk Search Export query is:

```text
status:new status:open status:pending via:mail
```

So `hold`, `solved`, and `closed` tickets are not intentionally fetched and then filtered out. A defensive local status check is still kept in case Zendesk ever returns an unexpected result.

The fetcher intentionally uses `/api/v2/search/export.json` instead of regular `/api/v2/search.json`. Regular Zendesk search is offset-paginated and fails after the first 1,000 results/page 10 for broad queries; Search Export is cursor-paginated and can continue past that limit.

The active-ticket fetch now follows the same performance pattern as the standalone historical export script:

1. Run one active-mail Search Export query by default.
2. Batch-load requester users with `/api/v2/users/show_many.json` instead of calling `/users/{id}.json` one ticket at a time.
3. Filter requester emails locally to `ups.com`, `dhl.com`, and `fedex.com`, including accepted subdomains through the shared carrier-domain guardrail.
4. Fetch comments concurrently only for the carrier-domain tickets that survived the requester-email filter.
5. Build active requests from the latest public requester message block only.

Useful optional environment variables:

```text
ZENDESK_FETCH_STRATEGY=broad_active
ZENDESK_SEARCH_EXPORT_PAGE_SIZE=500
ZENDESK_USER_FETCH_BATCH_SIZE=100
ZENDESK_COMMENT_FETCH_WORKERS=20
ZENDESK_MAX_RETRIES=5
```

`ZENDESK_MAX_RETRIES` applies to **every** Zendesk call, reads and writes alike (ticket updates, solve-without-response updates, attachment uploads, ticket-field lookups). All of them go through `ticket_fetcher.zendesk_request()`, which retries dropped connections (`Connection reset by peer`, chunked-encoding errors, timeouts) with exponential backoff (2s, 4s, 8s, ... capped at 30s), honors `Retry-After` on 429, and retries 500/502/503/504. Before re-sending a ticket update that carries a public comment, it checks whether the comment is already on the ticket, so a connection that Zendesk resets *after* applying the update can never produce a duplicate reply. Status/custom-field-only updates are idempotent and are simply re-sent. Non-retryable responses (e.g. 422) are returned unchanged so the existing required-field/closed-ticket handling still applies.

`ZENDESK_FETCH_STRATEGY=broad_active` is the recommended default. It avoids several repeated domain/full-text searches and is usually faster because user lookup is batched and comments are fetched only after domain filtering. For diagnostics only, `ZENDESK_FETCH_STRATEGY=narrow` restores the older configured-requester/domain-term query behavior. When using the narrow diagnostic mode, these legacy switches are still available:

```text
ZENDESK_CONFIG_REQUESTER_QUERY_BATCH_SIZE=45
ZENDESK_FETCH_CONFIGURED_REQUESTERS=true
ZENDESK_FETCH_CARRIER_DOMAIN_TERMS=true
ZENDESK_BROAD_ACTIVE_MAIL_FALLBACK=false
```

The config table still remains authoritative for classification:

1. If the normalized requester email exists in `tlg-business-intelligence-prd.til.log_one_tool_army_config`, the workflow keeps the configured `ticket_category` exactly as before.
2. If the requester email is a UPS/DHL/FedEx-domain address but is not in the config table, `customs_rules.classify_ticket_category_from_content()` infers one of the existing categories from the current requester message and subject:
   - `Returns Customs Clearance` for RPI/PRI, return proforma, returned-goods, reintroduction, proof/evidence-of-export, export-tracking, and RTS wording.
   - `Pending Order Release` for delivery/address/phone/contact/recipient-availability/FedEx Support Hub handoff wording.
   - `Order Customs Clearance` for commercial invoice, import/customs clearance, sdoganamento, goods description, country of origin, POA/AES/SED/SLI/EORI, and generic customs-document requests.

Carrier-domain notification emails that historically did not need a customer-facing response are excluded before the tracking-number guardrail. This covers high-volume patterns such as FedEx delivery notifications, FedEx Import Data Summary informational messages, UPS MRN/document automatic emails, UPS claim/inquiry status notifications, and carrier billing/no-reply notices. Actionable FedEx Support Hub requests are not excluded; they continue to route to human intervention.

**Request number 1 from `noreply*` / `autoreply*` senders — solved without analysis:** when the requester email (local part, case-insensitive, `Name <...>` wrapping tolerated) starts with `noreply` or `autoreply` **and** `request_number` is 1, the request is not analyzed at all. `customs_rules.is_noreply_autoreply_first_request()` is checked first in `regex_engine.detect()`, before every other rule, and returns `requested_data=["noreply_autoreply_first_request_solved"]` so no regex, guardrail, or LLM runs. No GET_FULL_ORDER lookup or document generation is done. `draft_response`/`final_response` are both set to a note explaining the outcome (never posted publicly), and `ticket_fetcher.submit_final_responses()` solves the ticket with the status-only `submit_ticket_solved_without_response()` update. Request number 2+ from these senders keeps the existing behavior described below.

Requester emails whose address starts with `noreply` are never answered automatically. They still get a `draft_response` that summarizes the detected request, `human_intervention_required=true`, an empty `final_response`, and no GET_FULL_ORDER/document-generation work. This keeps the request visible to a human without creating a Zendesk public reply to an automated mailbox.

**Exception — `noreply@fedex.com`:** every request from this exact address is submitted as `solved` with no Zendesk comment at all, instead of being left for human review. `customs_rules.is_noreply_fedex_requester_email()` matches only the exact address `noreply@fedex.com` (tolerating `Name <noreply@fedex.com>` wrapping and case), not every `noreply*` sender. For these rows:

- `draft_response` and `final_response` are both set to a note stating that the ticket was submitted as solved, without a response, because the request comes from `noreply@fedex.com`. This is the one case where `final_response` is intentionally non-blank even though no public comment is ever sent — the text exists purely to explain the outcome in the BigQuery history table.
- `ticket_fetcher.submit_final_responses()` checks `is_noreply_fedex_requester_email()` before the general noreply skip and routes these tickets to `submit_ticket_solved_without_response()`, which issues a Zendesk ticket update with `status: solved` and no `comment` field at all, so nothing is posted publicly. This is a different code path from `submit_ticket_response()`, which always requires a non-empty comment body. Zendesk still requires the same mandatory fields to solve a ticket even without a comment — Channel, Reason of contact, and Order Number (custom field ids `360000381300`, `23910471`, `36951465`) — so `submit_ticket_solved_without_response()` fills them exactly the same way `submit_ticket_response()` does for a normal automatic reply (and the per-brand "Country `<BRAND>`" field when resolvable), instead of omitting them. Without this, Zendesk previously rejected the solve with a 422 "is required" error.
- No GET_FULL_ORDER lookup or document generation is run for these rows, same as any other `noreply*` sender.

**Second solved-without-response case — missing tracking number, purely informative message:** when `extracted_tracking_number` is missing from the ticket (so regex/LLM analysis is skipped, see below), the pipeline now runs `customs_rules.is_purely_informative_request()` on the message text (subject + cleaned/raw request body). This checks for the absence of any actionable request language or customs-domain keyword (`customs_rules.has_actionable_request_language()`); blank text is treated conservatively as NOT purely informative. If the message is purely informative (no data or action requested), the row gets the same treatment as `noreply@fedex.com`:

- `draft_response` and `final_response` are both set via `response_generator.build_missing_tracking_solved_note()`, a note stating the ticket was submitted as solved, without a response, because the tracking number was not found and the message is purely informative.
- `ticket_fetcher.submit_final_responses()` routes these rows to `submit_ticket_solved_without_response()` alongside `noreply@fedex.com` rows, before the general `noreply*`/human-intervention skips, so the ticket is solved with a status-only update and no public comment.

If instead the message does contain data/action request language, the row falls back to the existing `HUMAN INTERVENTION REQUIRED` behavior below.

## Gemini LLM fallback

`intent_detection.py` uses `GEMINI_MODEL` as the primary model, defaulting to `gemini-2.5-flash`. If that exact primary model fails with a Gemini limit/quota error such as HTTP 429 or `RESOURCE_EXHAUSTED`, the detector immediately switches to the fallback model. The fallback defaults to `gemini-3.1-flash-lite` and can be overridden with either `GEMINI_RATE_LIMIT_FALLBACK_MODEL` or `GEMINI_FALLBACK_MODEL`.

Useful optional environment variables:

```text
GEMINI_MODEL=gemini-2.5-flash
GEMINI_RATE_LIMIT_FALLBACK_MODEL=gemini-3.1-flash-lite
GEMINI_FALLBACK_RATE_LIMIT_ATTEMPTS=3
```

Whenever Gemini analyzes a row, `intent_detection.py` writes the successful model into `llm_model_used` and the attempted model path into `llm_model_attempts`. `response_generator.py` then prepends `LLM analysis model: <model>.` to the internal `draft_response` only, including the attempted fallback path when applicable. This metadata is added after `final_response` is built, so public Zendesk replies never expose internal model-routing details.

The reviewed regex table generated from the historical-ticket analysis is included in:

```text
config/log_one_tool_army_request_regex_config_updated.csv
config/log_one_tool_army_request_regex_config_updated.xlsx
```

Load the CSV into `tlg-business-intelligence-prd.til.log_one_tool_army_request_regex_config` with columns `request_type` and `regex_pattern` when you want BigQuery regex detection to use the refreshed patterns.

## GET_FULL_ORDER enrichment

The GitHub Actions workflow runs `response_data_extractor.py` immediately after `intent_detection.py` and before `response_generator.py`. The extractor reads `output/request_intent_results.jsonl.gz`, enriches the rows after `requested_data` has been finalized, writes the enriched rows back to the same handoff file, and then `response_generator.py` builds the final draft responses. It calls the GET_FULL_ORDER API only for rows that need order-backed data:

- `return_proforma_invoice`
- `commercial_invoice`
- `customer_email`
- `customer_phone`
- `customer_name` from `shippingAddress.name` + `shippingAddress.surName`
- `shipping_address` from `shippingAddress.addressLine1`, `zip`, `stateOrProvince`, `country`, and `cityOrTown`
- `returned_items_confirmation` from `items[].sku`, `items[].productName`, and `items[].imageUrl`
- `authorization_letter` / LOA export date
- standard UPS-account replies that reference a generated LOA

Required repository secret:

```json
GET_FULL_ORDER_API_CREDENTIALS={
  "client_id": "...",
  "client_secret": "..."
}
```

Default API base URL:

```text
https://zelda.thelevelgroup.com/return/api/v1
```

For a `shipment_order_number` such as `DG-EUA01663254`, the API URL becomes:

```text
https://zelda.thelevelgroup.com/return/api/v1/brands/DG/orders/DG-EU-01663254
```

Special cases:

- For GET_FULL_ORDER URLs, the 6th character of the `shipment_order_number` is replaced with `-`. For example `DG-USB11591156` becomes `DG-US-11591156`, `DG-USC11589641` becomes `DG-US-11589641`, and `DG-EUB01614772` becomes `DG-EU-01614772`.
- Existing `EU-` and `US-` order numbers are already in the URL shape, so this replacement leaves them unchanged. Shipment-block lookup still normalizes those URL-shaped values to `EUA` / `USA`.
- `EUF` and `USF` market-code order numbers are the only exception. The client first tries the exact value, for example `DG-USF11591616`; if that URL returns 404 Not Found, it retries with the 6th character replaced by `-`, for example `DG-US-11591616`.

The client supports these optional environment variables:

- `GET_FULL_ORDER_API_BASE_URL` to override the base URL;
- `GET_FULL_ORDER_AUTH_MODE` with `auto`, `headers`, `basic`, `oauth2`, or `none`;
- `GET_FULL_ORDER_API_TOKEN_URL` when `GET_FULL_ORDER_AUTH_MODE=oauth2`;
- `GET_FULL_ORDER_API_TIMEOUT_SECONDS` to override the request timeout;
- `INVOICE_PDF_DOWNLOAD_TIMEOUT_SECONDS` to override the invoice-PDF download timeout;
- `INVOICE_PDF_DOWNLOAD_USER_AGENT` to override the browser-style user agent used when downloading invoice PDFs;
- `INVOICE_PDF_DOWNLOAD_MAX_ATTEMPTS` to override how many DocOpen wrapper/download targets are tried before blocking the draft.

`auto` tries OAuth2 only when a token URL is configured, then header credentials, then HTTP Basic auth.

## Generated PDF documents

`response_data_extractor.py` also generates the LOA and POA documents. PDF templates are stored in:

```text
templates/pdf/authorization_letter_template.pdf
templates/pdf/power_of_attorney_template.pdf
```

Generated and downloaded copies are written to the top-level `generated_documents` folder:

```text
generated_documents/authorization_letter/<extracted_tracking_number>.pdf
generated_documents/power_of_attorney/<extracted_tracking_number>.pdf
generated_documents/invoice/<invoice_filename_from_document_link>.pdf
generated_documents/shipping_labels/<shipment_order_number>.pdf
```

When `return_proforma_invoice` or `commercial_invoice` is requested, the extractor keeps the source link from `erpDocuments.invoiceDocuments[].documentLink`, downloads the PDF with a browser-style request, resolves DocOpen-style HTML wrappers, ASP.NET `__doPostBack` download buttons, and JavaScript/meta-refresh redirects when present, and saves the final PDF under `generated_documents/invoice`. The draft response can include the saved local document reference, and human-intervention drafts can include source `documentLink` references for reviewer use. The downloader does not rewrite `DocOpen.aspx?link=<file>.pdf` into a bare `/<file>.pdf` URL. A local PDF is required before an automatic public Zendesk reply can be sent for invoice/RPI documents; source links are internal-only and are not used as public-response fallbacks.

When a ticket references more than one `shipment_order_number` (semicolon-separated), GET_FULL_ORDER is queried once per shipment and the resulting invoice/RPI links are combined so **every** shipment's document is downloaded and attached, not only the first one. (Earlier versions of `enrich_dataframe_with_full_order_data` had this multi-shipment lookup nested unreachably after an early `return`, so only whichever document already had a cached path — normally just the first shipment — ever got attached; this has been fixed.)

### Shipping labels

Whenever `export_tracking_number` is part of a response (directly requested or auto-added), `response_generator.py` also auto-adds a `shipping_label` data key and `response_data_extractor.py` generates the return label PDF:

1. POST the shipment's `orderNumber` to the labels API (a Logic App request trigger — see `Labels_triggering_for_DGEUTST0001.py` for a batch-trigger example against the same workflow, configurable via `SHIPPING_LABELS_API_URL` / `SHIPPING_LABELS_API_SIG`).
2. The response contains one `shipment` block per label (outbound + return). The block where `isReturn == false` is the return label; its `labelData` field holds the ZPL, usually base64-encoded.
3. Decode the ZPL and render it to PNG via the Labelary API, then wrap it into a PDF with `reportlab` (see `ZPL_to_PDF_converter.py` for the reference implementation, reused as `render_zpl_label_pdf`).
4. Save to `generated_documents/shipping_labels/<shipment_order_number>.pdf` and attach it in the final response next to the export tracking number, one PDF per shipment on the ticket.

This requires `reportlab` and `pillow`, which are installed by the GitHub Actions workflow alongside the other pipeline dependencies.

**Robustness:** a shipping label that fails to generate/retrieve never withholds the rest of the reply. `missing_required_full_order_values()` deliberately does not treat a missing `shipping_label` PDF as a blocking condition, so a row is only routed to human intervention for reasons unrelated to the label. `response_generator.build_generic_response()` independently drops the `shipping_label` key from the displayed response (and from the "attached documents" intro wording) whenever `generated_document_is_ready(row, "shipping_label")` is false, while still rendering the `export_tracking_number` line with its real value. In short: if the tracking number was retrieved but the label PDF was not, the customer still gets the tracking number and simply doesn't see a "Shipping label" line; the ticket is not silently escalated to human intervention over the label alone.

The GitHub Actions workflow always uploads `generated_documents` as the `generated-customs-documents` artifact. Committing those files back into the repository is optional and controlled by the manual `workflow_dispatch` input `persist_generated_documents`, which defaults to `false`.



## Final Zendesk replies

`response_generator.py` now writes both:

- `draft_response`: the internal/audit draft, which may still include generated-document references;
- `final_response`: the exact public Zendesk comment body.

For rows that require human intervention, `final_response` is intentionally empty and no Zendesk comment is submitted. This includes `noreply*` requester emails, where the draft is an internal human-review summary rather than a customer-facing response. The one exception is `noreply@fedex.com` (see above): its `final_response` is intentionally non-blank — it documents that the ticket was solved without a response — but that text is still never posted as a Zendesk comment; the ticket is solved via a status-only update instead. For automatic rows, generated PDFs are uploaded to Zendesk as ticket attachments and attached-document links are removed from `final_response`; the public body only lists the data/document labels being provided, for example `- RPI`, `- Commercial invoice`, or `LOA`, never a GitHub URL, GET_FULL_ORDER source URL, RPI/invoice link, or local document path. If an invoice/RPI source link exists but no local PDF attachment can be produced, the row is routed to human intervention and the source link remains available only in `draft_response` for internal review.

The BigQuery history table is migrated additively at runtime with:

```sql
ALTER TABLE `tlg-business-intelligence-prd.til.log_one_tool_army_logs_history`
ADD COLUMN IF NOT EXISTS final_response STRING;
```

### Retrying requests that were never answered (`zendesk_submission_status`)

Previously `ticket_fetcher.py` skipped **every** request_id already present in the history table, and `response_generator.py` wrote the history row *before* submitting to Zendesk. So a request whose reply was built but not posted (dry run, Zendesk error, invoice PDF not yet downloadable, or an older code version that logged an empty `final_response`) was logged once and then ignored forever, while the ticket stayed open.

Now:

1. `response_generator.py` submits to Zendesk **first**, then logs history (in a `finally`, so failures are logged too).
2. Each history row records `zendesk_submission_status` (column added automatically with `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`):
   - terminal (request is not picked up again): `submitted`, `already_present`, `solved_without_response`, `human_intervention`, `excluded`, `noreply`, `no_final_response`;
   - retried on the next run while the ticket is still new/open/pending: `dry_run`, `failed`, `pending_submission`, `awaiting_order_data` (human-intervention only because GET_FULL_ORDER data / the invoice PDF was missing).
3. The fetcher and the history logger skip a request only when its **latest** history row is terminal. A retried request gets a new history row per run (new `request_run_id`), so the latest row always shows the latest outcome.
4. Legacy rows (NULL status) are skipped only if they were human-intervention (unless blocked only by missing GET_FULL_ORDER data) or excluded; every other legacy row on a still-active ticket is re-processed, which picks up the backlog of drafted-but-unanswered tickets.
5. A row can no longer end up as `human_intervention_required=false` with a draft but a blank `final_response`: `enforce_reply_consistency()` flags it for human review with an explicit reason.
6. `HISTORY_MAX_RETRY_RUNS` (default `12`) caps how many runs a request is retried.

To see the backlog that will be retried on the next run:

```sql
SELECT request_id, zendesk_submission_status, human_intervention_required, final_response IS NULL OR final_response = '' AS blank_final
FROM `tlg-business-intelligence-prd.til.log_one_tool_army_logs_history`
QUALIFY ROW_NUMBER() OVER (PARTITION BY request_id ORDER BY ingestion_timestamp DESC) = 1
```

Zendesk submission is controlled by one explicit flag:

```bash
SUBMIT_ZENDESK_RESPONSES=true   # submit public Zendesk replies and upload attachments
SUBMIT_ZENDESK_RESPONSES=false  # log only; do not update Zendesk tickets
ZENDESK_STATUS_AFTER_REPLY=solved  # status set when an automatic public reply is posted; keep tickets reopenable/editable until Zendesk closes them by normal policy
```

In GitHub Actions, this is exposed as the manual `workflow_dispatch` input `submit_zendesk_responses`. The default is `false` for safety, so a manually triggered run logs BigQuery history and builds `final_response` values without sending anything to Zendesk unless the input is deliberately switched on.

Repository persistence is controlled separately by `persist_generated_documents`. Leaving it set to `false` prevents the workflow from committing downloaded/generated PDFs to GitHub while still allowing the same local PDFs to be logged, attached to Zendesk replies when `submit_zendesk_responses=true`, and uploaded as workflow artifacts.

The submission decision is independent from BigQuery history de-duplication. This means a dry run with `submit_zendesk_responses=false` can be followed by another run with `submit_zendesk_responses=true`; the agent will still evaluate the current non-human `final_response` values for Zendesk submission. The duplicate-comment guard skips a ticket when the exact same public response body is already present.

### Per-brand "Country" ticket field

Some brands have a Zendesk custom ticket field named `Country <BRAND>` (e.g. `Country DJ`) that Zendesk requires to be set before a ticket can be moved to `solved`. When the GET_FULL_ORDER `shippingAddress.country` value and the brand prefix from `shipment_order_number` (e.g. `DJ` from `DJ-USC11593083`) are both available, `response_generator.py` builds a `country_{brand}_{iso2}` tag and `submit_ticket_response()` in `ticket_fetcher.py` sends it as that field's value. `response_data_extractor.extract_shipment_order_data()` stores the shipping-address country code in the `full_order_country_code` column for this purpose (a prior version of the extractor never populated this column, so the field was always empty and every solve on a country-gated brand hit the 422 error described below).

The brand → field-id mapping lives in `customs_rules.country_ticket_field_id_for_brand()`. It currently only knows about `DJ` (field id `360012198399`). Add more brands there as their field IDs are confirmed in Zendesk, or set them at runtime without a code change via:

```bash
ZENDESK_COUNTRY_FIELD_IDS_JSON='{"DJ": 360012198399, "DG": 123456789}'
```

`Country <BRAND>` is a FieldTagger, and Zendesk only stores a value that exists in that field's own `custom_field_options` list. A constructed tag like `country_dj_us` is silently discarded when it is not one of those options, which leaves the field empty and makes Zendesk reject the solve even though a value was sent. `ticket_fetcher._country_field_option_value()` therefore reads the field's real option list (cached per run) and matches the ISO country code against it: exact value shapes first (`country_{brand}_{iso}`, `{brand}_{iso}`, `country_{iso}`, `{iso}`), then any value whose last segment is the ISO code, then the option name or a name-shaped value (`United States` / `united_states`, via `customs_rules.country_names_for_iso2()`). If nothing matches, the option names/values are printed to the log and no value is sent — the first option is never used as a fallback, since that would file the ticket under the wrong country.

If the brand's country value can't be resolved (unmapped brand, missing shipping country, etc.) and Zendesk rejects the solve with `"Country <BRAND>: is required when solving a ticket"`, `submit_ticket_response()` first tries to recover: the 422 error names the exact `ticket_field_id`, `customs_rules.brand_for_country_ticket_field_id()` maps that id back to the brand, and the option value is resolved as described above using the ISO code from `full_order_country_code` (falling back to the ISO suffix of an existing tag). Resolved values override whatever was sent on the first attempt, and the update is retried with the status included so the ticket is still solved in one go. Only when no value can be resolved does it fall back to posting the comment without the status change, leaving the ticket for manual solving.

Additional response guardrails:

- If the tracking number is not found in the ticket, the request is not analyzed by regex or LLM. Instead, a lightweight purely-informative check runs on the message text: if it contains no actionable data/action request language (e.g. a thank-you/status-only note), the ticket is submitted as `solved` with no public comment, the same way as `noreply@fedex.com` — `draft_response`/`final_response` are both set to a note explaining the ticket was solved without a response because the tracking number was not found and no data or action was requested. Otherwise, the row receives an internal human-intervention `draft_response` saying that the tracking number was not found, and no public `final_response` is generated.
- Invoice/value/MRN/import-export discrepancies and requests for corrected return invoices or corrected values are routed to human intervention instead of sending a normal generated RPI/commercial-invoice reply — **except** on `request_number` `1`, where correction/discrepancy language is treated as noise and the reply is sent with the data found, same as the request_builder rule for `invoice_correction`/`corrected_invoice`/`value_confirmation`.
- For UPS-style `Returns Customs Clearance` request number `1`, when regex/effective requested-data detection finds at least one of `export_tracking_number`, `return_proforma_invoice`, `ups_account_number`, or `returned_items_confirmation`, the response data is expanded to include all four. This ensures a partial first-request regex hit still prepares the complete export TRK + RPI + UPS code + returned-items package.
- If the ticket request body contains `sdoganamento`, `export_tracking_number` is included in the response data so the draft and final public response mention the export tracking number.
- If `shipment_order_number` / `shipmentOrderNumber` starts with `SC`, the row is processed like any other row (it is no longer routed to human intervention). The only difference is the closing section of the public reply, which becomes `Sincerely,` + blank line + `Scarosso | Customer Care` (in Italian, `Cordiali saluti,` + blank line + `Scarosso | Customer Care`). It replaces the template's usual closing phrase and any signer line after it. See `apply_sc_closing()` in `response_generator.py`.
- Informational carrier notices, delivery/pickup notifications, import/export document receipts, automatic billing/case updates, acknowledgement-only replies, and customs-cleared/status-only messages are excluded from processing. They receive an internal `draft_response` reason and never produce a public Zendesk `final_response`.
- `noreply*` requester emails remain human-intervention rows with no public Zendesk reply. If the message is also a status/no-action notification, the draft summary says that no actionable data request was detected instead of listing misleading requested-data labels. The sole exception is `noreply@fedex.com`, which is instead submitted as `solved` with no comment (see above).
- A missing shipping-label PDF is never a reason by itself to block a reply or force human intervention; the export tracking number (and everything else requested) is still sent, with the shipping-label line/attachment simply omitted.
- UPS UK import-clearance instruction emails asking for customs procedure, commodity code, EORI, DAN/deferment approval, UPS credit-account usage, or possible extra charges are routed to human intervention and skipped for GET_FULL_ORDER/document generation.
- Cost/penalty/abandonment requests, including Spanish requests to accept generated expenses or sanctions and authorization-of-expenses letters, are routed to human intervention.
- BigQuery repeated-field JSON strings such as `{"v":[{"v":"customer_email"}]}` are flattened before response generation, so drafts use real requested-data labels rather than the raw JSON object.
- DHL `DATA / FROM / OGGETTO` current-message headers are preserved during cleaning so actionable Italian requests such as missing return invoice or HAWB/AWB export requests are not discarded as quoted history.
- The sender's own closing corporate signature block (name/title followed by labeled contact lines such as `Work Email:`, `Phone Number:`, `CER Team Email:`, `UPS Customer Support Numbers:`) is stripped from the cleaned request text via `customs_rules.strip_contact_signature_block()` before regex/LLM matching. Without this, a short acknowledgement like "Thank you, this invoice has been uploaded." followed by that block could be misread as the customer requesting our `customer_phone`/`customer_email`.

## Response data extractor script

All GET_FULL_ORDER API extraction and generated-PDF logic lives in `response_data_extractor.py`. The older split-module design (`full_order_api.py` plus `document_generator.py`) is no longer used.

The workflow triggers it directly with:

```bash
python response_data_extractor.py
```

By default, the script reads and overwrites:

```text
output/request_intent_results.jsonl.gz
```

Optional local/debug flags:

```bash
python response_data_extractor.py --input output/request_intent_results.jsonl.gz --output output/request_intent_results.jsonl.gz
python response_data_extractor.py --fetch-all
python response_data_extractor.py --skip-documents
python response_data_extractor.py --force-documents
```

The extractor expects the current GET_FULL_ORDER structure where the payload has top-level `order.customer`, top-level `shipments`, each shipment has `shipmentOrderNumber`, and the shipment date is `shippedAt`. It also accepts legacy `shipped_at` as a fallback.
