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

**What happens on each exchange.** The request number decides how much
autonomy the workflow has, and the rule gets stricter as a thread drags on:

| `request_number` | Who decides | Possible outcomes |
|---|---|---|
| 1 | regex first, LLM when regex does not match | answered, solved without reply, or handed to the group |
| 2 | **the LLM**, past the regex safety guard | answered, solved without reply, or handed to the group |
| 3+ | **the LLM, for one question only**: is anything being asked? | solved without reply if not, otherwise handed to the group |

Two things are settled before the LLM is reached at all, at every request
number:

- **A pure carrier status notification** (`is_no_action_carrier_notification`)
  is solved with no public comment and no LLM call. From the second exchange
  on this is checked ahead of the tracking-number guards, which would
  otherwise divert the row to the LLM first — a follow-up notification never
  costs a model call.
- **RC-US / RC-EU / LN-EU / LN-US orders** are left exactly as they are.

**Request number 2 — the LLM, not the regex guard.** `regex_engine` already
refuses to auto-answer from the second exchange on, and
`intent_detection.main()` now routes these rows to the LLM ahead of the regex
safety guard, which would otherwise hand a follow-up to a human without anyone
reading it. Then:

- the sender is only acknowledging the previous reply
  (`purely_informative_solved`) -> solved with **no public comment**;
- the sender wants more data or a clarification -> answered if the data is in
  the system, handed to the second-level logistics group if it is not
  (including when LLM confidence is below `LLM_CONFIDENCE_MIN`).

**Request number 3 or higher — a thank-you closes, everything else escalates.**
The thread has already gone back and forth twice without being settled, so no
data request is ever answered automatically, however confident the LLM is.
The one automatic outcome still allowed is the purely-informative solve: a
message asking for nothing is closed with no comment rather than queued for a
human to close by hand. That branch sits ahead of the request-number guard in
`build_llm_output_row()` for exactly this reason. Anything else goes to
`LOG II lvl` with no reply.

**A follow-up's subject is not its request.** The subject line never changes
down an email thread, so a second message whose body is only `Thank you.`
still arrives under `Needing Commercial Invoice - Tracking# ...`. The LLM read
the stale subject as a fresh request, returned `commercial_invoice` at 0.95
with the note "Body is an acknowledgement", and the invoice was sent to
someone who had just confirmed receiving it. Three things prevent that now:

- the batch payload carries `request_number` and `is_follow_up`, so the model
  can tell a first message from a fifth at all;
- the prompt tells it to decide from `request_body` alone on a follow-up, and
  to treat the subject only as background about which shipment the thread
  concerns;
- `build_llm_output_row()` overrides the model outright when the body of a
  request 2+ message is a bare acknowledgement
  (`customs_rules.is_acknowledgement_only`, a full-string match). The ticket
  is then solved with no reply, `engine=acknowledgement_only_follow_up_guard`.

The override is deliberately narrow: "thanks, but the invoice is unreadable"
is not an acknowledgement-only body and is classified normally, so a real
request hidden behind a thank-you is never suppressed.

The `noreply*` / `autoreply*` senders that are solved without analysis are
unaffected; `submit_final_responses()` routes them before any of this.


Requester emails whose address starts with `noreply` are never answered automatically. They still get a `draft_response` that summarizes the detected request, `human_intervention_required=true`, an empty `final_response`, and no GET_FULL_ORDER/document-generation work. This keeps the request visible to a human without creating a Zendesk public reply to an automated mailbox.

**Exception — `noreply@fedex.com`:** every request from this exact address is submitted as `solved` with no Zendesk comment at all, instead of being left for human review. `customs_rules.is_noreply_fedex_requester_email()` matches only the exact address `noreply@fedex.com` (tolerating `Name <noreply@fedex.com>` wrapping and case), not every `noreply*` sender. For these rows:

- `draft_response` and `final_response` are both set to a note stating that the ticket was submitted as solved, without a response, because the request comes from `noreply@fedex.com`. This is the one case where `final_response` is intentionally non-blank even though no public comment is ever sent — the text exists purely to explain the outcome in the BigQuery history table.
- `ticket_fetcher.submit_final_responses()` checks `is_noreply_fedex_requester_email()` before the general noreply skip and routes these tickets to `submit_ticket_solved_without_response()`, which issues a Zendesk ticket update with `status: solved` and no `comment` field at all, so nothing is posted publicly. This is a different code path from `submit_ticket_response()`, which always requires a non-empty comment body. Zendesk still requires the same mandatory fields to solve a ticket even without a comment — Channel, Reason of contact, and Order Number (custom field ids `360000381300`, `23910471`, `36951465`) — so `submit_ticket_solved_without_response()` fills them exactly the same way `submit_ticket_response()` does for a normal automatic reply (and the per-brand "Country `<BRAND>`" field when resolvable), instead of omitting them. Without this, Zendesk previously rejected the solve with a 422 "is required" error.
- No GET_FULL_ORDER lookup or document generation is run for these rows, same as any other `noreply*` sender.

**Second solved-without-response case — missing tracking number, purely informative message:** when `extracted_tracking_number` is missing from the ticket (so regex/LLM analysis is skipped, see below), the pipeline now runs `customs_rules.is_purely_informative_request()` on the message text (subject + cleaned/raw request body). This checks for the absence of any actionable request language or customs-domain keyword (`customs_rules.has_actionable_request_language()`); blank text is treated conservatively as NOT purely informative. If the message is purely informative (no data or action requested), the row gets the same treatment as `noreply@fedex.com`:

- `draft_response` and `final_response` are both set via `response_generator.build_missing_tracking_solved_note()`, a note stating the ticket was submitted as solved, without a response, because the tracking number was not found and the message is purely informative.
- `ticket_fetcher.submit_final_responses()` routes these rows to `submit_ticket_solved_without_response()` alongside `noreply@fedex.com` rows, before the general `noreply*`/human-intervention skips, so the ticket is solved with a status-only update and no public comment.

If instead the message does contain data/action request language, the row falls back to the existing `HUMAN INTERVENTION REQUIRED` behavior below.

### EORI numbers are answered with the invoice

The EORI number is printed on the invoice itself, so a request for it is a
request for the invoice. `eori_number` is therefore in
`DOCUMENT_EMBEDDED_REQUESTED_DATA` alongside `tax_information`,
`country_of_origin` and `product_description`: it collapses into
`commercial_invoice` (or `return_proforma_invoice` when the tracking role says
so), and `append_eori_invoice_note()` adds one line to the reply saying where
the number is:

```text
Il numero EORI è riportato all'interno della fattura allegata.
The EORI number is stated in the attached invoice.
```

This applies when EORI is the only thing requested, in which case the reply is
the invoice plus that line. Previously `eori_number` was silently dropped on
request number 1 (leaving `Regex candidates were removed by safety filters` and
no reply at all when it was the only match) and escalated to a human from
request 2 on; neither happens now. The line is added only when the reply
actually carries an invoice, so the usual guardrail still applies: no local
invoice PDF means human intervention, not a reply pointing at a document that
was never attached.

## LLM provider chain

`intent_detection.py` tries a chain of models for every batch and uses the
first one that answers. The chain is printed once per run:

```text
LLM provider chain: gemini-2.5-flash -> gemini-3.1-flash-lite -> gemini-2.5-flash-lite -> gemini-2.5-pro -> claude-sonnet-4-6
```

**Why this changed.** The old logic fell back only when *both* conditions held:
the error was a rate limit (429 / `RESOURCE_EXHAUSTED`), and the primary model
was exactly `gemini-2.5-flash`. A `503 UNAVAILABLE` ("This model is currently
experiencing high demand") is neither, so it was re-raised on the first attempt
with no retry and no second model — the batch died with
`LLM batch failed: 503 UNAVAILABLE`, every request in it was logged as
`LLM could not identify actionable requested_data`, and whole runs went to
`LOG II lvl` while Gemini was briefly busy.

Now:

1. `is_transient_model_error()` recognizes 503/`UNAVAILABLE`/overloaded/high
   demand/500/502/504/deadline/timeout/connection-reset, and
   `is_retryable_model_error()` is that plus the existing rate-limit check.
2. Each model is retried in place while the error stays retryable
   (`GEMINI_PRIMARY_ATTEMPTS`, default 2, for the primary;
   `GEMINI_FALLBACK_RATE_LIMIT_ATTEMPTS`, default 3, for the others), waiting
   `LLM_TRANSIENT_RETRY_SECONDS` (5s, scaled per attempt) for an outage and
   `LLM_RATE_LIMIT_RETRY_SECONDS` (30s) for a quota error. A non-retryable
   failure — unknown model id, malformed prompt, unparseable answer — is not
   slept on; the chain simply moves to the next model.
3. **Every** model in the chain is tried, whatever the primary is and whatever
   the error was. The `gemini-2.5-flash`-only condition is gone.
4. `llm_model_used` records the model that actually answered and
   `llm_model_attempts` the whole attempted path, so the history table shows
   e.g. `gemini-2.5-flash -> gemini-2.5-flash -> gemini-3.1-flash-lite`.

### Anthropic fallback

When every Gemini model has failed, the batch goes to the Anthropic Messages
API through `ClaudeJsonHelper`, using the **same `CLAUDE_API_KEY` secret** that
`response_data_extractor.py` already uses for address translation (raw key or
the `{"claude_api_key": "..."}` JSON shape, both accepted). It sends the
identical prompt with a JSON-only system instruction and parses the answer with
the same cleaner, so the rows it returns are indistinguishable downstream apart
from `llm_model_used = claude-sonnet-4-6`. If the secret is missing, the run
logs one line and continues with Gemini only.

Because this is the last resort for a provider-wide outage, it is a different
vendor on purpose: a second Gemini model does not help when Gemini itself is
the thing that is down.

Environment variables:

```text
GEMINI_MODEL=gemini-2.5-flash
GEMINI_FALLBACK_MODELS=gemini-3.1-flash-lite,gemini-2.5-flash-lite,gemini-2.5-pro
GEMINI_RATE_LIMIT_FALLBACK_MODEL=gemini-3.1-flash-lite   # legacy, still honored, placed first
GEMINI_PRIMARY_ATTEMPTS=2
GEMINI_FALLBACK_RATE_LIMIT_ATTEMPTS=3
LLM_TRANSIENT_RETRY_SECONDS=5
LLM_RATE_LIMIT_RETRY_SECONDS=30
LLM_CLAUDE_FALLBACK_ENABLED=true
CLAUDE_FALLBACK_MODEL=claude-sonnet-4-6
CLAUDE_FALLBACK_ATTEMPTS=2
CLAUDE_FALLBACK_MAX_TOKENS=8192
CLAUDE_API_TIMEOUT_SECONDS=120
```

`GEMINI_FALLBACK_MODELS` replaces the default chain entirely when set, so the
model ids can be corrected without a code change if Google retires one of them.

**The secret has to be passed to the step.** A repository secret is not
visible to a workflow step that does not name it, so
`CLAUDE_API_KEY is not set` can appear even when the secret exists — it just
means the `intent_detection.py` step has no `env:` entry for it, while the
`response_data_extractor.py` step (which already uses the key for address
translation) does. In `.github/workflows/agent_triggerer.yml`:

```yaml
- name: Detect requested data
  run: python intent_detection.py
  env:
    GEMINI_API_KEY: ${{ secrets.GEMINI_API_KEY }}
    CLAUDE_API_KEY: ${{ secrets.CLAUDE_API_KEY }}   # <- add this
```

The key itself is read lazily on every call and accepts the raw key, or JSON
under `claude_api_key`, `anthropic_api_key`, `api_key` or `key`, from either
`CLAUDE_API_KEY` or `ANTHROPIC_API_KEY`. If the secret is JSON with none of
those fields, the log now says which fields it did find instead of reporting
the key as absent.

Whenever an LLM analyzes a row, `intent_detection.py` writes the successful
model into `llm_model_used` and the attempted path into `llm_model_attempts`.
`response_generator.py` then prepends `LLM analysis model: <model>.` to the
internal `draft_response` only, including the attempted fallback path when
applicable. This metadata is added after `final_response` is built, so public
Zendesk replies never expose internal model-routing details.

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
### Invoice requested together with the dichiarazione di libera esportazione

Italian carrier customs desks ask for both in one sentence:

```text
la dogana necessita per la spedizione in oggetto di fattura e dichiarazione di libera esportazione   (FedEx)
e' priva di fattura accompagnatoria e dichiarazione di libera esportazione                            (DHL)
```

Only the declaration used to be detected, so the row was excluded with
`Only dichiarazione_di_libera_esportazione was regex-detected; no automatic
reply is required.` and the invoice -- which was in the same sentence and which
the pipeline can retrieve -- was never sent.

Two things changed:

- **Detection.** `EXPLICIT_INVOICE_REQUEST_RE` now covers request verbs used by
  those desks (`necessita`, `occorre`, `serve`, `richiede`, `trasmettere`, ...)
  plus `fattura accompagnatoria`/`proforma` and the `fattura ... dichiarazione
  di libera esportazione` pairing in either order;
  `MISSING_INVOICE_REQUEST_RE` accepts `priva **di** fattura` next to the
  existing `priva della fattura`. `non serve/non necessita` is excluded by a
  negative lookbehind, so acknowledgements are still not invoice requests.
  Both messages now resolve to `regex_request_types = [dichiarazione_di_libera_esportazione, invoice]`
  and `requested_data = [commercial_invoice]`.
- **Reply.** The declaration is still never generated as a document -- it is
  already filed with the carrier -- but it is no longer silently ignored.
  When the only response data is the invoice/RPI and the message also asked for
  the declaration, `build_invoice_with_libera_esportazione_response()` replaces
  the generic document-list reply with:

  ```text
  Buongiorno,

  In allegato quanto richiesto.
  La dichiarazione di libera esportazione e' gia' depositata presso i vostri sistemi.

  Cordiali saluti
  ```

  (English: "Please find attached the requested document. / The free export
  declaration is already filed in your systems.") The invoice PDF is attached
  by the usual `document_attachment_paths_for_response()` path, so the normal
  guardrails still apply: if no local invoice PDF can be produced, the row goes
  to human intervention instead of sending this reply with nothing attached.

The declaration on its own is unchanged: still no public reply.

- Informational carrier notices, delivery/pickup notifications, import/export document receipts, automatic billing/case updates, acknowledgement-only replies, and customs-cleared/status-only messages are excluded from processing. They receive an internal `draft_response` reason and never produce a public Zendesk `final_response`.
- `noreply*` requester emails remain human-intervention rows with no public Zendesk reply. If the message is also a status/no-action notification, the draft summary says that no actionable data request was detected instead of listing misleading requested-data labels. The sole exception is `noreply@fedex.com`, which is instead submitted as `solved` with no comment (see above).
- A missing shipping-label PDF is never a reason by itself to block a reply or force human intervention; the export tracking number (and everything else requested) is still sent, with the shipping-label line/attachment simply omitted.
- UPS UK import-clearance instruction emails asking for customs procedure, commodity code, EORI, DAN/deferment approval, UPS credit-account usage, or possible extra charges are routed to human intervention and skipped for GET_FULL_ORDER/document generation.
- Cost/penalty/abandonment requests, including Spanish requests to accept generated expenses or sanctions and authorization-of-expenses letters, are routed to human intervention.
- BigQuery repeated-field JSON strings such as `{"v":[{"v":"customer_email"}]}` are flattened before response generation, so drafts use real requested-data labels rather than the raw JSON object.
- DHL `DATA / FROM / OGGETTO` current-message headers are preserved during cleaning so actionable Italian requests such as missing return invoice or HAWB/AWB export requests are not discarded as quoted history.
- The sender's own closing corporate signature block (name/title followed by labeled contact lines such as `Work Email:`, `Phone Number:`, `CER Team Email:`, `UPS Customer Support Numbers:`) is stripped from the cleaned request text via `customs_rules.strip_contact_signature_block()` before regex/LLM matching. Without this, a short acknowledgement like "Thank you, this invoice has been uploaded." followed by that block could be misread as the customer requesting our `customer_phone`/`customer_email`.

## Resolving the order when the tracking number is not usable

`order_reference_resolver.py` holds the shared order-reference rules. The
pipeline resolves every request to a GET_FULL_ORDER order in this order, and
stops at the first step that succeeds:

1. **Tracking number -> `bi.shipping_platform_shipments`.** Unchanged historic
   behavior, in `ticket_fetcher.enrich_with_shipment_numbers()`.
2. **Order reference in the ticket subject, description or request body.**
   Collected at fetch time into the new `extracted_order_reference` column.
3. **Order reference inside a PDF attached to the ticket.** Only for requests
   steps 1 and 2 could not resolve. The PDF is read through its embedded text
   layer, falling back to local Tesseract OCR for scanned invoices, exactly as
   in the standalone `ticket_order_numbers.py` script.

An order reference always begins with one of the 113 five-character prefixes in
`ORDER_REFERENCE_PREFIXES` (`AL-EU`, `DG-US`, `CVXEU`, ...). The character
immediately after the prefix decides what it is:

| Next character | Meaning | Example | GET_FULL_ORDER order number |
| --- | --- | --- | --- |
| a letter | shipment number | `DG-EUA01663254` | `DG-EU-01663254` |
| `-` | order number | `DG-EU-01663254` | `DG-EU-01663254` |

A shipment-shaped reference therefore has the letter replaced by `-` before the
lookup. That is the same rule as the existing
`response_data_extractor._replace_sixth_character_with_dash()`, so a resolved
reference is written straight into `shipment_order_number` and the existing URL
builder handles both shapes unchanged. `CVXEU`-style prefixes without an
internal dash are now recognized by that helper too.

### Which shipment blocks are read

**The shape of the reference decides what is read.** A shipment number names
one specific shipment, so only that shipment block is read. An order number
covers the whole order, so every shipment block is read.

`shipment_selection_mode` records that decision and tells
`response_data_extractor.extract_shipment_order_data()` which shipment block(s)
of the GET_FULL_ORDER payload to read:

- `exact` — the order came from the tracking number, **or** from a
  shipment-shaped reference such as `DG-EUA01663254`. Only the matching
  shipment block is read.
- `all` — an order-shaped reference such as `DG-EU-01663254` with no usable
  tracking number: the data is taken from **every** shipment of the order.
- `by_tracking` — an order-shaped reference where the ticket did carry a
  tracking number that `bi.shipping_platform_shipments` did not know. The
  shipment block whose shipping label **or** return label carries that tracking
  number is used; when no block matches, every shipment is read.

| Reference found | Tracking in the ticket | Shipments read |
| --- | --- | --- |
| `DG-EUA01663254` (shipment) | any | that shipment only |
| `DG-EU-01663254` (order) | none / not usable | every shipment |
| `DG-EU-01663254` (order) | present but unknown to the shipments table | the block carrying it, else every shipment |

When a ticket carries several references of mixed shape, the row keeps the
order-level mode and the extractor narrows each shipment-shaped reference to its
own block individually.

When several shipments are read, invoice/RPI links are semicolon-joined (the
downloader already splits on `;`, so every shipment's document is attached) and
the returned-items block is newline-joined. Order-level values (customer,
shipping address, country, shipped date) keep the first non-blank value, so
`full_order_country_code` stays usable for the per-brand "Country `<BRAND>`"
ticket field.

### Export tracking number when the tracking lookup failed

The export tracking number used to come from `bi.shipping_platform_shipments`
only (the `shipment_tracking_number` column, i.e. the `is_return = false` row).
A request whose tracking number that table did not know therefore had no export
tracking number even when the order had been resolved from a reference in the
subject/description or in a PDF attachment: the generic reply printed
`[TO BE RETRIEVED]`, and the UPS/FedEx/DHL returns templates fell through to
"non disponibile, avvenuto con altro vettore" — telling the carrier the number
did not exist while it sat in the GET_FULL_ORDER payload.

It is now read out of the GET_FULL_ORDER shipment block as a fallback. Each
shipment block states its labels explicitly:

```json
"shippingLabels": [
  {"carrierCode": "UPS", "labelType": "SHPLBL", "trackingNumber": "1ZV8K2420402911398"},
  {"carrierCode": "UPS", "labelType": "RETLBL", "trackingNumber": "1ZY24G219036376287"}
]
```

`SHPLBL` is the export (outbound) leg and `RETLBL` is the return leg. That
`labelType` is authoritative — field-name heuristics are only a fallback for
payload shapes without a `shippingLabels[]` list, and the `isReturn` flag is
never consulted here because its polarity is inverted in the labels Logic App
(see `extract_return_label_data`).

**Which block is read** (`_resolve_export_tracking()` in
`response_data_extractor.py`), first match wins:

| Case | Block read | `full_order_export_tracking_source` |
| --- | --- | --- |
| The ticket carried a tracking number | the block whose `SHPLBL` **or** `RETLBL` carries it — searched across every shipment of the order, whatever the shape of the reference | `ticket_tracking_shipment_block` |
| No tracking number, shipment-shaped reference (`DG-EUA01663254`) | that shipment block only | `selected_shipment_block` |
| No tracking number, order-shaped reference (`DG-EU-01663254`) | every shipment block; the values are semicolon-joined, since the exact shipment is unknown | `all_shipment_blocks` |

A tracking number present in none of the blocks, or in a block with no
`SHPLBL`, falls back to the caller's normal shipment selection rather than
reporting nothing.

**Only READY and FULFILLED shipments are read when the selection is "every
shipment".** A shipment the order never dispatched (cancelled, on hold, still
being picked) has no usable export tracking number, invoice or returned-items
list, and reporting one would answer the carrier about a shipment that does not
exist. So `shipment_blocks_for_selection()` filters the order-level selection —
`all` mode, and the `by_tracking` fallback when the ticket's tracking number
matched no block — through `shipment_is_eligible()`.

A block named **explicitly** is read whatever its status, because that is the
shipment the ticket is about: the shipment-shaped reference in `exact` mode, and
the block carrying the ticket's own tracking number in `by_tracking` mode. A
block with no `status` field at all counts as eligible, so older payloads and
fixtures keep returning the data they always did.

When an order has shipments but none of them is READY or FULFILLED, the row gets
an explicit `full_order_api_error` naming the statuses that were found (rather
than the generic "shipment block not found"), so the reply is blocked and the
reviewer can see why.

The same `shippingLabels[]` entries also supply `carrierCode` per leg, so
`return_export_carriers_are_same()` can still choose between the same-carrier
and different-carrier reply when the shipments table resolved neither. A single
unambiguous value per leg is required; shipments that disagree leave it unset,
which keeps the existing "assume different carriers" reading. The export
tracking number is likewise the fallback source for the UPS account code
(`1ZV8K242…` -> `V8K242`) and for LOA/POA generation, so those are no longer
blocked either.

New columns: `full_order_export_tracking_number`,
`full_order_return_tracking_number`, `full_order_export_carrier_code`,
`full_order_return_carrier_code` and the internal-only
`full_order_export_tracking_source`. `full_order_export_tracking_number` is also
appended to the BigQuery history table through the existing
`_ADDITIVE_HISTORY_COLUMNS` migration, so the number actually sent is auditable.
`export_tracking_number` is in `FULL_ORDER_LOOKUP_KEYS` (it now triggers the
GET_FULL_ORDER call) but deliberately **not** in `FULL_ORDER_DATA_KEYS`, so a
missing value is not treated like a missing invoice.

The shipments-table value still takes precedence everywhere: when the tracking
lookup succeeds, nothing about the existing behavior changes.

When several shipments contribute an export tracking number, the public reply
renders them as `1Z…01, 1Z…02`: the `;` the pipeline joins on is an internal
separator and must never reach the customer, who would read it as part of the
number (`format_tracking_numbers_for_display()`).

### Fourth resolution step: `til.prd_shipmentStatus`

Order of resolution is now:

1. **order reference in the subject / description / request body**
   (`subject_or_description`);
2. tracking number -> `bi.shipping_platform_shipments` (`tracking_lookup`);
3. tracking number -> the carrier events in
   `tlg-wlfs-prd.til.prd_shipmentStatus` (`shipment_status_events`);
4. **tracking number -> the TIL event stream in
   `tlg-wlfs-prd.til.prd_tilEvents`** (`til_events`);
5. order reference inside a PDF attached to the ticket (`pdf_attachment`);
6. **tracking numbers inside that same PDF**, put back through 2, 3 and 4
   (`pdf_attachment_tracking`);
7. an order **another request in the same run** already resolved for the
   identical tracking number (`cross_request_tracking`).

Step 7 costs nothing: no API call, no OCR, just bookkeeping over work the run
has already done. Carriers open several tickets about one shipment and do not
quote the same tracking numbers in each. On run `7dc4149f`, request 5009740_1
quoted `1ZX662916663848348;1Z7E37218619724391` and resolved to
`WL-EUA00589535` through the shipments table, while **four** other tickets in
the same run quoting only `1ZX662916663848348` went to human review as "no
order reference found" — the answer was already in the dataframe. A tracking
number the run saw against two different orders is skipped rather than
guessed.

Steps 1 and 2 are decided inside `apply_order_reference_fallback()`; 3, 4 and 5
are post-passes over whatever is still unresolved, applied in that order by
`enrich_with_order_references()`.

Two things moved. **Text now outranks the tracking lookup**: a reference the
sender wrote in the subject or body is them naming the order directly, so it
is preferred over one derived from the tracking number. **The PDF scan runs
last** instead of second, so an attachment is only downloaded and OCR'd for a
request that nothing cheaper could resolve — previously a ticket with a
readable PDF never reached either event table.

When both a text reference and a tracking-resolved order exist, the
tracking-resolved one is kept **only if it names a different order**. Replaying
the history shows 44 rows where the two disagree, and in 43 of them they are
the same order written at a different granularity — stored `DG-EUD01670341`
(one shipment) against a body saying `DG-EU-01670341` (the whole order), or a
stored multi-shipment list against a body naming one of them. Merging those
would mix shipment- and order-shaped references, flip
`shipment_selection_mode`, and silently widen the reply to shipments the sender
never asked about, so `order_number_from_order_reference()` is used to drop
them. A genuinely different order is still merged, under
`subject_or_description+tracking_lookup`.

### Steps 5 and 6: the PDF attachments

`apply_pdf_attachment_order_lookup()` reads **two** things out of one
text/OCR pass, because that pass is the most expensive step in the pipeline
and asking twice would double it:

- an **order reference**, which resolves the row outright (`pdf_attachment`);
- every **tracking-shaped value**, used only when no order reference was
  found. Those go back through the same three tables the description's
  tracking number went through — `shipping_platform_shipments`,
  `prd_shipmentStatus`, `prd_tilEvents`, in that order — and a hit is recorded
  as `pdf_attachment_tracking`.

Step 6 exists because **the tracking number quoted in a ticket's description
is often the wrong one**. On a UPS overgoods invoice the "Waybill Number" is
the overgoods leg (`1ZX662916663848348`), which no TLG table ever received,
while the ORIGINAL export tracking number sits in the "Invoice No." field
(`1Z7E37218619724391`) and is the one that resolves to an order. Resolution
used to stop at the description's number and send the request to human review
with "no order reference found", with the answer sitting in the attachment.

`find_tracking_numbers()` matches UPS `1Z` + 16 alphanumerics first, then
12/15-digit FedEx and 10-digit DHL shapes. The patterns are deliberately
narrow and toll-free/dialling prefixes are dropped, because a looser rule
turns invoice totals, VAT numbers, dates and phone numbers on the same page
into phantom tracking numbers. Unmatched candidates cost one row in a
BigQuery `IN UNNEST`, so a few false positives are harmless; a false *order*
would not be.

A tracking number resolved this way is appended to the row's
`extracted_tracking_number`, since downstream shipment selection and the
export-tracking lookups key off that column and the description's number is
known not to resolve. `shipments_orders_for_trackings()` is a small
standalone query against `bi.shipping_platform_shipments` matching both the
forward and the return tracking column; like the event lookups, a tracking
number naming more than one order is skipped rather than resolved to an
arbitrary one.

### Step 4: `til.prd_tilEvents`

`apply_til_events_order_lookup()` unnests `requestPayload.data[]` and reads
`shipment.orderNumber`, matching the ticket's tracking number against all three
places it can appear in a payload:

```sql
JSON_VALUE(item, '$.trackingNumber'),
JSON_VALUE(item, '$.shipment.trackingNumber'),
JSON_VALUE(item, '$.carrierShipmentIdentificationNumber')
```

Semicolon-joined tracking columns are split before matching, so a row carrying
`877817678173;390221886621` is looked up as two numbers. The same guards as
step 3 apply: ambiguity is not resolved, every value goes through
`split_order_references()`, partial resolution stays incomplete, and a query
failure is logged rather than raised.

Three guards on what the events are allowed to produce:

- **Ambiguity is not resolved.** The events are grouped by
  (tracking number, order), so the usual PACKED-then-SHIPPED pair for one order
  collapses to a single row. A tracking number that genuinely names two
  different orders survives as two rows and is then skipped with a log line,
  rather than reporting whichever one `ANY_VALUE` happened to pick.
- **The value is validated.** Each `shipmentOrderNumber` goes through
  `split_order_references()`, the same check every other order passes, so a
  payload carrying something like `LN_SUP_DTM` cannot become a phantom order.
  A `_NEW` suffix in the payload is stripped by the same rule as everywhere
  else.
- **Partial resolution stays incomplete.** On a row with several tracking
  numbers, `order_resolution_complete` is set only when every one of them found
  an order, so a partly-resolved request still goes to a human.

**Separate credentials.** `til.prd_shipmentStatus` and `til.prd_tilEvents` live in the `tlg-wlfs-prd`
GCP project, which the BI service account cannot read, so the step uses its own
client built by `til_bigquery_client()` from the `TIL_BIGQUERY_CREDS` secret
(service-account JSON, same shape as `BI_BIGQUERY_CREDS`; exposed to the job in
`.github/workflows/agent_triggerer.yml`). The client's project comes from the
key's own `project_id`, so a key re-issued for another project needs no code
change; `TIL_PROJECT_ID` is only the fallback.

The secret is **optional by design**. A missing, blank or malformed value logs
one line and returns `None`, which skips the lookup — it is the last resort for
rows already heading to human review, so the whole run must not fail over it.
The same applies to the query: a failure, including a permissions error, is
caught and logged rather than raised. The resolved rows carry
`order_reference_source = shipment_status_events`; nothing keys off the
particular value, since every guard tests whether `shipment_order_number` is
present rather than where it came from.

### GET_FULL_ORDER 404 fallback: the `til` event tables

GET_FULL_ORDER answers `404 Client Error: Not Found` for orders it does not
carry (archived, or never registered on the return platform). Until now the
ticket went to human review as "no invoice found" / "no tracking number" even
when both existed.

They exist in the TIL event tables, written by NAV as the order moved, and
`fetch_shipment_order_data()` reads them whenever — and only whenever — the API
returns a 404. Both halves produce values for the **existing** `full_order_*`
columns, so nothing downstream changes: the invoice link is resolved and
downloaded by the same `download_invoice_pdf()` path (including the
`DocOpen.aspx?link=` indirection), and the tracking numbers feed the same
rules as numbers read out of a shipment block — the UPS account code, LOA/POA
generation, the same-carrier vs different-carrier reply.

#### Invoice / RPI — `prd_invoiceUpdate`, `eventType = 'FATV_RESULT'`

```json
{
  "eventType": "FATV_RESULT",
  "data": {
    "fatvResult": {
      "invoiceNumber": "INV-26115916",
      "shipmentOrderNumber": "HB-EU-00117943",
      "invoiceLink": "http://azureftp.thelevelgroup.com:9038/DocOpen.aspx?link=INV_HB-EU-00117943_INV-26115916_M_1CBBA7BBE8A94FDEB59DAFCCEEE153D3.pdf"
    }
  }
}
```

- **Several rows is the normal case** (retries, reissues, one per shipment).
  Only rows whose `invoiceLink` is a real URL (`http://`, `https://`) are
  usable; a bare file name or a share path is skipped rather than written into
  the reply as a broken link. Within one shipment the newest usable row per
  document type wins, so a reissued invoice answers with the current document.
- **RPI vs invoice** comes from the `RPI-…` / `INV-…` prefix on
  `invoiceNumber` / `fileName` / the link, which is what `documentType` does on
  the GET_FULL_ORDER side. Anything unrecognized is treated as a commercial
  invoice.
- **The envelope's top-level `success` is not filtered on.** It reports the
  whole NAV event and is routinely `false` on payloads carrying a perfectly
  usable `invoiceLink` — the example above is one.

#### The invoice lookup is not limited to the 404 path

Most tickets held for `GET_FULL_ORDER API data is missing for:
return_proforma_invoice` carry **no** `API detail:` line, which means the call
succeeded — the payload's `invoiceDocuments` simply had no entry of the wanted
`documentType`. That is the second way an invoice goes missing, and the 404
fallback never saw it.

So `_backfill_invoice_links_from_events()` runs in
`generate_documents_for_dataframe()` before the download loop, for any
requested invoice/RPI whose `full_order_*` column is still blank, whatever
left it blank. It writes the link into that column, and the existing loop
downloads it as usual.

**Either FATV link satisfies either request.** The PDF behind a FATV
`invoiceLink` carries both the commercial invoice and the RPI, so the
`INV-`/`RPI-` prefix only says which document NAV named the event after — not
which documents the file contains. Whichever link exists is therefore the
right answer to both `commercial_invoice` and `return_proforma_invoice`, and
the substitution runs in both directions; when both links exist the matching
prefix is still preferred. A substituted link is logged as such and the row
carries `full_order_invoice_link_source = prd_invoiceUpdate.FATV_RESULT`.

#### Tracking — `prd_shipmentStatus`, `eventType IN ('PACKED','SHIPPED')`

Those are the only two event types carrying `data.shippingData`. Each entry
there is labelled with a `shipType`:

- `SHIPPING` -> `full_order_export_tracking_number` (the outbound leg);
- `RETURN` -> `full_order_return_tracking_number`.

`carrierCode` fills `full_order_export_carrier_code` /
`full_order_return_carrier_code`, and only when a single unambiguous value
exists per leg — exactly what `_resolve_export_tracking()` does, so shipments
that disagree keep the safe "assume different carriers" reading. An entry with
an unknown or missing `shipType` is skipped rather than guessed at: reporting a
return label as the export tracking number would be worse than reporting
nothing. An entry whose `trackingNumber` is empty — the `RETURN` leg routinely
is — is skipped. Rows served this way carry
`full_order_export_tracking_source = shipment_status_events`.

#### Returned items — the same events' `products[]`

A ticket asking for both the tracking number and the returned-items
confirmation was still held for a human, because only the tracking half had a
fallback. The same PACKED/SHIPPED payload carries the shipment's contents at
the top level, beside `data{}`:

```json
{"products": [{"productId": "408052381130892", "quantity": 1,
               "data": {"cartonId": "…", "productLineId": "…"}}, …]}
```

There is no SKU, product name or image in the event, so **only the `productId`
values are read** and `full_order_returned_items` is rendered as
`  - Product ID: <id>` lines — the same shape `format_returned_items()`
produces, but labelled honestly: the event gives an internal identifier, and
calling it a SKU in a customer-facing reply would be wrong. Duplicates are
collapsed, payload order is kept, and when several shipments match, the newest
event per shipment wins and the lists are merged.

The `shippingData` UNNEST is a `LEFT JOIN` for this reason: an event with no
usable shipping entry must still contribute its products.

#### Export date for the LOA — the event's own `eventTimestamp`

`full_order_shipped_at` is what the authorization letter prints as the export
date, and a blank one blocks the LOA through
`authorization_letter_export_date`. GET_FULL_ORDER would have supplied
`shipment.shippedAt`; on a 404 the event timestamp takes its place:

- the latest `SHIPPED` event's `eventTimestamp`;
- failing that — an order that never produced a `SHIPPED` — the latest
  `PACKED` one.

The value is stored as the ISO string the event carries, so
`format_api_datetime()` renders it for the LOA exactly as it renders a
`shippedAt`.

This one is **not** limited to the 404 path. GET_FULL_ORDER leaves the export
date missing in two different ways — the order 404s, or the call succeeds but
the shipment block simply carries no `shippedAt` — and the LOA fails
identically in both, with `Cannot generate LOA without full_order_shipped_at`.
So `_backfill_shipped_at_from_events()` runs in
`generate_documents_for_dataframe()` immediately before every LOA whose date
is still blank, and writes the value into the dataframe so the
`authorization_letter_export_date` check sees it too. The event lookup is
cached per order, so a row whose date already came from the 404 fallback costs
nothing and a row with a usable `shippedAt` never queries at all.

#### Order known, shipment not

Both lookups match `shipmentOrderNumber` with a regex rather than equality
(`til_event_search_patterns()`), because what the workflow knows about the
ticket differs:

- **A shipment is named** (`HB-EUA00117943`): only that shipment's events are
  read. Both shapes are matched exactly, since NAV files the event under the
  order-shaped reference (`HB-EU-00117943`) even though the field is called
  `shipmentOrderNumber`, while the ticket may carry the shipment-shaped one.
- **Only the order is known** (`HB-EU-00117943`): the exact shipment is
  unknown, so **every** shipment of that order is read — the 6th character,
  the one that distinguishes `HB-EUA…` / `HB-EUB…` / `HB-EU-…`, is matched by a
  single `.`. Each distinct shipment contributes its own invoice/RPI and the
  links are `;`-joined, the same separator `_combine_shipment_results()` and
  the downloader already use. This is the "read all the blocks" rule
  `SHIPMENT_SELECTION_ALL` applies to a GET_FULL_ORDER payload, which the 404
  denied us.

#### Credentials and failure mode

Both tables are in `tlg-wlfs-prd`, so they use the same optional
`TIL_BIGQUERY_CREDS` secret as `ticket_fetcher.til_bigquery_client()` (already
exposed to the job in `agent_triggerer.yml`). The client is built once per
process and cached behind a lock, since the GET_FULL_ORDER prefetch runs
several threads; query results are cached per search pattern for the same
reason.

A missing secret, an unusable key or a failed query logs one line and returns
nothing, and the original 404 is then re-raised — the row behaves exactly as it
did before. **A non-404 error is never swallowed.** No other `full_order_*`
field is filled: these events know nothing about the customer or the address,
so the response builder reports those as missing as usual. Rows carrying a
recovered invoice also carry the internal-only
`full_order_invoice_link_source = prd_invoiceUpdate.FATV_RESULT`.

#### Why a failed shipping label is not the reason in the note

A label the labels API cannot return has never blocked the reply: the customer
still gets the tracking number, `missing_required_full_order_values()`
deliberately leaves `shipping_label` out, and `build_generic_response()` drops
just that line.

The human-intervention note said otherwise, though. `generated_document_error`
accumulates one `<data_key>: <message>` entry per failed document, and the
whole column was appended as `Document detail:` next to whatever actually
blocked the ticket — so a note held for a missing RPI read as though the label
had caused it. `_document_error_detail_for()` now quotes only the entries
naming something in the missing list, which for a label-only failure is
nothing at all.


### The duplicate-reply guard only looks at the last comment

`ZENDESK_SKIP_DUPLICATE_FINAL_RESPONSE` (on by default) exists for one case: a
reply that was posted but whose run did not record it — the status update was
rejected, the history write was lost, submission was toggled mid-flight —
which would otherwise be posted twice.

`zendesk_public_comment_exists()` used to scan the whole comment history for
an identical body, which made it fire on a perfectly legitimate *second*
request. A customer who writes back after being answered ("the invoice is
unreadable", "please resend") gets a new `request_number`, is analyzed
normally, and can quite reasonably need the same documents again — but the
reply was skipped because that text appeared somewhere above in the thread,
leaving the ticket open with the question unanswered.

Only the **last public comment** is compared now. If anyone has spoken since
our reply — the requester above all — it is a new exchange and the answer is
posted again. Internal notes are still ignored, so an agent's private note
does not cause a re-post.

Note that `already_present` is a terminal submission status: a request skipped
by this guard before the fix will not be retried on its own.

### `HB-EU`

`HB-EU` is in `ORDER_REFERENCE_PREFIXES` (114 prefixes, 29 brand codes). It
resolves through every entry point: ticket text, PDF/OCR, the stored
`shipment_order_number` column and the prd_shipmentStatus events; both shapes
work (`HB-EU-01234567` order, `HB-EUA01234567` shipment, the latter mapping to
`HB-EU-01234567` for GET_FULL_ORDER); it is not on a blocked prefix.

Note that the `HB` brand has **no** entry in `_COUNTRY_TICKET_FIELD_IDS`
(`customs_rules.py`). That is not an error — the pipeline checks for the field
and skips the country tag when a brand has none — but if Zendesk does define a
"Country HB" field, it must be added there or the solve transition will be
rejected with "Country HB: is required when solving a ticket". The field ID can
also be supplied at runtime through `ZENDESK_COUNTRY_FIELD_IDS_JSON` without a
code change.

### Order references ending in an underscore

`ORDER_REFERENCE_RE` delimited the reference with `\b`. Python's `\b` counts
`_` as a word character, and the UPS return label prints the reference with a
trailing underscore — `Reference No.1: CV-US-00042647_` — so the match failed on
exactly the line that carries the order number. The tail could not be shortened
to find a boundary either (the preceding character is still a digit), so the
whole reference was lost rather than truncated. The boundaries are now explicit
`(?<![A-Z0-9])` / `(?![A-Z0-9])` assertions: every previously-matching string
still matches, plus the underscore-delimited ones.

### Re-issued references: the `_NEW` suffix

A reference re-issued after a correction is printed with a `_NEW` suffix
(`DG-USB11607007_NEW`). The suffix is bookkeeping, not part of the number —
GET_FULL_ORDER and `bi.shipping_platform_shipments` both know the order only as
`DG-USB11607007` — so `strip_reissued_reference_suffix()` removes it everywhere
a reference is normalized: the ticket-text and PDF path
(`find_order_references()`) and the stored-column / pre-formed path
(`normalize_order_reference()`, and therefore `order_reference_prefix()`,
`is_shipment_shaped_order_reference()`, `order_number_from_order_reference()`
and the blocked-prefix guard).

`-NEW` and ` NEW` are stripped alongside `_NEW`. A separator before `NEW` is
**required**, so a genuine reference whose tail happens to end in those letters
is left alone, and a token that is nothing but `NEW` is returned unchanged
rather than emptied — an unresolvable token has to stay unresolvable so it
routes to human review instead of becoming a phantom order.

### The tracking guard stands down once the order is resolved

`intent_detection.build_llm_output_row()` escalated on the bare
`is_tracking_lookup_not_found()` flag, unlike the two other places that apply
the same rule (`main()`'s dispatch and
`response_generator.tracking_lookup_requires_human()`), which both pair it with
`not has_resolved_order(row)`. A request whose order **was** resolved — from the
subject/description or from a PDF attachment — still reached the LLM through the
final `else` branch of the dispatch (request number 2+, or no other guard
matching) and was then escalated here no matter what the LLM understood, with
`HUMAN INTERVENTION REQUIRED` and the note "Tracking number extracted from the
ticket was not found ... Regex processing was skipped". The ticket was handed to
`LOG II lvl` even though `shipment_order_number`, `order_reference_source` and
`order_resolution_complete` on the same row all showed the order was known and
GET_FULL_ORDER was queryable.

The branch is now qualified the same way as the other two. The guard still fires
when nothing identifies the order, and still fires on partial resolution
(`order_resolution_complete = False`), so a multi-tracking ticket is never
answered with partial data.

### No reply may contain `[TO BE RETRIEVED]`

As a backstop, `enforce_reply_consistency()` now routes any row whose
`final_response` still contains the `[TO BE RETRIEVED]` placeholder to human
intervention and blanks the public reply. A placeholder is a value the pipeline
failed to resolve; sending it tells the carrier nothing while looking like an
answer. This covers every template at once, so a data key that slips past the
per-key checks in `missing_required_full_order_values()` still cannot reach
Zendesk. The different-carrier wording ("non disponibile, avvenuto con altro
vettore") contains no placeholder and is unaffected.

### A `noreply*` request 1 outranks the blocked prefix

The noreply/autoreply request-1 solve is checked **before** the blocked-order-prefix
guard at every stage: `regex_engine.detect()`, `intent_detection.main()`,
`response_generator.build_final_response()` / `pre_submission_status()` and
`ticket_fetcher.submit_final_responses()`. The request is never analyzed, so
which order it happens to reference cannot change the outcome.

Previously the submitter checked the blocked prefix first while
`_build_response_body()` checked the noreply rule first, so a FedEx delivery
notification from `noreply@fedex.com` whose order sat on `LN-EU` / `RC-EU` had a
draft reading "Ticket submitted as solved, without a response" and was then
handed to `LOG II lvl` with `zendesk_submission_status = assigned_to_group` —
left open for a human, for an automated notification nobody needs to read. Those
rows are now solved with a status-only update like any other noreply request 1,
and `pre_submission_status()` returns `pending_submission` rather than the
terminal `blocked_order_prefix`, so a failed solve is retried.

### Matching the Zendesk "Country <BRAND>" drop-down

`Country <BRAND>` is a FieldTagger, so Zendesk only stores values that exist
in the field's own `custom_field_options`. `_country_field_option_value()`
fetches the real option list and matches the ISO code against it in three
passes: exact value, value whose last segment is the ISO code, then the
human-readable name.

Two shapes used to fall through to `no option matches country`:

- **Name-shaped values with a brand suffix.** The live options are
  `united_kingdom_ln`, `korea_ln`, `macau_ln`. Pass 2 splits the value and
  compares the last segment, which is `ln`, not the country. Pass 3 compared
  the raw value against the plain country name, so `united_kingdom_ln` never
  matched `united_kingdom` either; it only worked when the *name* happened to
  be spelled exactly like the ISO 3166 name. `_option_value_label_variants()`
  now strips the brand suffix and a leading `country_`, and
  `_normalize_country_label()` folds case, accents and punctuation, so
  `Côte d'Ivoire` matches `cote_d_ivoire_ln`.
- **Multiple countries in one column.** An order whose shipments go to
  different places arrives as `CH;GB`, which matched nothing at all.
  `split_destination_country_codes()` splits it and each code is tried in
  order.

Names that differ between ISO 3166 and the Zendesk list (`Macao` vs `Macau`,
`Eswatini` vs `Swaziland`, `Myanmar` vs `Burma`, the `St.`/`Saint` pairs, the
`SAR China` suffixes) are in `_ISO2_COUNTRY_NAME_ALIASES`. An unmatched code
is still never guessed: no option means no status change, the same fallback as
before, because filing a ticket under an arbitrary wrong country is worse than
leaving it open.

A multi-country order also blocks an `LN-EU` / `RC-EU` request if **any** leg
goes to US or GB: `CH;GB` is blocked, `CH;IT` is answered.

### A UPS discrepancy is not the first-request checklist

UPS Italia brokerage sends discrepancy reports in the *same* template, from
the same mailbox, as the routine documentation request. Ticket 4980950_1:

```text
La preghiamo di fornirci al piu' presto la seguente documentazione ...
-
Discrepanze; in ft export presente p.iva di The Level, in bolla presente
p.iva di Roberto Cavalli; non e possibile sdoganare in franchigia,
conferma modo di sdoganamento
```

The only regex hit in that body was **`Tax ID`, from the UPS legal footer**,
which maps to invoice/RPI. `expand_first_returns_customs_clearance_bundle()`
then turned that single boilerplate match into the full
reintroduzione-in-franchigia reply — export tracking, UPS code, RPI and a
returned-items list — while the carrier was asking which customs procedure to
use.

`reports_discrepancy()` now blocks the bundle expansion and escalates instead.

It is deliberately **narrower** than `contains_correction_or_discrepancy()`,
which the request>=2 guard in `response_generator` uses. That looser pattern
matches `Fattura di ritorno con valore corretto` and `documentazione per poter
procedere allo sdoganamento` — both standard checklist wording. Replaying the
history showed it would have escalated **55** correctly-answered first
requests. `reports_discrepancy()` requires an explicit mismatch word
(`discrepanze`, `non corrisponde`, `difforme`, `errata`, `mismatch`,
`do not match`); `corretto` on its own is not enough, because "with the
correct value" is a request, not a complaint.

#### The one discrepancy with a standard answer

`non e possibile sdoganare in franchigia, conferma modo di sdoganamento` is a
discrepancy report, but unlike the others it does not need a judgement call:
the same return documentation applies, it is simply supporting **definitive**
clearance rather than reintroduction under the franchise regime. So
`requires_definitive_clearance()` lets the bundle through, and
`build_ups_returns_definitive_clearance_response()` swaps one line:

```text
Buongiorno,

In allegato la documentazione per lo sdoganamento in definitiva:

- TRK in export: 1ZK0G0700430617178
- Cod UPS: K0G070
- Return Proforma Invoice: ...
```

Everything else -- the documents, the returned-items list, the closing -- is
identical to the franchise reply. The pattern deliberately does **not** match
`In alternativa vi preghiamo di fornirci autorizzazione allo sdoganamento in
via definitiva`, which offers definitive clearance as a fallback to a request
that is still answerable the normal way.

Against the September-October history the narrow rule flips **4** rows, all of
them genuine discrepancies that were answered automatically and should not
have been:

```text
4999831_1  "- Discrepanza valore"
4995543_1  "il valore della rpi non corrisponde con documentazione in nostro possesso"
4993357_1  "fornire fattura corretta in quanto gli importi non corrispondono"
4986995_1  "- Conferma DISCREPANZA TRA RPI E FATTURA"
```

### An unresolved extra tracking number no longer blocks a resolved order

`order_resolution_complete` used to go False whenever **any** tracking number
on the ticket was missing from `bi.shipping_platform_shipments`, even when the
order itself had been resolved. That flag switches off `has_resolved_order()`,
which re-arms the "tracking number not found" escalation in
`regex_engine`, `intent_detection` and `response_generator` — so requests that
had their order, their documents and everything else were never answered.

Sixteen rows in the September-October history, fourteen of them on a single
order. The "missing" second tracking number is almost never a second shipment:

```text
5009740_1  1ZX662916663848348 ; 1Z7E37218619724391   overgoods waybill + export leg
4999624_1  1ZGE71566804228046 ; 1ZGE71566881963393   original + re-ship number
5010265_1  JJD149990200085537821 ; 9911799752        DHL JJD + its AWB
4973575_1  877490063901 ; 919820673441               AWB + the recipient's PHONE number
5006360_1  1Z2V58999190429249 ; 1Z2V58999197485738   two shipments, one order
```

The row is now marked incomplete only when it is genuinely ambiguous: the
tracking numbers resolved to **more than one order** *and* some did not
resolve at all, so which order the unknown ones belong to cannot be decided.
A single resolved order is never ambiguous.

**Trade-off.** A ticket about two shipments where only one resolves is now
answered with the documents of the one that did, instead of going to a human.
Of the 16 affected rows, 14 are single-order, so that case is rare — but it is
a deliberate loosening of a guard that existed to prevent partial answers. To
restore the old behaviour, drop the
`and len(split_order_references(existing_order)) > 1` clause in
`apply_order_reference_fallback()`.

### Unpaid extra charges and `config/ddp_configuration.json`

When UPS reports that the consignee has not paid the additional costs, who
pays is decided by the `(destination country, brand)` table in
`config/ddp_configuration.json` — 901 pairs across 17 brands. `ddp=true` asks
the carrier to deliver and bill the TLG UPS account; `ddp=false` confirms the
return.

**That table was unreachable.** Three separate things had to be fixed:

1. `regex_engine.detect()` short-circuited every unpaid-charges request to
   `human_intervention_required`. The DDP branch in `response_generator`
   fires only on `requested_data == ["ups_account_number"]`, so it could
   never run. The regex stage now classifies these as `ups_account_number`
   and leaves the decision downstream, because the destination country comes
   from GET_FULL_ORDER and does not exist yet at that point.
2. `UNPAID_EXTRA_CHARGES_RE` had no English noun for the charge. The UPS
   wording is "has still not paid the **additional costs** of this shipment",
   and `costs` was missing from a list that had `charges`, `oneri`, `costi`,
   `spese`, `dazi`. Only notifications that also carried the Italian text
   matched. `costs?|fees?|duties|duty|taxes` are now in both copies of the
   pattern.
3. An unmatched lookup used to fall through to the return-confirmation reply
   in every case. It is now split in two, which is what reconciles
   `prompts/response_builder.md` (unmatched pair -> return confirmation) with
   the `_comment` in `config/ddp_configuration.json` (undetermined -> human):

| lookup | outcome |
| --- | --- |
| pair found, `ddp=true` | `Please proceed with the delivery of the shipment on topic.` |
| pair found, `ddp=false` | `I confirm you the return of shipment on topic.` |
| pair missing, **brand present** in the table | `I confirm you the return of shipment on topic.` — the table covers the brand, so this destination simply is not DDP |
| **brand absent** from the table entirely | human intervention |

`brand_in_ddp_configuration()` draws the line. HB has no rows at all, so HB
tickets stay with a human; DG shipping to a country that is not listed gets
the return-confirmation reply rather than an escalation.

### What the DDP table does NOT decide

It does not choose between *reintroduzione in franchigia* and *sdoganamento in
definitiva*. Those are the two UPS Returns first-request templates, and
`prompts/response_builder.md` ("UPS Returns Customs Clearance First Request")
keys them on the **carrier comparison** alone:

> Compare the `carrier_code` for the row where `is_return = true` with the
> `carrier_code` for the row where `is_return = false`. If the two values are
> the same, use the response that includes the export tracking. If different,
> use the response that says the export tracking is not available because the
> export shipment happened with another carrier.

Same carrier → `In allegato la documentazione per la reintroduzione in
franchigia:`. Different carrier → `Confermo la documentazione in vostro
possesso per lo sdoganamento in definitiva.` That is
`return_export_carriers_match()`, and it is independent of DDP.

The DDP table drives a different pair of replies entirely — the English
"Please proceed with the delivery…" / "I confirm you the return of shipment…"
pair, both with the letter of authorization attached — and only for the
unpaid-extra-charges case.

The escalation is checked **after** `missing_required_full_order_values()`,
so a row whose GET_FULL_ORDER lookup has not run yet keeps the retryable
`awaiting_order_data` status instead of being written off as unconfigured.

### LN-EU / RC-EU: blocked only when shipping to the US or GB

`RC-US` and `LN-US` are never processed, whatever the destination.

`RC-EU` and `LN-EU` are **answered normally**, unless the GET_FULL_ORDER
destination country is `US` or `GB`. The destination is
`shippingAddress.country`, already stored in `full_order_country_code`.

That makes the decision three-state, because the destination is not known
until the GET_FULL_ORDER lookup has run — which is after the regex and LLM
stages. `order_reference_resolver.order_processing_state()` returns:

| State | Meaning | Who acts on it |
| --- | --- | --- |
| `blocked` | `RC-US`/`LN-US`, or `RC-EU`/`LN-EU` going to US/GB, or the upstream `order_processing_blocked` flag with no reference to classify | every stage: no reply, no solve, assigned to `LOG II lvl` |
| `destination_pending` | `RC-EU`/`LN-EU` with no destination yet | the request is processed normally **through** the GET_FULL_ORDER lookup, then asked again |
| `allowed` | everything else | answered normally |

Two row-level wrappers in `customs_rules.py` pick the right question for the
stage:

- `row_order_processing_blocked_now()` — used by `regex_engine.detect()`,
  `intent_detection.main()` and `response_data_extractor.row_needs_full_order_lookup()`.
  A pending row is **not** blocked here, because these stages run before the
  lookup and blocking them would make the destination permanently unknowable.
- `row_order_processing_blocked_final()` — used by
  `response_generator.is_blocked_order_prefix_row()` and
  `ticket_fetcher.submit_final_responses()`. A pending row **is** blocked
  here: the lookup was its chance to prove the destination is outside US/GB,
  and without that proof the request goes to the logistics group rather than
  being answered on an assumption.

#### The destination lookup is its own reason to call GET_FULL_ORDER

`row_needs_full_order_lookup()` answers "does the *answer* need order data",
and that is a different question from "do we need the destination to decide
whether to answer at all". A destination-pending row whose `requested_data`
needed nothing from the API — or was empty — skipped the lookup entirely, so
the destination stayed unknown and the row was blocked for want of
information the API would happily have returned. That is why tickets were
rejected with "the GET_FULL_ORDER destination country could not be
determined" even though the order clearly had a country.

`customs_rules.row_needs_destination_lookup()` now forces the call for any
RC-EU / LN-EU row that has a `shipment_order_number` and no destination yet,
ahead of every "this row is not answerable" early return in both
`response_data_extractor.row_needs_full_order_lookup()` and
`response_generator.response_row_needs_full_order_lookup()`.

`row_has_full_order_attempt()` makes the matching exception: an attempt that
produced no destination has not answered the question the row is blocked on,
so a still-pending row is retried instead of being treated as already looked
up. Every other prefix keeps the old skip-if-attempted behaviour.

When a row is still blocked for lack of a destination, the note now says
which of four things happened: the lookup did not run, it failed (with the
API error), it returned the order but no `shippingAddress` country, or there
was no `shipment_order_number` to look up.

The internal "NOT PROCESSED AUTOMATICALLY" note now says which of the three
reasons applied (always-blocked prefix / shipping to US or GB / destination
unknown). It is never posted to Zendesk.

Replaying the September-October history over this change gives 418 differing
rows, **all of them that internal note and nothing else** — no change to
`requested_data`, `final_response`, human intervention, submission status,
document keys or reason of contact. That is the expected result: the export
has no `full_order_country_code` for these rows, because the old code blocked
them before the lookup that would have filled it.

### Blocked order prefixes

Orders starting with `RC-US`, `RC-EU`, `LN-EU` or `LN-US` are never processed.
The ticket is left **exactly** as it is: no public reply, no solve, and no group
re-assignment either. The flag is computed once into `order_processing_blocked`
and honored at every stage — `regex_engine.detect()` returns an excluded row
before any other rule, `intent_detection` skips the LLM, the extractor makes no
GET_FULL_ORDER call even with `--fetch-all`, `final_response` is empty, and
`submit_final_responses()` performs no ticket update. These rows are logged with
`zendesk_submission_status = blocked_order_prefix`.

### Tickets already in `LOG II lvl` are skipped entirely

A ticket already assigned to the escalation group was handed to a human on an
earlier run, so the agent must not touch it again. `fetch_ticket_rows()`
resolves the group id once per run and drops those tickets immediately after the
status filter, before requester lookup, comment fetching and analysis. They
never reach the active-ticket table, so **no history row is written for them at
all** — they are invisible to the run rather than logged with a skip status. The
per-run log line reports how many were dropped this way.

Set `SKIP_TICKETS_IN_ESCALATION_GROUP=false` to disable the filter. When the
group name cannot be resolved the filter is reported as unavailable and the run
continues without it.

### Unresolved tickets go to `LOG II lvl`

Any ticket the workflow neither answers nor solves — no data found, human
intervention required, `noreply*` sender, blank `final_response`, or a failed
submission — is assigned to the second-level logistics group, following the same
approach as the standalone `assign_tickets_to_group.py` script: the group is
set, the individual assignee is cleared, and the **status is deliberately left
untouched** so the ticket stays actionable for a human. Blocked-prefix tickets
are the one exception and are never re-assigned. These rows are logged with
`zendesk_submission_status = assigned_to_group`.

Group assignment respects `SUBMIT_ZENDESK_RESPONSES`, so a dry run never
modifies a ticket. Relevant environment variables:

```text
ASSIGN_UNHANDLED_TICKETS_TO_GROUP=true   # master switch, default true
SKIP_TICKETS_IN_ESCALATION_GROUP=true    # drop tickets already in the group
ZENDESK_ESCALATION_GROUP_NAME=LOG II lvl # resolved to an id via /api/v2/groups
ZENDESK_ESCALATION_GROUP_ID=             # optional, takes precedence over the name
ZENDESK_ESCALATION_CLEAR_ASSIGNEE=true   # clear the individual assignee
ORDER_REFERENCE_PDF_OCR_ENABLED=true     # OCR fallback for scanned PDFs
ORDER_REFERENCE_PDF_OCR_DPI=300
TESSERACT_EXE=                           # only when tesseract is not on PATH
```

Both `assign_unhandled_tickets_to_group` and `escalation_group_name` are also
exposed as `workflow_dispatch` inputs. The GitHub Actions workflow installs the
`tesseract-ocr` apt package and `pytesseract`; when either is unavailable the
PDF step degrades to "no order reference found" rather than failing the run.

### Tickets with several tracking or order numbers

Multi-value handling is preserved end to end. `extract_tracking_number()` still
returns every tracking number it finds as a `;`-joined string, the shipments
lookup maps each one, `find_order_references()` returns every reference in the
ticket, and `enrich_dataframe_with_full_order_data()` still queries
GET_FULL_ORDER once per `;`-separated order and merges the results, so every
shipment's invoice/RPI is attached.

Partial resolution is handled explicitly. When a ticket carries several
tracking numbers and only some of them are in
`bi.shipping_platform_shipments`, any order reference found in the ticket text
is **merged into** the orders already resolved from tracking, so the unresolved
shipment is not silently dropped from the reply. When no reference covers the
gap, the new `order_resolution_complete` column is set to `False` and the
request stays in human review exactly as it did before this feature existed —
it is never answered with partial data. Legacy rows without the column are
treated as complete.

Note that the blocked-prefix check is deliberately aggressive here: if **any**
order on a multi-order ticket is on a blocked prefix, the whole ticket is left
untouched.

### New columns

`extracted_order_reference`, `order_reference_source` (`tracking_lookup` /
`subject_or_description` / `pdf_attachment`, or `tracking_lookup+subject_or_description`
when the two were merged), `shipment_selection_mode`, `order_processing_blocked`,
`order_resolution_complete` and `full_order_export_tracking_number` are added to the active-ticket table and to the
history table. The history table is migrated additively at runtime through the
existing `_ADDITIVE_HISTORY_COLUMNS` mechanism, so no manual DDL is needed.

Because the tracking-number guardrails now stand down whenever an order was
resolved, a ticket with no tracking number — or with a tracking number unknown
to `bi.shipping_platform_shipments` — is analyzed and answered normally as long
as an order reference was found. It is only routed to human intervention when
nothing at all identifies the order.


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
