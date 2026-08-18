---
issue: https://github.com/praxis-proxy/ai/issues/715
discussion: https://github.com/praxis-proxy/ai/pull/750
status: proposed
authors:
  - jordigilh
graduation_criteria:
  - Compliance-tier gating approach reviewed and agreed by a security
    stakeholder
  - Classifier deployment shape (in-process vs. sidecar) confirmed, with
    disposition of #479/#480 decided accordingly
  - v1 accuracy bar for plain cosine-similarity classification confirmed,
    or fine-tuning pulled into v1 scope
  - How? section added in a follow-up PR once the above are resolved
stakeholders:
  - cnuland
  - shaneutt
---

# Semantic Classification & Routing

## What?

Add a new `semantic_classifier` filter that classifies an inference request's
**task type** (e.g. code, math, creative, general) and **complexity**
(easy/hard) from the prompt content itself, and promotes each as an
independent fact (not fused into a single combined capability) into the
same capability-kind seam `intelligent_route` already uses for
model-header and MCP-tool-based routing. No new routing mechanism is
introduced; this is a new *signal producer* feeding the existing
classify → route → branch pipeline. The classification *technique* is the
backend's concern, not this filter's: v1's default backend uses embedding
similarity against operator-defined seed-utterance routes, but the
contract this filter consumes — task/complexity facts in — is
technique-agnostic, so a future backend using a different method needs no
interface change.

### Goals

- A `semantic_classifier` HTTP filter, following the existing filter-model
  conventions (`anthropic_messages_format`, `ai_guardrails`): stateless,
  configured per-pipeline, promotes facts to headers/metadata for downstream
  filters to consume — introduces no new routing primitive.
- Two independent signals, not one combined capability kind: **task type**
  (e.g. code, math, creative, general) and **complexity** (easy/hard) are
  promoted as separate facts (e.g. `task=code`, `complexity=hard`), never
  fused into combinations like `code-hard` or `math-easy`. This keeps
  future classification dimensions addable without a combinatorial
  explosion of capability kinds.
- A pluggable, technique-agnostic classification backend: this filter
  consumes task/complexity facts regardless of whether the backend behind
  them uses embedding similarity, a fine-tuned encoder, or another
  classification method entirely. Which technique to use is the backend's
  decision, not an architectural commitment this filter's interface makes.
- Seed-utterance route definitions, configured per deployment (operators
  define their own routes/utterances; no fixed taxonomy baked into the
  filter).
- A four-state classification outcome, not just confident-vs-not:
  - `classified` — confident match against a configured route.
  - `unclassified` — classification ran, but the result was below the
    confidence threshold. Protects against misrouting on ambiguous prompts.
  - `skipped_by_policy` — the compliance-tier gate blocked the request
    before the classifier ever ran.
  - `unavailable` — the classifier itself could not run (backend down,
    timed out, errored).

  `intelligent_route` needs to tell these apart: "we confidently decided
  not to classify this prompt" is a materially different signal than "the
  classification system is broken," and only the latter should be treated
  as a health/availability concern. Each state is promoted as its own
  outcome fact, not just `classified` — so downstream filters can match
  all four explicitly rather than infer the rest from missing headers.
- On `unavailable`, this filter sets no task/complexity facts, but does
  promote the outcome itself (e.g. `X-Classification-Outcome:
  unavailable`) via the same header mechanism used above. This keeps the
  signal local and explicit, matching two production precedents for an
  external decision-maker going unavailable: Envoy `ext_proc`'s
  [`failure_mode_allow` toggle](https://github.com/envoyproxy/envoy/blob/main/api/envoy/extensions/filters/http/ext_proc/v3/ext_proc.proto)
  and [vLLM Semantic Router](https://github.com/vllm-project/semantic-router)'s
  own
  ["fail-safe design"](https://github.com/vllm-project/semantic-router/blob/29aba60e/website/docs/proposals/production-stack-integration.md)
  (itself built on `ext_proc`). An operator can then match
  `intelligent_route` candidates on this header via existing
  `conditions.headers` (same mechanism as `ai_guardrails` in Non-Goals)
  for explicit graceful degradation — not a new config knob, just the
  existing fact-promotion pattern extended to the outcome. It never fails
  open to an alternate classification path, since that would reopen the
  compliance side-channel this proposal exists to close; whether
  `unavailable` degrades gracefully or hard-fails is still
  `intelligent_route`'s route-table decision (fallback candidate or not),
  the same pattern `router` uses for catch-all routes — now explicitly
  reachable instead of only implicit.
- The signal contract with the classifier is not restricted to
  current-turn-only content — deciding how much context is enough is the
  classifier's concern, not Praxis's. Praxis's v1 implementation defaults
  to sending only the current turn, an implementation choice bounding v1's
  cost, not a hard-coded interface limit. Regardless of how much context a
  backend uses, v1 guarantees a classification decision is never cached or
  reused across turns.
- A compliance-tier gate that runs *before* any embedding/classification
  call, so requests whose tier forbids it never reach the classifier at all
  (`skipped_by_policy`, fail-closed not fail-open). **The tier itself must
  come from trusted policy context — declared tenant/session/app metadata
  evaluated upstream of this filter — never from a client-supplied header
  or derived from the request content itself.** A content-derived tier is
  circular: it would require classifying the request to decide whether
  classifying it is safe. Exactly which component sets the tier and how
  it's carried internally is a `How?` decision; this proposal only fixes
  the trust boundary.
- Wire the classification results into `intelligent_route` as new
  capability kinds, reusing the existing candidate-selection seam.

### Non-Goals

- Semantic caching (a related but separate feature; not in scope here).
- Jailbreak / prompt-injection / general content-safety detection — that's
  `ai_guardrails`'s domain, not this filter's. That said, nothing prevents
  `ai_guardrails` (or any other downstream filter) from scoping its own
  policy to this filter's output today: task/complexity facts are promoted
  as ordinary HTTP headers (`X-Task-Type`, `X-Complexity`), matchable via
  existing filter `conditions.headers`, with no interface change on either
  side.
- Multi-turn / context-level *detection* for safety — still
  `ai_guardrails`'s domain. A live, context-spanning safety check is only
  reachable today as a fixed-window approximation with known evasion risk,
  not mature enough to be a first deliverable. This is distinct from the
  routing classifier's context handling above: the *signal contract* isn't
  restricted to current-turn-only, only v1's default implementation is,
  for cost and decision-staleness reasons.
- A hybrid embedding + LLM-escalation or fine-tuned classifier for
  low-confidence matches. A candidate v2 follow-up pending the accuracy
  question in Open Questions below, not settled v1 scope. If an
  LLM-escalation path is ever built, it needs the same untrusted-content
  isolation framing tracked in
  [ai#754](https://github.com/praxis-proxy/ai/issues/754).
- Response-side classification (classifying model *output* for routing
  purposes) — out of scope; see Open Questions below for whether this is a
  v1-scoping choice or an architectural boundary. (Response-*content*
  inspection for safety/redaction is `ai_guardrails`'s domain, an unrelated
  operation on an already-complete response.)

## Why?

### Motivation

Praxis has no content-aware routing signal today: `intelligent_route`
matches only structured fields (model header, `mcp.method`), confirmed by a
code-search sweep across `praxis`, `praxis-ai`, and `praxis-grid` returning
zero embedding/classification code anywhere. Closing that gap surfaced two
decisions that needed resolving before scoping this as its own filter:

1. **~~No embedding backend chosen~~ — resolved, not blocking.** Embedding
   similarity is one classifier technique among several, not an
   architectural commitment Praxis should block on — Praxis consumes a
   technique-agnostic classification signal, and which technique produces
   it is the classifier's decision. The still-open question this doesn't
   resolve is classifier *deployment shape* (in-process vs. sidecar vs.
   remote), covered in Open Questions below.
2. **A compliance/security question.** Routing on shared context across
   models with different compliance postures — e.g. a local model cleared
   for PII vs. an external model that isn't — risks leaking sensitive data
   across an egress boundary *through the classification call itself*,
   independent of which model ultimately serves the request. Turn-level
   embeddings (not whole-conversation) are also needed to avoid semantic
   dilution as context grows.

Both open questions are independently corroborated by
[vLLM Semantic Router](https://github.com/vllm-project/semantic-router) — a
production semantic-routing layer for AI gateways (the reference
implementation for Envoy AI Gateway's semantic routing). Its architecture
treats safety/compliance signals as first-class and parallel to routing
classification specifically to avoid the same side-channel risk, and its
multi-turn handling is turn-scoped by default. Defaulting to local
classification for the routing decision rather than an external API —
[confirmed on `ai#750`](https://github.com/praxis-proxy/ai/pull/750#issuecomment-5297381194) —
is the right default: local classification inside the trusted inference
boundary, not routing every prompt to an external embedding provider. The
`How?` section of this proposal will follow once the open questions below
are resolved, per this repo's proposal convention.

### Open Questions

Two decisions raised in the `ai#715` discussion are not yet settled and
block writing `How?`:

1. **Classifier deployment shape.** The embedding-backend framing above
   (external API / local model / hybrid) undersold the real decision: this
   is a classifier-*ownership* problem, not just a vectors-*source*
   problem. An embedding backend alone hands back vectors and leaves
   Praxis owning anchors, thresholds, calibration, and model versioning —
   ML lifecycle that ages badly inside a proxy. The current recommendation
   is to run the classifier as a separate local service (e.g. a sidecar in
   Kubernetes, reachable over a low-latency loopback call) rather than
   linking Candle in-process, reusing `#476`'s already-shipped
   `/v1/embeddings` passthrough transport instead of building a new
   in-process dependency. `#479`'s Candle engine (model loading, tokenizer,
   pooling, inference parity) is deployment-agnostic — its own non-goals
   already exclude in-process HTTP filter registration — so it could still
   back a sidecar. `#480` (the in-process filter wiring) is likely
   superseded by this path, pending confirmation. If the sidecar becomes
   unavailable or degraded, this filter falls back to the `unavailable` /
   fail-closed behavior above rather than to any external classifier.
2. **v1 accuracy bar.** A POC measured plain cosine-similarity-vs-anchors
   (the mechanism described in this proposal's Goals, at a 0.75 threshold)
   at ~80% accuracy (MEDIUM recall 62.5%, COMPLEX absorbing neighbors at
   71.6% precision); fine-tuning the same ~23M-parameter model raised that
   to 98.53% on CPU. Does ~80% plus the `unclassified` confidence floor
   clear the bar for a demo-quality v1, or does fine-tuning need to move
   from the "candidate v2" Non-Goal above into v1 scope? Unresolved as of
   this writing.
3. **Sidecar call must use `SubRequestConnector`/`SubRequestClient`, not a
   bare HTTP client.** `ai_guardrails`'s external-provider call
   ([#755](https://github.com/praxis-proxy/ai/issues/755)) bypasses
   Praxis's own sub-request executor (`praxis-core`'s
   `SubRequestConnector`/`SubRequestClient`), so it has no admission cap,
   no circuit breaker, and no unified deadline — a degraded provider costs
   every concurrent request the full configured timeout instead of
   fast-failing. The classifier sidecar call proposed in Open Question 1
   must not repeat this: wiring through `SubRequestConnector`/
   `SubRequestClient` from the start lets a degraded or overloaded sidecar
   fail fast and shed load rather than amplify latency as request volume
   grows. Latency budget: v1's default (local embedding model,
   current-turn-only, no cross-turn caching) runs single-sentence CPU
   inference around 10–30ms per published benchmarks for the underlying
   model — small next to typical LLM-based guardrail-provider latencies
   (several hundred ms to ~1s per published NeMo Guardrails benchmarks), so
   no parallel-execution optimization against `ai_guardrails` is proposed
   for v1; revisit only if a future classification backend brings classify
   latency into the same order of magnitude as guardrails.
4. **Observability for the four-state classification outcome.** The
   `intelligent_route` filter already has a precedent for this shape of
   problem — [`grid#13`](https://github.com/praxis-proxy/grid/issues/13)
   requires a structured logging contract distinguishing its own six
   routing-outcome scenarios, specifically so operators can tell them apart
   without guessing. `semantic_classifier`'s four states need the same
   treatment so the SRE user story below (paging only on `unavailable`, not
   on `unclassified`) is actually actionable; exact fields/events are a
   `How?` decision.
5. **Is response-side classification (Non-Goals) an architectural boundary
   or a v1-scoping choice?** Proposed reading: a routing decision requires
   selecting a backend before a response exists, so classifying model
   *output* for the purpose of choosing a backend isn't coherent at any
   version, unlike the other Non-Goals above (which are genuinely
   deferred v2 candidates). No reviewer has raised this point either way —
   flagging it here for confirmation rather than asserting it in Non-Goals
   outright.

### User Stories

- As a platform operator, I want prompts classified by task type (code,
  math, creative, general) so that requests are routed to a
  task-specialized model without the client having to know or declare which
  model to call.
- As a platform operator, I want prompts classified by complexity so that
  simple queries route to smaller/cheaper models and complex reasoning
  routes to frontier models, without per-request client logic.
- As a security/compliance stakeholder, I want a guarantee that
  compliance-tagged traffic never reaches a classification backend that
  isn't cleared for it, that the compliance tier itself can never be
  forced by a client, and that a classification decision is never cached
  or reused past the turn it was computed for.
- As a developer, I want to add a new classification backend — regardless
  of technique — by implementing one interface, not by modifying the
  filter's core logic.
- As an operator, I want requests that don't confidently match any
  configured route to pass through unclassified rather than be misrouted on
  a low-confidence guess.
- As an SRE/operator, I want to distinguish "the classifier confidently
  chose not to classify this request" from "the classification system is
  broken" so that only the latter pages anyone or affects availability
  SLOs.
- As an SRE/operator, I want semantic routing failure modes that enable me
  to decide how to proceed when a classifier fails — via `intelligent_route`'s
  existing fallback-candidate configuration, not a new knob on this filter.
