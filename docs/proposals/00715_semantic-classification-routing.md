---
issue: https://github.com/praxis-proxy/ai/issues/715
discussion: https://github.com/praxis-proxy/ai/pull/750
status: proposed
authors:
  - jordigilh
graduation_criteria:
  - Embedding-backend decision (external API vs. local model vs. hybrid)
    confirmed by @cnuland on ai#715
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

Add a new `semantic_classify` filter that classifies an inference request's
**task type** (e.g. code, math, creative, general) and **complexity**
(easy/hard) from the prompt content itself — via embedding similarity
against operator-defined seed-utterance routes — and promotes each as an
independent fact (not fused into a single combined capability) into the
same capability-kind seam `intelligent_route` already uses for
model-header and MCP-tool-based routing. No new routing mechanism is
introduced; this is a new *signal producer* feeding the existing
classify → route → branch pipeline.

This closes MVP 3 ("semantic routing"), the one Praxis MVP with zero
existing code today, of the 3 the customer named (alongside
model-based/app-based multi-cluster routing and intelligent/Grid-aware
routing).

### Goals

- A `semantic_classify` HTTP filter, following the existing filter-model
  conventions (`anthropic_messages_format`, `ai_guardrails`): stateless,
  configured per-pipeline, promotes facts to headers/metadata for downstream
  filters to consume — introduces no new routing primitive.
- Two independent signals, not one combined capability kind: **task type**
  (e.g. code, math, creative, general) and **complexity** (easy/hard) are
  promoted as separate facts (e.g. `task=code`, `complexity=hard`), never
  fused into combinations like `code-hard` or `math-easy`. This keeps
  future classification dimensions addable without a combinatorial
  explosion of capability kinds.
- A pluggable classification backend (embedding model / provider), so the
  embedding-backend decision below doesn't get hard-coded into filter logic.
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
  as a health/availability concern.
- On `unavailable`, semantic routing **fails closed**: it routes on
  existing structured signals (model header, `mcp.method`) only, the same
  way Grid already fails closed elsewhere — e.g. `grid#7`'s GridSite
  eligibility gate excludes a peer when no matching site is found, rather
  than guessing. It never fails open to an alternate classification path,
  since that would reopen the exact compliance side-channel this proposal
  exists to close.
- Turn-level embedding only (the current user turn, not full conversation
  history) — avoids semantic dilution as multi-turn conversations grow, and
  bounds what content the classifier ever processes per request.
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
  `ai_guardrails`'s domain. This filter only classifies task type and
  complexity for routing purposes, not safety.
- A hybrid embedding + LLM-escalation or fine-tuned classifier for
  low-confidence matches. `ai#74`'s acceptance criteria mention this; it's
  a candidate v2 follow-up pending resolution of the accuracy question in
  Open Questions below, not settled v1 scope.
- Response-side classification (classifying model *output*, not the
  request). Request-side only, matching how `intelligent_route` already
  operates.

## Why?

### Motivation

Semantic routing is one of the 3 MVPs the customer named, but today it has
zero coverage anywhere in `praxis`, `praxis-ai`, or `praxis-grid` — confirmed
by a code-search sweep across all three repos returning zero
embedding/classification code; only structured-field matching (model
header, `mcp.method`) exists. It was tracked only as a stretch-goal bullet
inside the `ai#74` epic until being promoted to its own scoped issue,
[ai#715](https://github.com/praxis-proxy/ai/issues/715), specifically
because bundling it inside a larger epic left it with no independent
estimate and two unresolved decisions blocking any estimate from firming
up:

1. **No embedding backend chosen.** External API (new latency + credential
   surface on every routing decision) vs. local model (new runtime
   dependency Praxis doesn't have today) vs. a hybrid of the two.
2. **An unresolved compliance/security question**, not just a timeline
   risk. Prior research (`@cnuland`, Jul 31) found that routing on shared
   context across models with different compliance postures — e.g. a local
   model cleared for PII vs. an external model that isn't — risks leaking
   sensitive data across an egress boundary *through the classification
   call itself*, independent of which model ultimately serves the request.
   The same research found that turn-level embeddings (not
   whole-conversation) are also needed to avoid semantic dilution as
   context grows.

Both open questions are independently corroborated by
[vLLM Semantic Router](https://github.com/vllm-project/semantic-router) — a
production semantic-routing layer for AI gateways (the reference
implementation for Envoy AI Gateway's semantic routing). Its architecture
treats safety/compliance signals as first-class and parallel to routing
classification specifically to avoid the same side-channel risk, and its
multi-turn handling is turn-scoped by default. A revised recommendation
grounded in this precedent (defaulting to local classification for the
routing decision rather than an external API) has been posted on
[ai#715](https://github.com/praxis-proxy/ai/issues/715#issuecomment-5293888816)
and is awaiting `@cnuland`'s confirmation — the `How?` section of this
proposal will follow once the open questions below are resolved, per this
repo's proposal convention.

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

### User Stories

- As a platform operator, I want prompts classified by task type (code,
  math, creative, general) so that requests are routed to a
  task-specialized model without the client having to know or declare which
  model to call.
- As a platform operator, I want prompts classified by complexity so that
  simple queries route to smaller/cheaper models and complex reasoning
  routes to frontier models, without per-request client logic.
- As a security/compliance stakeholder, I want a guarantee that
  compliance-tagged traffic never reaches an embedding backend that isn't
  cleared for it, that the compliance tier itself can never be forced by a
  client, and that classification never processes more conversation
  context than the current turn.
- As a developer, I want to add a new embedding backend by implementing one
  interface, not by modifying the filter's core logic.
- As an operator, I want requests that don't confidently match any
  configured route to pass through unclassified rather than be misrouted on
  a low-confidence guess.
- As an SRE/operator, I want to distinguish "the classifier confidently
  chose not to classify this request" from "the classification system is
  broken" so that only the latter pages anyone or affects availability
  SLOs, and so that a broken classifier degrades to structured routing
  rather than silently guessing or falling back to an external service.
