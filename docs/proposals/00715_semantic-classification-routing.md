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
against operator-defined seed-utterance routes — and promotes the result to
the same capability-kind seam `intelligent_route` already uses for
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
- A pluggable classification backend (embedding model / provider), so the
  embedding-backend decision below doesn't get hard-coded into filter logic.
- Seed-utterance route definitions, configured per deployment (operators
  define their own routes/utterances; no fixed taxonomy baked into the
  filter).
- A confidence threshold below which the filter emits "unclassified" rather
  than a low-confidence guess — protects against misrouting on ambiguous
  prompts.
- Turn-level embedding only (the current user turn, not full conversation
  history) — avoids semantic dilution as multi-turn conversations grow, and
  bounds what content the classifier ever processes per request.
- A compliance-tier gate that runs *before* any embedding/classification
  call, so requests tagged with a compliance tier that forbids it never
  reach the classifier at all (fail-closed, not fail-open).
- Wire the classification result into `intelligent_route` as a new
  capability kind, reusing the existing candidate-selection seam.

### Non-Goals

- Semantic caching (a related but separate feature; not in scope here).
- Jailbreak / prompt-injection / general content-safety detection — that's
  `ai_guardrails`'s domain. This filter only classifies task type and
  complexity for routing purposes, not safety.
- A hybrid embedding + LLM-escalation classifier for low-confidence matches.
  `ai#74`'s acceptance criteria mention this; it's a candidate v2
  follow-up, not v1 scope.
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
proposal will follow once that's resolved, per this repo's proposal
convention.

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
  cleared for it, and that classification never processes more
  conversation context than the current turn.
- As a developer, I want to add a new embedding backend by implementing one
  interface, not by modifying the filter's core logic.
- As an operator, I want requests that don't confidently match any
  configured route to pass through unclassified rather than be misrouted on
  a low-confidence guess.
