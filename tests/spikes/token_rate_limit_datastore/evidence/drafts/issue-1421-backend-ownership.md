## Proposed HA ownership-boundary amendment

Related architectural direction:
[enhancements PR #17](https://github.com/praxis-proxy/enhancements/pull/17),
`proposals/00099_stateful-proxy-state-management.md` at `31aa004`. This is still an
open proposal, not an approved or merged API.

The proposal centralizes storage configuration and reload-surviving handles in a
named `StateRegistry`. Filters consume backend handles rather than constructing
clients or owning pools. It explicitly excludes scripted atomic token ledgers:
proposal `00121` owns those typed domain operations and borrows the named Valkey
backend pool. It also excludes the shared service definition and service lifecycle,
so this proposal must not invent that model or claim PR #17 already resolves it.

The backend may be replaced in future. Token-rate-limit filters should therefore
not own a Sentinel discovery/cache manager, failover state machine, connection
pool, or vendor-specific topology shim. HA/topology management should be behind
the shared backend boundary, implemented through its maintained native client or
supported deployment infrastructure. Do not merely move the same custom manager
to another module and treat the maintenance burden as solved.

This boundary does not imply that all backend driver code must live outside the
Praxis project. PR #17 permits opt-in backends in core and additional backends in
external crates. The concern is ownership and duplication, not the physical repo.

The backend infrastructure owns primary election and promotion. The supported
backend/client interface owns primary routing and normal connection lifecycle.
The typed ledger and its consumers retain accounting correctness, connection and
operation deadlines, 429/503/open-mode behavior, and no replay of an ambiguously
dispatched mutation by a client or intermediary.

Bare Sentinel does not itself expose a topology-transparent writable data
endpoint. A deployment-owned stable primary endpoint is one option; using a
maintained Sentinel-aware client behind the shared backend is another. PR #17
mandates neither. The selected integration must meet the safety contract without
a token-rate-limit-specific HA state machine.

The generic `KvStore` API and its `Capabilities::atomic` flag are not enough to
express the complete multi-key token ledger: the flag covers single-key CAS and
counter operations. Preserve typed atomic reserve/reconcile operations rather
than replacing the ledger with a sequence of generic KV calls. Any replacement
backend must qualify the ledger's required capabilities explicitly.

Align backend names/aliases and consumer configuration with the shared proposal,
rather than introducing a competing per-filter schema. The proposal currently
uses `kind: valkey`; a canonical `redis` selector with a compatible `valkey` alias
is a recommendation to agree centrally, not a naming decision already made there.

The user requested backend-owned HA rather than the existing custom manager.
#1421 currently requires filter-side discovery/configuration, so reconcile its
scope with this boundary before refactoring the branch or revising the issue.

The accepted asynchronous-replication policy remains unchanged: no production
`WAIT`/`WAITAOF`, no zero-write-loss guarantee. The native-Cluster key/accounting
redesign remains sharding work under #843.

The spike's passing standalone and Sentinel results remain valid for their tested
inputs. They do not automatically qualify a new endpoint/client integration.
Failover and failure-policy E2E tests must exercise the selected boundary and
verify that neither the client nor an intermediary replays ambiguous mutations.

Draft only. No issue comment or issue-body change has been published, and no
production branch code has been removed or replaced.
