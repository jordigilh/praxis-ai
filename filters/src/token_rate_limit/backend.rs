// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Praxis Contributors

//! Pluggable token-rate-limit state backends.
//!
//! Adapted, unmodified in logic, from the `token_rate_limit::backend` module
//! on nerdalert's `poc/distributed-token-rate-limit-demo` spike branch
//! (<https://github.com/nerdalert/ai/tree/poc/distributed-token-rate-limit-demo>).
//! `reserve`/`reconcile` are key-agnostic (`ReserveRequest`/`ReconcileRequest`
//! carry a plain `String` key). The filter resolves M5 dimensions into an
//! opaque key before calling these backends.

use std::{
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use async_trait::async_trait;
use metrics::counter;
use praxis_ai_apis::hash::Sha256;
use redis::{
    aio::MultiplexedConnection,
    sentinel::{SentinelClient, SentinelServerType},
};
use tokio::sync::mpsc;

use super::{
    ledger::{Budget, Decision, DenialReason, Ledger, Settlement},
    token_bucket_ledger::{self, TokenBucketLedger},
};

/// Backward-compatible standalone connection timeout.
pub(super) const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_millis(500);

/// Backward-compatible command response timeout.
pub(super) const DEFAULT_OPERATION_TIMEOUT: Duration = Duration::from_millis(500);

/// Default overall bound for Sentinel traversal, failover convergence, and
/// discovered-primary connection establishment.
pub(super) const DEFAULT_DISCOVERY_TIMEOUT: Duration = Duration::from_secs(3);

/// Delay between read-only Sentinel discovery attempts. The complete loop is
/// still bounded by [`ValkeyTimeouts::discovery`].
const SENTINEL_RETRY_INTERVAL: Duration = Duration::from_millis(50);

/// Record one read-only Sentinel discovery attempt with bounded labels.
pub(super) fn record_primary_discovery_attempt(backend: &'static str, result: &'static str) {
    counter!(
        "praxis_trl_primary_discovery_attempts_total",
        "backend" => backend,
        "result" => result,
    )
    .increment(1);
}

/// Record one cached data connection result.
pub(super) fn record_backend_connection(
    backend: &'static str,
    topology: &'static str,
    phase: &'static str,
    result: &'static str,
) {
    counter!(
        "praxis_trl_backend_connections_total",
        "backend" => backend,
        "phase" => phase,
        "result" => result,
        "topology" => topology,
    )
    .increment(1);
}

/// Record one coalesced Sentinel primary rediscovery result.
pub(super) fn record_primary_rediscovery(backend: &'static str, result: &'static str) {
    counter!(
        "praxis_trl_primary_rediscoveries_total",
        "backend" => backend,
        "result" => result,
    )
    .increment(1);
}

/// Record one stale cached connection invalidation.
pub(super) fn record_connection_invalidation(backend: &'static str, topology: &'static str) {
    counter!(
        "praxis_trl_backend_connection_invalidations_total",
        "backend" => backend,
        "topology" => topology,
    )
    .increment(1);
}

/// Version of the accounting semantics encoded by Valkey configuration
/// fingerprints. Bump this whenever an existing state value would be
/// interpreted differently by new Lua code.
const ACCOUNTING_CONFIG_SCHEMA: &str = "v1";

/// Finish a schema-versioned accounting configuration digest.
fn accounting_config_fingerprint(digest: Sha256) -> String {
    let hash = digest
        .finish()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    format!("{ACCOUNTING_CONFIG_SCHEMA}:{hash}")
}

/// Canonical fingerprint for state interpreted by the sliding-window Lua
/// scripts. Budget ordering is not semantic, so sort `(window, capacity)`
/// pairs before hashing them.
fn sliding_window_config_fingerprint(
    budgets: &[Budget],
    reservation_timeout_ms: u64,
    max_keys: usize,
    max_active_reservations: usize,
) -> String {
    let mut digest = Sha256::new();
    digest.update(b"praxis:token_rate_limit:accounting_config");
    digest.update(&[0]);
    digest.update(ACCOUNTING_CONFIG_SCHEMA.as_bytes());
    digest.update(&[0]);
    digest.update(b"sliding_window");
    digest.update(&[0]);

    let mut canonical_budgets = budgets
        .iter()
        .map(|budget| (budget.window_ms, budget.capacity))
        .collect::<Vec<_>>();
    canonical_budgets.sort_unstable();
    digest.update(&(canonical_budgets.len() as u64).to_be_bytes());
    for (window_ms, capacity) in canonical_budgets {
        digest.update(&window_ms.to_be_bytes());
        digest.update(&capacity.to_be_bytes());
    }
    digest.update(&reservation_timeout_ms.to_be_bytes());
    digest.update(max_keys.to_string().as_bytes());
    digest.update(&[0]);
    digest.update(max_active_reservations.to_string().as_bytes());

    accounting_config_fingerprint(digest)
}

/// Canonical fingerprint for state interpreted by the token-bucket Lua
/// scripts. Hash the exact IEEE-754 refill value passed to Lua rather than a
/// display-format approximation.
fn token_bucket_config_fingerprint(
    capacity: u64,
    refill_rate: f64,
    reservation_timeout_ms: u64,
    max_keys: usize,
    max_active_reservations: usize,
) -> String {
    let mut digest = Sha256::new();
    digest.update(b"praxis:token_rate_limit:accounting_config");
    digest.update(&[0]);
    digest.update(ACCOUNTING_CONFIG_SCHEMA.as_bytes());
    digest.update(&[0]);
    digest.update(b"token_bucket");
    digest.update(&[0]);
    digest.update(&capacity.to_be_bytes());
    digest.update(&refill_rate.to_bits().to_be_bytes());
    digest.update(&reservation_timeout_ms.to_be_bytes());
    digest.update(max_keys.to_string().as_bytes());
    digest.update(&[0]);
    digest.update(max_active_reservations.to_string().as_bytes());

    accounting_config_fingerprint(digest)
}

/// Request to admit an estimated token cost against a key's budget.
#[derive(Debug, Clone)]
pub(super) struct ReserveRequest {
    /// Opaque budget key resolved from the filter's key spec.
    pub(super) key: String,
    /// Estimated token cost to reserve if admitted.
    pub(super) estimate: u64,
    /// Caller's current time, in milliseconds.
    pub(super) now_ms: u64,
}

/// Request to settle a prior reservation against actual usage.
#[derive(Debug, Clone)]
pub(super) struct ReconcileRequest {
    /// Same key the original [`ReserveRequest`] used.
    pub(super) key: String,
    /// Reservation ID returned by [`BackendReserve::Admitted`].
    pub(super) reservation_id: u64,
    /// Actual token usage, if known; `None` charges at `estimate`.
    pub(super) actual: Option<u64>,
    /// The original reservation's estimate (for backends, like Valkey,
    /// that reconcile out-of-band and need it for a default charge).
    pub(super) estimate: u64,
    /// Caller's current time, in milliseconds.
    pub(super) now_ms: u64,
}

/// Result of a [`TokenRateLimitStateBackend::reserve`] call.
#[derive(Debug, Clone)]
pub(super) enum BackendReserve {
    /// Request may proceed with this reservation.
    Admitted {
        /// Opaque ID used for later reconciliation.
        reservation_id: u64,
        /// Estimate actually reserved.
        estimate: u64,
        /// Total committed usage in the current window (or tokens
        /// consumed from the bucket) *after* this reservation was
        /// placed. Used by the filter to evaluate graduated soft-limit
        /// tiers (proposal S1) — tiers whose capacity threshold is at
        /// or below this value fire their `inject` action.
        usage_after: u64,
    },
    /// Request must be rejected before routing.
    Denied {
        /// Conservative delay before another admission attempt.
        retry_after_ms: u64,
        /// Distinguishes budget exhaustion from the `max_keys` cap.
        reason: DenialReason,
    },
}

/// Result of a [`TokenRateLimitStateBackend::reconcile`] call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum BackendSettlement {
    /// Actual usage was applied exactly once.
    Applied {
        /// Actual tokens charged.
        actual: u64,
        /// Estimate returned to the budget.
        refund: u64,
        /// Usage above the estimate.
        overage: u64,
    },
    /// The reservation was already reconciled or conservatively expired.
    Noop,
}

/// Latest bounded state published by one rule backend.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct BackendSnapshot {
    /// Sum of the last calculated remaining balances for retained keys.
    pub(super) budget_remaining: u64,
    /// Reservations still awaiting reconciliation.
    pub(super) active_reservations: usize,
    /// Distinct budget keys currently retained.
    pub(super) active_keys: usize,
}

/// Shared last-observed Valkey state, updated from existing Lua replies.
#[derive(Default)]
struct ValkeyTelemetryState {
    /// Last aggregate remaining balance returned by Lua.
    budget_remaining: AtomicU64,
    /// Last rule-level pending reservation count returned by Lua.
    active_reservations: AtomicUsize,
    /// Last rule-level retained-key count returned by Lua.
    active_keys: AtomicUsize,
}

impl ValkeyTelemetryState {
    /// Validate one Lua reply's telemetry suffix and publish it as a snapshot.
    fn record_reply(&self, remaining: i64, active: i64, keys: i64) -> Result<(), BackendError> {
        self.update(
            u64::try_from(remaining).map_err(|_error| BackendError::InvalidResponse)?,
            usize::try_from(active).map_err(|_error| BackendError::InvalidResponse)?,
            usize::try_from(keys).map_err(|_error| BackendError::InvalidResponse)?,
        );
        Ok(())
    }

    /// Decode a Lua `reserve` reply: admitted (`1`), budget-denied (`0`),
    /// or per-rule `max_keys` (`2`).
    fn parse_reserve_reply(&self, response: &[i64]) -> Result<BackendReserve, BackendError> {
        match response {
            [3] => Err(BackendError::ConfigurationMismatch),
            [1, id, estimate, usage_after, remaining, active, keys] => {
                self.record_reply(*remaining, *active, *keys)?;
                Ok(BackendReserve::Admitted {
                    reservation_id: u64::try_from(*id).map_err(|_error| BackendError::InvalidResponse)?,
                    estimate: u64::try_from(*estimate).map_err(|_error| BackendError::InvalidResponse)?,
                    usage_after: u64::try_from(*usage_after).map_err(|_error| BackendError::InvalidResponse)?,
                })
            },
            [0, retry_after, remaining, active, keys] => {
                self.record_reply(*remaining, *active, *keys)?;
                Ok(BackendReserve::Denied {
                    retry_after_ms: u64::try_from(*retry_after).map_err(|_error| BackendError::InvalidResponse)?,
                    reason: DenialReason::WindowCapacity,
                })
            },
            [2, retry_after, remaining, active, keys] => {
                self.record_reply(*remaining, *active, *keys)?;
                Ok(BackendReserve::Denied {
                    retry_after_ms: u64::try_from(*retry_after).map_err(|_error| BackendError::InvalidResponse)?,
                    reason: DenialReason::KeyCapacity,
                })
            },
            _ => Err(BackendError::InvalidResponse),
        }
    }

    /// Decode a Lua reconciliation reply and publish its telemetry suffix.
    fn parse_reconcile_reply(&self, response: &[i64]) -> Result<BackendSettlement, BackendError> {
        match response {
            [3] => Err(BackendError::ConfigurationMismatch),
            [0, remaining, active, keys] => {
                self.record_reply(*remaining, *active, *keys)?;
                Ok(BackendSettlement::Noop)
            },
            [1, actual, refund, overage, remaining, active, keys] => {
                let settlement = BackendSettlement::Applied {
                    actual: u64::try_from(*actual).map_err(|_error| BackendError::InvalidResponse)?,
                    refund: u64::try_from(*refund).map_err(|_error| BackendError::InvalidResponse)?,
                    overage: u64::try_from(*overage).map_err(|_error| BackendError::InvalidResponse)?,
                };
                self.record_reply(*remaining, *active, *keys)?;
                Ok(settlement)
            },
            _ => Err(BackendError::InvalidResponse),
        }
    }

    /// Replace the complete last-observed snapshot.
    fn update(&self, budget_remaining: u64, active_reservations: usize, active_keys: usize) {
        self.budget_remaining.store(budget_remaining, Ordering::Relaxed);
        self.active_reservations.store(active_reservations, Ordering::Relaxed);
        self.active_keys.store(active_keys, Ordering::Relaxed);
    }

    /// Read the last-observed values without backend I/O.
    fn snapshot(&self) -> BackendSnapshot {
        BackendSnapshot {
            budget_remaining: self.budget_remaining.load(Ordering::Relaxed),
            active_reservations: self.active_reservations.load(Ordering::Relaxed),
            active_keys: self.active_keys.load(Ordering::Relaxed),
        }
    }
}

/// Failure modes shared by every [`TokenRateLimitStateBackend`] impl.
#[derive(Debug, thiserror::Error)]
pub(super) enum BackendError {
    /// The operation failed before a mutation was dispatched, or the server
    /// explicitly rejected it before execution.
    #[error("shared quota backend unavailable: {0}")]
    Unavailable(String),
    /// A dispatched mutation failed without a response proving whether it was
    /// applied. It must never be replayed automatically.
    #[error("shared quota backend mutation outcome is unconfirmed: {0}")]
    Unconfirmed(String),
    /// The backend responded, but not in the expected shape.
    #[error("shared quota backend returned an invalid response")]
    InvalidResponse,
    /// Existing shared state belongs to a different accounting
    /// configuration for the same namespace/rule/algorithm identity.
    #[error("shared quota backend accounting configuration does not match existing state")]
    ConfigurationMismatch,
}

/// Observable fail-open classification for a failed admission.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum AdmissionFailureOutcome {
    /// The reservation is known not to have run.
    Bypassed,
    /// The reservation may have committed before the failure was observed.
    Unconfirmed,
}

impl AdmissionFailureOutcome {
    /// Stable bounded telemetry value.
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::Bypassed => "bypassed",
            Self::Unconfirmed => "unconfirmed",
        }
    }
}

impl BackendError {
    /// Stable bounded value for structured accounting records.
    pub(super) const fn kind(&self) -> &'static str {
        match self {
            Self::Unavailable(_) => "unavailable",
            Self::Unconfirmed(_) => "unconfirmed",
            Self::InvalidResponse => "invalid_response",
            Self::ConfigurationMismatch => "configuration_mismatch",
        }
    }

    /// Classify dependency failures eligible for `backend.on_failure: open`.
    /// A configuration mismatch is intentionally excluded and always fails
    /// closed. An invalid successful reply follows dispatch and is therefore
    /// conservatively unconfirmed.
    pub(super) const fn admission_failure_outcome(&self) -> Option<AdmissionFailureOutcome> {
        match self {
            Self::Unavailable(_) => Some(AdmissionFailureOutcome::Bypassed),
            Self::Unconfirmed(_) | Self::InvalidResponse => Some(AdmissionFailureOutcome::Unconfirmed),
            Self::ConfigurationMismatch => None,
        }
    }

    /// Whether retrying a reconciliation is safe because the failed attempt is
    /// known not to have mutated shared state. Ambiguous command failures and
    /// malformed successful replies are deliberately never replayed.
    const fn reconciliation_retry_is_safe(&self) -> bool {
        matches!(self, Self::Unavailable(_))
    }
}

/// Where sliding-window admission state lives: in-process or shared.
#[async_trait]
pub(super) trait TokenRateLimitStateBackend: Send + Sync {
    /// Attempt to admit `request.estimate` against `request.key`'s budget.
    async fn reserve(&self, request: ReserveRequest) -> Result<BackendReserve, BackendError>;

    /// Settle a prior reservation against actual usage, awaiting
    /// completion. Backends that reconcile out-of-band (e.g. Valkey via
    /// [`Self::enqueue_reconcile`]) still implement this for their own
    /// background worker to call.
    async fn reconcile(&self, request: ReconcileRequest) -> Result<BackendSettlement, BackendError>;

    /// Settle a prior reservation without blocking the caller.
    ///
    /// For in-process state this may just reconcile synchronously (cheap,
    /// no I/O); for a networked backend this enqueues the work onto a
    /// background worker instead, so the response is never held up on a
    /// reconciliation round-trip.
    fn enqueue_reconcile(&self, request: ReconcileRequest) -> Result<(), BackendError>;

    /// The smallest configured budget capacity, for rate-limit headers.
    fn limit(&self) -> u64;

    /// Latest rule-level state available without backend I/O.
    fn snapshot(&self) -> BackendSnapshot;

    /// Stable backend label used by bounded telemetry.
    fn backend_name(&self) -> &'static str;

    /// Stable algorithm label used by bounded telemetry.
    fn algorithm_name(&self) -> &'static str;

    /// Configured rule name. Required by background reconciliation telemetry.
    fn rule_name(&self) -> &str {
        ""
    }

    /// Attempt an in-process, synchronous settlement (no I/O, no async
    /// dispatch) for a prior reservation.
    ///
    /// Returns `None` for backends whose state isn't local (e.g. a
    /// networked Valkey backend) -- callers should fall back to
    /// [`Self::enqueue_reconcile`] in that case. Every in-process backend
    /// (regardless of algorithm) implements this itself rather than
    /// exposing its concrete state type, so the filter never needs to
    /// know which algorithm produced it.
    fn reconcile_sync(&self, _request: &ReconcileRequest) -> Option<BackendSettlement> {
        None
    }

    /// Reclaim idle/orphaned in-process state and report current gauges.
    ///
    /// Returns `None` for backends with no local state to reap (e.g.
    /// Valkey, where expiry is handled by the Lua reserve script
    /// itself) -- callers should skip gauge reporting entirely in that
    /// case rather than reporting misleading zeros.
    fn cleanup(&self, _now_ms: u64, _max_keys_to_scan: usize) -> Option<CleanupReport> {
        None
    }
}

/// In-process state snapshot after a [`TokenRateLimitStateBackend::cleanup`]
/// pass, backend-agnostic so the filter can report gauges without knowing
/// which algorithm produced them.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct CleanupReport {
    /// Reservations reaped this pass because they exceeded
    /// `reservation_timeout` without being reconciled.
    pub(super) orphaned: usize,
    /// Reservations still awaiting reconciliation.
    pub(super) active_reservations: usize,
    /// Distinct budget keys currently retained.
    pub(super) active_keys: usize,
}

/// In-process sliding-window state: one gateway instance, one budget.
pub(super) struct InMemoryTokenRateLimitBackend {
    /// The underlying exact sliding-window ledger.
    ledger: Arc<Ledger>,
}

impl InMemoryTokenRateLimitBackend {
    /// Wrap an already-constructed [`Ledger`] as a backend.
    pub(super) fn new(ledger: Ledger) -> Self {
        Self {
            ledger: Arc::new(ledger),
        }
    }
}

#[async_trait]
impl TokenRateLimitStateBackend for InMemoryTokenRateLimitBackend {
    async fn reserve(&self, request: ReserveRequest) -> Result<BackendReserve, BackendError> {
        Ok(
            match self.ledger.reserve(&request.key, request.estimate, request.now_ms) {
                Decision::Admitted(reservation) => BackendReserve::Admitted {
                    reservation_id: reservation.id,
                    estimate: reservation.estimate,
                    usage_after: reservation.usage_after,
                },
                Decision::Denied { retry_after_ms, reason } => BackendReserve::Denied { retry_after_ms, reason },
            },
        )
    }

    async fn reconcile(&self, request: ReconcileRequest) -> Result<BackendSettlement, BackendError> {
        Ok(
            match self
                .ledger
                .reconcile(request.reservation_id, request.actual, request.now_ms)
            {
                Settlement::Applied {
                    actual,
                    refund,
                    overage,
                } => BackendSettlement::Applied {
                    actual,
                    refund,
                    overage,
                },
                Settlement::Noop => BackendSettlement::Noop,
            },
        )
    }

    fn enqueue_reconcile(&self, request: ReconcileRequest) -> Result<(), BackendError> {
        let _ = self
            .ledger
            .reconcile(request.reservation_id, request.actual, request.now_ms);
        Ok(())
    }

    fn limit(&self) -> u64 {
        self.ledger.limit()
    }

    fn snapshot(&self) -> BackendSnapshot {
        BackendSnapshot {
            budget_remaining: self.ledger.remaining_total(),
            active_reservations: self.ledger.active_count(),
            active_keys: self.ledger.key_count(),
        }
    }

    fn backend_name(&self) -> &'static str {
        "memory"
    }

    fn algorithm_name(&self) -> &'static str {
        "sliding_window"
    }

    fn reconcile_sync(&self, request: &ReconcileRequest) -> Option<BackendSettlement> {
        Some(
            match self
                .ledger
                .reconcile(request.reservation_id, request.actual, request.now_ms)
            {
                Settlement::Applied {
                    actual,
                    refund,
                    overage,
                } => BackendSettlement::Applied {
                    actual,
                    refund,
                    overage,
                },
                Settlement::Noop => BackendSettlement::Noop,
            },
        )
    }

    fn cleanup(&self, now_ms: u64, max_keys_to_scan: usize) -> Option<CleanupReport> {
        Some(CleanupReport {
            orphaned: self.ledger.cleanup(now_ms, max_keys_to_scan),
            active_reservations: self.ledger.active_count(),
            active_keys: self.ledger.key_count(),
        })
    }
}

/// In-process token-bucket state: one gateway instance, one budget,
/// continuously refilled rather than admitted against a trailing window.
pub(super) struct InMemoryTokenBucketBackend {
    /// The underlying exact token-bucket ledger.
    ledger: Arc<TokenBucketLedger>,
}

impl InMemoryTokenBucketBackend {
    /// Wrap an already-constructed [`TokenBucketLedger`] as a backend.
    pub(super) fn new(ledger: TokenBucketLedger) -> Self {
        Self {
            ledger: Arc::new(ledger),
        }
    }

    /// Shared reconcile path for `reconcile`/`enqueue_reconcile`/`reconcile_sync`.
    fn reconcile_ledger(&self, request: &ReconcileRequest) -> BackendSettlement {
        match self
            .ledger
            .reconcile(request.reservation_id, request.actual, request.now_ms)
        {
            token_bucket_ledger::Settlement::Applied {
                actual,
                refund,
                overage,
            } => BackendSettlement::Applied {
                actual,
                refund,
                overage,
            },
            token_bucket_ledger::Settlement::Noop => BackendSettlement::Noop,
        }
    }
}

#[async_trait]
impl TokenRateLimitStateBackend for InMemoryTokenBucketBackend {
    async fn reserve(&self, request: ReserveRequest) -> Result<BackendReserve, BackendError> {
        Ok(
            match self.ledger.reserve(&request.key, request.estimate, request.now_ms) {
                token_bucket_ledger::Decision::Admitted(reservation) => BackendReserve::Admitted {
                    reservation_id: reservation.id,
                    estimate: reservation.estimate,
                    usage_after: reservation.usage_after,
                },
                token_bucket_ledger::Decision::Denied { retry_after_ms, reason } => {
                    BackendReserve::Denied { retry_after_ms, reason }
                },
            },
        )
    }

    async fn reconcile(&self, request: ReconcileRequest) -> Result<BackendSettlement, BackendError> {
        Ok(self.reconcile_ledger(&request))
    }

    fn enqueue_reconcile(&self, request: ReconcileRequest) -> Result<(), BackendError> {
        let _ = self.reconcile_ledger(&request);
        Ok(())
    }

    fn limit(&self) -> u64 {
        self.ledger.limit()
    }

    fn snapshot(&self) -> BackendSnapshot {
        BackendSnapshot {
            budget_remaining: self.ledger.remaining_total(),
            active_reservations: self.ledger.active_count(),
            active_keys: self.ledger.key_count(),
        }
    }

    fn backend_name(&self) -> &'static str {
        "memory"
    }

    fn algorithm_name(&self) -> &'static str {
        "token_bucket"
    }

    fn reconcile_sync(&self, request: &ReconcileRequest) -> Option<BackendSettlement> {
        Some(self.reconcile_ledger(request))
    }

    fn cleanup(&self, now_ms: u64, max_keys_to_scan: usize) -> Option<CleanupReport> {
        Some(CleanupReport {
            orphaned: self.ledger.cleanup(now_ms, max_keys_to_scan),
            active_reservations: self.ledger.active_count(),
            active_keys: self.ledger.key_count(),
        })
    }
}

/// Atomically admit a reservation against every configured budget for one
/// key, or deny it -- the Valkey/Lua analog of [`Ledger::reserve`].
///
/// `KEYS`: `[1]` physical key, `[2]` settled zset, `[3]` active hash,
/// `[4]` namespace keys zset, `[5]` namespace active-count string,
/// `[6]` namespace reservation-id sequence, `[7]` namespace active-index
/// zset (global reservation-expiry tracking), followed by five rule-level
/// telemetry keys: active count/index, retained keys, per-key balances,
/// and aggregate remaining balance, then `[13]` the rule's persistent
/// accounting-configuration fingerprint. `ARGV`: reservation timeout (ms),
/// max keys, max active reservations, estimate, budget count,
/// `(window_ms, capacity)` pairs, then the expected fingerprint. Returns
/// `[1, id, estimate, usage_after, remaining, active, keys]` on admission,
/// `[0, retry_after_ms, remaining, active, keys]` on budget denial, or
/// `[2, retry_after_ms, remaining, active, keys]` when the per-rule
/// `max_keys` cap would be exceeded. `[3]` is an accounting-configuration
/// mismatch and is mapped to a fail-closed backend error.
const RESERVE_SCRIPT: &str = include_str!("lua/sliding_window_reserve.lua");

/// Atomically settle a prior reservation against actual usage -- the
/// Valkey/Lua analog of [`Ledger::reconcile`].
///
/// `KEYS`: same layout as [`RESERVE_SCRIPT`]. `ARGV`: `[1]` reservation
/// ID, `[2]` actual usage, `[3]` budget count, `[4]` reservation timeout,
/// `(window_ms, capacity)` pairs, then the expected fingerprint. Returns
/// `[0, remaining, active, keys]` if the reservation was already
/// reconciled/expired (no-op), `[1, actual, refund, overage, remaining,
/// active, keys]` when applied, or `[3]` on configuration mismatch.
const RECONCILE_SCRIPT: &str = include_str!("lua/sliding_window_reconcile.lua");

/// Drain `receiver`, reconciling each request against `worker`'s backend
/// with bounded retries, off the request/response path entirely.
///
/// Generic over any [`TokenRateLimitStateBackend`] (sliding-window,
/// token-bucket, or any future Valkey-backed algorithm) -- the retry/
/// audit behavior is identical regardless of which algorithm's Lua
/// script `worker.reconcile` ultimately calls.
///
/// A dropped/failed reconciliation after retries is intentionally *not*
/// escalated back to the request that triggered it (that response has
/// already been sent) -- it's counted and logged so operators can audit
/// it, and the reservation still expires and gets conservatively charged
/// via `reservation_timeout` regardless.
async fn run_reconcile_worker<B>(worker: B, mut receiver: mpsc::Receiver<ReconcileRequest>)
where
    B: TokenRateLimitStateBackend + 'static,
{
    while let Some(request) = receiver.recv().await {
        let mut attempts = 0;
        loop {
            match worker.reconcile(request.clone()).await {
                Ok(settlement) => {
                    record_completed_reconciliation(&worker, &settlement);
                    break;
                },
                Err(error) if error.reconciliation_retry_is_safe() && attempts < 2 => {
                    attempts += 1;
                    tracing::warn!(attempts, %error, "token-rate-limit reconciliation retry");
                    tokio::time::sleep(Duration::from_millis(25 * attempts)).await;
                },
                Err(error) => {
                    record_abandoned_reconciliation(&worker, &error);
                    break;
                },
            }
        }
    }
}

/// Publish one completed Valkey reconciliation through the same metrics and
/// accounting helpers as the synchronous in-memory path.
fn record_completed_reconciliation(backend: &impl TokenRateLimitStateBackend, settlement: &BackendSettlement) {
    counter!(
        "praxis_trl_backend_reconciliation_total",
        "backend" => backend.backend_name(),
        "result" => "completed",
        "rule" => backend.rule_name().to_owned(),
    )
    .increment(1);
    super::record_settlement_metrics(backend.rule_name(), settlement);
    super::record_accounting_settlement(backend.rule_name(), backend, settlement);
    super::record_state_metrics(backend.rule_name(), backend);
}

/// Count and log one reconciliation given up after its retries; the
/// reservation still expires and is charged at its estimate.
fn record_abandoned_reconciliation(backend: &impl TokenRateLimitStateBackend, error: &BackendError) {
    super::record_backend_error_metric(backend.rule_name(), backend.backend_name());
    tracing::warn!(
        target: "praxis_ai::token_rate_limit::accounting",
        phase = "reconciliation",
        rule = backend.rule_name(),
        algorithm = backend.algorithm_name(),
        backend = backend.backend_name(),
        result = "failed",
        error = error.kind(),
        "token rate limit accounting"
    );
    tracing::error!(%error, "token-rate-limit reconciliation abandoned");
}

/// Validated bounds for shared-backend network work.
#[derive(Debug, Clone, Copy)]
pub(super) struct ValkeyTimeouts {
    /// Data-node connection attempt.
    pub(super) connect: Duration,
    /// One command response after dispatch.
    pub(super) operation: Duration,
    /// Complete Sentinel discovery and connection loop.
    pub(super) discovery: Duration,
}

impl Default for ValkeyTimeouts {
    fn default() -> Self {
        Self {
            connect: DEFAULT_CONNECT_TIMEOUT,
            operation: DEFAULT_OPERATION_TIMEOUT,
            discovery: DEFAULT_DISCOVERY_TIMEOUT,
        }
    }
}

/// Standalone client or immutable Sentinel discovery inputs.
enum ValkeyTopology {
    /// One fixed data-node URL.
    Standalone(Box<redis::Client>),
    /// Multiple Sentinel URLs and one monitored primary service.
    Sentinel {
        /// Ordered Sentinel connection URLs.
        endpoints: Vec<String>,
        /// Monitored writable-primary service name.
        service_name: String,
    },
}

impl ValkeyTopology {
    /// Stable, bounded telemetry label.
    const fn label(&self) -> &'static str {
        match self {
            Self::Standalone(_) => "standalone",
            Self::Sentinel { .. } => "sentinel",
        }
    }
}

/// One cached primary connection and its monotonic identity. The generation
/// prevents a late error from an old socket from evicting a freshly discovered
/// primary installed by another concurrent request.
struct CachedConnection {
    /// Multiplexed writable-primary connection.
    connection: MultiplexedConnection,
    /// Identity used for conditional invalidation.
    generation: u64,
}

/// Shared cache state. Holding the mutex while establishing a connection
/// intentionally coalesces concurrent initial discovery and rediscovery.
#[derive(Default)]
struct ConnectionCache {
    /// Currently reusable connection, if one has been established.
    current: Option<CachedConnection>,
    /// Generation assigned to the next successful connection.
    next_generation: u64,
}

/// A cheap handle returned to one operation.
struct AcquiredConnection {
    /// Cheap clone of the cached multiplexed handle.
    connection: MultiplexedConnection,
    /// Generation observed with the handle.
    generation: u64,
}

/// Shared Redis/Valkey connection handling for every networked algorithm.
/// Healthy operations reuse one cached multiplexed writable-primary
/// connection, so Sentinel adds no steady-state command or network round trip.
/// A failed dispatched mutation is never replayed: the stale generation is
/// invalidated and only a later operation performs bounded rediscovery.
#[derive(Clone)]
pub(super) struct ValkeyEval {
    /// Configured equivalent selector (`redis` or `valkey`) for bounded
    /// operator-facing telemetry.
    backend_name: &'static str,
    /// Fixed standalone target or Sentinel discovery inputs.
    topology: Arc<ValkeyTopology>,
    /// Validated bounds for all network work.
    timeouts: ValkeyTimeouts,
    /// Shared, rediscovery-coalescing writable-primary cache.
    connection: Arc<tokio::sync::Mutex<ConnectionCache>>,
}

impl ValkeyEval {
    /// Open a backward-compatible standalone client with the historic 500ms
    /// connection and operation bounds.
    ///
    /// # Errors
    ///
    /// Returns [`BackendError::Unavailable`] if `url` is malformed.
    #[cfg(test)]
    pub(super) fn new(url: String) -> Result<Self, BackendError> {
        Self::standalone(url, ValkeyTimeouts::default(), "valkey")
    }

    /// Open a lazy standalone client with validated custom timeouts.
    pub(super) fn standalone(
        url: String,
        timeouts: ValkeyTimeouts,
        backend_name: &'static str,
    ) -> Result<Self, BackendError> {
        let client = redis::Client::open(url)
            .map_err(|error| BackendError::Unavailable(format!("Redis/Valkey configuration: {error}")))?;
        Ok(Self {
            backend_name,
            topology: Arc::new(ValkeyTopology::Standalone(Box::new(client))),
            timeouts,
            connection: Arc::new(tokio::sync::Mutex::new(ConnectionCache::default())),
        })
    }

    /// Build a lazy Sentinel client. Construction validates endpoint URLs;
    /// network discovery remains deferred until the first operation.
    pub(super) fn sentinel(
        endpoints: Vec<String>,
        service_name: String,
        timeouts: ValkeyTimeouts,
        backend_name: &'static str,
    ) -> Result<Self, BackendError> {
        SentinelClient::build(
            endpoints.clone(),
            service_name.clone(),
            None,
            SentinelServerType::Master,
        )
        .map_err(|error| BackendError::Unavailable(format!("Redis/Valkey Sentinel configuration: {error}")))?;
        Ok(Self {
            backend_name,
            topology: Arc::new(ValkeyTopology::Sentinel {
                endpoints,
                service_name,
            }),
            timeouts,
            connection: Arc::new(tokio::sync::Mutex::new(ConnectionCache::default())),
        })
    }

    /// `redis-rs` bounds a direct data-node connect and every later command
    /// response with these values. Sentinel traversal is additionally wrapped
    /// by the one overall discovery deadline.
    fn connection_config(&self) -> redis::AsyncConnectionConfig {
        redis::AsyncConnectionConfig::new()
            .set_connection_timeout(Some(self.timeouts.connect))
            .set_response_timeout(Some(self.timeouts.operation))
    }

    /// Establish a standalone connection or perform one complete bounded
    /// Sentinel discovery loop. Sentinel attempts are read-only and may be
    /// retried; no mutation is ever issued here.
    async fn connect(&self, deadline: Instant) -> Result<MultiplexedConnection, BackendError> {
        match self.topology.as_ref() {
            ValkeyTopology::Standalone(client) => {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    return Err(BackendError::Unavailable(
                        "Redis/Valkey connection deadline elapsed before connection establishment".into(),
                    ));
                }
                tokio::time::timeout(
                    remaining,
                    client.get_multiplexed_async_connection_with_config(&self.connection_config()),
                )
                .await
                .map_err(|_elapsed| BackendError::Unavailable("Redis/Valkey connection deadline elapsed".into()))?
                .map_err(|error| map_valkey_error("connection", &error))
            },
            ValkeyTopology::Sentinel {
                endpoints,
                service_name,
            } => Box::pin(self.connect_sentinel(endpoints, service_name, deadline)).await,
        }
    }

    /// Retry read-only Sentinel traversal and primary connection setup within
    /// one overall deadline. Rebuilding the client on each attempt avoids
    /// carrying a stale internal Sentinel cache across failover convergence.
    #[expect(
        clippy::too_many_lines,
        reason = "one bounded retry loop owns attempt timing, telemetry, and final classification"
    )]
    async fn connect_sentinel(
        &self,
        endpoints: &[String],
        service_name: &str,
        deadline: Instant,
    ) -> Result<MultiplexedConnection, BackendError> {
        let mut attempts = 0_u64;
        let mut last_error = None;
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }
            attempts = attempts.saturating_add(1);
            let attempt = Box::pin(async {
                // `SentinelClient::build` consumes its endpoint list. These are
                // immutable configuration strings, not request/payload data.
                let mut client =
                    SentinelClient::build(endpoints.to_vec(), service_name, None, SentinelServerType::Master)?;
                client.get_async_connection_with_config(&self.connection_config()).await
            });
            match tokio::time::timeout(remaining, attempt).await {
                Ok(Ok(connection)) => {
                    record_primary_discovery_attempt(self.backend_name, "success");
                    return Ok(connection);
                },
                Ok(Err(error)) => {
                    record_primary_discovery_attempt(self.backend_name, "retry");
                    last_error = Some(error);
                },
                Err(_elapsed) => {
                    record_primary_discovery_attempt(self.backend_name, "timeout");
                    break;
                },
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if !remaining.is_zero() {
                tokio::time::sleep(SENTINEL_RETRY_INTERVAL.min(remaining)).await;
            }
        }
        let detail = last_error.map_or_else(|| "deadline elapsed".to_owned(), |error| error.to_string());
        Err(BackendError::Unavailable(format!(
            "Redis/Valkey Sentinel discovery exceeded {:?} after {attempts} attempts: {detail}",
            self.timeouts.discovery
        )))
    }

    /// Return the cached writable-primary connection or coalesce one bounded
    /// connection/discovery attempt while holding the shared cache mutex.
    #[expect(
        clippy::too_many_lines,
        reason = "cache hit, coalesced connection, telemetry, and generation install are one atomic flow"
    )]
    async fn acquired_connection(&self) -> Result<AcquiredConnection, BackendError> {
        let budget = match self.topology.as_ref() {
            ValkeyTopology::Standalone(_) => self.timeouts.connect,
            ValkeyTopology::Sentinel { .. } => self.timeouts.discovery,
        };
        let deadline = Instant::now()
            .checked_add(budget)
            .ok_or_else(|| BackendError::Unavailable("Redis/Valkey connection deadline overflow".into()))?;
        let mut cache = tokio::time::timeout(budget, self.connection.lock())
            .await
            .map_err(|_elapsed| {
                BackendError::Unavailable(format!(
                    "Redis/Valkey {} connection cache wait exceeded {budget:?}",
                    self.topology.label()
                ))
            })?;
        if let Some(cached) = cache.current.as_ref() {
            return Ok(AcquiredConnection {
                connection: cached.connection.clone(),
                generation: cached.generation,
            });
        }
        let rediscovery = cache.next_generation > 0;
        let phase = if rediscovery { "rediscovery" } else { "initial" };
        let result = self.connect(deadline).await;
        record_backend_connection(
            self.backend_name,
            self.topology.label(),
            phase,
            if result.is_ok() { "success" } else { "failed" },
        );
        if rediscovery && matches!(self.topology.as_ref(), ValkeyTopology::Sentinel { .. }) {
            let result_label = if result.is_ok() { "success" } else { "failed" };
            record_primary_rediscovery(self.backend_name, result_label);
            if result.is_ok() {
                tracing::info!(
                    target: "praxis_ai::token_rate_limit::backend",
                    backend = self.backend_name,
                    topology = "sentinel",
                    phase = "rediscovery",
                    result = result_label,
                    "token rate limit backend connection"
                );
            } else {
                tracing::warn!(
                    target: "praxis_ai::token_rate_limit::backend",
                    backend = self.backend_name,
                    topology = "sentinel",
                    phase = "rediscovery",
                    result = result_label,
                    "token rate limit backend connection"
                );
            }
        }
        let connection = result?;
        let generation = cache.next_generation;
        cache.next_generation = cache.next_generation.saturating_add(1);
        cache.current = Some(CachedConnection {
            connection: connection.clone(),
            generation,
        });
        let acquired = AcquiredConnection { connection, generation };
        drop(cache);
        Ok(acquired)
    }

    /// Expose a connection handle to this module's live integration tests.
    /// Production mutations use [`Self::acquired_connection`] so they retain
    /// the generation needed for race-safe invalidation.
    #[cfg(test)]
    async fn connection(&self) -> Result<MultiplexedConnection, BackendError> {
        Ok(self.acquired_connection().await?.connection)
    }

    /// Invalidate only the generation that failed. A late error from an old
    /// connection must not evict a newer primary installed concurrently.
    async fn invalidate(&self, generation: u64) {
        let mut cache = self.connection.lock().await;
        let invalidated = cache.current.as_ref().map(|cached| cached.generation) == Some(generation);
        if invalidated {
            cache.current = None;
        }
        drop(cache);
        if invalidated {
            record_connection_invalidation(self.backend_name, self.topology.label());
            tracing::warn!(
                target: "praxis_ai::token_rate_limit::backend",
                backend = self.backend_name,
                topology = self.topology.label(),
                phase = "connection",
                result = "invalidated",
                "token rate limit backend connection"
            );
        }
    }

    /// Dispatch one mutation exactly once against the cached primary. Any
    /// command error invalidates that generation for the next request, but
    /// this call never redispatches the script because a timeout/disconnect is
    /// ambiguous.
    async fn dispatch<const N: usize>(
        &self,
        script: &str,
        keys: &[String; N],
        args: &[String],
    ) -> Result<(Vec<i64>, u64), BackendError> {
        let mut command = redis::cmd("EVAL");
        command.arg(script).arg(keys.len());
        for key in keys {
            command.arg(key);
        }
        for arg in args {
            command.arg(arg);
        }
        let mut acquired = self.acquired_connection().await?;
        let result: redis::RedisResult<Vec<i64>> = command.query_async(&mut acquired.connection).await;
        match result {
            Ok(value) => Ok((value, acquired.generation)),
            Err(error) => {
                self.invalidate(acquired.generation).await;
                Err(map_valkey_error("command", &error))
            },
        }
    }

    /// Dispatch and parse one mutation reply. A malformed successful reply is
    /// unconfirmed, so invalidate the exact connection generation just as for
    /// an ambiguous transport failure.
    async fn eval_parsed<const N: usize, T>(
        &self,
        script: &str,
        keys: &[String; N],
        args: &[String],
        parse: impl FnOnce(&[i64]) -> Result<T, BackendError>,
    ) -> Result<T, BackendError> {
        let (response, generation) = self.dispatch(script, keys, args).await?;
        let parsed = parse(&response);
        if matches!(parsed, Err(BackendError::InvalidResponse)) {
            self.invalidate(generation).await;
        }
        parsed
    }

    /// Raw test adapter for fault injection and topology qualification.
    #[cfg(test)]
    async fn eval<const N: usize>(
        &self,
        script: &str,
        keys: &[String; N],
        args: &[String],
    ) -> Result<Vec<i64>, BackendError> {
        self.dispatch(script, keys, args)
            .await
            .map(|(response, _generation)| response)
    }
}

/// Server errors that prove a mutation was rejected before execution. All
/// transport failures and other script errors remain conservatively
/// unconfirmed because Lua errors can occur after partial state mutation.
fn command_known_not_applied(error: &redis::RedisError) -> bool {
    matches!(error.code(), Some("NOREPLICAS" | "READONLY" | "MASTERDOWN"))
}

/// Preserve whether a failure occurred before or after mutation dispatch so
/// `on_failure: open` can distinguish bypassed from unconfirmed admission.
fn map_valkey_error(phase: &'static str, error: &redis::RedisError) -> BackendError {
    let message = format!("Redis/Valkey {phase}: {error}");
    if phase == "connection" || command_known_not_applied(error) {
        BackendError::Unavailable(message)
    } else {
        BackendError::Unconfirmed(message)
    }
}

/// Shared background-reconciliation scaffolding for every Valkey-backed
/// algorithm: a queue plus a spawn-at-most-once guard for
/// [`run_reconcile_worker`].
struct ReconcileWorker {
    /// Sending half of the reconciliation queue; cloned into the worker.
    tx: mpsc::Sender<ReconcileRequest>,
    /// Receiving half, taken exactly once by [`Self::start`].
    rx: Mutex<Option<mpsc::Receiver<ReconcileRequest>>>,
    /// Ensures the background worker is spawned at most once.
    started: OnceLock<()>,
}

impl ReconcileWorker {
    /// A live worker: holds a real receiver, ready for [`Self::start`].
    fn new() -> Self {
        let (tx, rx) = mpsc::channel(1024);
        Self {
            tx,
            rx: Mutex::new(Some(rx)),
            started: OnceLock::new(),
        }
    }

    /// A throwaway (never-sent-to, never-started) worker -- used only
    /// when cloning a backend to hand the *real* background worker its
    /// own handle to `reserve`/`reconcile`, without that clone holding
    /// the real sender (which would keep the channel open forever) or
    /// being able to spawn a second worker.
    fn detached() -> Self {
        let (tx, _rx) = mpsc::channel(1);
        Self {
            tx,
            rx: Mutex::new(None),
            started: OnceLock::new(),
        }
    }

    /// Enqueue a reconciliation request for the background worker.
    ///
    /// # Errors
    ///
    /// Returns [`BackendError::Unavailable`] if the queue is full or the
    /// worker has stopped.
    fn enqueue(&self, request: ReconcileRequest) -> Result<(), BackendError> {
        self.tx
            .try_send(request)
            .map_err(|error| BackendError::Unavailable(format!("reconciliation queue is full or stopped: {error}")))
    }

    /// Lazily spawn [`run_reconcile_worker`] on `runtime`, at most once.
    /// `make_worker` builds the backend clone the worker itself will
    /// call `reconcile` against (see [`Self::detached`]).
    fn start<B>(&self, runtime: &tokio::runtime::Handle, make_worker: impl FnOnce() -> B)
    where
        B: TokenRateLimitStateBackend + 'static,
    {
        self.started.get_or_init(|| {
            let Some(receiver) = self.rx.lock().ok().and_then(|mut guard| guard.take()) else {
                return;
            };
            runtime.spawn(run_reconcile_worker(make_worker(), receiver));
        });
    }
}

/// Valkey/Redis-backed sliding-window state, shared across every gateway
/// instance/replica pointed at the same `url`/`namespace`.
///
/// Admission (`reserve`) is synchronous with the request (an EVAL round-
/// trip); reconciliation (`enqueue_reconcile`) is deferred to a
/// background worker so it never adds latency to the response path.
pub(super) struct ValkeyTokenRateLimitBackend {
    /// Shared connection/EVAL handling, see [`ValkeyEval`].
    valkey: ValkeyEval,
    /// Key namespace prefix, see [`ValkeyBackendConfig::namespace`].
    namespace: String,
    /// Rule identifier, see [`ValkeyBackendConfig::rule`].
    rule: String,
    /// Sliding-window budgets enforced atomically per key.
    budgets: Vec<Budget>,
    /// See [`ValkeyBackendConfig::reservation_timeout_ms`].
    reservation_timeout_ms: u64,
    /// See [`ValkeyBackendConfig::max_keys`].
    max_keys: usize,
    /// See [`ValkeyBackendConfig::max_active_reservations`].
    max_active_reservations: usize,
    /// Schema-versioned digest of every setting that interprets or bounds
    /// this rule's shared accounting state.
    config_fingerprint: String,
    /// Smallest configured budget capacity, for rate-limit headers.
    limit: u64,
    /// Shared background-reconciliation scaffolding, see [`ReconcileWorker`].
    worker: ReconcileWorker,
    /// Last state returned by this rule's Lua operations, shared with its worker clone.
    telemetry: Arc<ValkeyTelemetryState>,
}

/// Construction parameters for [`ValkeyTokenRateLimitBackend`].
pub(super) struct ValkeyBackendConfig {
    /// Filter-level Valkey connection, shared (`Clone`d) across every
    /// Valkey-backed rule -- see [`ValkeyEval`]'s doc comment.
    pub(super) valkey: ValkeyEval,
    /// Key namespace prefix, isolating this rule's state from any other
    /// rule/deployment sharing the same Valkey instance.
    pub(super) namespace: String,
    /// Rule identifier, folded into the per-key hash alongside `namespace`.
    pub(super) rule: String,
    /// Sliding-window budgets enforced atomically per key.
    pub(super) budgets: Vec<Budget>,
    /// Time after which an ambiguous (never-reconciled) reservation is
    /// charged at its estimate, mirroring the in-memory ledger's own
    /// field of the same name.
    pub(super) reservation_timeout_ms: u64,
    /// Maximum distinct keys retained per rule.
    pub(super) max_keys: usize,
    /// Maximum reservations awaiting reconciliation across all keys in
    /// this namespace.
    pub(super) max_active_reservations: usize,
}

impl ValkeyTokenRateLimitBackend {
    /// Build this rule's backend from a filter-shared [`ValkeyEval`] client and
    /// lazy connection cache.
    pub(super) fn new(config: ValkeyBackendConfig) -> Self {
        let limit = config.budgets.iter().map(|budget| budget.capacity).min().unwrap_or(0);
        let config_fingerprint = sliding_window_config_fingerprint(
            &config.budgets,
            config.reservation_timeout_ms,
            config.max_keys,
            config.max_active_reservations,
        );
        Self {
            valkey: config.valkey,
            namespace: config.namespace,
            rule: config.rule,
            budgets: config.budgets,
            reservation_timeout_ms: config.reservation_timeout_ms,
            max_keys: config.max_keys,
            max_active_reservations: config.max_active_reservations,
            config_fingerprint,
            limit,
            worker: ReconcileWorker::new(),
            telemetry: Arc::new(ValkeyTelemetryState::default()),
        }
    }

    /// Clone this backend's connection/config, but with a detached
    /// [`ReconcileWorker`] -- used only to hand the background worker its
    /// own handle to `reserve`/`reconcile` (see [`ReconcileWorker::detached`]).
    fn clone_without_sender(&self) -> Self {
        Self {
            valkey: self.valkey.clone(),
            namespace: self.namespace.clone(),
            rule: self.rule.clone(),
            budgets: self.budgets.clone(),
            reservation_timeout_ms: self.reservation_timeout_ms,
            max_keys: self.max_keys,
            max_active_reservations: self.max_active_reservations,
            config_fingerprint: self.config_fingerprint.clone(),
            limit: self.limit,
            worker: ReconcileWorker::detached(),
            telemetry: Arc::clone(&self.telemetry),
        }
    }

    /// Lazily spawn the background reconciliation worker on the calling
    /// Tokio runtime, at most once per backend instance.
    ///
    /// # Errors
    ///
    /// Returns [`BackendError::Unavailable`] if called outside a Tokio
    /// runtime context.
    fn start_worker(&self) -> Result<(), BackendError> {
        let runtime = tokio::runtime::Handle::try_current()
            .map_err(|_error| BackendError::Unavailable("Valkey reconciliation requires a Tokio runtime".into()))?;
        self.worker.start(&runtime, || self.clone_without_sender());
        Ok(())
    }

    /// Deterministic per-key Valkey key names for this rule/namespace.
    #[expect(
        clippy::too_many_lines,
        reason = "the key layout is kept in one place so Lua KEYS indexes remain auditable"
    )]
    fn key_parts(&self, key: &str) -> [String; 13] {
        let mut rule_digest = Sha256::new();
        rule_digest.update(self.namespace.as_bytes());
        rule_digest.update(&[0]);
        rule_digest.update(self.rule.as_bytes());
        let rule_hash = rule_digest
            .finish()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        let rule_prefix = format!("{}:v1:rule:{rule_hash}", self.namespace);
        let mut digest = Sha256::new();
        digest.update(self.namespace.as_bytes());
        digest.update(&[0]);
        digest.update(self.rule.as_bytes());
        digest.update(&[0]);
        digest.update(key.as_bytes());
        let hash = digest
            .finish()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        let prefix = format!("{}:v1:{}", self.namespace, hash);
        [
            prefix.clone(),
            format!("{prefix}:settled"),
            format!("{prefix}:active"),
            format!("{}:keys", self.namespace),
            format!("{}:active-count", self.namespace),
            format!("{}:reservation-seq", self.namespace),
            format!("{}:active-index", self.namespace),
            format!("{rule_prefix}:active-count"),
            format!("{rule_prefix}:active-index"),
            format!("{rule_prefix}:keys"),
            format!("{rule_prefix}:balances"),
            format!("{rule_prefix}:remaining-total"),
            format!("{rule_prefix}:accounting-config"),
        ]
    }

    /// Arguments for [`RESERVE_SCRIPT`]: timeout/bounds, then one
    /// `(window_ms, capacity)` pair per configured budget and the expected
    /// accounting fingerprint.
    fn reserve_args(&self, request: &ReserveRequest) -> Vec<String> {
        let mut args = vec![
            self.reservation_timeout_ms.to_string(),
            self.max_keys.to_string(),
            self.max_active_reservations.to_string(),
            request.estimate.to_string(),
            self.budgets.len().to_string(),
        ];
        for budget in &self.budgets {
            args.push(budget.window_ms.to_string());
            args.push(budget.capacity.to_string());
        }
        args.push(self.config_fingerprint.clone());
        args
    }
}

#[async_trait]
impl TokenRateLimitStateBackend for ValkeyTokenRateLimitBackend {
    async fn reserve(&self, request: ReserveRequest) -> Result<BackendReserve, BackendError> {
        let keys = self.key_parts(&request.key);
        let args = self.reserve_args(&request);
        self.valkey
            .eval_parsed(RESERVE_SCRIPT, &keys, &args, |response| {
                self.telemetry.parse_reserve_reply(response)
            })
            .await
    }

    async fn reconcile(&self, request: ReconcileRequest) -> Result<BackendSettlement, BackendError> {
        let keys = self.key_parts(&request.key);
        let actual = request.actual.unwrap_or(request.estimate);
        let mut args = vec![
            request.reservation_id.to_string(),
            actual.to_string(),
            self.budgets.len().to_string(),
            self.reservation_timeout_ms.to_string(),
        ];
        for budget in &self.budgets {
            args.push(budget.window_ms.to_string());
            args.push(budget.capacity.to_string());
        }
        args.push(self.config_fingerprint.clone());
        self.valkey
            .eval_parsed(RECONCILE_SCRIPT, &keys, &args, |response| {
                self.telemetry.parse_reconcile_reply(response)
            })
            .await
    }

    fn enqueue_reconcile(&self, request: ReconcileRequest) -> Result<(), BackendError> {
        self.start_worker()?;
        self.worker.enqueue(request)
    }

    fn limit(&self) -> u64 {
        self.limit
    }

    fn snapshot(&self) -> BackendSnapshot {
        self.telemetry.snapshot()
    }

    fn backend_name(&self) -> &'static str {
        self.valkey.backend_name
    }

    fn algorithm_name(&self) -> &'static str {
        "sliding_window"
    }

    fn rule_name(&self) -> &str {
        &self.rule
    }
}

// -----------------------------------------------------------------------------
// Valkey-backed token bucket
// -----------------------------------------------------------------------------

/// Atomically admit a reservation against one key's token bucket, or deny
/// it -- the Valkey/Lua analog of [`TokenBucketLedger::reserve`].
///
/// `KEYS`: `[1]` physical state hash (`tokens`, `last_refill_ms`), `[2]`
/// active hash, `[3]` namespace keys zset, `[4]` namespace active-count
/// string, `[5]` namespace reservation-id sequence, `[6]` namespace
/// active-index zset, followed by the equivalent five rule-level telemetry
/// keys, then `[12]` the rule's persistent accounting-configuration
/// fingerprint. Deliberately namespaced with a `:tb:` segment
/// distinct from [`RESERVE_SCRIPT`]'s sliding-window keys (see
/// [`ValkeyTokenBucketBackend::key_parts`]) so a `token_bucket` rule and
/// a `sliding_window` rule can safely share one `namespace:` without
/// either algorithm's bookkeeping corrupting the other's. `ARGV`: `[1]`
/// capacity, `[2]` `refill_rate` (tokens/sec), `[3]` reservation timeout
/// (ms), `[4]` max keys, `[5]` max active reservations, `[6]` estimate,
/// `[7]` expected fingerprint.
/// Returns `[1, id, estimate, usage_after, remaining, active, keys]` on admission,
/// `[0, retry_after_ms, remaining, active, keys]` on budget denial, or
/// `[2, retry_after_ms, remaining, active, keys]` when the per-rule
/// `max_keys` cap would be exceeded. `[3]` is an accounting-configuration
/// mismatch and is mapped to a fail-closed backend error.
pub(super) const TOKEN_BUCKET_RESERVE_SCRIPT: &str = include_str!("lua/token_bucket_reserve.lua");

/// Atomically settle a prior token-bucket reservation against actual
/// usage -- the Valkey/Lua analog of [`TokenBucketLedger::reconcile`].
///
/// `KEYS`: same layout as [`TOKEN_BUCKET_RESERVE_SCRIPT`]. `ARGV`: `[1]`
/// reservation ID, `[2]` actual usage, `[3]` capacity, `[4]` `refill_rate`,
/// `[5]` reservation timeout, `[6]` expected fingerprint.
/// Returns `[0, remaining, active, keys]` if the reservation was already
/// reconciled/expired (no-op), or
/// `[1, actual, refund, overage, remaining, active, keys]`, or `[3]` on
/// configuration mismatch.
const TOKEN_BUCKET_RECONCILE_SCRIPT: &str = include_str!("lua/token_bucket_reconcile.lua");

/// Valkey/Redis-backed token-bucket state, shared across every gateway
/// instance/replica pointed at the same `url`/`namespace`.
pub(super) struct ValkeyTokenBucketBackend {
    /// Shared connection/EVAL handling, see [`ValkeyEval`].
    valkey: ValkeyEval,
    /// Key namespace prefix, see [`ValkeyBackendConfig::namespace`].
    namespace: String,
    /// Rule identifier, see [`ValkeyBackendConfig::rule`].
    rule: String,
    /// Maximum tokens held at once.
    capacity: u64,
    /// Tokens refilled per second, up to `capacity`.
    refill_rate: f64,
    /// See [`ValkeyBackendConfig::reservation_timeout_ms`].
    reservation_timeout_ms: u64,
    /// See [`ValkeyBackendConfig::max_keys`].
    max_keys: usize,
    /// See [`ValkeyBackendConfig::max_active_reservations`].
    max_active_reservations: usize,
    /// Schema-versioned digest of every setting that interprets or bounds
    /// this rule's shared accounting state.
    config_fingerprint: String,
    /// Shared background-reconciliation scaffolding, see [`ReconcileWorker`].
    worker: ReconcileWorker,
    /// Last state returned by this rule's Lua operations, shared with its worker clone.
    telemetry: Arc<ValkeyTelemetryState>,
}

/// Construction parameters for [`ValkeyTokenBucketBackend`].
pub(super) struct ValkeyTokenBucketConfig {
    /// Filter-level Valkey connection, shared (`Clone`d) across every
    /// Valkey-backed rule -- see [`ValkeyEval`]'s doc comment.
    pub(super) valkey: ValkeyEval,
    /// Key namespace prefix, isolating this rule's state from any other
    /// rule/deployment sharing the same Valkey instance.
    pub(super) namespace: String,
    /// Rule identifier, folded into the per-key hash alongside `namespace`.
    pub(super) rule: String,
    /// Maximum tokens held at once.
    pub(super) capacity: u64,
    /// Tokens refilled per second, up to `capacity`.
    pub(super) refill_rate: f64,
    /// Time after which an ambiguous (never-reconciled) reservation
    /// stops being tracked as active (it's already charged).
    pub(super) reservation_timeout_ms: u64,
    /// Maximum distinct keys retained per rule.
    pub(super) max_keys: usize,
    /// Maximum reservations awaiting reconciliation across all keys in
    /// this namespace/algorithm.
    pub(super) max_active_reservations: usize,
}

impl ValkeyTokenBucketBackend {
    /// Build this rule's backend from a filter-shared [`ValkeyEval`] client and
    /// lazy connection cache.
    ///
    /// # Errors
    ///
    /// Returns [`BackendError::Unavailable`] if `capacity`/`refill_rate`
    /// aren't positive and finite, or if `capacity` or `capacity /
    /// refill_rate` exceeds the bounds documented on
    /// [`token_bucket_ledger::MAX_F64_SAFE_INTEGER`]/
    /// [`token_bucket_ledger::MAX_CAPACITY_REFILL_RATE_RATIO_SECS`].
    pub(super) fn new(config: ValkeyTokenBucketConfig) -> Result<Self, BackendError> {
        token_bucket_ledger::validate_capacity_and_refill_rate(config.capacity, config.refill_rate)
            .map_err(BackendError::Unavailable)?;
        let config_fingerprint = token_bucket_config_fingerprint(
            config.capacity,
            config.refill_rate,
            config.reservation_timeout_ms,
            config.max_keys,
            config.max_active_reservations,
        );
        Ok(Self {
            valkey: config.valkey,
            namespace: config.namespace,
            rule: config.rule,
            capacity: config.capacity,
            refill_rate: config.refill_rate,
            reservation_timeout_ms: config.reservation_timeout_ms,
            max_keys: config.max_keys,
            max_active_reservations: config.max_active_reservations,
            config_fingerprint,
            worker: ReconcileWorker::new(),
            telemetry: Arc::new(ValkeyTelemetryState::default()),
        })
    }

    /// See [`ValkeyTokenRateLimitBackend::clone_without_sender`].
    fn clone_without_sender(&self) -> Self {
        Self {
            valkey: self.valkey.clone(),
            namespace: self.namespace.clone(),
            rule: self.rule.clone(),
            capacity: self.capacity,
            refill_rate: self.refill_rate,
            reservation_timeout_ms: self.reservation_timeout_ms,
            max_keys: self.max_keys,
            max_active_reservations: self.max_active_reservations,
            config_fingerprint: self.config_fingerprint.clone(),
            worker: ReconcileWorker::detached(),
            telemetry: Arc::clone(&self.telemetry),
        }
    }

    /// See [`ValkeyTokenRateLimitBackend::start_worker`].
    ///
    /// # Errors
    ///
    /// Returns [`BackendError::Unavailable`] if called outside a Tokio
    /// runtime context.
    fn start_worker(&self) -> Result<(), BackendError> {
        let runtime = tokio::runtime::Handle::try_current()
            .map_err(|_error| BackendError::Unavailable("Valkey reconciliation requires a Tokio runtime".into()))?;
        self.worker.start(&runtime, || self.clone_without_sender());
        Ok(())
    }

    /// Deterministic per-key Valkey key names for this rule/namespace,
    /// under a `:tb:` segment distinct from the sliding-window backend's
    /// [`ValkeyTokenRateLimitBackend::key_parts`] -- see
    /// [`TOKEN_BUCKET_RESERVE_SCRIPT`]'s doc comment for why the two
    /// algorithms must never share bookkeeping keys.
    #[expect(
        clippy::too_many_lines,
        reason = "the key layout is kept in one place so Lua KEYS indexes remain auditable"
    )]
    fn key_parts(&self, key: &str) -> [String; 12] {
        let mut rule_digest = Sha256::new();
        rule_digest.update(self.namespace.as_bytes());
        rule_digest.update(&[0]);
        rule_digest.update(b"token_bucket");
        rule_digest.update(&[0]);
        rule_digest.update(self.rule.as_bytes());
        let rule_hash = rule_digest
            .finish()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        let rule_prefix = format!("{}:v1:tb:rule:{rule_hash}", self.namespace);
        let mut digest = Sha256::new();
        digest.update(self.namespace.as_bytes());
        digest.update(&[0]);
        digest.update(b"token_bucket");
        digest.update(&[0]);
        digest.update(self.rule.as_bytes());
        digest.update(&[0]);
        digest.update(key.as_bytes());
        let hash = digest
            .finish()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        let prefix = format!("{}:v1:tb:{}", self.namespace, hash);
        [
            prefix.clone(),
            format!("{prefix}:active"),
            format!("{}:tb:keys", self.namespace),
            format!("{}:tb:active-count", self.namespace),
            format!("{}:tb:reservation-seq", self.namespace),
            format!("{}:tb:active-index", self.namespace),
            format!("{rule_prefix}:active-count"),
            format!("{rule_prefix}:active-index"),
            format!("{rule_prefix}:keys"),
            format!("{rule_prefix}:balances"),
            format!("{rule_prefix}:remaining-total"),
            format!("{rule_prefix}:accounting-config"),
        ]
    }
}

#[async_trait]
impl TokenRateLimitStateBackend for ValkeyTokenBucketBackend {
    async fn reserve(&self, request: ReserveRequest) -> Result<BackendReserve, BackendError> {
        let keys = self.key_parts(&request.key);
        let args = [
            self.capacity.to_string(),
            self.refill_rate.to_string(),
            self.reservation_timeout_ms.to_string(),
            self.max_keys.to_string(),
            self.max_active_reservations.to_string(),
            request.estimate.to_string(),
            self.config_fingerprint.clone(),
        ];
        self.valkey
            .eval_parsed(TOKEN_BUCKET_RESERVE_SCRIPT, &keys, &args, |response| {
                self.telemetry.parse_reserve_reply(response)
            })
            .await
    }

    async fn reconcile(&self, request: ReconcileRequest) -> Result<BackendSettlement, BackendError> {
        let keys = self.key_parts(&request.key);
        let actual = request.actual.unwrap_or(request.estimate);
        let args = [
            request.reservation_id.to_string(),
            actual.to_string(),
            self.capacity.to_string(),
            self.refill_rate.to_string(),
            self.reservation_timeout_ms.to_string(),
            self.config_fingerprint.clone(),
        ];
        self.valkey
            .eval_parsed(TOKEN_BUCKET_RECONCILE_SCRIPT, &keys, &args, |response| {
                self.telemetry.parse_reconcile_reply(response)
            })
            .await
    }

    fn enqueue_reconcile(&self, request: ReconcileRequest) -> Result<(), BackendError> {
        self.start_worker()?;
        self.worker.enqueue(request)
    }

    fn limit(&self) -> u64 {
        self.capacity
    }

    fn snapshot(&self) -> BackendSnapshot {
        self.telemetry.snapshot()
    }

    fn backend_name(&self) -> &'static str {
        self.valkey.backend_name
    }

    fn algorithm_name(&self) -> &'static str {
        "token_bucket"
    }

    fn rule_name(&self) -> &str {
        &self.rule
    }
}

#[cfg(test)]
#[expect(clippy::allow_attributes, reason = "blanket test suppressions")]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, reason = "tests")]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use super::*;
    use crate::token_rate_limit::ledger::LedgerConfig;

    fn memory_backend(capacity: u64) -> InMemoryTokenRateLimitBackend {
        let ledger = Ledger::new(LedgerConfig {
            budgets: vec![Budget {
                window_ms: 60_000,
                capacity,
            }],
            reservation_timeout_ms: 1_000,
            max_keys: 8,
            max_key_length: 64,
            max_active_reservations: 8,
        })
        .unwrap();
        InMemoryTokenRateLimitBackend::new(ledger)
    }

    #[test]
    fn valkey_telemetry_reply_updates_one_snapshot_and_rejects_negative_values() {
        let telemetry = ValkeyTelemetryState::default();
        telemetry.record_reply(90, 2, 3).unwrap();
        assert_eq!(
            telemetry.snapshot(),
            BackendSnapshot {
                budget_remaining: 90,
                active_reservations: 2,
                active_keys: 3,
            }
        );
        assert!(matches!(
            telemetry.record_reply(-1, 0, 0),
            Err(BackendError::InvalidResponse)
        ));
        assert!(matches!(
            telemetry.parse_reserve_reply(&[3]),
            Err(BackendError::ConfigurationMismatch)
        ));
    }

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "one focused test proves canonicalization plus every field covered by both fingerprints"
    )]
    fn accounting_config_fingerprints_are_canonical_and_cover_shared_semantics() {
        let budgets = vec![
            Budget {
                window_ms: 60_000,
                capacity: 1_000,
            },
            Budget {
                window_ms: 1_000,
                capacity: 100,
            },
        ];
        let mut reversed = budgets.clone();
        reversed.reverse();
        let sliding = sliding_window_config_fingerprint(&budgets, 30_000, 100, 1_000);
        assert!(sliding.starts_with("v1:"));
        assert_eq!(
            sliding,
            sliding_window_config_fingerprint(&reversed, 30_000, 100, 1_000),
            "budget declaration order must not create false configuration skew"
        );
        assert_ne!(
            sliding,
            sliding_window_config_fingerprint(
                &[Budget {
                    window_ms: 60_000,
                    capacity: 1_000
                }],
                30_000,
                100,
                1_000
            )
        );
        assert_ne!(sliding, sliding_window_config_fingerprint(&budgets, 30_001, 100, 1_000));
        assert_ne!(sliding, sliding_window_config_fingerprint(&budgets, 30_000, 101, 1_000));
        assert_ne!(sliding, sliding_window_config_fingerprint(&budgets, 30_000, 100, 1_001));

        let bucket = token_bucket_config_fingerprint(1_000, 1.25, 30_000, 100, 1_000);
        assert!(bucket.starts_with("v1:"));
        assert_ne!(sliding, bucket, "algorithm identity is part of the fingerprint");
        assert_ne!(bucket, token_bucket_config_fingerprint(1_001, 1.25, 30_000, 100, 1_000));
        assert_ne!(bucket, token_bucket_config_fingerprint(1_000, 1.5, 30_000, 100, 1_000));
        assert_ne!(bucket, token_bucket_config_fingerprint(1_000, 1.25, 30_001, 100, 1_000));
        assert_ne!(bucket, token_bucket_config_fingerprint(1_000, 1.25, 30_000, 101, 1_000));
        assert_ne!(bucket, token_bucket_config_fingerprint(1_000, 1.25, 30_000, 100, 1_001));
    }

    #[tokio::test]
    async fn reconcile_sync_settles_in_process_state_without_a_network_round_trip() {
        let backend = memory_backend(100);
        let admitted = backend
            .reserve(ReserveRequest {
                key: "a".into(),
                estimate: 40,
                now_ms: 0,
            })
            .await
            .unwrap();
        let BackendReserve::Admitted { reservation_id, .. } = admitted else {
            panic!("expected admission")
        };

        let settlement = backend.reconcile_sync(&ReconcileRequest {
            key: "a".into(),
            reservation_id,
            actual: Some(10),
            estimate: 40,
            now_ms: 0,
        });
        assert_eq!(
            settlement,
            Some(BackendSettlement::Applied {
                actual: 10,
                refund: 30,
                overage: 0
            }),
            "in-process backend must resolve reconcile_sync synchronously, without a Valkey-style enqueued worker"
        );
    }

    #[tokio::test]
    async fn cleanup_reports_active_reservations_and_keys_for_in_process_state() {
        let backend = memory_backend(100);
        backend
            .reserve(ReserveRequest {
                key: "a".into(),
                estimate: 10,
                now_ms: 0,
            })
            .await
            .unwrap();

        let report = backend
            .cleanup(0, 8)
            .expect("in-process backend must report cleanup state for gauges");
        assert_eq!(
            report.active_reservations, 1,
            "one un-reconciled reservation should be counted"
        );
        assert_eq!(report.active_keys, 1, "one distinct key should be tracked");
        assert_eq!(report.orphaned, 0, "nothing has timed out yet");
    }

    /// `reconcile`/`enqueue_reconcile` are part of the shared
    /// [`TokenRateLimitStateBackend`] trait contract -- callers reach an
    /// in-process backend exclusively through `reconcile_sync` today (see
    /// `TokenRateLimitFilter::reconcile`'s doc comment), but the trait
    /// methods themselves must still behave correctly for any future or
    /// generic (`Arc<dyn TokenRateLimitStateBackend>`) caller that goes
    /// through them instead.
    /// Reserve `estimate` against `backend`, returning the resulting
    /// reservation ID (panics if denied -- every caller below reserves
    /// well within its backend's configured capacity).
    async fn reserve_or_panic(backend: &impl TokenRateLimitStateBackend, estimate: u64) -> u64 {
        let admitted = backend
            .reserve(ReserveRequest {
                key: "a".into(),
                estimate,
                now_ms: 0,
            })
            .await
            .unwrap();
        let BackendReserve::Admitted { reservation_id, .. } = admitted else {
            panic!("expected admission")
        };
        reservation_id
    }

    /// The `reconcile` half of
    /// `assert_trait_reconcile_methods_apply_directly`, split out to keep
    /// both under clippy's function-length budget.
    async fn assert_trait_reconcile_applies_directly(backend: &impl TokenRateLimitStateBackend) {
        let reservation_id = reserve_or_panic(backend, 50).await;
        let settlement = backend
            .reconcile(ReconcileRequest {
                key: "a".into(),
                reservation_id,
                actual: Some(10),
                estimate: 50,
                now_ms: 0,
            })
            .await
            .unwrap();
        assert_eq!(
            settlement,
            BackendSettlement::Applied {
                actual: 10,
                refund: 40,
                overage: 0
            }
        );
    }

    /// The `enqueue_reconcile` half -- see
    /// `assert_trait_reconcile_applies_directly`. The in-process
    /// implementation applies it inline rather than truly deferring it,
    /// but must still succeed and take effect.
    async fn assert_trait_enqueue_reconcile_applies_directly(backend: &impl TokenRateLimitStateBackend) {
        let reservation_id = reserve_or_panic(backend, 40).await;
        backend
            .enqueue_reconcile(ReconcileRequest {
                key: "a".into(),
                reservation_id,
                actual: Some(5),
                estimate: 40,
                now_ms: 0,
            })
            .unwrap();
        let request = ReserveRequest {
            key: "a".into(),
            estimate: 35,
            now_ms: 0,
        };
        assert!(
            matches!(backend.reserve(request).await.unwrap(), BackendReserve::Admitted { .. }),
            "enqueue_reconcile must have released the 35 unused tokens from the second reservation"
        );
    }

    #[tokio::test]
    async fn in_memory_sliding_window_backend_trait_reconcile_methods_apply_directly() {
        let backend = memory_backend(100);
        assert_trait_reconcile_applies_directly(&backend).await;
        assert_trait_enqueue_reconcile_applies_directly(&backend).await;
    }

    /// The token-bucket analog of
    /// `in_memory_sliding_window_backend_trait_reconcile_methods_apply_directly`.
    #[tokio::test]
    async fn in_memory_token_bucket_backend_trait_reconcile_methods_apply_directly() {
        let backend = bucket_backend(100, 1.0);
        assert_trait_reconcile_applies_directly(&backend).await;
        assert_trait_enqueue_reconcile_applies_directly(&backend).await;
    }

    /// Reconciling an unknown/already-settled reservation ID must be a
    /// silent no-op (idempotent double-reconciliation), never a panic or
    /// a double-credit -- for both algorithms' in-process backends,
    /// through both the async `reconcile` trait method and the
    /// synchronous `reconcile_sync` fast path.
    #[tokio::test]
    async fn in_memory_backends_reconcile_is_noop_for_an_unknown_reservation_id() {
        let unknown_reservation = ReconcileRequest {
            key: "a".into(),
            reservation_id: 999_999,
            actual: Some(1),
            estimate: 1,
            now_ms: 0,
        };

        let sliding = memory_backend(100);
        assert_eq!(
            sliding.reconcile(unknown_reservation.clone()).await.unwrap(),
            BackendSettlement::Noop
        );
        assert_eq!(
            sliding.reconcile_sync(&unknown_reservation),
            Some(BackendSettlement::Noop)
        );

        let bucket = bucket_backend(100, 1.0);
        assert_eq!(
            bucket.reconcile(unknown_reservation.clone()).await.unwrap(),
            BackendSettlement::Noop
        );
        assert_eq!(
            bucket.reconcile_sync(&unknown_reservation),
            Some(BackendSettlement::Noop)
        );
    }

    /// [`ReconcileWorker::start`] on a [`ReconcileWorker::detached`]
    /// worker must be a no-op: there's no receiver to hand a spawned
    /// [`run_reconcile_worker`], so it must return without ever calling
    /// `make_worker` (a real caller passes a closure that builds a live
    /// backend clone there -- doing that unnecessarily would be wasted
    /// work at best and a logic error at worst).
    #[test]
    fn reconcile_worker_start_on_a_detached_worker_never_spawns() {
        let worker = ReconcileWorker::detached();
        let runtime = tokio::runtime::Builder::new_current_thread().build().unwrap();
        worker.start(runtime.handle(), || -> AlwaysFailsReconcile {
            panic!("a detached ReconcileWorker has no receiver to hand a spawned worker -- make_worker must not run")
        });
    }

    /// [`ValkeyEval::eval`] must invalidate its cached connection and
    /// surface an error on any command failure -- not just a connection
    /// failure -- so a subsequent call re-establishes a fresh connection
    /// rather than reusing one Valkey has already rejected a command on.
    /// Requires a live Valkey/Redis (see `TOKEN_RATE_LIMIT_VALKEY_URL` in
    /// `tests.rs`); skips otherwise.
    #[tokio::test]
    async fn valkey_eval_invalidates_the_connection_after_a_script_error_and_recovers() {
        let Ok(url) = std::env::var("TOKEN_RATE_LIMIT_VALKEY_URL") else {
            tracing::warn!("skipping: TOKEN_RATE_LIMIT_VALKEY_URL not set");
            return;
        };
        let valkey = ValkeyEval::new(url).unwrap();

        // A live, successfully-connected session: establishes and caches
        // the connection this test then forces `eval` to invalidate.
        // `{1}` (a Lua table), not a bare `1`, since `eval` deserializes
        // into `Vec<i64>` (a multi-bulk reply), same shape as the real
        // reserve/reconcile scripts.
        assert!(
            valkey.eval("return {1}", &["k".to_owned()], &[]).await.is_ok(),
            "sanity check: a trivial script must succeed against a live Valkey"
        );

        // A script Valkey's Lua interpreter rejects outright (unbalanced
        // syntax) -- the command round-trips successfully at the
        // connection level, but Valkey replies with an error, exercising
        // the `Ok(Err(error))` arm of `eval`'s match (as opposed to
        // `valkey_failure_fails_closed`'s unreachable-host connection
        // failure, which exercises the earlier `connection()` error path).
        let result = valkey.eval("this is not valid lua(", &["k".to_owned()], &[]).await;
        assert!(
            result.is_err(),
            "an invalid script must surface as a BackendError, not panic or hang"
        );

        // The connection must have been invalidated and cleanly
        // re-established, not left wedged, for the next legitimate call.
        assert!(
            valkey.eval("return {1}", &["k".to_owned()], &[]).await.is_ok(),
            "eval must recover on the next call after invalidating a failed connection"
        );
    }

    #[tokio::test]
    async fn live_valkey_malformed_reply_invalidates_only_the_observed_generation() {
        let Ok(url) = std::env::var("TOKEN_RATE_LIMIT_VALKEY_URL") else {
            tracing::warn!("skipping: TOKEN_RATE_LIMIT_VALKEY_URL not set");
            return;
        };
        let valkey = ValkeyEval::new(url).unwrap();

        let malformed = valkey
            .eval_parsed("return {99}", &["malformed".to_owned()], &[], |_response| {
                Err::<(), _>(BackendError::InvalidResponse)
            })
            .await;
        assert!(matches!(malformed, Err(BackendError::InvalidResponse)));
        {
            let cache = valkey.connection.lock().await;
            assert!(
                cache.current.is_none(),
                "a malformed successful reply must invalidate its connection"
            );
            assert_eq!(cache.next_generation, 1);
            drop(cache);
        }

        let replacement = valkey.acquired_connection().await.unwrap();
        assert_eq!(replacement.generation, 1);
        valkey.invalidate(0).await;
        let cache = valkey.connection.lock().await;
        assert_eq!(
            cache.current.as_ref().map(|cached| cached.generation),
            Some(1),
            "a late failure from an older generation must not evict its replacement"
        );
        drop(cache);
    }

    /// [`map_valkey_error`] distinguishes a known-not-dispatched connection
    /// failure from an ambiguous command failure.
    #[test]
    fn map_valkey_error_tags_the_message_with_which_phase_failed() {
        let timed_out = redis::RedisError::from(std::io::Error::from(std::io::ErrorKind::TimedOut));

        let BackendError::Unavailable(message) = map_valkey_error("connection", &timed_out) else {
            panic!("map_valkey_error must always return BackendError::Unavailable")
        };
        assert_eq!(message, "Redis/Valkey connection: timed out");

        let BackendError::Unconfirmed(message) = map_valkey_error("command", &timed_out) else {
            panic!("a command timeout must be classified as unconfirmed")
        };
        assert_eq!(message, "Redis/Valkey command: timed out");
    }

    #[test]
    fn explicit_pre_execution_server_rejections_are_bypassed_not_unconfirmed() {
        for code in ["NOREPLICAS", "READONLY", "MASTERDOWN"] {
            let error = redis::make_extension_error(code.to_owned(), Some("test rejection".to_owned()));
            assert!(command_known_not_applied(&error), "{code} must prove no mutation ran");
            assert!(matches!(
                map_valkey_error("command", &error),
                BackendError::Unavailable(_)
            ));
        }

        let script_error = redis::make_extension_error("ERR".to_owned(), Some("script failed".to_owned()));
        assert!(!command_known_not_applied(&script_error));
        assert!(matches!(
            map_valkey_error("command", &script_error),
            BackendError::Unconfirmed(_)
        ));
    }

    /// A one-shot TCP proxy in front of `upstream`'s `host:port`, for
    /// fault-injecting a Valkey that accepts a command and then hangs.
    /// Real Valkey/Redis has no config knob for this; `DEBUG SLEEP`
    /// comes closest but additionally requires `enable-debug-command`
    /// server-side and isn't callable from a script at all, so this
    /// proxies the real, unmodified Valkey under test instead of
    /// relying on either.
    ///
    /// Returns the proxy's local address and a flag that, once set,
    /// makes every open connection stop relaying upstream replies back
    /// to the client -- the bytes are still read off the wire (so the
    /// upstream Valkey itself never blocks or errors), just dropped.
    async fn spawn_wedgeable_proxy(upstream: &str) -> (std::net::SocketAddr, Arc<AtomicBool>) {
        let upstream = upstream
            .strip_prefix("redis://")
            .expect("test fixture: TOKEN_RATE_LIMIT_VALKEY_URL must be a bare redis://host:port URL")
            .to_owned();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy_addr = listener.local_addr().unwrap();
        let wedged = Arc::new(AtomicBool::new(false));

        let wedged_for_task = Arc::clone(&wedged);
        tokio::spawn(async move {
            while let Ok((client, _)) = listener.accept().await {
                tokio::spawn(relay_one_connection(
                    client,
                    upstream.clone(),
                    Arc::clone(&wedged_for_task),
                ));
            }
        });

        (proxy_addr, wedged)
    }

    /// One [`spawn_wedgeable_proxy`] connection's relay loop: client
    /// bytes always flow through to `upstream` unmodified; `upstream`'s
    /// replies flow back to the client unless/until `wedged` is set, at
    /// which point they're read off the wire (so `upstream` never
    /// blocks) but silently dropped instead of relayed.
    async fn relay_one_connection(mut client: tokio::net::TcpStream, upstream: String, wedged: Arc<AtomicBool>) {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

        let Ok(mut server) = tokio::net::TcpStream::connect(&upstream).await else {
            return;
        };
        let (mut client_read, mut client_write) = client.split();
        let (mut server_read, mut server_write) = server.split();
        tokio::join!(
            async {
                drop(tokio::io::copy(&mut client_read, &mut server_write).await);
            },
            async {
                let mut buffer = [0_u8; 4096];
                loop {
                    let Ok(read @ 1..) = server_read.read(&mut buffer).await else {
                        return;
                    };
                    if wedged.load(Ordering::SeqCst) {
                        continue; // Accepted off the wire, never relayed: a silent hang.
                    }
                    let Some(bytes) = buffer.get(..read) else {
                        return;
                    };
                    if client_write.write_all(bytes).await.is_err() {
                        return;
                    }
                }
            }
        );
    }

    /// [`ValkeyEval::eval`] must fail closed at (roughly)
    /// [`DEFAULT_OPERATION_TIMEOUT`]
    /// on a command Valkey accepts but never replies to, not hang
    /// indefinitely -- proving [`ValkeyEval::connection_config`] (not
    /// just `redis`'s own, possibly-different, default) is what's
    /// actually bounding the response wait. Requires a live
    /// Valkey/Redis (see `TOKEN_RATE_LIMIT_VALKEY_URL` in `tests.rs`);
    /// skips otherwise.
    #[tokio::test]
    #[expect(
        clippy::too_many_lines,
        reason = "the fault-injection lifecycle proves timeout, exact-once dispatch, invalidation, and recovery together"
    )]
    async fn eval_times_out_and_invalidates_the_connection_when_wedged() {
        let Ok(url) = std::env::var("TOKEN_RATE_LIMIT_VALKEY_URL") else {
            tracing::warn!("skipping: TOKEN_RATE_LIMIT_VALKEY_URL not set");
            return;
        };

        let (proxy_addr, wedged) = spawn_wedgeable_proxy(&url).await;
        let valkey = ValkeyEval::new(format!("redis://{proxy_addr}")).unwrap();

        assert!(
            valkey.eval("return {1}", &["k".to_owned()], &[]).await.is_ok(),
            "sanity check: a trivial script must succeed through an unwedged proxy"
        );

        let mutation_key = format!("praxis:test:no-replay:{}", std::process::id());
        let direct = redis::Client::open(url.clone()).unwrap();
        let mut direct_connection = direct.get_multiplexed_async_connection().await.unwrap();
        redis::cmd("DEL")
            .arg(&mutation_key)
            .query_async::<i64>(&mut direct_connection)
            .await
            .unwrap();

        wedged.store(true, Ordering::SeqCst);
        let started = Instant::now();
        let result = valkey
            .eval(
                "redis.call('INCR', KEYS[1]); return {1}",
                std::array::from_ref(&mutation_key),
                &[],
            )
            .await;
        let elapsed = started.elapsed();

        assert!(
            matches!(&result, Err(BackendError::Unconfirmed(message)) if message.starts_with("Redis/Valkey command:")),
            "a wedged command must fail closed with a command-phase error, not hang or panic: {result:?}"
        );
        assert!(
            elapsed < DEFAULT_OPERATION_TIMEOUT * 3,
            "must fail closed at ~DEFAULT_OPERATION_TIMEOUT ({DEFAULT_OPERATION_TIMEOUT:?}), not wait indefinitely \
             for the wedge to clear: took {elapsed:?}"
        );
        let mutation_count: i64 = redis::cmd("GET")
            .arg(&mutation_key)
            .query_async(&mut direct_connection)
            .await
            .unwrap();
        assert_eq!(
            mutation_count, 1,
            "an ambiguously completed mutation must be dispatched exactly once"
        );

        // Unwedge and confirm the connection was invalidated, not left
        // cached in its half-dead state -- the next call must
        // re-establish cleanly rather than time out again.
        wedged.store(false, Ordering::SeqCst);
        assert!(
            valkey.eval("return {1}", &["k".to_owned()], &[]).await.is_ok(),
            "eval must recover on the next call after invalidating a timed-out connection"
        );
        let mutation_count: i64 = redis::cmd("GET")
            .arg(&mutation_key)
            .query_async(&mut direct_connection)
            .await
            .unwrap();
        assert_eq!(mutation_count, 1, "rediscovery must not replay the prior mutation");
        redis::cmd("DEL")
            .arg(&mutation_key)
            .query_async::<i64>(&mut direct_connection)
            .await
            .unwrap();
    }

    /// Proves the actual mechanism the filter-level (not per-rule)
    /// `backend:` config relies on to avoid opening a redundant Valkey
    /// connection per rule: cloning a [`ValkeyEval`] must share the same
    /// underlying connection cache, not build an independent one.
    #[test]
    fn cloning_valkey_eval_shares_the_same_connection_cache() {
        let valkey = ValkeyEval::new("redis://127.0.0.1:1".into()).unwrap();
        let cloned = valkey.clone();
        assert!(
            Arc::ptr_eq(&valkey.connection, &cloned.connection),
            "a ValkeyEval clone (as handed to every Valkey-backed rule) must share one cached \
             connection, not each hold its own independent cache"
        );
    }

    #[tokio::test]
    #[expect(
        clippy::too_many_lines,
        reason = "one concurrency test proves the shared deadline, error class, and empty cache state"
    )]
    async fn sentinel_discovery_is_bounded_by_one_overall_deadline() {
        let valkey = ValkeyEval::sentinel(
            vec!["redis://127.0.0.1:1/".to_owned(), "redis://127.0.0.1:2/".to_owned()],
            "unreachable-test-primary".to_owned(),
            ValkeyTimeouts {
                connect: Duration::from_millis(50),
                operation: Duration::from_millis(50),
                discovery: Duration::from_millis(150),
            },
            "valkey",
        )
        .unwrap();

        let started = Instant::now();
        let mut tasks = Vec::new();
        for index in 0..8 {
            let valkey = valkey.clone();
            tasks.push(tokio::spawn(async move {
                valkey.eval("return {1}", &[format!("k-{index}")], &[]).await
            }));
        }
        for task in tasks {
            assert!(matches!(task.await.unwrap(), Err(BackendError::Unavailable(_))));
        }
        let elapsed = started.elapsed();
        assert!(
            elapsed < Duration::from_secs(1),
            "concurrent callers must share rather than multiply the configured discovery deadline, took {elapsed:?}"
        );
        let cache = valkey.connection.lock().await;
        assert!(cache.current.is_none());
        assert_eq!(
            cache.next_generation, 0,
            "a failed discovery must not install a generation"
        );
        drop(cache);
    }

    fn live_sentinel_settings() -> Option<(Vec<String>, String, Vec<String>)> {
        let required = std::env::var("TOKEN_RATE_LIMIT_SENTINEL_REQUIRED").is_ok_and(|value| value == "1");
        let parse_list = |name: &str| {
            std::env::var(name).ok().map(|value| {
                value
                    .split(',')
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(str::to_owned)
                    .collect::<Vec<_>>()
            })
        };
        let settings = match (
            parse_list("TOKEN_RATE_LIMIT_SENTINEL_URLS"),
            std::env::var("TOKEN_RATE_LIMIT_SENTINEL_SERVICE").ok(),
            parse_list("TOKEN_RATE_LIMIT_SENTINEL_NODES"),
        ) {
            (Some(endpoints), Some(service_name), Some(nodes)) if endpoints.len() == 3 && nodes.len() == 2 => {
                Some((endpoints, service_name, nodes))
            },
            _ => None,
        };
        assert!(
            !required || settings.is_some(),
            "live Sentinel qualification was required but its topology environment is missing or invalid"
        );
        settings
    }

    async fn live_node_identity(url: &str) -> Option<(String, String, String)> {
        let client = redis::Client::open(url).ok()?;
        let mut connection = client.get_multiplexed_async_connection().await.ok()?;
        let server: String = redis::cmd("INFO")
            .arg("SERVER")
            .query_async(&mut connection)
            .await
            .ok()?;
        let replication: String = redis::cmd("INFO")
            .arg("REPLICATION")
            .query_async(&mut connection)
            .await
            .ok()?;
        let field = |info: &str, name: &str| {
            info.lines()
                .filter_map(|line| line.trim_end().split_once(':'))
                .find_map(|(key, value)| (key == name).then(|| value.to_owned()))
        };
        Some((
            field(&server, "run_id")?,
            field(&replication, "role")?,
            field(&replication, "master_link_status").unwrap_or_default(),
        ))
    }

    async fn wait_for_live_sentinel_failover(nodes: &[String], old_primary_run_id: &str) -> (String, String) {
        let started = Instant::now();
        loop {
            let mut promoted = None;
            let mut connected_replica = false;
            for node in nodes {
                if let Some((run_id, role, link)) = live_node_identity(node).await {
                    if role == "master" && run_id != old_primary_run_id {
                        promoted = Some((node.clone(), run_id));
                    } else if matches!(role.as_str(), "slave" | "replica") && link == "up" {
                        connected_replica = true;
                    }
                }
            }
            if let Some(promoted) = promoted
                && connected_replica
            {
                return promoted;
            }
            assert!(
                started.elapsed() < Duration::from_secs(20),
                "Sentinel topology did not converge before the test deadline"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Live, product-neutral Sentinel contract. CI runs this unchanged against
    /// both exact Redis and Red Hat Valkey pins. It proves unavailable-endpoint
    /// traversal, coalesced discovery, cached steady state, replica-caught-up
    /// state survival, stale-primary invalidation, bounded rediscovery, and no
    /// automatic replay of the failed post-promotion mutation.
    #[tokio::test]
    #[expect(
        clippy::too_many_lines,
        reason = "one live topology lifecycle proves discovery, caching, failover, state survival, and no replay"
    )]
    async fn live_sentinel_discovers_coalesces_and_recovers_without_mutation_replay() {
        let Some((mut endpoints, service_name, nodes)) = live_sentinel_settings() else {
            tracing::warn!(
                "skipping: TOKEN_RATE_LIMIT_SENTINEL_URLS, TOKEN_RATE_LIMIT_SENTINEL_SERVICE, and TOKEN_RATE_LIMIT_SENTINEL_NODES not set"
            );
            return;
        };
        endpoints.insert(0, "redis://127.0.0.1:1/".to_owned());
        let valkey = ValkeyEval::sentinel(
            endpoints.clone(),
            service_name.clone(),
            ValkeyTimeouts {
                connect: Duration::from_millis(500),
                operation: Duration::from_millis(500),
                discovery: Duration::from_secs(5),
            },
            "valkey",
        )
        .unwrap();

        let mut tasks = Vec::new();
        for index in 0..16 {
            let valkey = valkey.clone();
            tasks.push(tokio::spawn(async move {
                valkey.eval("return {1}", &[format!("coalesced-{index}")], &[]).await
            }));
        }
        for task in tasks {
            assert_eq!(task.await.unwrap().unwrap(), [1]);
        }
        {
            let cache = valkey.connection.lock().await;
            assert_eq!(
                cache.next_generation, 1,
                "concurrent discovery must install one connection"
            );
            assert!(cache.current.is_some());
            drop(cache);
        }

        let mut old_primary = None;
        for node in &nodes {
            if let Some((run_id, role, _)) = live_node_identity(node).await
                && role == "master"
            {
                old_primary = Some((node.clone(), run_id));
                break;
            }
        }
        let (old_primary_url, old_primary_run_id) = old_primary.expect("test topology must expose one primary");
        let mutation_key = format!("praxis:test:sentinel:no-replay:{}", std::process::id());
        assert_eq!(
            valkey
                .eval(
                    "local value = redis.call('INCR', KEYS[1]); return {value}",
                    std::array::from_ref(&mutation_key),
                    &[],
                )
                .await
                .unwrap(),
            [1]
        );

        let old_primary = redis::Client::open(old_primary_url.clone()).unwrap();
        let mut old_primary_connection = old_primary.get_multiplexed_async_connection().await.unwrap();
        let replicas: i64 = redis::cmd("WAIT")
            .arg(1)
            .arg(5_000)
            .query_async(&mut old_primary_connection)
            .await
            .unwrap();
        assert_eq!(replicas, 1, "test-only acknowledgement must verify replica catch-up");

        let sentinel =
            redis::Client::open(endpoints.get(1).expect("two validated Sentinel endpoints").clone()).unwrap();
        let mut sentinel_connection = sentinel.get_multiplexed_async_connection().await.unwrap();
        let reply: String = redis::cmd("SENTINEL")
            .arg("FAILOVER")
            .arg(&service_name)
            .query_async(&mut sentinel_connection)
            .await
            .unwrap();
        assert_eq!(reply, "OK");
        let (new_primary_url, _new_primary_run_id) = wait_for_live_sentinel_failover(&nodes, &old_primary_run_id).await;

        let stale = valkey
            .eval(
                "local value = redis.call('INCR', KEYS[1]); return {value}",
                std::array::from_ref(&mutation_key),
                &[],
            )
            .await;
        assert!(stale.is_err(), "a cached connection to the demoted primary must fail");

        let new_primary = redis::Client::open(new_primary_url).unwrap();
        let mut new_primary_connection = new_primary.get_multiplexed_async_connection().await.unwrap();
        let value: i64 = redis::cmd("GET")
            .arg(&mutation_key)
            .query_async(&mut new_primary_connection)
            .await
            .unwrap();
        assert_eq!(value, 1, "the failed stale-primary mutation must not be replayed");

        assert_eq!(
            valkey
                .eval(
                    "local value = redis.call('INCR', KEYS[1]); return {value}",
                    std::array::from_ref(&mutation_key),
                    &[],
                )
                .await
                .unwrap(),
            [2],
            "the next operation must rediscover and use the promoted primary"
        );
        {
            let cache = valkey.connection.lock().await;
            assert_eq!(
                cache.next_generation, 2,
                "one failover must cause one coalesced rediscovery"
            );
            drop(cache);
        }
        redis::cmd("DEL")
            .arg(&mutation_key)
            .query_async::<i64>(&mut new_primary_connection)
            .await
            .unwrap();
    }

    #[test]
    fn valkey_backend_has_no_local_state_to_reconcile_or_clean_up_synchronously() {
        // Business behavior under test: a networked backend must never
        // silently answer a synchronous, no-I/O query -- the filter relies
        // on `None` here to route reconciliation through the background
        // worker (`enqueue_reconcile`) instead, and to skip gauge
        // reporting rather than publish misleading zeros. No live Valkey
        // is required: both methods short-circuit before any I/O.
        let backend = ValkeyTokenRateLimitBackend::new(ValkeyBackendConfig {
            valkey: ValkeyEval::new("redis://127.0.0.1:1".into()).unwrap(),
            namespace: "ns".into(),
            rule: "default".into(),
            budgets: vec![Budget {
                window_ms: 1_000,
                capacity: 10,
            }],
            reservation_timeout_ms: 1_000,
            max_keys: 8,
            max_active_reservations: 8,
        });

        assert!(
            backend.cleanup(0, 8).is_none(),
            "Valkey-backed state has no local ledger to clean up in-process"
        );
        assert!(
            backend
                .reconcile_sync(&ReconcileRequest {
                    key: "a".into(),
                    reservation_id: 1,
                    actual: Some(1),
                    estimate: 1,
                    now_ms: 0,
                })
                .is_none(),
            "Valkey-backed reconciliation must go through enqueue_reconcile, not reconcile_sync"
        );
    }

    // -------------------------------------------------------------------------
    // InMemoryTokenBucketBackend (trait-contract level -- exhaustive
    // business-scenario coverage for refill/refund/overage/DoS bounds
    // lives in `token_bucket_ledger::tests`; these confirm the backend
    // wrapper faithfully exposes that ledger through the shared trait).
    // -------------------------------------------------------------------------

    fn bucket_backend(capacity: u64, refill_rate: f64) -> InMemoryTokenBucketBackend {
        let ledger = TokenBucketLedger::new(token_bucket_ledger::TokenBucketConfig {
            capacity,
            refill_rate,
            reservation_timeout_ms: 1_000,
            max_keys: 8,
            max_key_length: 64,
            max_active_reservations: 8,
        })
        .unwrap();
        InMemoryTokenBucketBackend::new(ledger)
    }

    #[tokio::test]
    async fn token_bucket_backend_admits_within_capacity_and_denies_over_it() {
        let backend = bucket_backend(10, 1.0);
        assert!(matches!(
            backend
                .reserve(ReserveRequest {
                    key: "a".into(),
                    estimate: 10,
                    now_ms: 0
                })
                .await
                .unwrap(),
            BackendReserve::Admitted { .. }
        ));
        assert!(matches!(
            backend
                .reserve(ReserveRequest {
                    key: "a".into(),
                    estimate: 1,
                    now_ms: 0
                })
                .await
                .unwrap(),
            BackendReserve::Denied { .. }
        ));
    }

    #[tokio::test]
    async fn token_bucket_backend_limit_reports_configured_capacity() {
        let backend = bucket_backend(250, 5.0);
        assert_eq!(backend.limit(), 250);
    }

    #[tokio::test]
    async fn token_bucket_backend_reconcile_sync_credits_back_unused_estimate() {
        let backend = bucket_backend(100, 1.0);
        let admitted = backend
            .reserve(ReserveRequest {
                key: "a".into(),
                estimate: 50,
                now_ms: 0,
            })
            .await
            .unwrap();
        let BackendReserve::Admitted { reservation_id, .. } = admitted else {
            panic!("expected admission")
        };
        let settlement = backend.reconcile_sync(&ReconcileRequest {
            key: "a".into(),
            reservation_id,
            actual: Some(10),
            estimate: 50,
            now_ms: 0,
        });
        assert_eq!(
            settlement,
            Some(BackendSettlement::Applied {
                actual: 10,
                refund: 40,
                overage: 0
            })
        );
    }

    #[tokio::test]
    async fn token_bucket_backend_cleanup_reports_active_reservations_and_keys() {
        let backend = bucket_backend(100, 1.0);
        backend
            .reserve(ReserveRequest {
                key: "a".into(),
                estimate: 10,
                now_ms: 0,
            })
            .await
            .unwrap();
        let report = backend
            .cleanup(0, 8)
            .expect("in-process token bucket backend must report cleanup state for gauges");
        assert_eq!(report.active_reservations, 1);
        assert_eq!(report.active_keys, 1);
    }

    // -------------------------------------------------------------------------
    // ValkeyTokenBucketBackend construction-time validation and the
    // same no-I/O trait-contract checks as the sliding-window backend.
    // -------------------------------------------------------------------------

    #[test]
    fn valkey_token_bucket_backend_rejects_zero_capacity_or_refill_rate() {
        let base = || ValkeyTokenBucketConfig {
            valkey: ValkeyEval::new("redis://127.0.0.1:1".into()).unwrap(),
            namespace: "ns".into(),
            rule: "default".into(),
            capacity: 10,
            refill_rate: 1.0,
            reservation_timeout_ms: 1_000,
            max_keys: 8,
            max_active_reservations: 8,
        };
        assert!(ValkeyTokenBucketBackend::new(ValkeyTokenBucketConfig { capacity: 0, ..base() }).is_err());
        assert!(
            ValkeyTokenBucketBackend::new(ValkeyTokenBucketConfig {
                refill_rate: 0.0,
                ..base()
            })
            .is_err()
        );
    }

    #[test]
    fn valkey_token_bucket_backend_rejects_non_finite_refill_rate() {
        // Proves the shared validator (see non_finite_refill_rate_is_rejected)
        // is actually wired into this backend's constructor too.
        let base = || ValkeyTokenBucketConfig {
            valkey: ValkeyEval::new("redis://127.0.0.1:1".into()).unwrap(),
            namespace: "ns".into(),
            rule: "default".into(),
            capacity: 10,
            refill_rate: 1.0,
            reservation_timeout_ms: 1_000,
            max_keys: 8,
            max_active_reservations: 8,
        };
        for bad_rate in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            assert!(
                ValkeyTokenBucketBackend::new(ValkeyTokenBucketConfig {
                    refill_rate: bad_rate,
                    ..base()
                })
                .is_err(),
                "refill_rate {bad_rate} must be rejected as non-finite/non-positive"
            );
        }
    }

    #[test]
    fn valkey_token_bucket_backend_rejects_capacity_exceeding_f64_safe_integer() {
        // See MAX_F64_SAFE_INTEGER's doc comment for why this bound exists;
        // proven here for the Valkey backend's own constructor.
        let base = || ValkeyTokenBucketConfig {
            valkey: ValkeyEval::new("redis://127.0.0.1:1".into()).unwrap(),
            namespace: "ns".into(),
            rule: "default".into(),
            capacity: 10,
            refill_rate: 1.0,
            reservation_timeout_ms: 1_000,
            max_keys: 8,
            max_active_reservations: 8,
        };
        assert!(
            ValkeyTokenBucketBackend::new(ValkeyTokenBucketConfig {
                capacity: token_bucket_ledger::MAX_F64_SAFE_INTEGER + 1,
                ..base()
            })
            .is_err(),
            "capacity above 2^53 must be rejected before precision is silently lost"
        );
    }

    #[test]
    fn valkey_token_bucket_backend_rejects_a_refill_rate_ratio_exceeding_the_pexpire_ttl_bound() {
        // See MAX_CAPACITY_REFILL_RATE_RATIO_SECS's doc comment for why
        // this bound exists; proven here for the Valkey backend's own
        // constructor.
        let base = || ValkeyTokenBucketConfig {
            valkey: ValkeyEval::new("redis://127.0.0.1:1".into()).unwrap(),
            namespace: "ns".into(),
            rule: "default".into(),
            capacity: 10,
            refill_rate: 1.0,
            reservation_timeout_ms: 1_000,
            max_keys: 8,
            max_active_reservations: 8,
        };
        assert!(
            ValkeyTokenBucketBackend::new(ValkeyTokenBucketConfig {
                capacity: 10,
                refill_rate: 10.0 / (token_bucket_ledger::MAX_CAPACITY_REFILL_RATE_RATIO_SECS * 2.0),
                ..base()
            })
            .is_err(),
            "a capacity/refill_rate ratio beyond the PEXPIRE TTL bound must be rejected"
        );
    }

    #[test]
    fn valkey_token_bucket_backend_has_no_local_state_to_reconcile_or_clean_up_synchronously() {
        let backend = ValkeyTokenBucketBackend::new(ValkeyTokenBucketConfig {
            valkey: ValkeyEval::new("redis://127.0.0.1:1".into()).unwrap(),
            namespace: "ns".into(),
            rule: "default".into(),
            capacity: 10,
            refill_rate: 1.0,
            reservation_timeout_ms: 1_000,
            max_keys: 8,
            max_active_reservations: 8,
        })
        .unwrap();
        assert!(backend.cleanup(0, 8).is_none());
        assert!(
            backend
                .reconcile_sync(&ReconcileRequest {
                    key: "a".into(),
                    reservation_id: 1,
                    actual: Some(1),
                    estimate: 1,
                    now_ms: 0,
                })
                .is_none()
        );
    }

    /// A [`ValkeyTokenBucketBackend`] and a [`ValkeyTokenRateLimitBackend`]
    /// sharing the same `namespace`/`rule` -- the plausible, even likely,
    /// operator config that
    /// [`valkey_token_bucket_and_sliding_window_key_parts_never_collide_even_in_the_same_namespace`] exercises.
    fn same_namespace_backends() -> (ValkeyTokenBucketBackend, ValkeyTokenRateLimitBackend) {
        let bucket = ValkeyTokenBucketBackend::new(ValkeyTokenBucketConfig {
            valkey: ValkeyEval::new("redis://127.0.0.1:1".into()).unwrap(),
            namespace: "shared".into(),
            rule: "same-rule-name".into(),
            capacity: 10,
            refill_rate: 1.0,
            reservation_timeout_ms: 1_000,
            max_keys: 8,
            max_active_reservations: 8,
        })
        .unwrap();
        let sliding = ValkeyTokenRateLimitBackend::new(ValkeyBackendConfig {
            valkey: ValkeyEval::new("redis://127.0.0.1:1".into()).unwrap(),
            namespace: "shared".into(),
            rule: "same-rule-name".into(),
            budgets: vec![Budget {
                window_ms: 1_000,
                capacity: 10,
            }],
            reservation_timeout_ms: 1_000,
            max_keys: 8,
            max_active_reservations: 8,
        });
        (bucket, sliding)
    }

    #[test]
    fn valkey_token_bucket_and_sliding_window_key_parts_never_collide_even_in_the_same_namespace() {
        // Two rules sharing one `namespace:` must never let one
        // algorithm's Lua script reap or mutate the other's
        // physical/bookkeeping keys.
        let (bucket, sliding) = same_namespace_backends();
        let bucket_keys = bucket.key_parts("same-key");
        let sliding_keys = sliding.key_parts("same-key");
        for bucket_key in &bucket_keys {
            assert!(
                !sliding_keys.contains(bucket_key),
                "token_bucket key {bucket_key} collided with a sliding_window key"
            );
        }
    }

    #[test]
    fn valkey_accounting_config_key_is_rule_wide_but_algorithm_isolated() {
        let (bucket, sliding) = same_namespace_backends();
        let bucket_alice = bucket.key_parts("alice");
        let bucket_bob = bucket.key_parts("bob");
        let sliding_alice = sliding.key_parts("alice");
        let sliding_bob = sliding.key_parts("bob");

        assert_eq!(bucket_alice[11], bucket_bob[11]);
        assert_eq!(sliding_alice[12], sliding_bob[12]);
        assert_ne!(bucket_alice[11], sliding_alice[12]);
        assert_ne!(bucket_alice[0], bucket_bob[0]);
        assert_ne!(sliding_alice[0], sliding_bob[0]);
    }

    #[derive(Debug, PartialEq, Eq)]
    struct RawValkeyState {
        values: Vec<Option<Vec<u8>>>,
        pttls: Vec<i64>,
    }

    async fn raw_valkey_state<const N: usize>(valkey: &ValkeyEval, keys: &[String; N]) -> RawValkeyState {
        let mut connection = valkey.connection().await.unwrap();
        let mut values = Vec::with_capacity(N);
        let mut pttls = Vec::with_capacity(N);
        for key in keys {
            let value: Option<Vec<u8>> = redis::cmd("DUMP").arg(key).query_async(&mut connection).await.unwrap();
            let pttl: i64 = redis::cmd("PTTL").arg(key).query_async(&mut connection).await.unwrap();
            values.push(value);
            pttls.push(pttl);
        }
        RawValkeyState { values, pttls }
    }

    fn assert_mismatch_did_not_change_state(before: &RawValkeyState, after: &RawValkeyState) {
        assert_eq!(
            after.values, before.values,
            "a rejected configuration mismatch mutated shared values"
        );
        for (before_ttl, after_ttl) in before.pttls.iter().zip(&after.pttls) {
            match *before_ttl {
                ttl @ 1.. => {
                    assert!(
                        *after_ttl > 0,
                        "a rejected mismatch expired live state: {before_ttl} -> {after_ttl}"
                    );
                    assert!(
                        *after_ttl <= ttl,
                        "a rejected mismatch extended a TTL despite performing no mutation: {before_ttl} -> {after_ttl}"
                    );
                    assert!(
                        ttl - *after_ttl < 1_000,
                        "a rejected mismatch shortened a TTL horizon: {before_ttl} -> {after_ttl}"
                    );
                },
                _ => assert_eq!(after_ttl, before_ttl),
            }
        }
    }

    async fn assert_mismatched_reserve_preserves_state<const N: usize>(
        owner: &ValkeyEval,
        keys: &[String; N],
        mismatched: &impl TokenRateLimitStateBackend,
        estimate: u64,
    ) {
        let before = raw_valkey_state(owner, keys).await;
        let outcome = mismatched
            .reserve(ReserveRequest {
                key: "shared-subject".into(),
                estimate,
                now_ms: 0,
            })
            .await;
        assert!(
            matches!(outcome, Err(BackendError::ConfigurationMismatch)),
            "configuration skew must fail closed distinctly, got {outcome:?}"
        );
        let after = raw_valkey_state(owner, keys).await;
        assert_mismatch_did_not_change_state(&before, &after);
    }

    async fn assert_mismatched_reconcile_preserves_state<const N: usize>(
        owner: &ValkeyEval,
        keys: &[String; N],
        mismatched: &impl TokenRateLimitStateBackend,
        reservation_id: u64,
        estimate: u64,
    ) {
        let before = raw_valkey_state(owner, keys).await;
        let outcome = mismatched
            .reconcile(ReconcileRequest {
                key: "shared-subject".into(),
                reservation_id,
                actual: Some(estimate),
                estimate,
                now_ms: 0,
            })
            .await;
        assert!(
            matches!(outcome, Err(BackendError::ConfigurationMismatch)),
            "configuration-skewed reconciliation must fail before mutation, got {outcome:?}"
        );
        let after = raw_valkey_state(owner, keys).await;
        assert_mismatch_did_not_change_state(&before, &after);
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "test constructor keeps each mismatched field explicit"
    )]
    fn live_sliding_backend(
        valkey: &ValkeyEval,
        namespace: &str,
        window_ms: u64,
        capacity: u64,
        timeout_ms: u64,
        max_keys: usize,
        max_active_reservations: usize,
    ) -> ValkeyTokenRateLimitBackend {
        ValkeyTokenRateLimitBackend::new(ValkeyBackendConfig {
            valkey: valkey.clone(),
            namespace: namespace.into(),
            rule: "shared-rule".into(),
            budgets: vec![Budget { window_ms, capacity }],
            reservation_timeout_ms: timeout_ms,
            max_keys,
            max_active_reservations,
        })
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "test constructor keeps each mismatched field explicit"
    )]
    fn live_bucket_backend(
        valkey: &ValkeyEval,
        namespace: &str,
        capacity: u64,
        refill_rate: f64,
        timeout_ms: u64,
        max_keys: usize,
        max_active_reservations: usize,
    ) -> ValkeyTokenBucketBackend {
        ValkeyTokenBucketBackend::new(ValkeyTokenBucketConfig {
            valkey: valkey.clone(),
            namespace: namespace.into(),
            rule: "shared-rule".into(),
            capacity,
            refill_rate,
            reservation_timeout_ms: timeout_ms,
            max_keys,
            max_active_reservations,
        })
        .unwrap()
    }

    #[tokio::test]
    #[expect(
        clippy::too_many_lines,
        reason = "one live regression covers every sliding-window fingerprint field and post-mismatch reconciliation"
    )]
    async fn live_valkey_sliding_window_rejects_config_skew_before_mutation() {
        let Ok(url) = std::env::var("TOKEN_RATE_LIMIT_VALKEY_URL") else {
            return;
        };
        let valkey = ValkeyEval::new(url).unwrap();
        let namespace = format!("praxis:test:sw-config-guard:{}", std::process::id());
        let owner = live_sliding_backend(&valkey, &namespace, 10_000, 5, 5_000, 10, 100);
        let peer = live_sliding_backend(&valkey, &namespace, 10_000, 5, 5_000, 10, 100);
        let keys = owner.key_parts("shared-subject");

        let BackendReserve::Admitted { reservation_id, .. } = owner
            .reserve(ReserveRequest {
                key: "shared-subject".into(),
                estimate: 5,
                now_ms: 0,
            })
            .await
            .unwrap()
        else {
            panic!("the owning writer must admit its initial reservation")
        };
        assert!(
            matches!(
                peer.reserve(ReserveRequest {
                    key: "shared-subject".into(),
                    estimate: 1,
                    now_ms: 0
                })
                .await,
                Ok(BackendReserve::Denied { .. })
            ),
            "an independent writer with the same fingerprint must share the exhausted budget"
        );

        for mismatched in [
            live_sliding_backend(&valkey, &namespace, 1_000, 5, 5_000, 10, 100),
            live_sliding_backend(&valkey, &namespace, 10_000, 6, 5_000, 10, 100),
            live_sliding_backend(&valkey, &namespace, 10_000, 5, 1_000, 10, 100),
            live_sliding_backend(&valkey, &namespace, 10_000, 5, 5_000, 11, 100),
            live_sliding_backend(&valkey, &namespace, 10_000, 5, 5_000, 10, 101),
        ] {
            assert_mismatched_reserve_preserves_state(&valkey, &keys, &mismatched, 1).await;
        }
        let mismatched_reconciler = live_sliding_backend(&valkey, &namespace, 1_000, 5, 5_000, 10, 100);
        assert_mismatched_reconcile_preserves_state(&valkey, &keys, &mismatched_reconciler, reservation_id, 5).await;

        assert_eq!(
            peer.reconcile(ReconcileRequest {
                key: "shared-subject".into(),
                reservation_id,
                actual: Some(5),
                estimate: 5,
                now_ms: 0,
            })
            .await
            .unwrap(),
            BackendSettlement::Applied {
                actual: 5,
                refund: 0,
                overage: 0,
            },
            "the original reservation must still reconcile through a same-config writer after rejected skew"
        );
        assert_eq!(
            owner
                .reconcile(ReconcileRequest {
                    key: "shared-subject".into(),
                    reservation_id,
                    actual: Some(5),
                    estimate: 5,
                    now_ms: 0,
                })
                .await
                .unwrap(),
            BackendSettlement::Noop,
            "the reservation must reconcile exactly once"
        );
        assert!(matches!(
            owner
                .reserve(ReserveRequest {
                    key: "shared-subject".into(),
                    estimate: 1,
                    now_ms: 0
                })
                .await,
            Ok(BackendReserve::Denied { .. })
        ));

        let reverse_namespace = format!("{namespace}:reverse");
        let short_owner = live_sliding_backend(&valkey, &reverse_namespace, 1_000, 5, 5_000, 10, 100);
        short_owner
            .reserve(ReserveRequest {
                key: "shared-subject".into(),
                estimate: 1,
                now_ms: 0,
            })
            .await
            .unwrap();
        let reverse_keys = short_owner.key_parts("shared-subject");
        let long_writer = live_sliding_backend(&valkey, &reverse_namespace, 10_000, 5, 5_000, 10, 100);
        assert_mismatched_reserve_preserves_state(&valkey, &reverse_keys, &long_writer, 1).await;
    }

    #[tokio::test]
    #[expect(
        clippy::too_many_lines,
        reason = "the live regression keeps the cross-writer reserve/reconcile sequence visible"
    )]
    async fn live_valkey_sliding_window_uses_canonical_budget_order_and_maximum_retention() {
        let Ok(url) = std::env::var("TOKEN_RATE_LIMIT_VALKEY_URL") else {
            return;
        };
        let valkey = ValkeyEval::new(url).unwrap();
        let namespace = format!("praxis:test:sw-canonical-budgets:{}", std::process::id());
        let long = Budget {
            window_ms: 10_000,
            capacity: 5,
        };
        let short = Budget {
            window_ms: 10,
            capacity: 100,
        };
        let backend = |budgets| {
            ValkeyTokenRateLimitBackend::new(ValkeyBackendConfig {
                valkey: valkey.clone(),
                namespace: namespace.clone(),
                rule: "shared-rule".into(),
                budgets,
                reservation_timeout_ms: 5_000,
                max_keys: 10,
                max_active_reservations: 100,
            })
        };
        let owner = backend(vec![long.clone(), short.clone()]);
        let peer = backend(vec![short, long]);
        let keys = owner.key_parts("shared-subject");

        let BackendReserve::Admitted { reservation_id, .. } = owner
            .reserve(ReserveRequest {
                key: "shared-subject".into(),
                estimate: 5,
                now_ms: 0,
            })
            .await
            .unwrap()
        else {
            panic!("the owning writer must admit its initial reservation")
        };
        owner
            .reconcile(ReconcileRequest {
                key: "shared-subject".into(),
                reservation_id,
                actual: Some(5),
                estimate: 5,
                now_ms: 0,
            })
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(30)).await;

        assert!(matches!(
            peer.reserve(ReserveRequest {
                key: "shared-subject".into(),
                estimate: 1,
                now_ms: 0,
            })
            .await,
            Ok(BackendReserve::Denied {
                reason: DenialReason::WindowCapacity,
                ..
            })
        ));
        let mut connection = valkey.connection().await.unwrap();
        let settled_entries: usize = redis::cmd("ZCARD")
            .arg(&keys[1])
            .query_async(&mut connection)
            .await
            .unwrap();
        assert_eq!(
            settled_entries, 1,
            "the short budget must not prune history still required by the long budget"
        );
    }

    async fn set_long_ttl(valkey: &ValkeyEval, key: &str) -> i64 {
        let mut connection = valkey.connection().await.unwrap();
        let changed: bool = redis::cmd("PEXPIRE")
            .arg(key)
            .arg(120_000)
            .query_async(&mut connection)
            .await
            .unwrap();
        assert!(changed, "test key {key} must exist before its TTL is extended");
        redis::cmd("PTTL").arg(key).query_async(&mut connection).await.unwrap()
    }

    async fn assert_long_ttl_was_not_shortened(valkey: &ValkeyEval, key: &str, before: i64) {
        let mut connection = valkey.connection().await.unwrap();
        let after: i64 = redis::cmd("PTTL").arg(key).query_async(&mut connection).await.unwrap();
        assert!(after <= before, "TTL unexpectedly grew for {key}: {before} -> {after}");
        assert!(
            after > 100_000,
            "an accounting operation shortened the deliberately longer TTL for {key}: {before} -> {after}"
        );
    }

    #[tokio::test]
    #[expect(
        clippy::too_many_lines,
        reason = "the live regression covers both reserve and reconcile TTL updates"
    )]
    async fn live_valkey_sliding_window_reserve_and_reconcile_never_shorten_ttls() {
        let Ok(url) = std::env::var("TOKEN_RATE_LIMIT_VALKEY_URL") else {
            return;
        };
        let valkey = ValkeyEval::new(url).unwrap();
        let namespace = format!("praxis:test:sw-monotonic-ttl:{}", std::process::id());
        let backend = live_sliding_backend(&valkey, &namespace, 10_000, 10, 1_000, 10, 100);
        let keys = backend.key_parts("shared-subject");
        let BackendReserve::Admitted { reservation_id, .. } = backend
            .reserve(ReserveRequest {
                key: "shared-subject".into(),
                estimate: 1,
                now_ms: 0,
            })
            .await
            .unwrap()
        else {
            panic!("the initial reservation must be admitted")
        };

        let mut before = Vec::new();
        for key in [&keys[2], &keys[7]] {
            before.push((key, set_long_ttl(&valkey, key).await));
        }
        backend
            .reserve(ReserveRequest {
                key: "shared-subject".into(),
                estimate: 1,
                now_ms: 0,
            })
            .await
            .unwrap();
        for (key, ttl) in before {
            assert_long_ttl_was_not_shortened(&valkey, key, ttl).await;
        }

        let mut before = Vec::new();
        for key in [&keys[2], &keys[7]] {
            before.push((key, set_long_ttl(&valkey, key).await));
        }
        backend
            .reconcile(ReconcileRequest {
                key: "shared-subject".into(),
                reservation_id,
                actual: Some(1),
                estimate: 1,
                now_ms: 0,
            })
            .await
            .unwrap();
        for (key, ttl) in before {
            assert_long_ttl_was_not_shortened(&valkey, key, ttl).await;
        }
    }

    #[tokio::test]
    #[expect(
        clippy::too_many_lines,
        reason = "one live regression covers every token-bucket fingerprint field and post-mismatch reconciliation"
    )]
    async fn live_valkey_token_bucket_rejects_config_skew_before_mutation() {
        let Ok(url) = std::env::var("TOKEN_RATE_LIMIT_VALKEY_URL") else {
            return;
        };
        let valkey = ValkeyEval::new(url).unwrap();
        let namespace = format!("praxis:test:tb-config-guard:{}", std::process::id());
        let owner = live_bucket_backend(&valkey, &namespace, 10, 1.0, 5_000, 10, 100);
        let peer = live_bucket_backend(&valkey, &namespace, 10, 1.0, 5_000, 10, 100);
        let keys = owner.key_parts("shared-subject");

        let BackendReserve::Admitted { reservation_id, .. } = owner
            .reserve(ReserveRequest {
                key: "shared-subject".into(),
                estimate: 10,
                now_ms: 0,
            })
            .await
            .unwrap()
        else {
            panic!("the owning writer must admit its initial reservation")
        };
        assert!(
            matches!(
                peer.reserve(ReserveRequest {
                    key: "shared-subject".into(),
                    estimate: 10,
                    now_ms: 0
                })
                .await,
                Ok(BackendReserve::Denied { .. })
            ),
            "an independent writer with the same fingerprint must share the depleted bucket"
        );

        for mismatched in [
            live_bucket_backend(&valkey, &namespace, 11, 1.0, 5_000, 10, 100),
            live_bucket_backend(&valkey, &namespace, 10, 10.0, 5_000, 10, 100),
            live_bucket_backend(&valkey, &namespace, 10, 1.0, 1_000, 10, 100),
            live_bucket_backend(&valkey, &namespace, 10, 1.0, 5_000, 11, 100),
            live_bucket_backend(&valkey, &namespace, 10, 1.0, 5_000, 10, 101),
        ] {
            assert_mismatched_reserve_preserves_state(&valkey, &keys, &mismatched, 1).await;
        }
        let mismatched_reconciler = live_bucket_backend(&valkey, &namespace, 10, 10.0, 5_000, 10, 100);
        assert_mismatched_reconcile_preserves_state(&valkey, &keys, &mismatched_reconciler, reservation_id, 10).await;

        assert_eq!(
            peer.reconcile(ReconcileRequest {
                key: "shared-subject".into(),
                reservation_id,
                actual: Some(10),
                estimate: 10,
                now_ms: 0,
            })
            .await
            .unwrap(),
            BackendSettlement::Applied {
                actual: 10,
                refund: 0,
                overage: 0,
            },
            "the original reservation must still reconcile through a same-config writer after rejected skew"
        );
        assert_eq!(
            owner
                .reconcile(ReconcileRequest {
                    key: "shared-subject".into(),
                    reservation_id,
                    actual: Some(10),
                    estimate: 10,
                    now_ms: 0,
                })
                .await
                .unwrap(),
            BackendSettlement::Noop,
            "the reservation must reconcile exactly once"
        );
        assert!(matches!(
            owner
                .reserve(ReserveRequest {
                    key: "shared-subject".into(),
                    estimate: 10,
                    now_ms: 0
                })
                .await,
            Ok(BackendReserve::Denied { .. })
        ));

        let reverse_namespace = format!("{namespace}:reverse");
        let fast_owner = live_bucket_backend(&valkey, &reverse_namespace, 10, 10.0, 5_000, 10, 100);
        fast_owner
            .reserve(ReserveRequest {
                key: "shared-subject".into(),
                estimate: 1,
                now_ms: 0,
            })
            .await
            .unwrap();
        let reverse_keys = fast_owner.key_parts("shared-subject");
        let slow_writer = live_bucket_backend(&valkey, &reverse_namespace, 10, 1.0, 5_000, 10, 100);
        assert_mismatched_reserve_preserves_state(&valkey, &reverse_keys, &slow_writer, 1).await;
    }

    #[tokio::test]
    #[expect(
        clippy::too_many_lines,
        reason = "the live regression covers both reserve and reconcile TTL updates"
    )]
    async fn live_valkey_token_bucket_reserve_and_reconcile_never_shorten_ttls() {
        let Ok(url) = std::env::var("TOKEN_RATE_LIMIT_VALKEY_URL") else {
            return;
        };
        let valkey = ValkeyEval::new(url).unwrap();
        let namespace = format!("praxis:test:tb-monotonic-ttl:{}", std::process::id());
        let backend = live_bucket_backend(&valkey, &namespace, 10, 1.0, 1_000, 10, 100);
        let keys = backend.key_parts("shared-subject");
        let BackendReserve::Admitted { reservation_id, .. } = backend
            .reserve(ReserveRequest {
                key: "shared-subject".into(),
                estimate: 1,
                now_ms: 0,
            })
            .await
            .unwrap()
        else {
            panic!("the initial reservation must be admitted")
        };

        let mut before = Vec::new();
        for key in [&keys[0], &keys[1], &keys[6]] {
            before.push((key, set_long_ttl(&valkey, key).await));
        }
        backend
            .reserve(ReserveRequest {
                key: "shared-subject".into(),
                estimate: 1,
                now_ms: 0,
            })
            .await
            .unwrap();
        for (key, ttl) in before {
            assert_long_ttl_was_not_shortened(&valkey, key, ttl).await;
        }

        let mut before = Vec::new();
        for key in [&keys[0], &keys[1], &keys[6]] {
            before.push((key, set_long_ttl(&valkey, key).await));
        }
        backend
            .reconcile(ReconcileRequest {
                key: "shared-subject".into(),
                reservation_id,
                actual: Some(1),
                estimate: 1,
                now_ms: 0,
            })
            .await
            .unwrap();
        for (key, ttl) in before {
            assert_long_ttl_was_not_shortened(&valkey, key, ttl).await;
        }
    }

    #[tokio::test]
    #[expect(
        clippy::too_many_lines,
        reason = "the gated live test constructs, exercises, and verifies one complete backend snapshot"
    )]
    async fn live_valkey_telemetry_reply_matches_the_backend_contract() {
        let Ok(url) = std::env::var("TOKEN_RATE_LIMIT_VALKEY_URL") else {
            return;
        };
        let backend = ValkeyTokenRateLimitBackend::new(ValkeyBackendConfig {
            valkey: ValkeyEval::new(url).unwrap(),
            namespace: format!("praxis:test:telemetry:{}", std::process::id()),
            rule: "engineering".into(),
            budgets: vec![Budget {
                window_ms: 60_000,
                capacity: 100,
            }],
            reservation_timeout_ms: 1_000,
            max_keys: 8,
            max_active_reservations: 8,
        });

        let outcome = backend
            .reserve(ReserveRequest {
                key: "alice".into(),
                estimate: 10,
                now_ms: 0,
            })
            .await;
        assert!(outcome.is_ok(), "Valkey reserve failed: {outcome:?}");
        assert_eq!(
            backend.snapshot(),
            BackendSnapshot {
                budget_remaining: 90,
                active_reservations: 1,
                active_keys: 1,
            }
        );

        let keys = backend.key_parts("alice");
        let mut connection = backend.valkey.connection().await.unwrap();
        for key in &keys[7..12] {
            let ttl_ms: i64 = redis::cmd("PTTL").arg(key).query_async(&mut connection).await.unwrap();
            assert!(ttl_ms > 0, "rule telemetry key {key} must expire, got PTTL={ttl_ms}");
            assert!(
                ttl_ms <= 61_000,
                "rule telemetry key {key} outlived its state: PTTL={ttl_ms}"
            );
        }
        let config_ttl_ms: i64 = redis::cmd("PTTL")
            .arg(&keys[12])
            .query_async(&mut connection)
            .await
            .unwrap();
        assert_eq!(
            config_ttl_ms, -1,
            "accounting configuration must persist until an explicit reset"
        );
    }

    #[tokio::test]
    async fn live_valkey_remaining_total_stays_saturated_across_key_updates() {
        let Ok(url) = std::env::var("TOKEN_RATE_LIMIT_VALKEY_URL") else {
            return;
        };
        let backend = ValkeyTokenRateLimitBackend::new(ValkeyBackendConfig {
            valkey: ValkeyEval::new(url).unwrap(),
            namespace: format!("praxis:test:saturated-telemetry:{}", std::process::id()),
            rule: "engineering".into(),
            budgets: vec![Budget {
                window_ms: 60_000,
                capacity: token_bucket_ledger::MAX_F64_SAFE_INTEGER,
            }],
            reservation_timeout_ms: 1_000,
            max_keys: 8,
            max_active_reservations: 8,
        });

        for key in ["alice", "bob"] {
            let outcome = backend
                .reserve(ReserveRequest {
                    key: key.into(),
                    estimate: 1,
                    now_ms: 0,
                })
                .await;
            assert!(outcome.is_ok(), "Valkey reserve failed: {outcome:?}");
            assert_eq!(
                backend.snapshot().budget_remaining,
                super::super::MAX_REPORTED_REMAINING
            );
        }
    }

    #[tokio::test]
    #[expect(
        clippy::too_many_lines,
        reason = "the live upgrade-compatibility test constructs legacy reservation bookkeeping before reconciliation"
    )]
    async fn live_valkey_sliding_window_reconcile_does_not_create_balance_for_an_unretained_key() {
        let Ok(url) = std::env::var("TOKEN_RATE_LIMIT_VALKEY_URL") else {
            return;
        };
        let backend = ValkeyTokenRateLimitBackend::new(ValkeyBackendConfig {
            valkey: ValkeyEval::new(url).unwrap(),
            namespace: format!("praxis:test:sw-legacy-reconcile:{}", std::process::id()),
            rule: "engineering".into(),
            budgets: vec![Budget {
                window_ms: 60_000,
                capacity: 100,
            }],
            reservation_timeout_ms: 1_000,
            max_keys: 8,
            max_active_reservations: 8,
        });
        let BackendReserve::Admitted { reservation_id, .. } = backend
            .reserve(ReserveRequest {
                key: "alice".into(),
                estimate: 20,
                now_ms: 0,
            })
            .await
            .unwrap()
        else {
            panic!("expected admission")
        };

        let keys = backend.key_parts("alice");
        let mut connection = backend.valkey.connection().await.unwrap();
        let _: i64 = redis::cmd("ZREM")
            .arg(&keys[9])
            .arg(&keys[0])
            .query_async(&mut connection)
            .await
            .unwrap();
        let _: i64 = redis::cmd("HDEL")
            .arg(&keys[10])
            .arg(&keys[0])
            .query_async(&mut connection)
            .await
            .unwrap();
        let _: () = redis::cmd("SET")
            .arg(&keys[11])
            .arg(0)
            .query_async(&mut connection)
            .await
            .unwrap();

        backend
            .reconcile(ReconcileRequest {
                key: "alice".into(),
                reservation_id,
                actual: Some(20),
                estimate: 20,
                now_ms: 0,
            })
            .await
            .unwrap();
        assert_eq!(backend.snapshot().budget_remaining, 0);
        let balance: Option<String> = redis::cmd("HGET")
            .arg(&keys[10])
            .arg(&keys[0])
            .query_async(&mut connection)
            .await
            .unwrap();
        assert!(
            balance.is_none(),
            "reconcile must not create a balance outside the retained-key zset"
        );
    }

    #[tokio::test]
    #[expect(
        clippy::too_many_lines,
        reason = "the gated live test verifies saturation and TTLs for every rule telemetry key"
    )]
    async fn live_valkey_token_bucket_telemetry_is_saturated_and_expiring() {
        let Ok(url) = std::env::var("TOKEN_RATE_LIMIT_VALKEY_URL") else {
            return;
        };
        let backend = ValkeyTokenBucketBackend::new(ValkeyTokenBucketConfig {
            valkey: ValkeyEval::new(url).unwrap(),
            namespace: format!("praxis:test:tb-saturated-telemetry:{}", std::process::id()),
            rule: "engineering".into(),
            capacity: token_bucket_ledger::MAX_F64_SAFE_INTEGER,
            refill_rate: 10_000_000.0,
            reservation_timeout_ms: 1_000,
            max_keys: 8,
            max_active_reservations: 8,
        })
        .unwrap();

        for key in ["alice", "bob"] {
            let outcome = backend
                .reserve(ReserveRequest {
                    key: key.into(),
                    estimate: 1,
                    now_ms: 0,
                })
                .await;
            assert!(outcome.is_ok(), "Valkey reserve failed: {outcome:?}");
            assert_eq!(
                backend.snapshot().budget_remaining,
                super::super::MAX_REPORTED_REMAINING
            );
        }

        let keys = backend.key_parts("alice");
        let mut connection = backend.valkey.connection().await.unwrap();
        for key in &keys[6..11] {
            let ttl_ms: i64 = redis::cmd("PTTL").arg(key).query_async(&mut connection).await.unwrap();
            assert!(ttl_ms > 0, "rule telemetry key {key} must expire, got PTTL={ttl_ms}");
        }
        let config_ttl_ms: i64 = redis::cmd("PTTL")
            .arg(&keys[11])
            .query_async(&mut connection)
            .await
            .unwrap();
        assert_eq!(
            config_ttl_ms, -1,
            "accounting configuration must persist until an explicit reset"
        );
    }

    #[tokio::test]
    #[expect(
        clippy::too_many_lines,
        reason = "the live upgrade-compatibility test constructs legacy reservation bookkeeping before reconciliation"
    )]
    async fn live_valkey_token_bucket_reconcile_does_not_create_balance_for_an_unretained_key() {
        let Ok(url) = std::env::var("TOKEN_RATE_LIMIT_VALKEY_URL") else {
            return;
        };
        let backend = ValkeyTokenBucketBackend::new(ValkeyTokenBucketConfig {
            valkey: ValkeyEval::new(url).unwrap(),
            namespace: format!("praxis:test:tb-legacy-reconcile:{}", std::process::id()),
            rule: "engineering".into(),
            capacity: 100,
            refill_rate: 1.0,
            reservation_timeout_ms: 1_000,
            max_keys: 8,
            max_active_reservations: 8,
        })
        .unwrap();
        let BackendReserve::Admitted { reservation_id, .. } = backend
            .reserve(ReserveRequest {
                key: "alice".into(),
                estimate: 20,
                now_ms: 0,
            })
            .await
            .unwrap()
        else {
            panic!("expected admission")
        };

        let keys = backend.key_parts("alice");
        let mut connection = backend.valkey.connection().await.unwrap();
        let _: i64 = redis::cmd("ZREM")
            .arg(&keys[8])
            .arg(&keys[0])
            .query_async(&mut connection)
            .await
            .unwrap();
        let _: i64 = redis::cmd("HDEL")
            .arg(&keys[9])
            .arg(&keys[0])
            .query_async(&mut connection)
            .await
            .unwrap();
        let _: () = redis::cmd("SET")
            .arg(&keys[10])
            .arg(0)
            .query_async(&mut connection)
            .await
            .unwrap();

        backend
            .reconcile(ReconcileRequest {
                key: "alice".into(),
                reservation_id,
                actual: Some(20),
                estimate: 20,
                now_ms: 0,
            })
            .await
            .unwrap();
        assert_eq!(backend.snapshot().budget_remaining, 0);
        let balance: Option<String> = redis::cmd("HGET")
            .arg(&keys[9])
            .arg(&keys[0])
            .query_async(&mut connection)
            .await
            .unwrap();
        assert!(
            balance.is_none(),
            "reconcile must not create a balance outside the retained-key zset"
        );
    }

    #[tokio::test]
    #[expect(
        clippy::too_many_lines,
        reason = "the gated live test verifies repeated denials and the physical-key invariant"
    )]
    async fn live_valkey_token_bucket_denials_do_not_create_unretained_keys() {
        let Ok(url) = std::env::var("TOKEN_RATE_LIMIT_VALKEY_URL") else {
            return;
        };
        let backend = ValkeyTokenBucketBackend::new(ValkeyTokenBucketConfig {
            valkey: ValkeyEval::new(url).unwrap(),
            namespace: format!("praxis:test:tb-denied-key:{}", std::process::id()),
            rule: "engineering".into(),
            capacity: 100,
            refill_rate: 1.0,
            reservation_timeout_ms: 1_000,
            max_keys: 1,
            max_active_reservations: 8,
        })
        .unwrap();

        assert!(matches!(
            backend
                .reserve(ReserveRequest {
                    key: "alice".into(),
                    estimate: 1,
                    now_ms: 0,
                })
                .await,
            Ok(BackendReserve::Admitted { .. })
        ));

        for _ in 0..2 {
            assert!(matches!(
                backend
                    .reserve(ReserveRequest {
                        key: "bob".into(),
                        estimate: 1,
                        now_ms: 0,
                    })
                    .await,
                Ok(BackendReserve::Denied { .. })
            ));
        }
        assert_eq!(backend.snapshot().active_keys, 1);

        let bob_keys = backend.key_parts("bob");
        let mut connection = backend.valkey.connection().await.unwrap();
        let exists: i64 = redis::cmd("EXISTS")
            .arg(&bob_keys[0])
            .query_async(&mut connection)
            .await
            .unwrap();
        assert_eq!(exists, 0, "a denied new key must not leave a physical hash behind");
    }

    #[tokio::test]
    #[expect(
        clippy::too_many_lines,
        reason = "the gated live test verifies max-active denial leaves no new physical key"
    )]
    async fn live_valkey_token_bucket_max_active_denial_does_not_create_a_new_key() {
        let Ok(url) = std::env::var("TOKEN_RATE_LIMIT_VALKEY_URL") else {
            return;
        };
        let backend = ValkeyTokenBucketBackend::new(ValkeyTokenBucketConfig {
            valkey: ValkeyEval::new(url).unwrap(),
            namespace: format!("praxis:test:tb-denied-active:{}", std::process::id()),
            rule: "engineering".into(),
            capacity: 100,
            refill_rate: 1.0,
            reservation_timeout_ms: 1_000,
            max_keys: 8,
            max_active_reservations: 1,
        })
        .unwrap();

        assert!(matches!(
            backend
                .reserve(ReserveRequest {
                    key: "alice".into(),
                    estimate: 1,
                    now_ms: 0,
                })
                .await,
            Ok(BackendReserve::Admitted { .. })
        ));
        assert!(matches!(
            backend
                .reserve(ReserveRequest {
                    key: "bob".into(),
                    estimate: 1,
                    now_ms: 0,
                })
                .await,
            Ok(BackendReserve::Denied { .. })
        ));

        let bob_keys = backend.key_parts("bob");
        let mut connection = backend.valkey.connection().await.unwrap();
        let exists: i64 = redis::cmd("EXISTS")
            .arg(&bob_keys[0])
            .query_async(&mut connection)
            .await
            .unwrap();
        assert_eq!(exists, 0, "a max-active denial for a new key must not create its hash");
    }

    #[test]
    fn worker_enqueue_fails_once_its_receiver_is_gone() {
        let worker = ReconcileWorker::detached();
        let request = ReconcileRequest {
            key: "a".into(),
            reservation_id: 1,
            actual: Some(1),
            estimate: 1,
            now_ms: 0,
        };
        assert!(worker.enqueue(request).is_err());
    }

    /// Failure returned by [`AlwaysFailsReconcile`].
    #[derive(Clone, Copy)]
    enum ReconcileFailure {
        /// Safe to retry because no mutation was dispatched.
        Unavailable,
        /// Unsafe to retry because the mutation may have committed.
        Unconfirmed,
        /// Unsafe to retry because a successful reply was malformed after the
        /// mutation may have committed.
        InvalidResponse,
    }

    /// A backend whose `reconcile` always fails, to drive
    /// [`run_reconcile_worker`]'s retry classification.
    struct AlwaysFailsReconcile {
        attempts: Arc<AtomicUsize>,
        failure: ReconcileFailure,
    }

    #[async_trait]
    impl TokenRateLimitStateBackend for AlwaysFailsReconcile {
        async fn reserve(&self, _request: ReserveRequest) -> Result<BackendReserve, BackendError> {
            panic!("not exercised by this test")
        }

        async fn reconcile(&self, _request: ReconcileRequest) -> Result<BackendSettlement, BackendError> {
            self.attempts.fetch_add(1, Ordering::SeqCst);
            Err(match self.failure {
                ReconcileFailure::Unavailable => BackendError::Unavailable("simulated failure".into()),
                ReconcileFailure::Unconfirmed => BackendError::Unconfirmed("simulated failure".into()),
                ReconcileFailure::InvalidResponse => BackendError::InvalidResponse,
            })
        }

        fn enqueue_reconcile(&self, _request: ReconcileRequest) -> Result<(), BackendError> {
            panic!("not exercised by this test")
        }

        fn limit(&self) -> u64 {
            0
        }

        fn snapshot(&self) -> BackendSnapshot {
            BackendSnapshot::default()
        }

        fn backend_name(&self) -> &'static str {
            "test"
        }

        fn algorithm_name(&self) -> &'static str {
            "test"
        }
    }

    #[tokio::test]
    async fn reconcile_worker_retries_then_abandons_a_persistently_failing_reconcile() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let (tx, rx) = mpsc::channel(1);
        tokio::spawn(run_reconcile_worker(
            AlwaysFailsReconcile {
                attempts: Arc::clone(&attempts),
                failure: ReconcileFailure::Unavailable,
            },
            rx,
        ));
        tx.send(ReconcileRequest {
            key: "a".into(),
            reservation_id: 1,
            actual: Some(1),
            estimate: 1,
            now_ms: 0,
        })
        .await
        .unwrap();
        drop(tx);

        // 1 initial attempt + 2 retries (25ms, 50ms backoff) before abandoning.
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            3,
            "must retry exactly twice, then abandon"
        );
    }

    #[tokio::test]
    async fn reconcile_worker_never_replays_an_ambiguous_or_malformed_mutation() {
        for failure in [ReconcileFailure::Unconfirmed, ReconcileFailure::InvalidResponse] {
            let attempts = Arc::new(AtomicUsize::new(0));
            let (tx, rx) = mpsc::channel(1);
            tokio::spawn(run_reconcile_worker(
                AlwaysFailsReconcile {
                    attempts: Arc::clone(&attempts),
                    failure,
                },
                rx,
            ));
            tx.send(ReconcileRequest {
                key: "a".into(),
                reservation_id: 1,
                actual: Some(1),
                estimate: 1,
                now_ms: 0,
            })
            .await
            .unwrap();
            drop(tx);

            tokio::time::sleep(Duration::from_millis(100)).await;
            assert_eq!(
                attempts.load(Ordering::SeqCst),
                1,
                "an ambiguously dispatched reconciliation must never be replayed"
            );
        }
    }
}
