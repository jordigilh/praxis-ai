// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Praxis Contributors

//! Valkey-backed state for the `token_rate_limit` filter, on plain commands.
//!
//! Admission and reconciliation are composed from plain Valkey commands
//! instead of server-side scripts. The sliding window uses commutative
//! operations (`INCRBY`, `ZADD`, `ZREMRANGEBYSCORE`, TTLs): two admissions
//! racing on one key may both pass, and the proposal accepts that momentary
//! overshoot; what is never lost is the usage they write. The token bucket's
//! refill is a read-modify-write, so it runs under `WATCH`/`MULTI`/`EXEC`.
//! In both, each reservation lives in its own key that expires with the
//! reservation timeout, and it is settled exactly once: both algorithms
//! delete it in a transaction that `WATCH` aborts when another settlement
//! got there first.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use praxis_ai_apis::hash::Sha256;
use redis::aio::MultiplexedConnection;

use super::{
    AccountingPolicy,
    backend::{BackendError, BackendSnapshot},
    ledger::Budget,
};

/// Version of the accounting semantics encoded by Valkey configuration
/// fingerprints. Bump this whenever an existing state value would be
/// interpreted differently by the plain-command backend.
const ACCOUNTING_CONFIG_SCHEMA: &str = "v3";

/// Finish a schema-versioned accounting configuration digest.
fn accounting_config_fingerprint(digest: Sha256) -> String {
    let hash = digest
        .finish()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    format!("{ACCOUNTING_CONFIG_SCHEMA}:{hash}")
}

/// Add a platform-independent `usize` field to a configuration digest.
fn digest_usize(digest: &mut Sha256, value: usize) {
    digest.update(&u64::try_from(value).unwrap_or(u64::MAX).to_be_bytes());
}

/// Add a length-delimited string to a configuration digest.
fn digest_string(digest: &mut Sha256, value: &str) {
    digest_usize(digest, value.len());
    digest.update(value.as_bytes());
}

/// Add an optional token count to a configuration digest.
fn digest_optional_u64(digest: &mut Sha256, value: Option<u64>) {
    match value {
        Some(value) => {
            digest.update(&[1]);
            digest.update(&value.to_be_bytes());
        },
        None => digest.update(&[0]),
    }
}

/// Add a validated floating-point policy value to a configuration digest.
/// Normalize both zero representations because they have identical accounting
/// semantics after validation.
fn digest_f64(digest: &mut Sha256, value: f64) {
    let bits = if value == 0.0 {
        0.0_f64.to_bits()
    } else {
        value.to_bits()
    };
    digest.update(&bits.to_be_bytes());
}

/// Add the effective request-estimation policy, excluding request-derived
/// values such as an individual request's `max_tokens`, to a digest.
#[expect(
    clippy::too_many_lines,
    reason = "each estimation strategy has a distinct canonical encoding"
)]
fn digest_estimation(digest: &mut Sha256, policy: &AccountingPolicy) {
    digest.update(b"estimation");
    match &policy.estimation {
        super::CompiledEstimation::Fixed { estimate } => {
            digest.update(b"fixed");
            digest.update(&estimate.to_be_bytes());
        },
        super::CompiledEstimation::MaxTokens {
            fallback_estimate,
            multiplier,
        } => {
            digest.update(b"max_tokens");
            digest_optional_u64(digest, *fallback_estimate);
            digest_f64(digest, *multiplier);
        },
        super::CompiledEstimation::InputPlusMaxTokens {
            fallback_estimate,
            multiplier,
            bytes_per_token,
        } => {
            digest.update(b"input_plus_max_tokens");
            digest_optional_u64(digest, *fallback_estimate);
            digest_f64(digest, *multiplier);
            digest_f64(digest, *bytes_per_token);
        },
        super::CompiledEstimation::ModelScaled {
            fallback_estimate,
            model_multipliers,
            default_multiplier,
        } => {
            digest.update(b"model_scaled");
            digest_optional_u64(digest, *fallback_estimate);
            digest_f64(digest, *default_multiplier);
            digest_usize(digest, model_multipliers.len());
            for (model, multiplier) in model_multipliers {
                digest_string(digest, model);
                digest_f64(digest, *multiplier);
            }
        },
    }
}

/// Add the resolved per-token-type reconciliation weights to a digest.
fn digest_weights(digest: &mut Sha256, weights: super::weights::TokenWeights) {
    digest.update(b"weights");
    for weight in [
        weights.input,
        weights.output,
        weights.cached_input,
        weights.cache_write,
        weights.reasoning,
    ] {
        digest_f64(digest, weight);
    }
}

/// Canonical fingerprint for state interpreted by the Valkey sliding-window
/// backend. Budget declaration order is not semantic, so sort the budget
/// pairs before hashing them.
pub(super) fn sliding_window_config_fingerprint(
    budgets: &[Budget],
    reservation_timeout_ms: u64,
    max_keys: usize,
    max_active_reservations: usize,
    accounting: &AccountingPolicy,
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
    digest.update(&u64::try_from(canonical_budgets.len()).unwrap_or(u64::MAX).to_be_bytes());
    for (window_ms, capacity) in canonical_budgets {
        digest.update(&window_ms.to_be_bytes());
        digest.update(&capacity.to_be_bytes());
    }
    digest.update(&reservation_timeout_ms.to_be_bytes());
    digest_usize(&mut digest, max_keys);
    digest_usize(&mut digest, max_active_reservations);
    digest_estimation(&mut digest, accounting);
    digest_weights(&mut digest, accounting.weights);
    digest.update(b"key_policy");
    digest_string(&mut digest, &accounting.key_fingerprint);

    accounting_config_fingerprint(digest)
}

/// Canonical fingerprint for state interpreted by the Valkey token-bucket
/// backend. Hash the exact IEEE-754 refill value used by the backend rather
/// than a display-format approximation.
#[expect(
    clippy::too_many_arguments,
    reason = "fingerprint fields mirror the token-bucket accounting contract"
)]
pub(super) fn token_bucket_config_fingerprint(
    capacity: u64,
    refill_rate: f64,
    reservation_timeout_ms: u64,
    max_keys: usize,
    max_active_reservations: usize,
    accounting: &AccountingPolicy,
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
    digest_usize(&mut digest, max_keys);
    digest_usize(&mut digest, max_active_reservations);
    digest_estimation(&mut digest, accounting);
    digest_weights(&mut digest, accounting.weights);
    digest.update(b"key_policy");
    digest_string(&mut digest, &accounting.key_fingerprint);

    accounting_config_fingerprint(digest)
}

/// Stable, algorithm-isolated key for the configuration marker.
pub(super) fn accounting_config_key(namespace: &str, algorithm: &str, rule: &str) -> String {
    format!(
        "{namespace}:v2:{algorithm}:rule:{}:accounting-config",
        key_hash(&[rule.as_bytes()])
    )
}

/// Initialize or validate a persistent accounting configuration marker before
/// a backend touches quota state.
pub(super) async fn ensure_accounting_config(
    valkey: &ValkeyConnection,
    marker: &str,
    expected: &str,
    state_index: &str,
) -> Result<(), BackendError> {
    let mut marker_read = redis::pipe();
    marker_read.cmd("GET").arg(marker);
    let (stored,): (Option<String>,) = valkey.pipeline(&marker_read).await?;
    if let Some(stored) = stored {
        return validate_accounting_config(Some(&stored), expected);
    }

    let mut retry = AbortRetry::start();
    loop {
        let mut transaction = valkey.transaction().await?;
        let outcome = ensure_accounting_config_attempt(transaction.inner(), marker, expected, state_index).await;
        match outcome {
            Ok(outcome) => {
                transaction.finish().await;
                if let Some(()) = outcome {
                    return Ok(());
                }
            },
            Err(error) => {
                drop(transaction);
                return Err(error);
            },
        }
        retry.pause().await?;
    }
}

/// Atomically claim an empty state generation, or validate its marker.
///
/// The marker and the per-rule retained-key index are watched together. If
/// the marker is absent but the index already exists, this is state written by
/// a pre-marker v2 writer; claiming it would make the new policy reinterpret
/// live quota, so the caller fails closed instead. A concurrent legacy writer
/// changing the index aborts the claim and is retried.
async fn ensure_accounting_config_attempt(
    connection: &mut MultiplexedConnection,
    marker: &str,
    expected: &str,
    state_index: &str,
) -> Result<Option<()>, BackendError> {
    let mut read = redis::pipe();
    read.cmd("WATCH").arg(marker).arg(state_index).ignore();
    read.cmd("GET").arg(marker);
    read.cmd("EXISTS").arg(state_index);
    let reply: redis::Value = read
        .query_async(connection)
        .await
        .map_err(|error| command_error(&error))?;
    let (stored, state_exists): (Option<String>, i64) =
        redis::from_redis_value(reply).map_err(|_error| BackendError::InvalidResponse)?;

    if let Some(stored) = stored {
        unwatch(connection).await?;
        validate_accounting_config(Some(&stored), expected)?;
        return Ok(Some(()));
    }
    if state_exists != 0 {
        unwatch(connection).await?;
        return Err(BackendError::ConfigurationMismatch);
    }

    let mut init = redis::pipe();
    init.atomic();
    init.cmd("SET").arg(marker).arg(expected).ignore();
    init.query_async(connection)
        .await
        .map_err(|error| command_error(&error))
}

/// Validate the marker value returned by Valkey after the initialize-or-read
/// pipeline. Kept separate so every outcome, including a missing marker after
/// an unexpected concurrent deletion, is unit-tested without requiring a
/// timing-dependent datastore race.
fn validate_accounting_config(stored: Option<&str>, expected: &str) -> Result<(), BackendError> {
    match stored {
        Some(stored) if stored == expected => Ok(()),
        Some(_) => Err(BackendError::ConfigurationMismatch),
        None => Err(BackendError::InvalidResponse),
    }
}

mod connection;
mod sliding_window;
mod token_bucket;
mod window;

pub(super) use connection::ValkeyConnection;
use connection::{AbortRetry, command_error, unwatch};
pub(super) use sliding_window::{ValkeySlidingWindowBackend, ValkeySlidingWindowConfig};
pub(super) use token_bucket::{ValkeyTokenBucketBackend, ValkeyTokenBucketConfig};

/// Last values a rule's Valkey backend observed, published as gauges under
/// the rule's label. Only `remaining` is the rule's own; `active` and
/// `active` is shared by every rule of the same algorithm in the namespace;
/// `keys` is scoped to this rule.
#[derive(Debug, Default)]
pub(super) struct RuleTelemetry {
    /// Remaining budget for the key this replica last decided.
    remaining: AtomicU64,
    /// Active reservations of this namespace and algorithm, after the last
    /// decision.
    active: AtomicUsize,
    /// Retained keys of this rule, after the last
    /// decision.
    keys: AtomicUsize,
}

impl RuleTelemetry {
    /// Replace the snapshot.
    pub(super) fn record(&self, remaining: u64, active: usize, keys: usize) {
        self.remaining.store(remaining, Ordering::Relaxed);
        self.active.store(active, Ordering::Relaxed);
        self.keys.store(keys, Ordering::Relaxed);
    }

    /// Read the snapshot without I/O.
    pub(super) fn snapshot(&self) -> BackendSnapshot {
        BackendSnapshot {
            budget_remaining: self
                .remaining
                .load(Ordering::Relaxed)
                .min(super::MAX_REPORTED_REMAINING),
            active_reservations: self.active.load(Ordering::Relaxed),
            active_keys: self.keys.load(Ordering::Relaxed),
        }
    }
}

/// Hex SHA-256 of `parts` joined by a zero byte: every Valkey key name
/// derived from a namespace, rule, or budget key goes through this, so no
/// user identifier appears in the keyspace.
pub(super) fn key_hash(parts: &[&[u8]]) -> String {
    let mut digest = Sha256::new();
    for (index, part) in parts.iter().enumerate() {
        if index > 0 {
            digest.update(&[0]);
        }
        digest.update(part);
    }
    digest.finish().iter().map(|byte| format!("{byte:02x}")).collect()
}

/// Extend a namespace-wide key's TTL to at least `ttl_ms` without ever
/// shortening it: `NX` covers the key having no TTL yet (a fresh
/// namespace), `GT` then only ever lengthens an existing one, so a rule
/// with a short TTL can never cut short another rule's still-live entries
/// in the same namespace.
pub(super) fn extend_shared_ttl(pipe: &mut redis::Pipeline, key: &str, ttl_ms: u64) {
    pipe.cmd("PEXPIRE").arg(key).arg(ttl_ms).arg("NX").ignore();
    pipe.cmd("PEXPIRE").arg(key).arg(ttl_ms).arg("GT").ignore();
}

/// Parse `"estimate|admitted_at_ms"` read from a per-reservation key.
pub(super) fn parse_reservation(value: &str) -> Result<(u64, u64), BackendError> {
    let (estimate, admitted_at) = value.split_once('|').ok_or(BackendError::InvalidResponse)?;
    Ok((
        estimate.parse().map_err(|_error| BackendError::InvalidResponse)?,
        admitted_at.parse().map_err(|_error| BackendError::InvalidResponse)?,
    ))
}

/// A non-negative reply used as a count.
pub(super) fn count(value: i64) -> Result<usize, BackendError> {
    usize::try_from(value).map_err(|_error| BackendError::InvalidResponse)
}

/// A non-negative reply used as a token amount.
pub(super) fn amount(value: i64) -> Result<u64, BackendError> {
    u64::try_from(value).map_err(|_error| BackendError::InvalidResponse)
}

#[cfg(test)]
#[expect(clippy::allow_attributes, reason = "blanket test suppressions")]
#[allow(clippy::unwrap_used, reason = "tests")]
mod tests {
    use super::{
        BackendError, RuleTelemetry, accounting_config_key, amount, count, key_hash, parse_reservation,
        sliding_window_config_fingerprint, token_bucket_config_fingerprint, validate_accounting_config,
    };
    use crate::token_rate_limit::{AccountingPolicy, CompiledEstimation, ledger::Budget, weights::TokenWeights};

    fn test_policy() -> AccountingPolicy {
        AccountingPolicy {
            estimation: CompiledEstimation::Fixed { estimate: 100 },
            weights: TokenWeights::UNITY,
            key_fingerprint: "test-key-policy".to_owned(),
        }
    }

    #[test]
    fn accounting_fingerprints_canonicalize_budgets() {
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
        let policy = test_policy();

        let sliding = sliding_window_config_fingerprint(&budgets, 30_000, 100, 1_000, &policy);
        assert_eq!(
            sliding,
            sliding_window_config_fingerprint(&reversed, 30_000, 100, 1_000, &policy),
            "budget declaration order must not create configuration skew"
        );
        assert_ne!(
            sliding,
            sliding_window_config_fingerprint(&budgets, 30_001, 100, 1_000, &policy)
        );
    }

    #[test]
    fn accounting_fingerprints_are_algorithm_specific() {
        let policy = test_policy();
        let sliding = sliding_window_config_fingerprint(
            &[Budget {
                window_ms: 60_000,
                capacity: 1_000,
            }],
            30_000,
            100,
            1_000,
            &policy,
        );
        let bucket = token_bucket_config_fingerprint(1_000, 1.25, 30_000, 100, 1_000, &policy);
        assert_ne!(sliding, bucket, "algorithm identity is part of the fingerprint");
        assert_ne!(
            bucket,
            token_bucket_config_fingerprint(1_000, 1.5, 30_000, 100, 1_000, &policy)
        );
        assert_ne!(
            accounting_config_key("ns", "sw", "rule"),
            accounting_config_key("ns", "tb", "rule"),
            "algorithm markers must never collide"
        );
    }

    #[test]
    fn sliding_fingerprint_includes_state_bounds() {
        let budgets = [
            Budget {
                window_ms: 60_000,
                capacity: 1_000,
            },
            Budget {
                window_ms: 1_000,
                capacity: 100,
            },
        ];
        let policy = test_policy();
        let sliding = sliding_window_config_fingerprint(&budgets, 30_000, 100, 1_000, &policy);
        assert_ne!(
            sliding,
            sliding_window_config_fingerprint(&budgets, 30_000, 101, 1_000, &policy)
        );
        assert_ne!(
            sliding,
            sliding_window_config_fingerprint(&budgets, 30_000, 100, 1_001, &policy)
        );
    }

    #[expect(
        clippy::too_many_lines,
        reason = "covers estimation and reconciliation-policy changes for both algorithms"
    )]
    #[test]
    fn accounting_fingerprints_include_stable_reservation_policy() {
        let budgets = [Budget {
            window_ms: 60_000,
            capacity: 1_000,
        }];
        let policy = test_policy();
        let changed_estimation = AccountingPolicy {
            estimation: CompiledEstimation::Fixed { estimate: 101 },
            weights: policy.weights,
            key_fingerprint: policy.key_fingerprint.clone(),
        };
        let changed_weights = AccountingPolicy {
            estimation: policy.estimation.clone(),
            weights: TokenWeights {
                cached_input: 0.5,
                ..policy.weights
            },
            key_fingerprint: policy.key_fingerprint.clone(),
        };
        let changed_key = AccountingPolicy {
            estimation: policy.estimation.clone(),
            weights: policy.weights,
            key_fingerprint: "different-key-policy".to_owned(),
        };

        let sliding = sliding_window_config_fingerprint(&budgets, 30_000, 100, 1_000, &policy);
        assert_ne!(
            sliding,
            sliding_window_config_fingerprint(&budgets, 30_000, 100, 1_000, &changed_estimation)
        );
        assert_ne!(
            sliding,
            sliding_window_config_fingerprint(&budgets, 30_000, 100, 1_000, &changed_weights)
        );
        assert_ne!(
            sliding,
            sliding_window_config_fingerprint(&budgets, 30_000, 100, 1_000, &changed_key)
        );

        let bucket = token_bucket_config_fingerprint(1_000, 1.25, 30_000, 100, 1_000, &policy);
        assert_ne!(
            bucket,
            token_bucket_config_fingerprint(1_000, 1.25, 30_000, 100, 1_000, &changed_estimation)
        );
        assert_ne!(
            bucket,
            token_bucket_config_fingerprint(1_000, 1.25, 30_000, 100, 1_000, &changed_weights)
        );
        assert_ne!(
            bucket,
            token_bucket_config_fingerprint(1_000, 1.25, 30_000, 100, 1_000, &changed_key)
        );
    }

    #[test]
    fn accounting_marker_validation_fails_closed_for_mismatch_or_loss() {
        assert!(validate_accounting_config(Some("expected"), "expected").is_ok());
        assert!(matches!(
            validate_accounting_config(Some("other"), "expected"),
            Err(BackendError::ConfigurationMismatch)
        ));
        assert!(matches!(
            validate_accounting_config(None, "expected"),
            Err(BackendError::InvalidResponse)
        ));
    }

    #[test]
    fn key_hash_is_stable_and_separator_sensitive() {
        let a = key_hash(&[b"ns", b"rule", b"alice"]);
        assert_eq!(a, key_hash(&[b"ns", b"rule", b"alice"]), "same parts, same hash");
        assert_ne!(
            a,
            key_hash(&[b"nsrule", b"alice"]),
            "parts are separated, not concatenated"
        );
        assert_eq!(a.len(), 64, "hex sha-256");
    }

    #[test]
    fn telemetry_snapshot_reports_the_last_recorded_values() {
        let telemetry = RuleTelemetry::default();
        telemetry.record(40, 2, 3);
        let snapshot = telemetry.snapshot();
        assert_eq!(
            snapshot.budget_remaining, 40,
            "remaining must round-trip through record/snapshot"
        );
        assert_eq!(
            snapshot.active_reservations, 2,
            "active must round-trip through record/snapshot"
        );
        assert_eq!(snapshot.active_keys, 3, "keys must round-trip through record/snapshot");
    }

    #[test]
    fn negative_replies_are_invalid_responses() {
        assert!(count(-1).is_err(), "a negative count cannot come from ZCARD");
        assert!(amount(-5).is_err(), "a negative amount cannot come from a counter sum");
        assert_eq!(
            amount(7).unwrap(),
            7,
            "a non-negative amount reply must pass through unchanged"
        );
        assert_eq!(
            count(7).unwrap(),
            7,
            "a non-negative count reply must pass through unchanged"
        );
    }

    #[test]
    fn reservation_values_parse_or_are_invalid_responses() {
        assert_eq!(
            parse_reservation("60|1000").unwrap(),
            (60, 1_000),
            "estimate and admission time round-trip"
        );
        for malformed in ["60", "x|1000", "60|", "-1|1000"] {
            assert!(
                matches!(parse_reservation(malformed), Err(BackendError::InvalidResponse)),
                "{malformed:?} is not a reservation value"
            );
        }
    }
}
