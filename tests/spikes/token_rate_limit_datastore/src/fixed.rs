use std::{
    collections::BTreeMap,
    fmt::Write as _,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};
use clap::ValueEnum;
use redis::{Script, aio::MultiplexedConnection};
use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::key_schema::{fixed_sliding_window_keys, fixed_token_bucket_keys};

const SOURCE_COMMIT: &str = "b6fe4704af72c05167a280d5070a291997d2085d";
const CONFIG_SCHEMA: &str = "v1";
const CONNECT_TIMEOUT: Duration = Duration::from_millis(500);
const RESPONSE_TIMEOUT: Duration = Duration::from_millis(500);
const OPERATION_DEADLINE: Duration = Duration::from_millis(1_000);
const SLIDING_RESERVE: &str =
    include_str!("../fixtures/source/fixed/lua/sliding_window_reserve.lua");
const SLIDING_RECONCILE: &str =
    include_str!("../fixtures/source/fixed/lua/sliding_window_reconcile.lua");
const BUCKET_RESERVE: &str = include_str!("../fixtures/source/fixed/lua/token_bucket_reserve.lua");
const BUCKET_RECONCILE: &str =
    include_str!("../fixtures/source/fixed/lua/token_bucket_reconcile.lua");

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Invocation {
    Eval,
    EvalSha,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SlidingConfig {
    pub(crate) budgets: Vec<(u64, u64)>,
    pub(crate) reservation_timeout_ms: u64,
    pub(crate) max_keys: usize,
    pub(crate) max_active_reservations: usize,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct BucketConfig {
    pub(crate) capacity: u64,
    pub(crate) refill_rate: f64,
    pub(crate) reservation_timeout_ms: u64,
    pub(crate) max_keys: usize,
    pub(crate) max_active_reservations: usize,
}

#[derive(Debug, Serialize)]
pub struct FixedStandaloneReport {
    schema_version: u8,
    candidate: String,
    source_commit: String,
    product: String,
    expected_version: String,
    protocol: String,
    client: ClientProfile,
    server_identity: BTreeMap<String, String>,
    server_config: BTreeMap<String, String>,
    invocations: Vec<InvocationReport>,
    eval_evalsha_semantics_equal: bool,
    eval_evalsha_invariant_outcomes_equal: bool,
    failures: Vec<String>,
}

#[derive(Debug, Serialize)]
struct ClientProfile {
    crate_name: String,
    version: String,
    checksum: String,
    features: Vec<String>,
    connect_timeout_ms: u64,
    response_timeout_ms: u64,
    operation_deadline_ms: u64,
    automatic_mutation_retries: u8,
}

#[derive(Debug, Serialize)]
pub(crate) struct InvocationReport {
    invocation: Invocation,
    events: Vec<Event>,
    invariants: Vec<Invariant>,
    command_stats: BTreeMap<String, String>,
    scripts_present_before: Vec<i64>,
    scripts_present_after: Vec<i64>,
    final_active_counts: BTreeMap<String, i64>,
    passed: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct SemanticEvent {
    name: String,
    outcome: String,
    values: Vec<i64>,
}

#[derive(Debug, Serialize)]
struct Event {
    name: String,
    elapsed_us: u128,
    reply: Vec<i64>,
    semantic: SemanticEvent,
    passed: bool,
    assertion: String,
}

#[derive(Debug, Serialize)]
struct Invariant {
    name: String,
    assertion: String,
    observed: serde_json::Value,
    passed: bool,
}

#[derive(Clone, Debug)]
struct RawKeySnapshot {
    key: String,
    dump: Option<Vec<u8>>,
    pttl_ms: i64,
    expires_at_unix_ms: i64,
}

#[derive(Debug, Serialize)]
struct KeySnapshotEvidence {
    key: String,
    dump_sha256: Option<String>,
    dump_bytes: usize,
    pttl_ms: i64,
    expires_at_unix_ms: i64,
}

#[derive(Clone, Copy)]
enum ScriptId {
    SlidingReserve,
    SlidingReconcile,
    BucketReserve,
    BucketReconcile,
}

pub(crate) struct FixedExecutor {
    invocation: Invocation,
    sliding_reserve: Script,
    sliding_reconcile: Script,
    bucket_reserve: Script,
    bucket_reconcile: Script,
}

impl FixedExecutor {
    pub(crate) fn new(invocation: Invocation) -> Self {
        Self {
            invocation,
            sliding_reserve: Script::new(SLIDING_RESERVE),
            sliding_reconcile: Script::new(SLIDING_RECONCILE),
            bucket_reserve: Script::new(BUCKET_RESERVE),
            bucket_reconcile: Script::new(BUCKET_RECONCILE),
        }
    }

    fn script(&self, id: ScriptId) -> (&str, &Script) {
        match id {
            ScriptId::SlidingReserve => (SLIDING_RESERVE, &self.sliding_reserve),
            ScriptId::SlidingReconcile => (SLIDING_RECONCILE, &self.sliding_reconcile),
            ScriptId::BucketReserve => (BUCKET_RESERVE, &self.bucket_reserve),
            ScriptId::BucketReconcile => (BUCKET_RECONCILE, &self.bucket_reconcile),
        }
    }

    async fn invoke(
        &self,
        connection: &mut MultiplexedConnection,
        id: ScriptId,
        keys: &[String],
        args: &[String],
    ) -> Result<Vec<i64>> {
        let (source, script) = self.script(id);
        match self.invocation {
            Invocation::Eval => {
                let mut command = redis::cmd("EVAL");
                command.arg(source).arg(keys.len()).arg(keys).arg(args);
                command.query_async(connection).await.context("EVAL failed")
            }
            Invocation::EvalSha => {
                let mut invocation = script.prepare_invoke();
                for key in keys {
                    invocation.key(key);
                }
                for arg in args {
                    invocation.arg(arg);
                }
                invocation
                    .invoke_async(connection)
                    .await
                    .context("redis::Script invocation failed")
            }
        }
    }

    pub(crate) async fn sliding_reserve(
        &self,
        connection: &mut MultiplexedConnection,
        keys: &[String],
        config: &SlidingConfig,
        estimate: u64,
    ) -> Result<Vec<i64>> {
        self.invoke(
            connection,
            ScriptId::SlidingReserve,
            keys,
            &sliding_reserve_args(config, estimate),
        )
        .await
    }

    pub(crate) async fn sliding_reconcile(
        &self,
        connection: &mut MultiplexedConnection,
        keys: &[String],
        config: &SlidingConfig,
        reservation_id: i64,
        actual: u64,
    ) -> Result<Vec<i64>> {
        self.invoke(
            connection,
            ScriptId::SlidingReconcile,
            keys,
            &sliding_reconcile_args(config, reservation_id, actual),
        )
        .await
    }

    pub(crate) async fn bucket_reserve(
        &self,
        connection: &mut MultiplexedConnection,
        keys: &[String],
        config: &BucketConfig,
        estimate: u64,
    ) -> Result<Vec<i64>> {
        self.invoke(
            connection,
            ScriptId::BucketReserve,
            keys,
            &bucket_reserve_args(config, estimate),
        )
        .await
    }

    pub(crate) async fn bucket_reconcile(
        &self,
        connection: &mut MultiplexedConnection,
        keys: &[String],
        config: &BucketConfig,
        reservation_id: i64,
        actual: u64,
    ) -> Result<Vec<i64>> {
        self.invoke(
            connection,
            ScriptId::BucketReconcile,
            keys,
            &bucket_reconcile_args(config, reservation_id, actual),
        )
        .await
    }

    pub(crate) fn hashes(&self) -> Vec<String> {
        [
            &self.sliding_reserve,
            &self.sliding_reconcile,
            &self.bucket_reserve,
            &self.bucket_reconcile,
        ]
        .into_iter()
        .map(|script| script.get_hash().to_owned())
        .collect()
    }
}

fn digest_hex(digest: Sha256) -> String {
    digest
        .finalize()
        .iter()
        .fold(String::with_capacity(64), |mut output, byte| {
            write!(output, "{byte:02x}").expect("writing to a String cannot fail");
            output
        })
}

pub(crate) fn sliding_fingerprint(config: &SlidingConfig) -> String {
    let mut digest = Sha256::new();
    digest.update(b"praxis:token_rate_limit:accounting_config");
    digest.update([0]);
    digest.update(CONFIG_SCHEMA.as_bytes());
    digest.update([0]);
    digest.update(b"sliding_window");
    digest.update([0]);
    let mut budgets = config.budgets.clone();
    budgets.sort_unstable();
    digest.update((budgets.len() as u64).to_be_bytes());
    for (window_ms, capacity) in budgets {
        digest.update(window_ms.to_be_bytes());
        digest.update(capacity.to_be_bytes());
    }
    digest.update(config.reservation_timeout_ms.to_be_bytes());
    digest.update(config.max_keys.to_string().as_bytes());
    digest.update([0]);
    digest.update(config.max_active_reservations.to_string().as_bytes());
    format!("{CONFIG_SCHEMA}:{}", digest_hex(digest))
}

pub(crate) fn bucket_fingerprint(config: &BucketConfig) -> String {
    let mut digest = Sha256::new();
    digest.update(b"praxis:token_rate_limit:accounting_config");
    digest.update([0]);
    digest.update(CONFIG_SCHEMA.as_bytes());
    digest.update([0]);
    digest.update(b"token_bucket");
    digest.update([0]);
    digest.update(config.capacity.to_be_bytes());
    digest.update(config.refill_rate.to_bits().to_be_bytes());
    digest.update(config.reservation_timeout_ms.to_be_bytes());
    digest.update(config.max_keys.to_string().as_bytes());
    digest.update([0]);
    digest.update(config.max_active_reservations.to_string().as_bytes());
    format!("{CONFIG_SCHEMA}:{}", digest_hex(digest))
}

fn sliding_reserve_args(config: &SlidingConfig, estimate: u64) -> Vec<String> {
    let mut args = vec![
        config.reservation_timeout_ms.to_string(),
        config.max_keys.to_string(),
        config.max_active_reservations.to_string(),
        estimate.to_string(),
        config.budgets.len().to_string(),
    ];
    for (window_ms, capacity) in &config.budgets {
        args.push(window_ms.to_string());
        args.push(capacity.to_string());
    }
    args.push(sliding_fingerprint(config));
    args
}

fn sliding_reconcile_args(config: &SlidingConfig, reservation_id: i64, actual: u64) -> Vec<String> {
    let mut args = vec![
        reservation_id.to_string(),
        actual.to_string(),
        config.budgets.len().to_string(),
        config.reservation_timeout_ms.to_string(),
    ];
    for (window_ms, capacity) in &config.budgets {
        args.push(window_ms.to_string());
        args.push(capacity.to_string());
    }
    args.push(sliding_fingerprint(config));
    args
}

fn bucket_reserve_args(config: &BucketConfig, estimate: u64) -> Vec<String> {
    vec![
        config.capacity.to_string(),
        config.refill_rate.to_string(),
        config.reservation_timeout_ms.to_string(),
        config.max_keys.to_string(),
        config.max_active_reservations.to_string(),
        estimate.to_string(),
        bucket_fingerprint(config),
    ]
}

fn bucket_reconcile_args(config: &BucketConfig, reservation_id: i64, actual: u64) -> Vec<String> {
    vec![
        reservation_id.to_string(),
        actual.to_string(),
        config.capacity.to_string(),
        config.refill_rate.to_string(),
        config.reservation_timeout_ms.to_string(),
        bucket_fingerprint(config),
    ]
}

fn parse_info(info: &str) -> BTreeMap<String, String> {
    info.lines()
        .filter(|line| !line.starts_with('#'))
        .filter_map(|line| line.trim_end().split_once(':'))
        .map(|(key, value)| (key.to_owned(), value.to_owned()))
        .collect()
}

pub(crate) fn selected_server_identity(info: &str) -> BTreeMap<String, String> {
    let parsed = parse_info(info);
    [
        "redis_version",
        "valkey_version",
        "redis_mode",
        "os",
        "arch_bits",
        "process_id",
        "run_id",
    ]
    .into_iter()
    .filter_map(|key| parsed.get(key).map(|value| (key.to_owned(), value.clone())))
    .collect()
}

fn semantic(name: &str, reply: &[i64]) -> SemanticEvent {
    let token_bucket = name.starts_with("token_bucket.");
    let (outcome, values) = if name.contains("reserve") {
        match reply.first() {
            Some(1) if reply.len() == 7 && token_bucket => {
                ("admitted", vec![reply[2], reply[5], reply[6]])
            }
            Some(1) if reply.len() == 7 => ("admitted", reply[2..].to_vec()),
            Some(0) if reply.len() == 5 && token_bucket => ("denied", vec![reply[3], reply[4]]),
            Some(0) if reply.len() == 5 => ("denied", reply[2..].to_vec()),
            Some(3) if reply.len() == 1 => ("configuration_mismatch", Vec::new()),
            _ => ("invalid", reply.to_vec()),
        }
    } else {
        match reply.first() {
            Some(1) if reply.len() == 7 && token_bucket => (
                "applied",
                vec![reply[1], reply[2], reply[3], reply[5], reply[6]],
            ),
            Some(1) if reply.len() == 7 => ("applied", reply[1..].to_vec()),
            Some(0) if reply.len() == 4 && token_bucket => ("noop", vec![reply[2], reply[3]]),
            Some(0) if reply.len() == 4 => ("noop", reply[1..].to_vec()),
            Some(3) if reply.len() == 1 => ("configuration_mismatch", Vec::new()),
            _ => ("invalid", reply.to_vec()),
        }
    };
    SemanticEvent {
        name: name.to_owned(),
        outcome: outcome.to_owned(),
        values,
    }
}

async fn event<F>(
    events: &mut Vec<Event>,
    name: &str,
    assertion: &str,
    operation: F,
    predicate: impl FnOnce(&[i64]) -> bool,
) -> Result<Vec<i64>>
where
    F: Future<Output = Result<Vec<i64>>>,
{
    let started = Instant::now();
    let reply = tokio::time::timeout(OPERATION_DEADLINE, operation)
        .await
        .with_context(|| format!("{name} exceeded the end-to-end operation deadline"))??;
    let passed = predicate(&reply);
    events.push(Event {
        name: name.to_owned(),
        elapsed_us: started.elapsed().as_micros(),
        semantic: semantic(name, &reply),
        reply: reply.clone(),
        passed,
        assertion: assertion.to_owned(),
    });
    Ok(reply)
}

async fn snapshot(
    connection: &mut MultiplexedConnection,
    keys: &[String],
) -> Result<Vec<RawKeySnapshot>> {
    let mut snapshots = Vec::with_capacity(keys.len());
    for key in keys {
        let dump: Option<Vec<u8>> = redis::cmd("DUMP").arg(key).query_async(connection).await?;
        let pttl_ms: i64 = redis::cmd("PTTL").arg(key).query_async(connection).await?;
        let expires_at_unix_ms: i64 = redis::cmd("PEXPIRETIME")
            .arg(key)
            .query_async(connection)
            .await?;
        snapshots.push(RawKeySnapshot {
            key: key.clone(),
            dump,
            pttl_ms,
            expires_at_unix_ms,
        });
    }
    Ok(snapshots)
}

fn snapshot_evidence(snapshot: &[RawKeySnapshot]) -> Vec<KeySnapshotEvidence> {
    snapshot
        .iter()
        .map(|entry| KeySnapshotEvidence {
            key: entry.key.clone(),
            dump_sha256: entry.dump.as_ref().map(|dump| {
                let mut digest = Sha256::new();
                digest.update(dump);
                digest_hex(digest)
            }),
            dump_bytes: entry.dump.as_ref().map_or(0, Vec::len),
            pttl_ms: entry.pttl_ms,
            expires_at_unix_ms: entry.expires_at_unix_ms,
        })
        .collect()
}

fn snapshot_unchanged(before: &[RawKeySnapshot], after: &[RawKeySnapshot]) -> bool {
    if before.len() != after.len() {
        return false;
    }
    before.iter().zip(after).all(|(before, after)| {
        before.key == after.key
            && before.dump == after.dump
            && before.expires_at_unix_ms == after.expires_at_unix_ms
    })
}

#[allow(clippy::too_many_lines)]
async fn sliding_lifecycle(
    executor: &FixedExecutor,
    connection: &mut MultiplexedConnection,
    namespace: &str,
    events: &mut Vec<Event>,
) -> Result<i64> {
    let keys = fixed_sliding_window_keys(namespace, "sliding-rule", "subject-a");
    let config = SlidingConfig {
        budgets: vec![(60_000, 10)],
        reservation_timeout_ms: 60_000,
        max_keys: 100,
        max_active_reservations: 100,
    };
    let first = event(
        events,
        "sliding_window.reserve.first",
        "first reserve admits estimate=4 with usage=4, remaining=6, active=1, keys=1",
        executor.sliding_reserve(connection, &keys, &config, 4),
        |reply| reply.len() == 7 && reply[0] == 1 && reply[2..] == [4, 4, 6, 1, 1],
    )
    .await?;
    let second = event(
        events,
        "sliding_window.reserve.second",
        "second reserve admits and leaves two tokens",
        executor.sliding_reserve(connection, &keys, &config, 4),
        |reply| reply.len() == 7 && reply[0] == 1 && reply[2..] == [4, 8, 2, 2, 1],
    )
    .await?;
    event(
        events,
        "sliding_window.reserve.denied",
        "third reserve is denied without creating another active reservation",
        executor.sliding_reserve(connection, &keys, &config, 4),
        |reply| reply == [0, 60_000, 2, 2, 1],
    )
    .await?;
    event(
        events,
        "sliding_window.reconcile.refund",
        "reconcile applies a two-token refund",
        executor.sliding_reconcile(connection, &keys, &config, first[1], 2),
        |reply| reply == [1, 2, 2, 0, 4, 1, 1],
    )
    .await?;
    event(
        events,
        "sliding_window.reconcile.duplicate",
        "duplicate reconcile is a no-op",
        executor.sliding_reconcile(connection, &keys, &config, first[1], 2),
        |reply| reply == [0, 4, 1, 1],
    )
    .await?;
    let third = event(
        events,
        "sliding_window.reserve.after_refund",
        "refund makes room for one further reservation",
        executor.sliding_reserve(connection, &keys, &config, 4),
        |reply| reply.len() == 7 && reply[0] == 1 && reply[2..] == [4, 10, 0, 2, 1],
    )
    .await?;
    event(
        events,
        "sliding_window.reconcile.overage",
        "reconcile applies a two-token overage",
        executor.sliding_reconcile(connection, &keys, &config, second[1], 6),
        |reply| reply == [1, 6, 0, 2, 0, 1, 1],
    )
    .await?;
    event(
        events,
        "sliding_window.reconcile.equal",
        "equal actual usage settles the final active reservation",
        executor.sliding_reconcile(connection, &keys, &config, third[1], 4),
        |reply| reply == [1, 4, 0, 0, 0, 0, 1],
    )
    .await?;
    Ok(redis::cmd("GET")
        .arg(&keys[7])
        .query_async::<Option<i64>>(connection)
        .await?
        .unwrap_or_default())
}

#[allow(clippy::too_many_lines)]
async fn bucket_lifecycle(
    executor: &FixedExecutor,
    connection: &mut MultiplexedConnection,
    namespace: &str,
    events: &mut Vec<Event>,
) -> Result<i64> {
    let keys = fixed_token_bucket_keys(namespace, "bucket-rule", "subject-a");
    let config = BucketConfig {
        capacity: 10,
        refill_rate: 0.0001,
        reservation_timeout_ms: 60_000,
        max_keys: 100,
        max_active_reservations: 100,
    };
    let first = event(
        events,
        "token_bucket.reserve.first",
        "first reserve admits estimate=4",
        executor.bucket_reserve(connection, &keys, &config, 4),
        |reply| {
            reply.len() == 7 && reply[0] == 1 && reply[2] == 4 && reply[3] == 4 && reply[4] == 6
        },
    )
    .await?;
    let second = event(
        events,
        "token_bucket.reserve.second",
        "second reserve admits and leaves approximately two whole tokens",
        executor.bucket_reserve(connection, &keys, &config, 4),
        |reply| {
            reply.len() == 7
                && reply[0] == 1
                && reply[2] == 4
                && reply[3] >= 7
                && (1..=2).contains(&reply[4])
        },
    )
    .await?;
    event(
        events,
        "token_bucket.reserve.denied",
        "third reserve is denied with a positive retry delay",
        executor.bucket_reserve(connection, &keys, &config, 4),
        |reply| reply.len() == 5 && reply[0] == 0 && reply[1] > 0 && reply[3] == 2,
    )
    .await?;
    event(
        events,
        "token_bucket.reconcile.refund",
        "reconcile applies a two-token refund",
        executor.bucket_reconcile(connection, &keys, &config, first[1], 2),
        |reply| reply.len() == 7 && reply[0..4] == [1, 2, 2, 0] && reply[5] == 1,
    )
    .await?;
    event(
        events,
        "token_bucket.reconcile.duplicate",
        "duplicate reconcile is a no-op",
        executor.bucket_reconcile(connection, &keys, &config, first[1], 2),
        |reply| reply.len() == 4 && reply[0] == 0 && reply[2] == 1,
    )
    .await?;
    let third = event(
        events,
        "token_bucket.reserve.after_refund",
        "refund makes room for one further reservation",
        executor.bucket_reserve(connection, &keys, &config, 4),
        |reply| reply.len() == 7 && reply[0] == 1 && reply[2] == 4,
    )
    .await?;
    event(
        events,
        "token_bucket.reconcile.overage",
        "reconcile applies a two-token overage",
        executor.bucket_reconcile(connection, &keys, &config, second[1], 6),
        |reply| reply.len() == 7 && reply[0..4] == [1, 6, 0, 2] && reply[5] == 1,
    )
    .await?;
    event(
        events,
        "token_bucket.reconcile.equal",
        "equal actual usage settles the final active reservation",
        executor.bucket_reconcile(connection, &keys, &config, third[1], 4),
        |reply| reply.len() == 7 && reply[0..4] == [1, 4, 0, 0] && reply[5] == 0,
    )
    .await?;
    Ok(redis::cmd("GET")
        .arg(&keys[6])
        .query_async::<Option<i64>>(connection)
        .await?
        .unwrap_or_default())
}

async fn sliding_mismatch_case(
    executor: &FixedExecutor,
    connection: &mut MultiplexedConnection,
    namespace: &str,
    case: &str,
    owner: &SlidingConfig,
    contender: &SlidingConfig,
) -> Result<Invariant> {
    let keys = fixed_sliding_window_keys(namespace, "rule", "subject");
    let admitted = executor
        .sliding_reserve(connection, &keys, owner, 1)
        .await?;
    if admitted.first() != Some(&1) {
        bail!("sliding mismatch case {case} setup was denied: {admitted:?}");
    }
    let before = snapshot(connection, &keys).await?;
    let reserve_mismatch = executor
        .sliding_reserve(connection, &keys, contender, 2)
        .await?;
    let after_reserve = snapshot(connection, &keys).await?;
    let reconcile_mismatch = executor
        .sliding_reconcile(connection, &keys, contender, admitted[1], 1)
        .await?;
    let after_reconcile = snapshot(connection, &keys).await?;
    let owner_reconcile = executor
        .sliding_reconcile(connection, &keys, owner, admitted[1], 1)
        .await?;
    let owner_duplicate = executor
        .sliding_reconcile(connection, &keys, owner, admitted[1], 1)
        .await?;
    let reserve_unchanged = snapshot_unchanged(&before, &after_reserve);
    let reconcile_unchanged = snapshot_unchanged(&before, &after_reconcile);
    let passed = sliding_fingerprint(owner) != sliding_fingerprint(contender)
        && reserve_mismatch == [3]
        && reconcile_mismatch == [3]
        && reserve_unchanged
        && reconcile_unchanged
        && owner_reconcile.first() == Some(&1)
        && owner_duplicate.first() == Some(&0);
    Ok(Invariant {
        name: format!("sliding_window.fingerprint.{case}"),
        assertion: "each semantic configuration mismatch is rejected before reserve or reconcile mutates state, and the owning reservation remains exactly-once reconcilable"
            .to_owned(),
        observed: serde_json::json!({
            "owner_fingerprint": sliding_fingerprint(owner),
            "contender_fingerprint": sliding_fingerprint(contender),
            "reserve_mismatch_reply": reserve_mismatch,
            "reconcile_mismatch_reply": reconcile_mismatch,
            "reserve_snapshot_unchanged": reserve_unchanged,
            "reconcile_snapshot_unchanged": reconcile_unchanged,
            "owner_reconcile_reply": owner_reconcile,
            "owner_duplicate_reply": owner_duplicate,
            "snapshot_before": snapshot_evidence(&before),
            "snapshot_after_mismatches": snapshot_evidence(&after_reconcile),
        }),
        passed,
    })
}

async fn bucket_mismatch_case(
    executor: &FixedExecutor,
    connection: &mut MultiplexedConnection,
    namespace: &str,
    case: &str,
    owner: &BucketConfig,
    contender: &BucketConfig,
) -> Result<Invariant> {
    let keys = fixed_token_bucket_keys(namespace, "rule", "subject");
    let admitted = executor.bucket_reserve(connection, &keys, owner, 1).await?;
    if admitted.first() != Some(&1) {
        bail!("token-bucket mismatch case {case} setup was denied: {admitted:?}");
    }
    let before = snapshot(connection, &keys).await?;
    let reserve_mismatch = executor
        .bucket_reserve(connection, &keys, contender, 2)
        .await?;
    let after_reserve = snapshot(connection, &keys).await?;
    let reconcile_mismatch = executor
        .bucket_reconcile(connection, &keys, contender, admitted[1], 1)
        .await?;
    let after_reconcile = snapshot(connection, &keys).await?;
    let owner_reconcile = executor
        .bucket_reconcile(connection, &keys, owner, admitted[1], 1)
        .await?;
    let owner_duplicate = executor
        .bucket_reconcile(connection, &keys, owner, admitted[1], 1)
        .await?;
    let reserve_unchanged = snapshot_unchanged(&before, &after_reserve);
    let reconcile_unchanged = snapshot_unchanged(&before, &after_reconcile);
    let passed = bucket_fingerprint(owner) != bucket_fingerprint(contender)
        && reserve_mismatch == [3]
        && reconcile_mismatch == [3]
        && reserve_unchanged
        && reconcile_unchanged
        && owner_reconcile.first() == Some(&1)
        && owner_duplicate.first() == Some(&0);
    Ok(Invariant {
        name: format!("token_bucket.fingerprint.{case}"),
        assertion: "each semantic configuration mismatch is rejected before reserve or reconcile mutates state, and the owning reservation remains exactly-once reconcilable"
            .to_owned(),
        observed: serde_json::json!({
            "owner_fingerprint": bucket_fingerprint(owner),
            "contender_fingerprint": bucket_fingerprint(contender),
            "reserve_mismatch_reply": reserve_mismatch,
            "reconcile_mismatch_reply": reconcile_mismatch,
            "reserve_snapshot_unchanged": reserve_unchanged,
            "reconcile_snapshot_unchanged": reconcile_unchanged,
            "owner_reconcile_reply": owner_reconcile,
            "owner_duplicate_reply": owner_duplicate,
            "snapshot_before": snapshot_evidence(&before),
            "snapshot_after_mismatches": snapshot_evidence(&after_reconcile),
        }),
        passed,
    })
}

#[allow(clippy::too_many_lines)]
async fn fingerprint_matrix(
    executor: &FixedExecutor,
    connection: &mut MultiplexedConnection,
    namespace: &str,
) -> Result<Vec<Invariant>> {
    let sliding_base = SlidingConfig {
        budgets: vec![(10_000, 10)],
        reservation_timeout_ms: 2_000,
        max_keys: 100,
        max_active_reservations: 100,
    };
    let sliding_cases = [
        (
            "budget_window",
            SlidingConfig {
                budgets: vec![(20_000, 10)],
                ..sliding_base.clone()
            },
        ),
        (
            "budget_capacity",
            SlidingConfig {
                budgets: vec![(10_000, 11)],
                ..sliding_base.clone()
            },
        ),
        (
            "budget_set",
            SlidingConfig {
                budgets: vec![(10_000, 10), (60_000, 100)],
                ..sliding_base.clone()
            },
        ),
        (
            "reservation_timeout",
            SlidingConfig {
                reservation_timeout_ms: 2_001,
                ..sliding_base.clone()
            },
        ),
        (
            "max_keys",
            SlidingConfig {
                max_keys: 101,
                ..sliding_base.clone()
            },
        ),
        (
            "max_active_reservations",
            SlidingConfig {
                max_active_reservations: 101,
                ..sliding_base.clone()
            },
        ),
    ];
    let bucket_base = BucketConfig {
        capacity: 10,
        refill_rate: 1.0,
        reservation_timeout_ms: 2_000,
        max_keys: 100,
        max_active_reservations: 100,
    };
    let bucket_cases = [
        (
            "capacity",
            BucketConfig {
                capacity: 11,
                ..bucket_base.clone()
            },
        ),
        (
            "refill_rate",
            BucketConfig {
                refill_rate: 1.5,
                ..bucket_base.clone()
            },
        ),
        (
            "reservation_timeout",
            BucketConfig {
                reservation_timeout_ms: 2_001,
                ..bucket_base.clone()
            },
        ),
        (
            "max_keys",
            BucketConfig {
                max_keys: 101,
                ..bucket_base.clone()
            },
        ),
        (
            "max_active_reservations",
            BucketConfig {
                max_active_reservations: 101,
                ..bucket_base.clone()
            },
        ),
    ];

    let mut invariants = Vec::new();
    for (case, changed) in sliding_cases {
        for (direction, owner, contender) in [
            ("forward", &sliding_base, &changed),
            ("reverse", &changed, &sliding_base),
        ] {
            invariants.push(
                sliding_mismatch_case(
                    executor,
                    connection,
                    &format!("{namespace}:sliding:{case}:{direction}"),
                    &format!("{case}.{direction}"),
                    owner,
                    contender,
                )
                .await?,
            );
        }
    }
    for (case, changed) in bucket_cases {
        for (direction, owner, contender) in [
            ("forward", &bucket_base, &changed),
            ("reverse", &changed, &bucket_base),
        ] {
            invariants.push(
                bucket_mismatch_case(
                    executor,
                    connection,
                    &format!("{namespace}:bucket:{case}:{direction}"),
                    &format!("{case}.{direction}"),
                    owner,
                    contender,
                )
                .await?,
            );
        }
    }

    let canonical_a = SlidingConfig {
        budgets: vec![(10_000, 10), (60_000, 100)],
        ..sliding_base
    };
    let canonical_b = SlidingConfig {
        budgets: vec![(60_000, 100), (10_000, 10)],
        ..canonical_a.clone()
    };
    let keys =
        fixed_sliding_window_keys(&format!("{namespace}:sliding:canonical"), "rule", "subject");
    let first = executor
        .sliding_reserve(connection, &keys, &canonical_a, 1)
        .await?;
    let second = executor
        .sliding_reserve(connection, &keys, &canonical_b, 2)
        .await?;
    if first.first() == Some(&1) {
        executor
            .sliding_reconcile(connection, &keys, &canonical_a, first[1], 1)
            .await?;
    }
    if second.first() == Some(&1) {
        executor
            .sliding_reconcile(connection, &keys, &canonical_b, second[1], 2)
            .await?;
    }
    invariants.push(Invariant {
        name: "sliding_window.fingerprint.canonical_budget_order_and_estimate_exclusion".to_owned(),
        assertion: "budget order and per-request estimate are excluded from the persistent semantic fingerprint, so equivalent writers share one budget"
            .to_owned(),
        observed: serde_json::json!({
            "first_fingerprint": sliding_fingerprint(&canonical_a),
            "second_fingerprint": sliding_fingerprint(&canonical_b),
            "first_estimate": 1,
            "second_estimate": 2,
            "first_reply": first,
            "second_reply": second,
        }),
        passed: sliding_fingerprint(&canonical_a) == sliding_fingerprint(&canonical_b)
            && first.first() == Some(&1)
            && second.first() == Some(&1),
    });
    Ok(invariants)
}

#[allow(clippy::too_many_lines)]
async fn functional_skew_regressions(
    executor: &FixedExecutor,
    connection: &mut MultiplexedConnection,
    namespace: &str,
) -> Result<Vec<Invariant>> {
    let sliding_keys = fixed_sliding_window_keys(
        &format!("{namespace}:functional:sliding"),
        "rule",
        "subject",
    );
    let sliding_long = SlidingConfig {
        budgets: vec![(10_000, 5)],
        reservation_timeout_ms: 2_000,
        max_keys: 100,
        max_active_reservations: 100,
    };
    let sliding_short = SlidingConfig {
        budgets: vec![(10, 5)],
        reservation_timeout_ms: 100,
        ..sliding_long.clone()
    };
    let first = executor
        .sliding_reserve(connection, &sliding_keys, &sliding_long, 5)
        .await?;
    if first.first() != Some(&1) {
        bail!("functional sliding setup was denied: {first:?}");
    }
    executor
        .sliding_reconcile(connection, &sliding_keys, &sliding_long, first[1], 5)
        .await?;
    tokio::time::sleep(Duration::from_millis(30)).await;
    let before = snapshot(connection, &sliding_keys).await?;
    let mismatch = executor
        .sliding_reserve(connection, &sliding_keys, &sliding_short, 1)
        .await?;
    let after = snapshot(connection, &sliding_keys).await?;
    let retry = executor
        .sliding_reserve(connection, &sliding_keys, &sliding_long, 1)
        .await?;
    let settled_entries: i64 = redis::cmd("ZCARD")
        .arg(&sliding_keys[1])
        .query_async(connection)
        .await?;

    let bucket_keys =
        fixed_token_bucket_keys(&format!("{namespace}:functional:bucket"), "rule", "subject");
    let bucket_slow = BucketConfig {
        capacity: 10,
        refill_rate: 1.0,
        reservation_timeout_ms: 2_000,
        max_keys: 100,
        max_active_reservations: 100,
    };
    let bucket_fast = BucketConfig {
        refill_rate: 10.0,
        reservation_timeout_ms: 100,
        ..bucket_slow.clone()
    };
    let depleted = executor
        .bucket_reserve(connection, &bucket_keys, &bucket_slow, 10)
        .await?;
    if depleted.first() != Some(&1) {
        bail!("functional token-bucket setup was denied: {depleted:?}");
    }
    executor
        .bucket_reconcile(connection, &bucket_keys, &bucket_slow, depleted[1], 10)
        .await?;
    let bucket_before = snapshot(connection, &bucket_keys).await?;
    let bucket_mismatch = executor
        .bucket_reserve(connection, &bucket_keys, &bucket_fast, 10)
        .await?;
    let bucket_after = snapshot(connection, &bucket_keys).await?;
    let pttl_after_mismatch: i64 = redis::cmd("PTTL")
        .arg(&bucket_keys[0])
        .query_async(connection)
        .await?;
    tokio::time::sleep(Duration::from_millis(1_200)).await;
    let state_exists: i64 = redis::cmd("EXISTS")
        .arg(&bucket_keys[0])
        .query_async(connection)
        .await?;
    let bucket_retry = executor
        .bucket_reserve(connection, &bucket_keys, &bucket_slow, 10)
        .await?;

    Ok(vec![
        Invariant {
            name: "sliding_window.mixed_configuration_preserves_long_window_history".to_owned(),
            assertion: "the mismatched short-window writer is rejected without deleting settled history, and the owning long-window writer remains denied"
                .to_owned(),
            observed: serde_json::json!({
                "mismatch_reply": mismatch,
                "snapshot_unchanged": snapshot_unchanged(&before, &after),
                "settled_entries_after_mismatch": settled_entries,
                "long_writer_reply_after_mismatch": retry,
            }),
            passed: mismatch == [3]
                && snapshot_unchanged(&before, &after)
                && settled_entries == 1
                && retry.first() == Some(&0),
        },
        Invariant {
            name: "token_bucket.mixed_configuration_preserves_slow_refill_state".to_owned(),
            assertion: "the mismatched fast-refill writer is rejected without shortening depleted state, which remains enforced after 1.2 seconds"
                .to_owned(),
            observed: serde_json::json!({
                "mismatch_reply": bucket_mismatch,
                "snapshot_unchanged": snapshot_unchanged(&bucket_before, &bucket_after),
                "pttl_after_mismatch_ms": pttl_after_mismatch,
                "state_exists_after_1200ms": state_exists == 1,
                "slow_writer_reply_after_1200ms": bucket_retry,
            }),
            passed: bucket_mismatch == [3]
                && snapshot_unchanged(&bucket_before, &bucket_after)
                && pttl_after_mismatch > 10_000
                && state_exists == 1
                && bucket_retry.first() == Some(&0),
        },
    ])
}

async fn reset_server(connection: &mut MultiplexedConnection) -> Result<()> {
    redis::cmd("FLUSHDB")
        .arg("SYNC")
        .query_async::<String>(connection)
        .await
        .context("FLUSHDB failed")?;
    redis::cmd("SCRIPT")
        .arg("FLUSH")
        .arg("SYNC")
        .query_async::<String>(connection)
        .await
        .context("SCRIPT FLUSH failed")?;
    redis::cmd("CONFIG")
        .arg("RESETSTAT")
        .query_async::<String>(connection)
        .await
        .context("CONFIG RESETSTAT failed")?;
    Ok(())
}

async fn command_stats(connection: &mut MultiplexedConnection) -> Result<BTreeMap<String, String>> {
    let info: String = redis::cmd("INFO")
        .arg("COMMANDSTATS")
        .query_async(connection)
        .await?;
    Ok(parse_info(&info)
        .into_iter()
        .filter(|(key, _)| {
            [
                "eval", "evalsha", "script", "time", "get", "set", "hget", "hset", "zadd",
                "zrange", "dump", "pttl",
            ]
            .iter()
            .any(|command| key.starts_with(&format!("cmdstat_{command}")))
        })
        .collect())
}

pub(crate) async fn run_invocation(
    invocation: Invocation,
    connection: &mut MultiplexedConnection,
    product: &str,
) -> Result<InvocationReport> {
    reset_server(connection).await?;
    let executor = FixedExecutor::new(invocation);
    let namespace = format!("praxis:spike:fixed:{product}:{invocation:?}").to_lowercase();
    let hashes = executor.hashes();
    let scripts_present_before: Vec<i64> = redis::cmd("SCRIPT")
        .arg("EXISTS")
        .arg(&hashes)
        .query_async(connection)
        .await?;
    let mut events = Vec::new();
    let sliding_active = sliding_lifecycle(&executor, connection, &namespace, &mut events).await?;
    let bucket_active = bucket_lifecycle(&executor, connection, &namespace, &mut events).await?;
    let mut invariants = fingerprint_matrix(&executor, connection, &namespace).await?;
    invariants.extend(functional_skew_regressions(&executor, connection, &namespace).await?);
    let scripts_present_after: Vec<i64> = redis::cmd("SCRIPT")
        .arg("EXISTS")
        .arg(hashes)
        .query_async(connection)
        .await?;
    let final_active_counts = [
        ("sliding_window".to_owned(), sliding_active),
        ("token_bucket".to_owned(), bucket_active),
    ]
    .into_iter()
    .collect::<BTreeMap<_, _>>();
    let passed = events.iter().all(|event| event.passed)
        && invariants.iter().all(|invariant| invariant.passed)
        && final_active_counts.values().all(|count| *count == 0);
    Ok(InvocationReport {
        invocation,
        events,
        invariants,
        command_stats: command_stats(connection).await?,
        scripts_present_before,
        scripts_present_after,
        final_active_counts,
        passed,
    })
}

impl InvocationReport {
    #[must_use]
    pub(crate) fn passed(&self) -> bool {
        self.passed
    }
}

fn normalized(report: &InvocationReport) -> Vec<SemanticEvent> {
    report
        .events
        .iter()
        .map(|event| event.semantic.clone())
        .collect()
}

fn invariant_outcomes(report: &InvocationReport) -> Vec<(&str, bool)> {
    report
        .invariants
        .iter()
        .map(|invariant| (invariant.name.as_str(), invariant.passed))
        .collect()
}

pub async fn run_standalone(
    url: &str,
    product: &str,
    expected_version: &str,
) -> Result<FixedStandaloneReport> {
    let client = redis::Client::open(url).context("invalid datastore URL")?;
    let config = redis::AsyncConnectionConfig::new()
        .set_connection_timeout(Some(CONNECT_TIMEOUT))
        .set_response_timeout(Some(RESPONSE_TIMEOUT));
    let mut connection = tokio::time::timeout(
        OPERATION_DEADLINE,
        client.get_multiplexed_async_connection_with_config(&config),
    )
    .await
    .context("standalone connection exceeded the end-to-end deadline")?
    .context("standalone connection failed")?;
    let pong: String = tokio::time::timeout(
        OPERATION_DEADLINE,
        redis::cmd("PING").query_async(&mut connection),
    )
    .await
    .context("PING exceeded the end-to-end deadline")??;
    if pong != "PONG" {
        bail!("unexpected PING reply: {pong}");
    }
    let server_info: String = redis::cmd("INFO")
        .arg("SERVER")
        .query_async(&mut connection)
        .await?;
    let server_identity = selected_server_identity(&server_info);
    let server_config: BTreeMap<String, String> = redis::cmd("CONFIG")
        .arg("GET")
        .arg("appendonly")
        .arg("appendfsync")
        .arg("save")
        .arg("maxmemory")
        .arg("maxmemory-policy")
        .arg("min-replicas-to-write")
        .arg("min-replicas-max-lag")
        .arg("repl-diskless-sync")
        .query_async(&mut connection)
        .await?;
    let version_matches = ["redis_version", "valkey_version"]
        .into_iter()
        .filter_map(|key| server_identity.get(key))
        .any(|value| value.starts_with(expected_version));
    let eval = run_invocation(Invocation::Eval, &mut connection, product).await?;
    let evalsha = run_invocation(Invocation::EvalSha, &mut connection, product).await?;
    let semantics_equal = normalized(&eval) == normalized(&evalsha);
    let invariant_outcomes_equal = invariant_outcomes(&eval) == invariant_outcomes(&evalsha);
    let mut failures = Vec::new();
    if !version_matches {
        failures.push(format!(
            "server identity does not contain expected version {expected_version}"
        ));
    }
    for report in [&eval, &evalsha] {
        if !report.passed {
            failures.push(format!(
                "{:?} repaired-candidate correctness gate failed",
                report.invocation
            ));
        }
    }
    if !semantics_equal {
        failures.push("EVAL and EVALSHA normalized semantics differ".to_owned());
    }
    if !invariant_outcomes_equal {
        failures.push("EVAL and EVALSHA invariant outcomes differ".to_owned());
    }
    Ok(FixedStandaloneReport {
        schema_version: 1,
        candidate: "mixed_configuration_repair".to_owned(),
        source_commit: SOURCE_COMMIT.to_owned(),
        product: product.to_owned(),
        expected_version: expected_version.to_owned(),
        protocol: "RESP2".to_owned(),
        client: ClientProfile {
            crate_name: "redis".to_owned(),
            version: "1.7.0".to_owned(),
            checksum: "2acbc41a996f7652b2ddd9dfd98cc4ff602cfd742ae35382f07f608405ab50ed".to_owned(),
            features: vec![
                "tokio-comp".to_owned(),
                "sentinel".to_owned(),
                "cluster-async".to_owned(),
                "script".to_owned(),
            ],
            connect_timeout_ms: 500,
            response_timeout_ms: 500,
            operation_deadline_ms: 1_000,
            automatic_mutation_retries: 0,
        },
        server_identity,
        server_config,
        invocations: vec![eval, evalsha],
        eval_evalsha_semantics_equal: semantics_equal,
        eval_evalsha_invariant_outcomes_equal: invariant_outcomes_equal,
        failures,
    })
}

impl FixedStandaloneReport {
    #[must_use]
    pub fn passed(&self) -> bool {
        self.failures.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::{
        BucketConfig, RawKeySnapshot, SlidingConfig, bucket_fingerprint, sliding_fingerprint,
        snapshot_unchanged,
    };

    #[test]
    fn snapshot_comparison_uses_absolute_expiration() {
        let before = vec![RawKeySnapshot {
            key: "expiring-key".to_owned(),
            dump: Some(vec![1, 2, 3]),
            pttl_ms: 60_000,
            expires_at_unix_ms: 1_800_000_000_000,
        }];
        let naturally_decayed = vec![RawKeySnapshot {
            pttl_ms: 59_800,
            ..before[0].clone()
        }];
        assert!(snapshot_unchanged(&before, &naturally_decayed));

        let expiration_reset = vec![RawKeySnapshot {
            pttl_ms: 59_900,
            expires_at_unix_ms: 1_800_000_000_100,
            ..before[0].clone()
        }];
        assert!(!snapshot_unchanged(&before, &expiration_reset));
    }

    #[test]
    fn fingerprint_canonicalization_matches_candidate_contract() {
        let sliding = SlidingConfig {
            budgets: vec![(1_000, 10), (60_000, 100)],
            reservation_timeout_ms: 30_000,
            max_keys: 100,
            max_active_reservations: 1_000,
        };
        let mut reordered = sliding.clone();
        reordered.budgets.reverse();
        assert_eq!(
            sliding_fingerprint(&sliding),
            sliding_fingerprint(&reordered)
        );
        let mut changed = sliding.clone();
        changed.max_keys += 1;
        assert_ne!(sliding_fingerprint(&sliding), sliding_fingerprint(&changed));

        let bucket = BucketConfig {
            capacity: 1_000,
            refill_rate: 1.25,
            reservation_timeout_ms: 30_000,
            max_keys: 100,
            max_active_reservations: 1_000,
        };
        let mut changed = bucket.clone();
        changed.refill_rate = 1.5;
        assert_ne!(bucket_fingerprint(&bucket), bucket_fingerprint(&changed));
        assert_ne!(sliding_fingerprint(&sliding), bucket_fingerprint(&bucket));
    }
}
