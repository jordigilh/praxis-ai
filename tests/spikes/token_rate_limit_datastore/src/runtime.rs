use std::{collections::BTreeMap, time::Duration, time::Instant};

use anyhow::{Context, Result, bail};
use redis::{AsyncCommands as _, Script, aio::MultiplexedConnection};
use serde::Serialize;

use crate::key_schema::{sliding_window_keys, token_bucket_keys};

const SLIDING_RESERVE: &str =
    include_str!("../fixtures/source/baseline/lua/sliding_window_reserve.lua");
const SLIDING_RECONCILE: &str =
    include_str!("../fixtures/source/baseline/lua/sliding_window_reconcile.lua");
const BUCKET_RESERVE: &str =
    include_str!("../fixtures/source/baseline/lua/token_bucket_reserve.lua");
const BUCKET_RECONCILE: &str =
    include_str!("../fixtures/source/baseline/lua/token_bucket_reconcile.lua");
const CONNECT_TIMEOUT: Duration = Duration::from_millis(500);
const RESPONSE_TIMEOUT: Duration = Duration::from_millis(500);
const OPERATION_DEADLINE: Duration = Duration::from_millis(1_000);

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
enum Candidate {
    Eval,
    EvalSha,
}

#[derive(Debug, Serialize)]
pub struct StandaloneReport {
    schema_version: u8,
    product: String,
    expected_version: String,
    protocol: String,
    client: ClientProfile,
    server_identity: BTreeMap<String, String>,
    server_config: BTreeMap<String, String>,
    candidates: Vec<CandidateReport>,
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
struct CandidateReport {
    candidate: Candidate,
    events: Vec<Event>,
    invariants: Vec<Invariant>,
    algorithm_results: Vec<AlgorithmResult>,
    command_stats: BTreeMap<String, String>,
    scripts_present_before: Vec<i64>,
    scripts_present_after: Vec<i64>,
    final_active_counts: BTreeMap<String, i64>,
    passed: bool,
}

#[derive(Debug, Serialize)]
struct AlgorithmResult {
    algorithm: String,
    passed: bool,
    failed_assertions: Vec<String>,
}

#[derive(Debug, Serialize)]
struct Invariant {
    name: String,
    assertion: String,
    observed: serde_json::Value,
    passed: bool,
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
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

struct Executor {
    candidate: Candidate,
    sliding_reserve: Script,
    sliding_reconcile: Script,
    bucket_reserve: Script,
    bucket_reconcile: Script,
}

#[derive(Clone, Copy)]
enum ScriptId {
    SlidingReserve,
    SlidingReconcile,
    BucketReserve,
    BucketReconcile,
}

impl Executor {
    fn new(candidate: Candidate) -> Self {
        Self {
            candidate,
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
        match self.candidate {
            Candidate::Eval => {
                let mut command = redis::cmd("EVAL");
                command.arg(source).arg(keys.len()).arg(keys).arg(args);
                command.query_async(connection).await.context("EVAL failed")
            }
            Candidate::EvalSha => {
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

    fn hashes(&self) -> Vec<String> {
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

fn parse_info(info: &str) -> BTreeMap<String, String> {
    info.lines()
        .filter(|line| !line.starts_with('#'))
        .filter_map(|line| line.trim_end().split_once(':'))
        .map(|(key, value)| (key.to_owned(), value.to_owned()))
        .collect()
}

fn selected_server_identity(info: &str) -> BTreeMap<String, String> {
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

fn ttl_not_shortened(before_ms: i64, after_ms: i64, elapsed: Duration) -> bool {
    let elapsed_ms = i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX);
    after_ms >= before_ms.saturating_sub(elapsed_ms).saturating_sub(25)
}

#[allow(clippy::too_many_lines)]
async fn mixed_configuration_sliding_prune_invariant(
    executor: &Executor,
    connection: &mut MultiplexedConnection,
    namespace: &str,
) -> Result<Invariant> {
    let keys = sliding_window_keys(
        &format!("{namespace}:mixed-prune:sliding"),
        "rule",
        "subject",
    );
    let long_reserve = ["2000", "100", "100", "5", "1", "10000", "5"]
        .into_iter()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let long_first = executor
        .invoke(connection, ScriptId::SlidingReserve, &keys, &long_reserve)
        .await?;
    if long_first.first() != Some(&1) {
        bail!("long-window setup reservation was unexpectedly denied: {long_first:?}");
    }
    let long_reconcile = [
        long_first[1].to_string(),
        "5".to_owned(),
        "1".to_owned(),
        "2000".to_owned(),
        "10000".to_owned(),
        "5".to_owned(),
    ];
    executor
        .invoke(
            connection,
            ScriptId::SlidingReconcile,
            &keys,
            &long_reconcile,
        )
        .await?;
    let settled_before: i64 = redis::cmd("ZCARD")
        .arg(&keys[1])
        .query_async(connection)
        .await?;

    tokio::time::sleep(Duration::from_millis(30)).await;
    let short_reserve = ["100", "100", "100", "1", "1", "10", "5"]
        .into_iter()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let short_reply = executor
        .invoke(connection, ScriptId::SlidingReserve, &keys, &short_reserve)
        .await?;
    let settled_after_short_writer: i64 = redis::cmd("ZCARD")
        .arg(&keys[1])
        .query_async(connection)
        .await?;

    let long_retry = ["2000", "100", "100", "1", "1", "10000", "5"]
        .into_iter()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let long_reply_after_short_writer = executor
        .invoke(connection, ScriptId::SlidingReserve, &keys, &long_retry)
        .await?;

    for (reply, timeout, window) in [
        (&short_reply, "100", "10"),
        (&long_reply_after_short_writer, "2000", "10000"),
    ] {
        if reply.first() == Some(&1) {
            let reconcile = [
                reply[1].to_string(),
                "1".to_owned(),
                "1".to_owned(),
                timeout.to_owned(),
                window.to_owned(),
                "5".to_owned(),
            ];
            executor
                .invoke(connection, ScriptId::SlidingReconcile, &keys, &reconcile)
                .await?;
        }
    }

    let passed = settled_before == 1
        && settled_after_short_writer == 1
        && long_reply_after_short_writer.first() == Some(&0);
    Ok(Invariant {
        name: "sliding_window.mixed_configuration_preserves_long_window_history".to_owned(),
        assertion: "a short-window writer must not delete usage still enforced by a coexisting long-window writer"
            .to_owned(),
        observed: serde_json::json!({
            "settled_entries_before_short_writer": settled_before,
            "settled_entries_after_short_writer": settled_after_short_writer,
            "short_writer_reply": short_reply,
            "long_writer_reply_after_short_writer": long_reply_after_short_writer,
            "long_window_ms": 10_000,
            "short_window_ms": 10,
            "capacity": 5,
        }),
        passed,
    })
}

async fn mixed_configuration_bucket_expiry_invariant(
    executor: &Executor,
    connection: &mut MultiplexedConnection,
    namespace: &str,
) -> Result<Invariant> {
    let keys = token_bucket_keys(
        &format!("{namespace}:mixed-expiry:bucket"),
        "rule",
        "subject",
    );
    let slow_reserve = ["10", "1", "2000", "100", "100", "10"]
        .into_iter()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let first = executor
        .invoke(connection, ScriptId::BucketReserve, &keys, &slow_reserve)
        .await?;
    if first.first() != Some(&1) {
        bail!("slow-refill setup reservation was unexpectedly denied: {first:?}");
    }
    let first_reconcile = [
        first[1].to_string(),
        "10".to_owned(),
        "10".to_owned(),
        "1".to_owned(),
        "2000".to_owned(),
    ];
    executor
        .invoke(
            connection,
            ScriptId::BucketReconcile,
            &keys,
            &first_reconcile,
        )
        .await?;

    let fast_reserve = ["10", "10", "100", "100", "100", "10"]
        .into_iter()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let fast_reply = executor
        .invoke(connection, ScriptId::BucketReserve, &keys, &fast_reserve)
        .await?;
    let shortened_pttl_ms: i64 = redis::cmd("PTTL")
        .arg(&keys[0])
        .query_async(connection)
        .await?;
    tokio::time::sleep(Duration::from_millis(1_200)).await;
    let exists_after_short_ttl: i64 = redis::cmd("EXISTS")
        .arg(&keys[0])
        .query_async(connection)
        .await?;
    let slow_reply_after_expiry = executor
        .invoke(connection, ScriptId::BucketReserve, &keys, &slow_reserve)
        .await?;

    if slow_reply_after_expiry.first() == Some(&1) {
        let reconcile = [
            slow_reply_after_expiry[1].to_string(),
            "10".to_owned(),
            "10".to_owned(),
            "1".to_owned(),
            "2000".to_owned(),
        ];
        executor
            .invoke(connection, ScriptId::BucketReconcile, &keys, &reconcile)
            .await?;
    }

    let passed = fast_reply.first() == Some(&0)
        && exists_after_short_ttl == 1
        && slow_reply_after_expiry.first() == Some(&0);
    Ok(Invariant {
        name: "token_bucket.mixed_configuration_preserves_slow_refill_state".to_owned(),
        assertion: "a fast-refill writer must not expire a depleted slow-refill bucket and restore full capacity early"
            .to_owned(),
        observed: serde_json::json!({
            "fast_writer_reply": fast_reply,
            "pttl_after_fast_writer_ms": shortened_pttl_ms,
            "state_exists_after_1200ms": exists_after_short_ttl == 1,
            "slow_writer_reply_after_1200ms": slow_reply_after_expiry,
            "slow_refill_tokens_per_second": 1,
            "capacity": 10,
        }),
        passed,
    })
}

#[allow(clippy::too_many_lines)]
async fn mixed_configuration_ttl_scenarios(
    executor: &Executor,
    connection: &mut MultiplexedConnection,
    namespace: &str,
) -> Result<Vec<Invariant>> {
    let sliding_keys =
        sliding_window_keys(&format!("{namespace}:mixed-ttl:sliding"), "rule", "subject");
    let sliding_long = ["2000", "100", "100", "1", "1", "10000", "1000"]
        .into_iter()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let sliding_short = ["100", "100", "100", "1", "1", "1000", "1000"]
        .into_iter()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let sliding_first = executor
        .invoke(
            connection,
            ScriptId::SlidingReserve,
            &sliding_keys,
            &sliding_long,
        )
        .await?;
    let sliding_before: i64 = redis::cmd("PTTL")
        .arg(&sliding_keys[2])
        .query_async(connection)
        .await?;
    let sliding_started = Instant::now();
    let sliding_second = executor
        .invoke(
            connection,
            ScriptId::SlidingReserve,
            &sliding_keys,
            &sliding_short,
        )
        .await?;
    let sliding_elapsed = sliding_started.elapsed();
    let sliding_after: i64 = redis::cmd("PTTL")
        .arg(&sliding_keys[2])
        .query_async(connection)
        .await?;
    let sliding_passed = ttl_not_shortened(sliding_before, sliding_after, sliding_elapsed);

    for (reply, config) in [
        (&sliding_first, &sliding_long),
        (&sliding_second, &sliding_short),
    ] {
        if reply.first() == Some(&1) {
            let args = [
                reply[1].to_string(),
                "1".to_owned(),
                "1".to_owned(),
                config[0].clone(),
                config[5].clone(),
                config[6].clone(),
            ];
            executor
                .invoke(connection, ScriptId::SlidingReconcile, &sliding_keys, &args)
                .await?;
        }
    }

    let bucket_keys =
        token_bucket_keys(&format!("{namespace}:mixed-ttl:bucket"), "rule", "subject");
    let bucket_long = ["10", "1", "2000", "100", "100", "1"]
        .into_iter()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let bucket_short = ["10", "10", "100", "100", "100", "1"]
        .into_iter()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let bucket_first = executor
        .invoke(
            connection,
            ScriptId::BucketReserve,
            &bucket_keys,
            &bucket_long,
        )
        .await?;
    let bucket_before: i64 = redis::cmd("PTTL")
        .arg(&bucket_keys[0])
        .query_async(connection)
        .await?;
    let bucket_started = Instant::now();
    let bucket_second = executor
        .invoke(
            connection,
            ScriptId::BucketReserve,
            &bucket_keys,
            &bucket_short,
        )
        .await?;
    let bucket_elapsed = bucket_started.elapsed();
    let bucket_after: i64 = redis::cmd("PTTL")
        .arg(&bucket_keys[0])
        .query_async(connection)
        .await?;
    let bucket_passed = ttl_not_shortened(bucket_before, bucket_after, bucket_elapsed);

    for (reply, config) in [
        (&bucket_first, &bucket_long),
        (&bucket_second, &bucket_short),
    ] {
        if reply.first() == Some(&1) {
            let args = [
                reply[1].to_string(),
                "1".to_owned(),
                config[0].clone(),
                config[1].clone(),
                config[2].clone(),
            ];
            executor
                .invoke(connection, ScriptId::BucketReconcile, &bucket_keys, &args)
                .await?;
        }
    }

    let sliding_prune =
        mixed_configuration_sliding_prune_invariant(executor, connection, namespace).await?;
    let bucket_expiry =
        mixed_configuration_bucket_expiry_invariant(executor, connection, namespace).await?;

    Ok(vec![
        Invariant {
            name: "sliding_window.mixed_configuration_ttl_never_shortens".to_owned(),
            assertion: "a short-window writer must not shorten state still required by a long-window writer"
                .to_owned(),
            observed: serde_json::json!({
                "long_writer_pttl_ms": sliding_before,
                "after_short_writer_pttl_ms": sliding_after,
                "elapsed_ms": sliding_elapsed.as_millis(),
                "key": sliding_keys[2],
            }),
            passed: sliding_passed,
        },
        Invariant {
            name: "token_bucket.mixed_configuration_ttl_never_shortens".to_owned(),
            assertion: "a fast-refill writer must not shorten state still required by a slow-refill writer"
                .to_owned(),
            observed: serde_json::json!({
                "slow_refill_writer_pttl_ms": bucket_before,
                "after_fast_refill_writer_pttl_ms": bucket_after,
                "elapsed_ms": bucket_elapsed.as_millis(),
                "key": bucket_keys[0],
            }),
            passed: bucket_passed,
        },
        sliding_prune,
        bucket_expiry,
    ])
}

#[allow(clippy::too_many_lines)]
async fn sliding_scenario(
    executor: &Executor,
    connection: &mut MultiplexedConnection,
    namespace: &str,
    events: &mut Vec<Event>,
) -> Result<()> {
    let keys = sliding_window_keys(namespace, "sliding-rule", "subject-a");
    let reserve_args = ["60000", "100", "100", "4", "1", "60000", "10"]
        .into_iter()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let first = event(
        events,
        "sliding_window.reserve.first",
        "first reserve admits estimate=4 with usage=4, remaining=6, active=1, keys=1",
        executor.invoke(connection, ScriptId::SlidingReserve, &keys, &reserve_args),
        |reply| reply.len() == 7 && reply[0] == 1 && reply[2..] == [4, 4, 6, 1, 1],
    )
    .await?;
    let first_id = first[1];

    let second = event(
        events,
        "sliding_window.reserve.second",
        "second reserve admits and leaves two tokens",
        executor.invoke(connection, ScriptId::SlidingReserve, &keys, &reserve_args),
        |reply| reply.len() == 7 && reply[0] == 1 && reply[2..] == [4, 8, 2, 2, 1],
    )
    .await?;
    let second_id = second[1];

    event(
        events,
        "sliding_window.reserve.denied",
        "third reserve is denied without creating another active reservation",
        executor.invoke(connection, ScriptId::SlidingReserve, &keys, &reserve_args),
        |reply| reply == [0, 60_000, 2, 2, 1],
    )
    .await?;

    let reconcile_first = [
        first_id.to_string(),
        "2".to_owned(),
        "1".to_owned(),
        "60000".to_owned(),
        "60000".to_owned(),
        "10".to_owned(),
    ];
    event(
        events,
        "sliding_window.reconcile.refund",
        "reconcile applies a two-token refund",
        executor.invoke(
            connection,
            ScriptId::SlidingReconcile,
            &keys,
            &reconcile_first,
        ),
        |reply| reply == [1, 2, 2, 0, 4, 1, 1],
    )
    .await?;
    event(
        events,
        "sliding_window.reconcile.duplicate",
        "duplicate reconcile is a no-op",
        executor.invoke(
            connection,
            ScriptId::SlidingReconcile,
            &keys,
            &reconcile_first,
        ),
        |reply| reply == [0, 4, 1, 1],
    )
    .await?;

    let third = event(
        events,
        "sliding_window.reserve.after_refund",
        "refund makes room for one further reservation",
        executor.invoke(connection, ScriptId::SlidingReserve, &keys, &reserve_args),
        |reply| reply.len() == 7 && reply[0] == 1 && reply[2..] == [4, 10, 0, 2, 1],
    )
    .await?;
    let third_id = third[1];

    let reconcile_second = [
        second_id.to_string(),
        "6".to_owned(),
        "1".to_owned(),
        "60000".to_owned(),
        "60000".to_owned(),
        "10".to_owned(),
    ];
    event(
        events,
        "sliding_window.reconcile.overage",
        "reconcile applies a two-token overage",
        executor.invoke(
            connection,
            ScriptId::SlidingReconcile,
            &keys,
            &reconcile_second,
        ),
        |reply| reply == [1, 6, 0, 2, 0, 1, 1],
    )
    .await?;

    let reconcile_third = [
        third_id.to_string(),
        "4".to_owned(),
        "1".to_owned(),
        "60000".to_owned(),
        "60000".to_owned(),
        "10".to_owned(),
    ];
    event(
        events,
        "sliding_window.reconcile.equal",
        "equal actual usage settles the final active reservation",
        executor.invoke(
            connection,
            ScriptId::SlidingReconcile,
            &keys,
            &reconcile_third,
        ),
        |reply| reply == [1, 4, 0, 0, 0, 0, 1],
    )
    .await?;
    Ok(())
}

#[allow(clippy::too_many_lines)]
async fn bucket_scenario(
    executor: &Executor,
    connection: &mut MultiplexedConnection,
    namespace: &str,
    events: &mut Vec<Event>,
) -> Result<()> {
    let keys = token_bucket_keys(namespace, "bucket-rule", "subject-a");
    let reserve_args = ["10", "0.0001", "60000", "100", "100", "4"]
        .into_iter()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let first = event(
        events,
        "token_bucket.reserve.first",
        "first reserve admits estimate=4",
        executor.invoke(connection, ScriptId::BucketReserve, &keys, &reserve_args),
        |reply| {
            reply.len() == 7 && reply[0] == 1 && reply[2] == 4 && reply[3] == 4 && reply[4] == 6
        },
    )
    .await?;
    let first_id = first[1];
    let second = event(
        events,
        "token_bucket.reserve.second",
        "second reserve admits and leaves approximately two whole tokens",
        executor.invoke(connection, ScriptId::BucketReserve, &keys, &reserve_args),
        |reply| {
            reply.len() == 7
                && reply[0] == 1
                && reply[2] == 4
                && reply[3] >= 7
                && (1..=2).contains(&reply[4])
        },
    )
    .await?;
    let second_id = second[1];
    event(
        events,
        "token_bucket.reserve.denied",
        "third reserve is denied with a positive retry delay",
        executor.invoke(connection, ScriptId::BucketReserve, &keys, &reserve_args),
        |reply| reply.len() == 5 && reply[0] == 0 && reply[1] > 0 && reply[3] == 2,
    )
    .await?;

    let reconcile_first = [
        first_id.to_string(),
        "2".to_owned(),
        "10".to_owned(),
        "0.0001".to_owned(),
        "60000".to_owned(),
    ];
    event(
        events,
        "token_bucket.reconcile.refund",
        "reconcile applies a two-token refund",
        executor.invoke(
            connection,
            ScriptId::BucketReconcile,
            &keys,
            &reconcile_first,
        ),
        |reply| reply.len() == 7 && reply[0..4] == [1, 2, 2, 0] && reply[5] == 1,
    )
    .await?;
    event(
        events,
        "token_bucket.reconcile.duplicate",
        "duplicate reconcile is a no-op",
        executor.invoke(
            connection,
            ScriptId::BucketReconcile,
            &keys,
            &reconcile_first,
        ),
        |reply| reply.len() == 4 && reply[0] == 0 && reply[2] == 1,
    )
    .await?;

    let third = event(
        events,
        "token_bucket.reserve.after_refund",
        "refund makes room for one further reservation",
        executor.invoke(connection, ScriptId::BucketReserve, &keys, &reserve_args),
        |reply| reply.len() == 7 && reply[0] == 1 && reply[2] == 4,
    )
    .await?;
    let third_id = third[1];

    let reconcile_second = [
        second_id.to_string(),
        "6".to_owned(),
        "10".to_owned(),
        "0.0001".to_owned(),
        "60000".to_owned(),
    ];
    event(
        events,
        "token_bucket.reconcile.overage",
        "reconcile applies a two-token overage",
        executor.invoke(
            connection,
            ScriptId::BucketReconcile,
            &keys,
            &reconcile_second,
        ),
        |reply| reply.len() == 7 && reply[0..4] == [1, 6, 0, 2] && reply[5] == 1,
    )
    .await?;

    let reconcile_third = [
        third_id.to_string(),
        "4".to_owned(),
        "10".to_owned(),
        "0.0001".to_owned(),
        "60000".to_owned(),
    ];
    event(
        events,
        "token_bucket.reconcile.equal",
        "equal actual usage settles the final active reservation",
        executor.invoke(
            connection,
            ScriptId::BucketReconcile,
            &keys,
            &reconcile_third,
        ),
        |reply| reply.len() == 7 && reply[0..4] == [1, 4, 0, 0] && reply[5] == 0,
    )
    .await?;
    Ok(())
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
                "eval", "evalsha", "script", "time", "hget", "hset", "zadd", "zrange",
            ]
            .iter()
            .any(|command| key.starts_with(&format!("cmdstat_{command}")))
        })
        .collect())
}

async fn run_candidate(
    candidate: Candidate,
    connection: &mut MultiplexedConnection,
    product: &str,
) -> Result<CandidateReport> {
    reset_server(connection).await?;
    let executor = Executor::new(candidate);
    let namespace = format!(
        "praxis:spike:{product}:{}",
        match candidate {
            Candidate::Eval => "eval",
            Candidate::EvalSha => "evalsha",
        }
    );
    let hashes = executor.hashes();
    let scripts_present_before: Vec<i64> = redis::cmd("SCRIPT")
        .arg("EXISTS")
        .arg(&hashes)
        .query_async(connection)
        .await?;
    let mut events = Vec::new();
    sliding_scenario(&executor, connection, &namespace, &mut events).await?;
    bucket_scenario(&executor, connection, &namespace, &mut events).await?;
    let invariants = mixed_configuration_ttl_scenarios(&executor, connection, &namespace).await?;
    let scripts_present_after: Vec<i64> = redis::cmd("SCRIPT")
        .arg("EXISTS")
        .arg(hashes)
        .query_async(connection)
        .await?;
    let sliding_active_count: Option<i64> =
        connection.get(format!("{namespace}:active-count")).await?;
    let bucket_active_count: Option<i64> = connection
        .get(format!("{namespace}:tb:active-count"))
        .await?;
    let sliding_active_count = sliding_active_count.unwrap_or_default();
    let bucket_active_count = bucket_active_count.unwrap_or_default();
    let algorithm_results = vec![
        algorithm_result("sliding_window", &events, &invariants, sliding_active_count),
        algorithm_result("token_bucket", &events, &invariants, bucket_active_count),
    ];
    let passed = algorithm_results.iter().all(|result| result.passed);
    let final_active_counts = [
        ("sliding_window".to_owned(), sliding_active_count),
        ("token_bucket".to_owned(), bucket_active_count),
    ]
    .into_iter()
    .collect();
    Ok(CandidateReport {
        candidate,
        events,
        invariants,
        algorithm_results,
        command_stats: command_stats(connection).await?,
        scripts_present_before,
        scripts_present_after,
        final_active_counts,
        passed,
    })
}

fn normalized(report: &CandidateReport) -> Vec<SemanticEvent> {
    report
        .events
        .iter()
        .map(|event| event.semantic.clone())
        .collect()
}

fn invariant_outcomes(report: &CandidateReport) -> Vec<(&str, bool)> {
    report
        .invariants
        .iter()
        .map(|invariant| (invariant.name.as_str(), invariant.passed))
        .collect()
}

fn algorithm_result(
    algorithm: &str,
    events: &[Event],
    invariants: &[Invariant],
    final_active_count: i64,
) -> AlgorithmResult {
    let mut failed_assertions = events
        .iter()
        .filter(|event| event.name.starts_with(algorithm) && !event.passed)
        .map(|event| event.name.clone())
        .chain(
            invariants
                .iter()
                .filter(|invariant| invariant.name.starts_with(algorithm) && !invariant.passed)
                .map(|invariant| invariant.name.clone()),
        )
        .collect::<Vec<_>>();
    if final_active_count != 0 {
        failed_assertions.push(format!("{algorithm}.final_active_count_is_zero"));
    }
    AlgorithmResult {
        algorithm: algorithm.to_owned(),
        passed: failed_assertions.is_empty(),
        failed_assertions,
    }
}

pub async fn run_standalone(
    url: &str,
    product: &str,
    expected_version: &str,
) -> Result<StandaloneReport> {
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
    let eval = run_candidate(Candidate::Eval, &mut connection, product).await?;
    let evalsha = run_candidate(Candidate::EvalSha, &mut connection, product).await?;
    let semantics_equal = normalized(&eval) == normalized(&evalsha);
    let invariant_outcomes_equal = invariant_outcomes(&eval) == invariant_outcomes(&evalsha);
    let mut failures = Vec::new();
    if !version_matches {
        failures.push(format!(
            "server identity does not contain expected version {expected_version}"
        ));
    }
    for (candidate, report) in [("eval", &eval), ("evalsha", &evalsha)] {
        for algorithm in report
            .algorithm_results
            .iter()
            .filter(|algorithm| !algorithm.passed)
        {
            failures.push(format!(
                "{candidate}/{} correctness gate failed: {}",
                algorithm.algorithm,
                algorithm.failed_assertions.join(", ")
            ));
        }
    }
    if !semantics_equal {
        failures.push("EVAL and EVALSHA normalized semantics differ".to_owned());
    }
    if !invariant_outcomes_equal {
        failures.push("EVAL and EVALSHA invariant outcomes differ".to_owned());
    }
    Ok(StandaloneReport {
        schema_version: 1,
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
        candidates: vec![eval, evalsha],
        eval_evalsha_semantics_equal: semantics_equal,
        eval_evalsha_invariant_outcomes_equal: invariant_outcomes_equal,
        failures,
    })
}

impl StandaloneReport {
    #[must_use]
    pub fn passed(&self) -> bool {
        self.failures.is_empty()
    }
}
