use std::{collections::BTreeMap, num::NonZeroUsize, time::Duration};

use anyhow::{Context, Result, bail};
use redis::{Script, cluster::ClusterClientBuilder};
use serde::Serialize;

use crate::key_schema::{
    cluster_slot, plain_bucket_transaction_keys, plain_sliding_mget_keys,
    plain_sliding_transaction_keys, sliding_window_keys, token_bucket_keys,
};

const SLIDING_RESERVE: &str =
    include_str!("../fixtures/source/baseline/lua/sliding_window_reserve.lua");
const BUCKET_RESERVE: &str =
    include_str!("../fixtures/source/baseline/lua/token_bucket_reserve.lua");
const DEADLINE: Duration = Duration::from_millis(1_000);

#[derive(Debug, Serialize)]
pub struct ClusterNegativeReport {
    schema_version: u8,
    evidence_class: String,
    product: String,
    expected_version: String,
    server_identity: BTreeMap<String, String>,
    topology: TopologyEvidence,
    client: ClientEvidence,
    negative_cases: Vec<NegativeCase>,
    decomposition_cases: Vec<DecompositionCase>,
    evalsha_cache_load: CacheLoadEvidence,
    per_node_command_stats: BTreeMap<String, BTreeMap<String, String>>,
    passed: bool,
}

#[derive(Debug, Serialize)]
struct TopologyEvidence {
    profile: String,
    seeds: Vec<String>,
    cluster_info: BTreeMap<String, String>,
    cluster_nodes: String,
    server_config: BTreeMap<String, String>,
}

#[derive(Debug, Serialize)]
struct ClientEvidence {
    crate_name: String,
    version: String,
    checksum: String,
    features: Vec<String>,
    retries: u32,
    max_connection_attempts: usize,
    connection_timeout_ms: u64,
    response_timeout_ms: u64,
    overall_response_timeout_ms: u64,
    outer_deadline_ms: u64,
    read_routing: String,
}

#[derive(Debug, Serialize)]
struct ErrorEvidence {
    kind: String,
    code: Option<String>,
    detail: Option<String>,
    display: String,
}

#[derive(Debug, Serialize)]
struct NegativeCase {
    id: String,
    candidate: String,
    algorithm: String,
    expected: String,
    key_slots: Vec<u16>,
    error: Option<ErrorEvidence>,
    passed: bool,
}

#[derive(Debug, Serialize)]
struct DecompositionCase {
    id: String,
    candidate: String,
    command: String,
    key_slots: Vec<u16>,
    reply: serde_json::Value,
    error: Option<ErrorEvidence>,
    observation: String,
    passed: bool,
}

#[derive(Debug, Serialize)]
struct CacheLoadEvidence {
    script_hash: String,
    cold_reply: Option<String>,
    script_exists_by_seed: BTreeMap<String, bool>,
    standard_client_fanout_observed: bool,
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
        "run_id",
    ]
    .into_iter()
    .filter_map(|key| parsed.get(key).map(|value| (key.to_owned(), value.clone())))
    .collect()
}

async fn direct_connection(seed: &str) -> Result<redis::aio::MultiplexedConnection> {
    let client = redis::Client::open(seed).with_context(|| format!("invalid seed URL {seed}"))?;
    let config = redis::AsyncConnectionConfig::new()
        .set_connection_timeout(Some(Duration::from_millis(500)))
        .set_response_timeout(Some(Duration::from_millis(500)));
    tokio::time::timeout(
        DEADLINE,
        client.get_multiplexed_async_connection_with_config(&config),
    )
    .await
    .with_context(|| format!("connecting to {seed} exceeded deadline"))?
    .with_context(|| format!("connecting to {seed} failed"))
}

fn error_evidence(error: &redis::RedisError) -> ErrorEvidence {
    ErrorEvidence {
        kind: format!("{:?}", error.kind()),
        code: error.code().map(str::to_owned),
        detail: error.detail().map(str::to_owned),
        display: error.to_string(),
    }
}

fn negative_case<T>(
    id: &str,
    candidate: &str,
    algorithm: &str,
    keys: &[String],
    result: redis::RedisResult<T>,
) -> NegativeCase {
    let error = result.err().map(|error| error_evidence(&error));
    let passed = error.is_some();
    NegativeCase {
        id: id.to_owned(),
        candidate: candidate.to_owned(),
        algorithm: algorithm.to_owned(),
        expected: "the cross-slot operation is rejected rather than routed with weakened atomicity"
            .to_owned(),
        key_slots: keys.iter().map(|key| cluster_slot(key)).collect(),
        error,
        passed,
    }
}

async fn reset_nodes(seeds: &[String]) -> Result<()> {
    for seed in seeds {
        let mut connection = direct_connection(seed).await?;
        redis::cmd("FLUSHALL")
            .arg("SYNC")
            .query_async::<String>(&mut connection)
            .await
            .with_context(|| format!("FLUSHALL failed on {seed}"))?;
        redis::cmd("SCRIPT")
            .arg("FLUSH")
            .arg("SYNC")
            .query_async::<String>(&mut connection)
            .await
            .with_context(|| format!("SCRIPT FLUSH failed on {seed}"))?;
        redis::cmd("CONFIG")
            .arg("RESETSTAT")
            .query_async::<String>(&mut connection)
            .await
            .with_context(|| format!("CONFIG RESETSTAT failed on {seed}"))?;
    }
    Ok(())
}

async fn command_stats(seeds: &[String]) -> Result<BTreeMap<String, BTreeMap<String, String>>> {
    let mut output = BTreeMap::new();
    for seed in seeds {
        let mut connection = direct_connection(seed).await?;
        let info: String = redis::cmd("INFO")
            .arg("COMMANDSTATS")
            .query_async(&mut connection)
            .await?;
        let selected = parse_info(&info)
            .into_iter()
            .filter(|(key, _)| {
                [
                    "eval", "evalsha", "script", "mget", "multi", "exec", "watch", "cluster",
                    "readonly", "client", "hello", "auth", "select", "ping", "asking",
                ]
                .iter()
                .any(|command| key.starts_with(&format!("cmdstat_{command}")))
            })
            .collect();
        output.insert(seed.clone(), selected);
    }
    Ok(output)
}

async fn cache_load_evidence(
    connection: &mut redis::cluster_async::ClusterConnection,
    seeds: &[String],
) -> Result<CacheLoadEvidence> {
    let script = Script::new("return redis.call('GET', KEYS[1])");
    let key = "praxis:spike:cluster:{cache-load}:value";
    redis::cmd("SET")
        .arg(key)
        .arg("cache-evidence")
        .query_async::<String>(connection)
        .await?;
    let cold_reply: Option<String> = script.key(key).invoke_async(connection).await?;
    let mut script_exists_by_seed = BTreeMap::new();
    for seed in seeds {
        let mut direct = direct_connection(seed).await?;
        let exists: Vec<i64> = redis::cmd("SCRIPT")
            .arg("EXISTS")
            .arg(script.get_hash())
            .query_async(&mut direct)
            .await?;
        script_exists_by_seed.insert(seed.clone(), exists == [1]);
    }
    let standard_client_fanout_observed = script_exists_by_seed.values().all(|exists| *exists);
    Ok(CacheLoadEvidence {
        script_hash: script.get_hash().to_owned(),
        cold_reply,
        script_exists_by_seed,
        standard_client_fanout_observed,
    })
}

fn tagged_script_keys(tag: &str) -> Vec<String> {
    let prefix = format!("praxis:spike:cluster:{{{tag}}}");
    (0..12)
        .map(|index| format!("{prefix}:key-{index}"))
        .collect()
}

fn primary_partition(slot: u16) -> u8 {
    match slot {
        0..=5460 => 0,
        5461..=10922 => 1,
        _ => 2,
    }
}

fn physical_key_on_another_primary(declared_slot: u16) -> String {
    let declared_partition = primary_partition(declared_slot);
    (0_u32..10_000)
        .map(|index| format!("praxis:spike:cluster:{{dynamic-{index}}}:physical"))
        .find(|key| primary_partition(cluster_slot(key)) != declared_partition)
        .expect("the finite Cluster slot space contains another primary partition")
}

#[allow(clippy::too_many_lines)]
pub async fn run(
    seeds: Vec<String>,
    product: &str,
    expected_version: &str,
) -> Result<ClusterNegativeReport> {
    if seeds.len() != 3 {
        bail!("the sharding-only comparison point requires exactly three primary seeds");
    }
    reset_nodes(&seeds).await?;

    let mut first = direct_connection(&seeds[0]).await?;
    let server_info: String = redis::cmd("INFO")
        .arg("SERVER")
        .query_async(&mut first)
        .await?;
    let server_identity = selected_server_identity(&server_info);
    let version_matches = ["redis_version", "valkey_version"]
        .into_iter()
        .filter_map(|key| server_identity.get(key))
        .any(|value| value.starts_with(expected_version));
    let cluster_info_text: String = redis::cmd("CLUSTER")
        .arg("INFO")
        .query_async(&mut first)
        .await?;
    let cluster_info = parse_info(&cluster_info_text);
    let cluster_nodes: String = redis::cmd("CLUSTER")
        .arg("NODES")
        .query_async(&mut first)
        .await?;
    let server_config: BTreeMap<String, String> = redis::cmd("CONFIG")
        .arg("GET")
        .arg("appendonly")
        .arg("save")
        .arg("maxmemory-policy")
        .arg("cluster-enabled")
        .arg("cluster-require-full-coverage")
        .arg("cluster-node-timeout")
        .query_async(&mut first)
        .await?;

    let cluster_client = ClusterClientBuilder::new(seeds.clone())
        .retries(0)
        .connection_timeout(Duration::from_millis(500))
        .response_timeout(Duration::from_millis(500))
        .overall_response_timeout(Some(DEADLINE))
        .max_connection_attempts(NonZeroUsize::new(1).expect("one is nonzero"))
        .build()?;
    let mut connection = tokio::time::timeout(DEADLINE, cluster_client.get_async_connection())
        .await
        .context("Cluster bootstrap exceeded the outer deadline")??;

    let evalsha_cache_load = cache_load_evidence(&mut connection, &seeds).await?;
    let namespace = "praxis:spike:cluster:negative";
    let sliding_keys = sliding_window_keys(namespace, "rule", "subject");
    let sliding_args = ["60000", "100", "100", "1", "1", "60000", "10"];
    let mut sliding_eval = redis::cmd("EVAL");
    sliding_eval
        .arg(SLIDING_RESERVE)
        .arg(sliding_keys.len())
        .arg(&sliding_keys)
        .arg(&sliding_args);
    let sliding_eval_result: redis::RedisResult<Vec<i64>> =
        sliding_eval.query_async(&mut connection).await;

    let sliding_script = Script::new(SLIDING_RESERVE);
    let mut sliding_invocation = sliding_script.prepare_invoke();
    for key in &sliding_keys {
        sliding_invocation.key(key);
    }
    for arg in sliding_args {
        sliding_invocation.arg(arg);
    }
    let sliding_evalsha_result: redis::RedisResult<Vec<i64>> =
        sliding_invocation.invoke_async(&mut connection).await;

    let bucket_keys = token_bucket_keys(namespace, "rule", "subject");
    let bucket_args = ["10", "1", "60000", "100", "100", "1"];
    let mut bucket_eval = redis::cmd("EVAL");
    bucket_eval
        .arg(BUCKET_RESERVE)
        .arg(bucket_keys.len())
        .arg(&bucket_keys)
        .arg(&bucket_args);
    let bucket_eval_result: redis::RedisResult<Vec<i64>> =
        bucket_eval.query_async(&mut connection).await;

    let bucket_script = Script::new(BUCKET_RESERVE);
    let mut bucket_invocation = bucket_script.prepare_invoke();
    for key in &bucket_keys {
        bucket_invocation.key(key);
    }
    for arg in bucket_args {
        bucket_invocation.arg(arg);
    }
    let bucket_evalsha_result: redis::RedisResult<Vec<i64>> =
        bucket_invocation.invoke_async(&mut connection).await;

    let plain_sliding_keys = plain_sliding_transaction_keys(namespace, "rule", "subject");
    let mut plain_sliding = redis::pipe();
    plain_sliding.atomic();
    plain_sliding
        .cmd("INCRBY")
        .arg(&plain_sliding_keys[0])
        .arg(1)
        .ignore()
        .cmd("SET")
        .arg(&plain_sliding_keys[1])
        .arg("1|1")
        .ignore()
        .cmd("ZADD")
        .arg(&plain_sliding_keys[2])
        .arg(1)
        .arg("member")
        .ignore();
    let plain_sliding_result: redis::RedisResult<()> =
        plain_sliding.query_async(&mut connection).await;

    let plain_bucket_keys = plain_bucket_transaction_keys(namespace, "rule", "subject");
    let plain_bucket_result: redis::RedisResult<String> = redis::cmd("WATCH")
        .arg(&plain_bucket_keys[0])
        .arg(&plain_bucket_keys[1])
        .query_async(&mut connection)
        .await;

    let declared_keys = tagged_script_keys("declared");
    let dynamic_physical = physical_key_on_another_primary(cluster_slot(&declared_keys[8]));
    redis::cmd("ZADD")
        .arg(&declared_keys[8])
        .arg(0)
        .arg(format!("{dynamic_physical}|1"))
        .query_async::<i64>(&mut connection)
        .await?;
    let mut dynamic_eval = redis::cmd("EVAL");
    dynamic_eval
        .arg(SLIDING_RESERVE)
        .arg(declared_keys.len())
        .arg(&declared_keys)
        .arg(&sliding_args);
    let dynamic_result: redis::RedisResult<Vec<i64>> =
        dynamic_eval.query_async(&mut connection).await;

    let negative_cases = vec![
        negative_case(
            "cluster.current_eval.sliding_cross_slot",
            "current_eval",
            "sliding_window",
            &sliding_keys,
            sliding_eval_result,
        ),
        negative_case(
            "cluster.current_evalsha.sliding_cross_slot",
            "current_evalsha",
            "sliding_window",
            &sliding_keys,
            sliding_evalsha_result,
        ),
        negative_case(
            "cluster.current_eval.token_bucket_cross_slot",
            "current_eval",
            "token_bucket",
            &bucket_keys,
            bucket_eval_result,
        ),
        negative_case(
            "cluster.current_evalsha.token_bucket_cross_slot",
            "current_evalsha",
            "token_bucket",
            &bucket_keys,
            bucket_evalsha_result,
        ),
        negative_case(
            "cluster.plain.sliding_transaction_cross_slot",
            "plain_commands_pr_1380",
            "sliding_window",
            &plain_sliding_keys,
            plain_sliding_result,
        ),
        negative_case(
            "cluster.script.dynamic_non_local_key",
            "same_slot_declared_keys_diagnostic",
            "sliding_window",
            &[
                declared_keys[8].clone(),
                format!("{dynamic_physical}:active"),
            ],
            dynamic_result,
        ),
    ];

    let mget_keys = plain_sliding_mget_keys(namespace, "rule", "mget-subject");
    for (index, key) in mget_keys.iter().enumerate() {
        redis::cmd("SET")
            .arg(key)
            .arg(format!("value-{index}"))
            .query_async::<String>(&mut connection)
            .await?;
    }
    let mget_reply: Vec<Option<String>> = redis::cmd("MGET")
        .arg(&mget_keys)
        .query_async(&mut connection)
        .await?;
    let mget_slots = mget_keys
        .iter()
        .map(|key| cluster_slot(key))
        .collect::<Vec<_>>();
    let watch_slots = plain_bucket_keys[..2]
        .iter()
        .map(|key| cluster_slot(key))
        .collect::<Vec<_>>();
    let (watch_reply, watch_error) = match plain_bucket_result {
        Ok(reply) => (serde_json::json!(reply), None),
        Err(error) => (serde_json::Value::Null, Some(error_evidence(&error))),
    };
    let decomposition_cases = vec![
        DecompositionCase {
            id: "cluster.redis_rs.cross_slot_mget_decomposes".to_owned(),
            candidate: "plain_commands_pr_1380".to_owned(),
            command: "MGET".to_owned(),
            key_slots: mget_slots.clone(),
            reply: serde_json::json!(mget_reply),
            error: None,
            observation: "a cross-slot MGET succeeded through redis-rs even though a server node cannot execute that command atomically; the client decomposed it by slot"
                .to_owned(),
            passed: mget_slots[0] != mget_slots[1]
                && mget_reply == [Some("value-0".to_owned()), Some("value-1".to_owned())],
        },
        DecompositionCase {
            id: "cluster.redis_rs.cross_slot_watch_decomposes".to_owned(),
            candidate: "plain_commands_pr_1380".to_owned(),
            command: "WATCH".to_owned(),
            key_slots: watch_slots.clone(),
            passed: watch_slots[0] != watch_slots[1] && watch_reply == serde_json::json!("OK"),
            reply: watch_reply,
            error: watch_error,
            observation: "a cross-slot WATCH returned success and per-node command statistics show WATCH on multiple primaries; it is not one server-side transaction watch set"
                .to_owned(),
        },
    ];

    let per_node_command_stats = command_stats(&seeds).await?;
    let passed = version_matches
        && cluster_info
            .get("cluster_state")
            .is_some_and(|state| state == "ok")
        && negative_cases.iter().all(|case| case.passed)
        && decomposition_cases.iter().all(|case| case.passed)
        && evalsha_cache_load.standard_client_fanout_observed;

    Ok(ClusterNegativeReport {
        schema_version: 1,
        evidence_class: "diagnostic_negative_control_not_qualification".to_owned(),
        product: product.to_owned(),
        expected_version: expected_version.to_owned(),
        server_identity,
        topology: TopologyEvidence {
            profile: "native_cluster_three_primaries_zero_replicas".to_owned(),
            seeds,
            cluster_info,
            cluster_nodes,
            server_config,
        },
        client: ClientEvidence {
            crate_name: "redis".to_owned(),
            version: "1.7.0".to_owned(),
            checksum: "2acbc41a996f7652b2ddd9dfd98cc4ff602cfd742ae35382f07f608405ab50ed".to_owned(),
            features: vec![
                "tokio-comp".to_owned(),
                "cluster-async".to_owned(),
                "script".to_owned(),
            ],
            retries: 0,
            max_connection_attempts: 1,
            connection_timeout_ms: 500,
            response_timeout_ms: 500,
            overall_response_timeout_ms: 1_000,
            outer_deadline_ms: 1_000,
            read_routing: "primary_only_default".to_owned(),
        },
        negative_cases,
        decomposition_cases,
        evalsha_cache_load,
        per_node_command_stats,
        passed,
    })
}

impl ClusterNegativeReport {
    #[must_use]
    pub fn passed(&self) -> bool {
        self.passed
    }
}
