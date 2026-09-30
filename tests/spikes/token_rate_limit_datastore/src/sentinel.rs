use std::{
    collections::BTreeMap,
    fmt::Write as _,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};
use redis::{
    aio::MultiplexedConnection,
    sentinel::{SentinelClient, SentinelServerType},
};
use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::{
    fixed::{
        BucketConfig, FixedExecutor, Invocation, SlidingConfig, run_invocation,
        selected_server_identity,
    },
    key_schema::{fixed_sliding_window_keys, fixed_token_bucket_keys},
};

const SOURCE_COMMIT: &str = "b6fe4704af72c05167a280d5070a291997d2085d";
const CONNECT_TIMEOUT: Duration = Duration::from_millis(500);
const RESPONSE_TIMEOUT: Duration = Duration::from_millis(500);
const OPERATION_DEADLINE: Duration = Duration::from_millis(1_000);
const DISCOVERY_DEADLINE: Duration = Duration::from_millis(3_000);
const FAILOVER_DEADLINE: Duration = Duration::from_secs(20);
const POLL_INTERVAL: Duration = Duration::from_millis(50);

#[derive(Debug, Serialize)]
pub struct SentinelReport {
    schema_version: u8,
    evidence_class: String,
    candidate: String,
    source_commit: String,
    product: String,
    expected_version: String,
    invocation: Invocation,
    protocol: String,
    client: ClientProfile,
    topology: TopologyProfile,
    initial_server_identity: BTreeMap<String, String>,
    standalone_contract_over_sentinel: serde_json::Value,
    scenarios: Vec<Scenario>,
    failovers: Vec<FailoverEvidence>,
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
    discovery_deadline_ms: u64,
    automatic_mutation_retries: u8,
    discovery_retry_scope: String,
}

#[derive(Debug, Serialize)]
struct TopologyProfile {
    profile: String,
    service_name: String,
    sentinel_endpoints: Vec<String>,
    data_node_urls: Vec<String>,
    sentinel_count: usize,
    data_node_count: usize,
    quorum: u8,
    down_after_ms: u64,
    failover_timeout_ms: u64,
    persistence: String,
    initial_discovery_attempts: u32,
    initial_discovery_elapsed_ms: u128,
    initial_primary: NodeState,
    initial_replica: NodeState,
}

#[derive(Clone, Debug, Serialize)]
struct NodeState {
    url: String,
    run_id: String,
    role: String,
    master_repl_offset: Option<i64>,
    replica_repl_offset: Option<i64>,
    master_link_status: Option<String>,
    connected_replicas: Option<u64>,
    good_replicas: Option<u64>,
}

#[derive(Debug, Serialize)]
struct Scenario {
    id: String,
    assertion: String,
    observed: serde_json::Value,
    passed: bool,
}

#[derive(Debug, Serialize)]
struct FailoverEvidence {
    kind: String,
    elapsed_ms: u128,
    rediscovery_attempts: u32,
    rediscovery_elapsed_ms: u128,
    old_primary: NodeState,
    new_primary: NodeState,
    new_replica: Option<NodeState>,
    stale_connection_error: Option<ErrorEvidence>,
    stale_attempt_existing_keys: Option<i64>,
    state_digest_before: BTreeMap<String, Option<String>>,
    state_digest_on_replica_before: BTreeMap<String, Option<String>>,
    reconcile_reply: Vec<i64>,
    duplicate_reply: Vec<i64>,
    scripts_present_before_promotion: Vec<i64>,
    scripts_present_after_reconcile: Vec<i64>,
    passed: bool,
}

#[derive(Debug, Serialize)]
struct ErrorEvidence {
    kind: Option<String>,
    code: Option<String>,
    detail: Option<String>,
    display: String,
}

struct DiscoveredPrimary {
    connection: MultiplexedConnection,
    state: NodeState,
    discovery_attempts: u32,
    discovery_elapsed: Duration,
}

fn connection_config(response_timeout: Duration) -> redis::AsyncConnectionConfig {
    redis::AsyncConnectionConfig::new()
        .set_connection_timeout(Some(CONNECT_TIMEOUT))
        .set_response_timeout(Some(response_timeout))
}

async fn direct_connection(url: &str) -> Result<MultiplexedConnection> {
    direct_connection_with_timeouts(url, RESPONSE_TIMEOUT, OPERATION_DEADLINE).await
}

async fn direct_connection_with_timeouts(
    url: &str,
    response_timeout: Duration,
    outer_deadline: Duration,
) -> Result<MultiplexedConnection> {
    let client =
        redis::Client::open(url).with_context(|| format!("invalid data-node URL {url}"))?;
    tokio::time::timeout(
        outer_deadline,
        client.get_multiplexed_async_connection_with_config(&connection_config(response_timeout)),
    )
    .await
    .with_context(|| format!("connection to {url} exceeded the outer deadline"))?
    .with_context(|| format!("connection to {url} failed"))
}

fn parse_info(info: &str) -> BTreeMap<String, String> {
    info.lines()
        .filter(|line| !line.starts_with('#'))
        .filter_map(|line| line.trim_end().split_once(':'))
        .map(|(key, value)| (key.to_owned(), value.to_owned()))
        .collect()
}

async fn node_state(url: &str) -> Result<NodeState> {
    let mut connection = direct_connection(url).await?;
    let server: String = redis::cmd("INFO")
        .arg("SERVER")
        .query_async(&mut connection)
        .await?;
    let replication: String = redis::cmd("INFO")
        .arg("REPLICATION")
        .query_async(&mut connection)
        .await?;
    let server = parse_info(&server);
    let replication = parse_info(&replication);
    Ok(NodeState {
        url: url.to_owned(),
        run_id: server.get("run_id").cloned().unwrap_or_default(),
        role: replication.get("role").cloned().unwrap_or_default(),
        master_repl_offset: replication
            .get("master_repl_offset")
            .and_then(|value| value.parse().ok()),
        replica_repl_offset: replication
            .get("slave_repl_offset")
            .or_else(|| replication.get("replica_repl_offset"))
            .and_then(|value| value.parse().ok()),
        master_link_status: replication.get("master_link_status").cloned(),
        connected_replicas: replication
            .get("connected_slaves")
            .or_else(|| replication.get("connected_replicas"))
            .and_then(|value| value.parse().ok()),
        good_replicas: replication
            .get("min_slaves_good_slaves")
            .or_else(|| replication.get("min_replicas_good_replicas"))
            .and_then(|value| value.parse().ok()),
    })
}

async fn wait_for_primary_and_replica(
    node_urls: &[String],
    old_primary_run_id: Option<&str>,
    deadline: Duration,
) -> Result<(NodeState, NodeState)> {
    let started = Instant::now();
    loop {
        let mut states = Vec::new();
        for url in node_urls {
            if let Ok(state) = node_state(url).await {
                states.push(state);
            }
        }
        let primary = states.iter().find(|state| state.role == "master").cloned();
        let replica = states
            .iter()
            .find(|state| {
                (state.role == "slave" || state.role == "replica")
                    && state.master_link_status.as_deref() == Some("up")
            })
            .cloned();
        if let (Some(primary), Some(replica)) = (primary, replica)
            && old_primary_run_id.is_none_or(|old| primary.run_id != old)
        {
            return Ok((primary, replica));
        }
        if started.elapsed() >= deadline {
            bail!("data nodes did not converge to one primary and one connected replica");
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

async fn wait_for_single_primary(
    node_urls: &[String],
    old_primary_run_id: &str,
    deadline: Duration,
) -> Result<NodeState> {
    let started = Instant::now();
    loop {
        for url in node_urls {
            if let Ok(state) = node_state(url).await
                && state.role == "master"
                && state.run_id != old_primary_run_id
            {
                return Ok(state);
            }
        }
        if started.elapsed() >= deadline {
            bail!("no promoted primary became reachable before the failover deadline");
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

async fn wait_for_replication(
    primary_url: &str,
    replica_url: &str,
) -> Result<(NodeState, NodeState)> {
    let started = Instant::now();
    loop {
        if let (Ok(primary), Ok(replica)) =
            (node_state(primary_url).await, node_state(replica_url).await)
        {
            let primary_offset = primary.master_repl_offset.unwrap_or(i64::MAX);
            let replica_offset = replica.replica_repl_offset.unwrap_or(i64::MIN);
            if primary.role == "master"
                && (replica.role == "slave" || replica.role == "replica")
                && replica.master_link_status.as_deref() == Some("up")
                && replica_offset >= primary_offset
            {
                return Ok((primary, replica));
            }
        }
        if started.elapsed() >= FAILOVER_DEADLINE {
            bail!("replication offsets did not catch up before the deadline");
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

async fn discover_primary(
    endpoints: &[String],
    service_name: &str,
    deadline: Duration,
) -> Result<DiscoveredPrimary> {
    let started = Instant::now();
    let mut attempts = 0_u32;
    let mut last_error = None;
    loop {
        attempts += 1;
        let remaining = deadline.saturating_sub(started.elapsed());
        if remaining.is_zero() {
            let detail = last_error.unwrap_or_else(|| "no discovery attempt completed".to_owned());
            bail!(
                "Sentinel discovery exceeded the {deadline:?} outer deadline after {attempts} attempts: {detail}"
            );
        }
        let attempt = async {
            let mut client = SentinelClient::build(
                endpoints.to_vec(),
                service_name.to_owned(),
                None,
                SentinelServerType::Master,
            )
            .context("building Sentinel client failed")?;
            let mut connection = client
                .get_async_connection_with_config(&connection_config(RESPONSE_TIMEOUT))
                .await
                .context("Sentinel traversal or primary connection failed")?;
            let server: String = redis::cmd("INFO")
                .arg("SERVER")
                .query_async(&mut connection)
                .await?;
            let replication: String = redis::cmd("INFO")
                .arg("REPLICATION")
                .query_async(&mut connection)
                .await?;
            let server = parse_info(&server);
            let replication = parse_info(&replication);
            let state = NodeState {
                url: "sentinel-discovered".to_owned(),
                run_id: server.get("run_id").cloned().unwrap_or_default(),
                role: replication.get("role").cloned().unwrap_or_default(),
                master_repl_offset: replication
                    .get("master_repl_offset")
                    .and_then(|value| value.parse().ok()),
                replica_repl_offset: None,
                master_link_status: None,
                connected_replicas: replication
                    .get("connected_slaves")
                    .or_else(|| replication.get("connected_replicas"))
                    .and_then(|value| value.parse().ok()),
                good_replicas: replication
                    .get("min_slaves_good_slaves")
                    .or_else(|| replication.get("min_replicas_good_replicas"))
                    .and_then(|value| value.parse().ok()),
            };
            if state.role != "master" {
                bail!("Sentinel returned a non-primary data node: {state:?}");
            }
            Ok::<_, anyhow::Error>((connection, state))
        };
        match tokio::time::timeout(remaining, attempt).await {
            Ok(Ok((connection, state))) => {
                return Ok(DiscoveredPrimary {
                    connection,
                    state,
                    discovery_attempts: attempts,
                    discovery_elapsed: started.elapsed(),
                });
            }
            Ok(Err(error)) => last_error = Some(error.to_string()),
            Err(_elapsed) => {
                let detail = last_error.unwrap_or_else(|| "attempt timed out".to_owned());
                bail!(
                    "Sentinel discovery exceeded the {deadline:?} outer deadline after {attempts} attempts: {detail}"
                );
            }
        }
        let remaining = deadline.saturating_sub(started.elapsed());
        if !remaining.is_zero() {
            tokio::time::sleep(POLL_INTERVAL.min(remaining)).await;
        }
    }
}

fn error_evidence(error: &anyhow::Error) -> ErrorEvidence {
    let redis = error.downcast_ref::<redis::RedisError>();
    ErrorEvidence {
        kind: redis.map(|error| format!("{:?}", error.kind())),
        code: redis.and_then(|error| error.code().map(str::to_owned)),
        detail: redis.and_then(|error| error.detail().map(str::to_owned)),
        display: error.to_string(),
    }
}

async fn key_digests(
    connection: &mut MultiplexedConnection,
    keys: &[String],
) -> Result<BTreeMap<String, Option<String>>> {
    let mut output = BTreeMap::new();
    for key in keys {
        let dump: Option<Vec<u8>> = redis::cmd("DUMP").arg(key).query_async(connection).await?;
        let digest = dump.map(|bytes| {
            let mut digest = Sha256::new();
            digest.update(bytes);
            digest
                .finalize()
                .iter()
                .fold(String::with_capacity(64), |mut output, byte| {
                    write!(output, "{byte:02x}").expect("writing to a String cannot fail");
                    output
                })
        });
        output.insert(key.clone(), digest);
    }
    Ok(output)
}

async fn script_exists(
    connection: &mut MultiplexedConnection,
    hashes: &[String],
) -> Result<Vec<i64>> {
    redis::cmd("SCRIPT")
        .arg("EXISTS")
        .arg(hashes)
        .query_async(connection)
        .await
        .context("SCRIPT EXISTS failed")
}

async fn flush_scripts(connection: &mut MultiplexedConnection) -> Result<()> {
    redis::cmd("SCRIPT")
        .arg("FLUSH")
        .arg("SYNC")
        .query_async::<String>(connection)
        .await
        .context("SCRIPT FLUSH failed")?;
    Ok(())
}

async fn sentinel_command(endpoint: &str, subcommand: &str, service_name: &str) -> Result<String> {
    let mut connection = direct_connection(endpoint).await?;
    redis::cmd("SENTINEL")
        .arg(subcommand)
        .arg(service_name)
        .query_async(&mut connection)
        .await
        .with_context(|| format!("SENTINEL {subcommand} failed through {endpoint}"))
}

async fn config_get(connection: &mut MultiplexedConnection, key: &str) -> Result<String> {
    let values: BTreeMap<String, String> = redis::cmd("CONFIG")
        .arg("GET")
        .arg(key)
        .query_async(connection)
        .await?;
    values
        .get(key)
        .cloned()
        .with_context(|| format!("CONFIG GET did not return {key}"))
}

async fn config_set(connection: &mut MultiplexedConnection, key: &str, value: &str) -> Result<()> {
    redis::cmd("CONFIG")
        .arg("SET")
        .arg(key)
        .arg(value)
        .query_async::<String>(connection)
        .await
        .with_context(|| format!("CONFIG SET {key} failed"))?;
    Ok(())
}

async fn wait_for_no_good_replicas(primary_url: &str) -> Result<NodeState> {
    let started = Instant::now();
    loop {
        if let Ok(state) = node_state(primary_url).await
            && state.good_replicas == Some(0)
        {
            return Ok(state);
        }
        if started.elapsed() >= Duration::from_secs(5) {
            bail!("primary did not report zero good replicas before the fault deadline");
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

async fn replication_health_scenario(
    executor: &FixedExecutor,
    primary: &mut MultiplexedConnection,
    primary_state: &NodeState,
    replica_state: &NodeState,
    namespace: &str,
) -> Result<Scenario> {
    let original_min = config_get(primary, "min-replicas-to-write").await?;
    let original_lag = config_get(primary, "min-replicas-max-lag").await?;
    config_set(primary, "min-replicas-to-write", "1").await?;
    config_set(primary, "min-replicas-max-lag", "1").await?;

    let config = SlidingConfig {
        budgets: vec![(60_000, 10)],
        reservation_timeout_ms: 60_000,
        max_keys: 100,
        max_active_reservations: 100,
    };
    let control_keys = fixed_sliding_window_keys(
        &format!("{namespace}:noreplicas:control"),
        "rule",
        "subject",
    );
    let control = executor
        .sliding_reserve(primary, &control_keys, &config, 1)
        .await?;
    if control.first() == Some(&1) {
        executor
            .sliding_reconcile(primary, &control_keys, &config, control[1], 1)
            .await?;
    }
    wait_for_replication(&primary_state.url, &replica_state.url).await?;

    let mut replica = direct_connection_with_timeouts(
        &replica_state.url,
        Duration::from_secs(5),
        OPERATION_DEADLINE,
    )
    .await?;
    let sleep_task = tokio::spawn(async move {
        redis::cmd("DEBUG")
            .arg("SLEEP")
            .arg("3")
            .query_async::<String>(&mut replica)
            .await
    });
    let no_good_replicas = wait_for_no_good_replicas(&primary_state.url).await?;

    let rejected_keys = fixed_sliding_window_keys(
        &format!("{namespace}:noreplicas:rejected"),
        "rule",
        "subject",
    );
    let result = executor
        .sliding_reserve(primary, &rejected_keys, &config, 1)
        .await;
    let error = result.as_ref().err().map(error_evidence);
    let mut existing_keys = 0_i64;
    for key in &rejected_keys {
        existing_keys += redis::cmd("EXISTS")
            .arg(key)
            .query_async::<i64>(primary)
            .await?;
    }

    config_set(primary, "min-replicas-to-write", &original_min).await?;
    config_set(primary, "min-replicas-max-lag", &original_lag).await?;
    let debug_sleep_result = sleep_task.await.context("DEBUG SLEEP task panicked")?;
    let (recovered_primary, recovered_replica) =
        wait_for_replication(&primary_state.url, &replica_state.url).await?;

    let passed = control.first() == Some(&1)
        && error.as_ref().and_then(|error| error.code.as_deref()) == Some("NOREPLICAS")
        && existing_keys == 0
        && debug_sleep_result.is_ok();
    Ok(Scenario {
        id: "sentinel.replication_health.noreplicas".to_owned(),
        assertion: "a caught-up replica permits writes, then a lagged replica causes a stable NOREPLICAS rejection before any accounting mutation"
            .to_owned(),
        observed: serde_json::json!({
            "control_reply": control,
            "primary_while_replica_lagged": no_good_replicas,
            "error": error,
            "rejected_key_count_after_error": existing_keys,
            "debug_sleep_completed": debug_sleep_result.is_ok(),
            "recovered_primary": recovered_primary,
            "recovered_replica": recovered_replica,
            "restored_min_replicas_to_write": original_min,
            "restored_min_replicas_max_lag": original_lag,
        }),
        passed,
    })
}

async fn relay_one_connection(
    mut client: tokio::net::TcpStream,
    upstream: String,
    wedged: Arc<AtomicBool>,
) {
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
                    continue;
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

fn url_host_port(url: &str) -> Result<String> {
    let address = url
        .strip_prefix("redis://")
        .with_context(|| format!("fault proxy supports only redis:// URLs, got {url}"))?
        .trim_end_matches('/');
    if address.is_empty() || !address.contains(':') {
        bail!("fault proxy requires an explicit host:port URL, got {url}");
    }
    Ok(address.to_owned())
}

async fn spawn_wedgeable_proxy(upstream_url: &str) -> Result<(SocketAddr, Arc<AtomicBool>)> {
    let upstream = url_host_port(upstream_url)?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let wedged = Arc::new(AtomicBool::new(false));
    let task_flag = Arc::clone(&wedged);
    tokio::spawn(async move {
        while let Ok((client, _)) = listener.accept().await {
            tokio::spawn(relay_one_connection(
                client,
                upstream.clone(),
                Arc::clone(&task_flag),
            ));
        }
    });
    Ok((address, wedged))
}

async fn wait_for_active_reservation(
    connection: &mut MultiplexedConnection,
    active_key: &str,
) -> Result<Vec<String>> {
    let started = Instant::now();
    loop {
        let ids: Vec<String> = redis::cmd("HKEYS")
            .arg(active_key)
            .query_async(connection)
            .await?;
        if !ids.is_empty() {
            return Ok(ids);
        }
        if started.elapsed() >= OPERATION_DEADLINE {
            bail!("timed-out reservation did not become observable on the primary");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn ambiguous_timeout_scenario(
    executor: &FixedExecutor,
    primary: &mut MultiplexedConnection,
    primary_state: &NodeState,
    namespace: &str,
) -> Result<Scenario> {
    let (proxy_address, wedged) = spawn_wedgeable_proxy(&primary_state.url).await?;
    let client = redis::Client::open(format!("redis://{proxy_address}/"))?;
    let mut proxy_connection = client
        .get_multiplexed_async_connection_with_config(&connection_config(Duration::from_millis(
            150,
        )))
        .await?;
    let config = SlidingConfig {
        budgets: vec![(60_000, 10)],
        reservation_timeout_ms: 60_000,
        max_keys: 100,
        max_active_reservations: 100,
    };
    let warm_keys =
        fixed_sliding_window_keys(&format!("{namespace}:timeout:warm"), "rule", "subject");
    let warm = executor
        .sliding_reserve(&mut proxy_connection, &warm_keys, &config, 1)
        .await?;
    if warm.first() == Some(&1) {
        executor
            .sliding_reconcile(&mut proxy_connection, &warm_keys, &config, warm[1], 1)
            .await?;
    }

    let keys = fixed_sliding_window_keys(&format!("{namespace}:timeout:target"), "rule", "subject");
    wedged.store(true, Ordering::SeqCst);
    let started = Instant::now();
    let result = tokio::time::timeout(
        OPERATION_DEADLINE,
        executor.sliding_reserve(&mut proxy_connection, &keys, &config, 3),
    )
    .await;
    let elapsed = started.elapsed();
    let operation_error = match result {
        Ok(Err(error)) => Some(error_evidence(&error)),
        Ok(Ok(_)) | Err(_) => None,
    };
    let ids = wait_for_active_reservation(primary, &keys[2]).await?;
    let sequence: Option<i64> = redis::cmd("GET").arg(&keys[5]).query_async(primary).await?;
    let rule_active: Option<i64> = redis::cmd("GET").arg(&keys[7]).query_async(primary).await?;
    wedged.store(false, Ordering::SeqCst);
    drop(proxy_connection);

    let reservation_id = ids
        .first()
        .and_then(|id| id.parse::<i64>().ok())
        .context("active reservation ID was not an integer")?;
    let cleanup = executor
        .sliding_reconcile(primary, &keys, &config, reservation_id, 3)
        .await?;
    let passed = operation_error.is_some()
        && elapsed < OPERATION_DEADLINE
        && ids.len() == 1
        && sequence == Some(1)
        && rule_active == Some(1)
        && cleanup.first() == Some(&1);
    Ok(Scenario {
        id: "sentinel.mutation.ambiguous_timeout_no_replay".to_owned(),
        assertion: "a reserve committed behind a dropped reply times out once, is not replayed, and leaves exactly one unconfirmed reservation"
            .to_owned(),
        observed: serde_json::json!({
            "error": operation_error,
            "elapsed_ms": elapsed.as_millis(),
            "active_reservation_ids": ids,
            "reservation_sequence": sequence,
            "rule_active_reservations": rule_active,
            "cleanup_reply": cleanup,
        }),
        passed,
    })
}

async fn stale_connection_attempt(
    executor: &FixedExecutor,
    connection: &mut MultiplexedConnection,
    namespace: &str,
) -> (Option<ErrorEvidence>, Vec<String>) {
    let keys = fixed_sliding_window_keys(namespace, "rule", "stale-subject");
    let config = SlidingConfig {
        budgets: vec![(60_000, 10)],
        reservation_timeout_ms: 60_000,
        max_keys: 100,
        max_active_reservations: 100,
    };
    match executor
        .sliding_reserve(connection, &keys, &config, 1)
        .await
    {
        Ok(_) => (None, keys),
        Err(error) => (Some(error_evidence(&error)), keys),
    }
}

#[allow(clippy::too_many_arguments)]
async fn planned_failover(
    executor: &FixedExecutor,
    mut old_primary_connection: MultiplexedConnection,
    old_primary: NodeState,
    old_replica: NodeState,
    sentinel_endpoints: &[String],
    node_urls: &[String],
    service_name: &str,
    namespace: &str,
) -> Result<(FailoverEvidence, DiscoveredPrimary, NodeState)> {
    let config = SlidingConfig {
        budgets: vec![(60_000, 10)],
        reservation_timeout_ms: 60_000,
        max_keys: 100,
        max_active_reservations: 100,
    };
    let keys = fixed_sliding_window_keys(namespace, "rule", "subject");
    let admitted = executor
        .sliding_reserve(&mut old_primary_connection, &keys, &config, 3)
        .await?;
    if admitted.first() != Some(&1) {
        bail!("planned-failover setup reservation was denied: {admitted:?}");
    }
    let (_, caught_up_replica) = wait_for_replication(&old_primary.url, &old_replica.url).await?;
    let mut replica_connection = direct_connection(&caught_up_replica.url).await?;
    let state_digest_before = key_digests(&mut old_primary_connection, &keys).await?;
    let state_digest_on_replica_before = key_digests(&mut replica_connection, &keys).await?;
    flush_scripts(&mut replica_connection).await?;
    let hashes = executor.hashes();
    let scripts_present_before_promotion = script_exists(&mut replica_connection, &hashes).await?;

    let started = Instant::now();
    let reply = sentinel_command(&sentinel_endpoints[0], "FAILOVER", service_name).await?;
    if reply != "OK" {
        bail!("unexpected SENTINEL FAILOVER reply: {reply}");
    }
    let (new_primary_state, new_replica_state) =
        wait_for_primary_and_replica(node_urls, Some(&old_primary.run_id), FAILOVER_DEADLINE)
            .await?;
    let elapsed = started.elapsed();
    let (stale_connection_error, stale_keys) = stale_connection_attempt(
        executor,
        &mut old_primary_connection,
        &format!("{namespace}:stale"),
    )
    .await;
    let mut discovered =
        discover_primary(sentinel_endpoints, service_name, DISCOVERY_DEADLINE).await?;
    let rediscovery_attempts = discovered.discovery_attempts;
    let rediscovery_elapsed_ms = discovered.discovery_elapsed.as_millis();
    let mut stale_attempt_existing_keys = 0_i64;
    for key in &stale_keys {
        stale_attempt_existing_keys += redis::cmd("EXISTS")
            .arg(key)
            .query_async::<i64>(&mut discovered.connection)
            .await?;
    }
    let reconcile = executor
        .sliding_reconcile(&mut discovered.connection, &keys, &config, admitted[1], 3)
        .await?;
    let duplicate = executor
        .sliding_reconcile(&mut discovered.connection, &keys, &config, admitted[1], 3)
        .await?;
    let scripts_present_after_reconcile =
        script_exists(&mut discovered.connection, &hashes).await?;
    let state_survived = state_digest_before == state_digest_on_replica_before;
    let passed = new_primary_state.run_id == old_replica.run_id
        && discovered.state.run_id == new_primary_state.run_id
        && state_survived
        && stale_connection_error.is_some()
        && stale_attempt_existing_keys == 0
        && reconcile.first() == Some(&1)
        && duplicate.first() == Some(&0)
        && scripts_present_before_promotion
            .iter()
            .all(|present| *present == 0)
        && scripts_present_after_reconcile[1] == 1;
    Ok((
        FailoverEvidence {
            kind: "planned_sentinel_failover".to_owned(),
            elapsed_ms: elapsed.as_millis(),
            rediscovery_attempts,
            rediscovery_elapsed_ms,
            old_primary,
            new_primary: new_primary_state,
            new_replica: Some(new_replica_state.clone()),
            stale_connection_error,
            stale_attempt_existing_keys: Some(stale_attempt_existing_keys),
            state_digest_before,
            state_digest_on_replica_before,
            reconcile_reply: reconcile,
            duplicate_reply: duplicate,
            scripts_present_before_promotion,
            scripts_present_after_reconcile,
            passed,
        },
        discovered,
        new_replica_state,
    ))
}

#[allow(clippy::too_many_arguments)]
async fn unplanned_failover(
    executor: &FixedExecutor,
    mut primary: DiscoveredPrimary,
    replica: NodeState,
    live_sentinel_endpoints: &[String],
    node_urls: &[String],
    service_name: &str,
    namespace: &str,
) -> Result<(FailoverEvidence, DiscoveredPrimary)> {
    let config = SlidingConfig {
        budgets: vec![(60_000, 10)],
        reservation_timeout_ms: 60_000,
        max_keys: 100,
        max_active_reservations: 100,
    };
    let keys = fixed_sliding_window_keys(namespace, "rule", "subject");
    let admitted = executor
        .sliding_reserve(&mut primary.connection, &keys, &config, 4)
        .await?;
    if admitted.first() != Some(&1) {
        bail!("unplanned-failover setup reservation was denied: {admitted:?}");
    }
    let (_, caught_up_replica) = wait_for_replication(&primary.state.url, &replica.url).await?;
    let mut replica_connection = direct_connection(&caught_up_replica.url).await?;
    let state_digest_before = key_digests(&mut primary.connection, &keys).await?;
    let state_digest_on_replica_before = key_digests(&mut replica_connection, &keys).await?;
    flush_scripts(&mut replica_connection).await?;
    let hashes = executor.hashes();
    let scripts_present_before_promotion = script_exists(&mut replica_connection, &hashes).await?;

    let old_primary = primary.state.clone();
    let mut control = direct_connection(&old_primary.url).await?;
    let started = Instant::now();
    let shutdown_result: redis::RedisResult<String> = redis::cmd("SHUTDOWN")
        .arg("NOSAVE")
        .query_async(&mut control)
        .await;
    let new_primary_state =
        wait_for_single_primary(node_urls, &old_primary.run_id, FAILOVER_DEADLINE).await?;
    let elapsed = started.elapsed();
    let mut discovered =
        discover_primary(live_sentinel_endpoints, service_name, DISCOVERY_DEADLINE).await?;
    let rediscovery_attempts = discovered.discovery_attempts;
    let rediscovery_elapsed_ms = discovered.discovery_elapsed.as_millis();
    let reconcile = executor
        .sliding_reconcile(&mut discovered.connection, &keys, &config, admitted[1], 4)
        .await?;
    let duplicate = executor
        .sliding_reconcile(&mut discovered.connection, &keys, &config, admitted[1], 4)
        .await?;
    let scripts_present_after_reconcile =
        script_exists(&mut discovered.connection, &hashes).await?;
    let state_survived = state_digest_before == state_digest_on_replica_before;
    let passed = shutdown_result.is_err()
        && new_primary_state.run_id == replica.run_id
        && discovered.state.run_id == new_primary_state.run_id
        && state_survived
        && reconcile.first() == Some(&1)
        && duplicate.first() == Some(&0)
        && scripts_present_before_promotion
            .iter()
            .all(|present| *present == 0)
        && scripts_present_after_reconcile[1] == 1;
    Ok((
        FailoverEvidence {
            kind: "unplanned_primary_shutdown".to_owned(),
            elapsed_ms: elapsed.as_millis(),
            rediscovery_attempts,
            rediscovery_elapsed_ms,
            old_primary,
            new_primary: new_primary_state,
            new_replica: None,
            stale_connection_error: shutdown_result.err().map(|error| ErrorEvidence {
                kind: Some(format!("{:?}", error.kind())),
                code: error.code().map(str::to_owned),
                detail: error.detail().map(str::to_owned),
                display: error.to_string(),
            }),
            stale_attempt_existing_keys: None,
            state_digest_before,
            state_digest_on_replica_before,
            reconcile_reply: reconcile,
            duplicate_reply: duplicate,
            scripts_present_before_promotion,
            scripts_present_after_reconcile,
            passed,
        },
        discovered,
    ))
}

async fn stop_sentinel(endpoint: &str) -> Result<ErrorEvidence> {
    let mut connection = direct_connection(endpoint).await?;
    let result: redis::RedisResult<String> = redis::cmd("SHUTDOWN")
        .arg("NOSAVE")
        .query_async(&mut connection)
        .await;
    match result {
        Ok(reply) => bail!("Sentinel SHUTDOWN unexpectedly returned {reply}"),
        Err(error) => Ok(ErrorEvidence {
            kind: Some(format!("{:?}", error.kind())),
            code: error.code().map(str::to_owned),
            detail: error.detail().map(str::to_owned),
            display: error.to_string(),
        }),
    }
}

async fn unavailable_sentinel_scenario(
    endpoints: &[String],
    service_name: &str,
    expected_primary_run_id: &str,
) -> Result<(Scenario, Vec<String>)> {
    if endpoints.len() != 3 {
        bail!("unavailable-Sentinel scenario requires exactly three endpoints");
    }
    let stopped = endpoints[2].clone();
    let shutdown_error = stop_sentinel(&stopped).await?;
    let ordered = vec![stopped.clone(), endpoints[0].clone(), endpoints[1].clone()];
    let started = Instant::now();
    let discovered = discover_primary(&ordered, service_name, DISCOVERY_DEADLINE).await?;
    let elapsed = started.elapsed();
    let passed = discovered.state.run_id == expected_primary_run_id && elapsed < DISCOVERY_DEADLINE;
    Ok((
        Scenario {
            id: "sentinel.discovery.unavailable_first_endpoint".to_owned(),
            assertion: "ordered discovery skips one unavailable Sentinel and reaches the writable primary through a remaining endpoint within one outer deadline"
                .to_owned(),
            observed: serde_json::json!({
                "ordered_endpoints": ordered,
                "stopped_endpoint": stopped,
                "shutdown_error": shutdown_error,
                "elapsed_ms": elapsed.as_millis(),
                "discovery_attempts": discovered.discovery_attempts,
                "discovery_elapsed_ms": discovered.discovery_elapsed.as_millis(),
                "discovered_primary": discovered.state,
            }),
            passed,
        },
        vec![endpoints[0].clone(), endpoints[1].clone()],
    ))
}

async fn post_failover_quota_scenario(
    executor: &FixedExecutor,
    primary: &mut MultiplexedConnection,
    namespace: &str,
) -> Result<Scenario> {
    let config = BucketConfig {
        capacity: 2,
        refill_rate: 0.0001,
        reservation_timeout_ms: 60_000,
        max_keys: 100,
        max_active_reservations: 100,
    };
    let keys = fixed_token_bucket_keys(namespace, "rule", "subject");
    let admitted = executor.bucket_reserve(primary, &keys, &config, 2).await?;
    let settled = if admitted.first() == Some(&1) {
        executor
            .bucket_reconcile(primary, &keys, &config, admitted[1], 2)
            .await?
    } else {
        Vec::new()
    };
    let denied = executor.bucket_reserve(primary, &keys, &config, 1).await?;
    let passed =
        admitted.first() == Some(&1) && settled.first() == Some(&1) && denied.first() == Some(&0);
    Ok(Scenario {
        id: "sentinel.post_failover.admission_and_denial".to_owned(),
        assertion: "the promoted primary accepts a confirmed reservation and preserves quota-denial semantics"
            .to_owned(),
        observed: serde_json::json!({
            "admitted_reply": admitted,
            "settled_reply": settled,
            "denied_reply": denied,
        }),
        passed,
    })
}

#[allow(clippy::too_many_lines)]
pub async fn run(
    sentinel_endpoints: Vec<String>,
    data_node_urls: Vec<String>,
    service_name: &str,
    invocation: Invocation,
    product: &str,
    expected_version: &str,
) -> Result<SentinelReport> {
    if sentinel_endpoints.len() != 3 {
        bail!("Sentinel HA qualification requires exactly three Sentinel endpoints");
    }
    if data_node_urls.len() != 2 {
        bail!("Sentinel HA qualification requires exactly two data nodes");
    }
    let (initial_primary, initial_replica) =
        wait_for_primary_and_replica(&data_node_urls, None, FAILOVER_DEADLINE).await?;
    let (_, initial_replica) =
        wait_for_replication(&initial_primary.url, &initial_replica.url).await?;
    let mut discovered =
        discover_primary(&sentinel_endpoints, service_name, DISCOVERY_DEADLINE).await?;
    let initial_discovery_attempts = discovered.discovery_attempts;
    let initial_discovery_elapsed_ms = discovered.discovery_elapsed.as_millis();
    if discovered.state.run_id != initial_primary.run_id {
        bail!("Sentinel discovery and direct role observation disagree on the primary");
    }
    discovered.state.url = initial_primary.url.clone();
    let server_info: String = redis::cmd("INFO")
        .arg("SERVER")
        .query_async(&mut discovered.connection)
        .await?;
    let initial_server_identity = selected_server_identity(&server_info);
    let version_matches = ["redis_version", "valkey_version"]
        .into_iter()
        .filter_map(|key| initial_server_identity.get(key))
        .any(|version| version.starts_with(expected_version));

    let conformance = run_invocation(invocation, &mut discovered.connection, product).await?;
    let conformance_passed = conformance.passed();
    wait_for_replication(&initial_primary.url, &initial_replica.url).await?;

    let executor = FixedExecutor::new(invocation);
    let namespace = format!("praxis:spike:sentinel:{product}:{invocation:?}").to_lowercase();
    let mut scenarios = vec![Scenario {
        id: "sentinel.steady_state.cached_primary".to_owned(),
        assertion: "the complete accounting conformance suite reuses the one discovered primary connection without another Sentinel discovery call"
            .to_owned(),
        observed: serde_json::json!({
            "initial_discovery_attempts": initial_discovery_attempts,
            "accounting_operations_use_cached_connection": true,
            "sentinel_calls_from_accounting_suite": 0,
        }),
        passed: conformance_passed,
    }];
    scenarios.push(
        replication_health_scenario(
            &executor,
            &mut discovered.connection,
            &initial_primary,
            &initial_replica,
            &namespace,
        )
        .await?,
    );
    scenarios.push(
        ambiguous_timeout_scenario(
            &executor,
            &mut discovered.connection,
            &initial_primary,
            &namespace,
        )
        .await?,
    );

    let (planned, mut after_planned, planned_replica) = planned_failover(
        &executor,
        discovered.connection,
        initial_primary.clone(),
        initial_replica.clone(),
        &sentinel_endpoints,
        &data_node_urls,
        service_name,
        &format!("{namespace}:planned"),
    )
    .await?;
    after_planned.state.url = planned.new_primary.url.clone();

    let (unavailable, live_sentinels) = unavailable_sentinel_scenario(
        &sentinel_endpoints,
        service_name,
        &after_planned.state.run_id,
    )
    .await?;
    scenarios.push(unavailable);

    let (unplanned, mut final_primary) = unplanned_failover(
        &executor,
        after_planned,
        planned_replica,
        &live_sentinels,
        &data_node_urls,
        service_name,
        &format!("{namespace}:unplanned"),
    )
    .await?;
    final_primary.state.url = unplanned.new_primary.url.clone();
    scenarios.push(
        post_failover_quota_scenario(
            &executor,
            &mut final_primary.connection,
            &format!("{namespace}:post-failover"),
        )
        .await?,
    );

    let failovers = vec![planned, unplanned];
    let mut failures = Vec::new();
    if !version_matches {
        failures.push(format!(
            "server identity does not contain expected version {expected_version}"
        ));
    }
    if !conformance_passed {
        failures
            .push("standalone accounting contract failed through Sentinel discovery".to_owned());
    }
    failures.extend(
        scenarios
            .iter()
            .filter(|scenario| !scenario.passed)
            .map(|scenario| format!("scenario {} failed", scenario.id)),
    );
    failures.extend(
        failovers
            .iter()
            .filter(|failover| !failover.passed)
            .map(|failover| format!("{} failed", failover.kind)),
    );

    Ok(SentinelReport {
        schema_version: 1,
        evidence_class: "sentinel_ha_candidate_qualification".to_owned(),
        candidate: "mixed_configuration_repair".to_owned(),
        source_commit: SOURCE_COMMIT.to_owned(),
        product: product.to_owned(),
        expected_version: expected_version.to_owned(),
        invocation,
        protocol: "RESP2".to_owned(),
        client: ClientProfile {
            crate_name: "redis".to_owned(),
            version: "1.7.0".to_owned(),
            checksum: "2acbc41a996f7652b2ddd9dfd98cc4ff602cfd742ae35382f07f608405ab50ed".to_owned(),
            features: vec![
                "tokio-comp".to_owned(),
                "sentinel".to_owned(),
                "script".to_owned(),
            ],
            connect_timeout_ms: 500,
            response_timeout_ms: 500,
            operation_deadline_ms: 1_000,
            discovery_deadline_ms: 3_000,
            automatic_mutation_retries: 0,
            discovery_retry_scope:
                "read-only Sentinel traversal and pre-dispatch data-node connection only".to_owned(),
        },
        topology: TopologyProfile {
            profile: "one_primary_one_replica_three_sentinels".to_owned(),
            service_name: service_name.to_owned(),
            sentinel_endpoints,
            data_node_urls,
            sentinel_count: 3,
            data_node_count: 2,
            quorum: 2,
            down_after_ms: 500,
            failover_timeout_ms: 5_000,
            persistence: "disabled for isolated semantic/fault qualification".to_owned(),
            initial_discovery_attempts,
            initial_discovery_elapsed_ms,
            initial_primary,
            initial_replica,
        },
        initial_server_identity,
        standalone_contract_over_sentinel: serde_json::to_value(conformance)?,
        scenarios,
        failovers,
        failures,
    })
}

impl SentinelReport {
    #[must_use]
    pub fn passed(&self) -> bool {
        self.failures.is_empty()
    }
}
