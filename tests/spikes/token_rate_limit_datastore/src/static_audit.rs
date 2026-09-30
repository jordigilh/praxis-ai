use std::collections::{BTreeMap, BTreeSet};

use serde::Serialize;

use crate::key_schema::{
    cluster_slot, plain_bucket_transaction_keys, plain_sliding_mget_keys,
    plain_sliding_transaction_keys, sliding_window_keys, tagged_subject_atomic_keys,
    token_bucket_keys,
};

const SOURCE_MANIFEST: &str = include_str!("../fixtures/source/manifest.json");
const BASELINE_CARGO: &str = include_str!("../fixtures/source/baseline/Cargo.toml");
const SLIDING_RESERVE: &str =
    include_str!("../fixtures/source/baseline/lua/sliding_window_reserve.lua");
const SLIDING_RECONCILE: &str =
    include_str!("../fixtures/source/baseline/lua/sliding_window_reconcile.lua");
const BUCKET_RESERVE: &str =
    include_str!("../fixtures/source/baseline/lua/token_bucket_reserve.lua");
const BUCKET_RECONCILE: &str =
    include_str!("../fixtures/source/baseline/lua/token_bucket_reconcile.lua");
const PLAIN_CONNECTION: &str = include_str!("../fixtures/source/plain/connection.rs");
const PLAIN_SLIDING: &str = include_str!("../fixtures/source/plain/sliding_window.rs");
const PLAIN_BUCKET: &str = include_str!("../fixtures/source/plain/token_bucket.rs");

#[derive(Debug, Serialize)]
pub struct StaticAudit {
    schema_version: u8,
    source_manifest: serde_json::Value,
    baseline_redis_dependency: String,
    lua_commands: BTreeMap<String, Vec<String>>,
    lua_dynamic_key_access: BTreeMap<String, Vec<String>>,
    plain_commands: BTreeMap<String, Vec<String>>,
    plain_source_findings: Vec<SourceFinding>,
    key_layouts: BTreeMap<String, KeyLayoutAudit>,
    native_cluster_contract: NativeClusterAudit,
}

#[derive(Debug, Serialize)]
struct KeyLayoutAudit {
    key_count: usize,
    distinct_slots: usize,
    keys: Vec<KeySlot>,
    same_slot: bool,
}

#[derive(Debug, Serialize)]
struct KeySlot {
    key: String,
    slot: u16,
}

#[derive(Debug, Serialize)]
struct SourceFinding {
    id: String,
    algorithms: Vec<String>,
    assertion: String,
    observation: String,
    immutable_source_references: Vec<String>,
    qualification: String,
}

#[derive(Debug, Serialize)]
struct NativeClusterAudit {
    unchanged_contract_satisfiable: bool,
    conclusion: String,
    proof: Vec<String>,
    per_subject_same_slot_example: KeyLayoutAudit,
    representative_subject_count: usize,
    representative_distinct_slot_count: usize,
    contract_preserving_options: Vec<String>,
    non_qualifying_options: Vec<String>,
}

fn extract_quoted_after<'a>(line: &'a str, marker: &str) -> Option<&'a str> {
    let rest = line.split_once(marker)?.1;
    let quote = rest.as_bytes().first().copied()?;
    if quote != b'\'' && quote != b'"' {
        return None;
    }
    let rest = &rest[1..];
    let end = rest.find(char::from(quote))?;
    Some(&rest[..end])
}

fn lua_commands(source: &str) -> Vec<String> {
    source
        .lines()
        .filter_map(|line| extract_quoted_after(line, "redis.call("))
        .map(str::to_uppercase)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn dynamic_key_lines(source: &str) -> Vec<String> {
    source
        .lines()
        .enumerate()
        .filter(|(_, line)| {
            (line.contains("physical ..") || line.contains("active_key"))
                && line.contains("redis.call")
        })
        .map(|(index, line)| format!("{}: {}", index + 1, line.trim()))
        .collect()
}

fn production_source(source: &str) -> &str {
    source.split("\n#[cfg(test)]").next().unwrap_or(source)
}

fn plain_commands(source: &str) -> Vec<String> {
    let source = production_source(source);
    let mut commands = BTreeSet::new();
    for line in source.lines() {
        for marker in ["redis::cmd(", ".cmd("] {
            if let Some(command) = extract_quoted_after(line, marker) {
                commands.insert(command.to_uppercase());
            }
        }
    }
    if source.contains(".atomic()") {
        commands.insert("EXEC".to_owned());
        commands.insert("MULTI".to_owned());
    }
    commands.into_iter().collect()
}

fn key_layout(keys: Vec<String>) -> KeyLayoutAudit {
    let keys = keys
        .into_iter()
        .map(|key| KeySlot {
            slot: cluster_slot(&key),
            key,
        })
        .collect::<Vec<_>>();
    let distinct_slots = keys
        .iter()
        .map(|entry| entry.slot)
        .collect::<BTreeSet<_>>()
        .len();
    KeyLayoutAudit {
        key_count: keys.len(),
        distinct_slots,
        same_slot: distinct_slots == 1,
        keys,
    }
}

fn plain_source_findings() -> Vec<SourceFinding> {
    vec![
        SourceFinding {
            id: "plain.sliding.reconcile_split_claim_update".to_owned(),
            algorithms: vec!["sliding_window".to_owned()],
            assertion: "reconcile must apply a refund or overage exactly once, or not claim the reservation"
                .to_owned(),
            observation: "GETDEL claims the reservation in one round trip; the later MULTI applies the delta. The frozen source explicitly states that a failed settle loses the delta and that a lost overage under-charges the window."
                .to_owned(),
            immutable_source_references: vec![
                "plain/sliding_window.rs:442-468".to_owned(),
                "plain/sliding_window.rs:560-577".to_owned(),
            ],
            qualification: "fail_source_proven".to_owned(),
        },
        SourceFinding {
            id: "plain.sliding.reserve_check_write_race".to_owned(),
            algorithms: vec!["sliding_window".to_owned()],
            assertion: "concurrent reserves must obey an explicitly approved over-admission bound"
                .to_owned(),
            observation: "reserve reads and decides before a separate write transaction; the frozen candidate test explicitly accepts that two 60-token reservations may both admit against capacity 100. No such weakened bound is approved for this spike."
                .to_owned(),
            immutable_source_references: vec![
                "plain/sliding_window.rs:530-547".to_owned(),
                "plain/sliding_window.rs:1035-1062".to_owned(),
            ],
            qualification: "fail_source_proven".to_owned(),
        },
        SourceFinding {
            id: "plain.sliding.namespace_cap_race".to_owned(),
            algorithms: vec!["sliding_window".to_owned()],
            assertion: "concurrent distinct-key admissions must not exceed exact retained-key or active-reservation caps"
                .to_owned(),
            observation: "namespace cap zsets are read before the later reservation transaction and are not watched, so distinct subjects can independently observe capacity and both commit."
                .to_owned(),
            immutable_source_references: vec![
                "plain/sliding_window.rs:246-271".to_owned(),
                "plain/sliding_window.rs:385-423".to_owned(),
                "plain/sliding_window.rs:530-547".to_owned(),
            ],
            qualification: "fail_source_proven".to_owned(),
        },
        SourceFinding {
            id: "plain.token_bucket.namespace_cap_race".to_owned(),
            algorithms: vec!["token_bucket".to_owned()],
            assertion: "concurrent distinct-key admissions must not exceed exact retained-key or active-reservation caps"
                .to_owned(),
            observation: "WATCH covers only the subject bucket, while namespace cap zsets are read and later changed without being watched; distinct subjects therefore do not conflict."
                .to_owned(),
            immutable_source_references: vec![
                "plain/token_bucket.rs:428-462".to_owned(),
                "plain/token_bucket.rs:496-529".to_owned(),
            ],
            qualification: "fail_source_proven".to_owned(),
        },
        SourceFinding {
            id: "plain.mixed_configuration_ttl_shortening".to_owned(),
            algorithms: vec!["sliding_window".to_owned(), "token_bucket".to_owned()],
            assertion: "one writer must not shorten another writer's still-required state TTL"
                .to_owned(),
            observation: "per-subject counter and bucket TTLs use unconditional PEXPIRE with the local writer's horizon; the shared-index NX/GT helper does not protect those keys."
                .to_owned(),
            immutable_source_references: vec![
                "plain/mod.rs:83-91".to_owned(),
                "plain/sliding_window.rs:430-439".to_owned(),
                "plain/token_bucket.rs:343-363".to_owned(),
            ],
            qualification: "fail_source_proven".to_owned(),
        },
    ]
}

fn native_cluster_audit() -> NativeClusterAudit {
    let representative_subject_count = 256;
    let slots = (0..representative_subject_count)
        .map(|index| {
            let keys =
                tagged_subject_atomic_keys("praxis:spike", "rule-a", &format!("subject-{index}"));
            cluster_slot(&keys[0])
        })
        .collect::<BTreeSet<_>>();
    NativeClusterAudit {
        unchanged_contract_satisfiable: false,
        conclusion: "Exact rule- or namespace-wide caps, a shared reservation sequence, and atomic aggregate telemetry cannot coexist with cross-primary subject distribution in one Redis/Valkey Cluster atomic operation."
            .to_owned(),
        proof: vec![
            "Every key touched by one script, WATCH transaction, or atomic multi-key command must occupy one Cluster slot."
                .to_owned(),
            "An exact aggregate cap must be checked and incremented atomically with each admitted subject; therefore its shared key must occupy the same slot as every subject state it governs."
                .to_owned(),
            "Two subject states on different primary-owned slots cannot both share one slot with the same aggregate key; co-locating all subjects satisfies atomicity but fails the required sharding distribution assertion."
                .to_owned(),
            "Splitting caps, IDs, cleanup, or telemetry by slot changes their scope; coordinating them across slots adds a Praxis coordination layer and ambiguous partial-commit states."
                .to_owned(),
        ],
        per_subject_same_slot_example: key_layout(tagged_subject_atomic_keys(
            "praxis:spike",
            "rule-a",
            "subject-a",
        )),
        representative_subject_count,
        representative_distinct_slot_count: slots.len(),
        contract_preserving_options: vec![
            "keep the backend standalone or Sentinel HA-only".to_owned(),
            "obtain explicit product approval to remove or redefine aggregate semantics, then version a new Cluster candidate"
                .to_owned(),
        ],
        non_qualifying_options: vec![
            "co-locate the whole namespace in one hash slot".to_owned(),
            "use per-shard caps or IDs without an approved contract change".to_owned(),
            "add Praxis shard maps, routing, coordination, or rebalancing".to_owned(),
            "layer Sentinel over shards".to_owned(),
        ],
    }
}

#[must_use]
pub fn run() -> StaticAudit {
    let scripts = [
        ("sliding_window_reserve", SLIDING_RESERVE),
        ("sliding_window_reconcile", SLIDING_RECONCILE),
        ("token_bucket_reserve", BUCKET_RESERVE),
        ("token_bucket_reconcile", BUCKET_RECONCILE),
    ];
    let lua_commands = scripts
        .iter()
        .map(|(name, source)| ((*name).to_owned(), lua_commands(source)))
        .collect();
    let lua_dynamic_key_access = scripts
        .iter()
        .map(|(name, source)| ((*name).to_owned(), dynamic_key_lines(source)))
        .collect();
    let plain_commands = [
        ("connection", PLAIN_CONNECTION),
        ("sliding_window", PLAIN_SLIDING),
        ("token_bucket", PLAIN_BUCKET),
    ]
    .into_iter()
    .map(|(name, source)| (name.to_owned(), plain_commands(source)))
    .collect();
    let key_layouts = [
        (
            "sliding_window".to_owned(),
            key_layout(sliding_window_keys("praxis:spike", "rule-a", "subject-a")),
        ),
        (
            "token_bucket".to_owned(),
            key_layout(token_bucket_keys("praxis:spike", "rule-a", "subject-a")),
        ),
        (
            "plain_sliding_window_transaction".to_owned(),
            key_layout(plain_sliding_transaction_keys(
                "praxis:spike",
                "rule-a",
                "subject-a",
            )),
        ),
        (
            "plain_sliding_window_mget".to_owned(),
            key_layout(plain_sliding_mget_keys(
                "praxis:spike",
                "rule-a",
                "subject-a",
            )),
        ),
        (
            "plain_token_bucket_transaction".to_owned(),
            key_layout(plain_bucket_transaction_keys(
                "praxis:spike",
                "rule-a",
                "subject-a",
            )),
        ),
    ]
    .into_iter()
    .collect();
    let baseline_redis_dependency = BASELINE_CARGO
        .lines()
        .find(|line| line.trim_start().starts_with("redis ="))
        .unwrap_or("redis dependency not found")
        .to_owned();

    StaticAudit {
        schema_version: 1,
        source_manifest: serde_json::from_str(SOURCE_MANIFEST)
            .expect("generated source manifest is valid JSON"),
        baseline_redis_dependency,
        lua_commands,
        lua_dynamic_key_access,
        plain_commands,
        plain_source_findings: plain_source_findings(),
        key_layouts,
        native_cluster_contract: native_cluster_audit(),
    }
}

#[cfg(test)]
mod tests {
    use super::run;

    #[test]
    fn frozen_layouts_are_cross_slot_negative_controls() {
        let audit = run();
        assert!(!audit.key_layouts["sliding_window"].same_slot);
        assert!(!audit.key_layouts["token_bucket"].same_slot);
        assert!(!audit.key_layouts["plain_sliding_window_transaction"].same_slot);
        assert!(!audit.key_layouts["plain_token_bucket_transaction"].same_slot);
    }

    #[test]
    fn frozen_cleanup_has_dynamic_key_access() {
        let audit = run();
        assert!(!audit.lua_dynamic_key_access["sliding_window_reserve"].is_empty());
        assert!(!audit.lua_dynamic_key_access["token_bucket_reserve"].is_empty());
    }

    #[test]
    fn unchanged_cluster_contract_is_unsatisfiable() {
        let audit = run();
        assert!(!audit.native_cluster_contract.unchanged_contract_satisfiable);
        assert!(
            audit
                .native_cluster_contract
                .per_subject_same_slot_example
                .same_slot
        );
        assert!(
            audit
                .native_cluster_contract
                .representative_distinct_slot_count
                > 1
        );
    }
}
