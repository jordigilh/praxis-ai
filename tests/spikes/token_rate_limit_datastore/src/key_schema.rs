use std::fmt::Write as _;

use sha2::{Digest, Sha256};

fn digest_hex(parts: &[&[u8]]) -> String {
    let mut digest = Sha256::new();
    for (index, part) in parts.iter().enumerate() {
        if index > 0 {
            digest.update([0]);
        }
        digest.update(part);
    }
    digest
        .finalize()
        .iter()
        .fold(String::with_capacity(64), |mut output, byte| {
            write!(output, "{byte:02x}").expect("writing to a String cannot fail");
            output
        })
}

#[must_use]
pub fn sliding_window_keys(namespace: &str, rule: &str, subject: &str) -> Vec<String> {
    let rule_hash = digest_hex(&[namespace.as_bytes(), rule.as_bytes()]);
    let rule_prefix = format!("{namespace}:v1:rule:{rule_hash}");
    let hash = digest_hex(&[namespace.as_bytes(), rule.as_bytes(), subject.as_bytes()]);
    let prefix = format!("{namespace}:v1:{hash}");
    vec![
        prefix.clone(),
        format!("{prefix}:settled"),
        format!("{prefix}:active"),
        format!("{namespace}:keys"),
        format!("{namespace}:active-count"),
        format!("{namespace}:reservation-seq"),
        format!("{namespace}:active-index"),
        format!("{rule_prefix}:active-count"),
        format!("{rule_prefix}:active-index"),
        format!("{rule_prefix}:keys"),
        format!("{rule_prefix}:balances"),
        format!("{rule_prefix}:remaining-total"),
    ]
}

/// Keys used by the mixed-configuration repair candidate. The first twelve
/// entries deliberately preserve the frozen layout; the final persistent key
/// stores the rule/algorithm accounting fingerprint.
#[must_use]
pub fn fixed_sliding_window_keys(namespace: &str, rule: &str, subject: &str) -> Vec<String> {
    let mut keys = sliding_window_keys(namespace, rule, subject);
    let rule_hash = digest_hex(&[namespace.as_bytes(), rule.as_bytes()]);
    keys.push(format!("{namespace}:v1:rule:{rule_hash}:accounting-config"));
    keys
}

#[must_use]
pub fn token_bucket_keys(namespace: &str, rule: &str, subject: &str) -> Vec<String> {
    let rule_hash = digest_hex(&[namespace.as_bytes(), b"token_bucket", rule.as_bytes()]);
    let rule_prefix = format!("{namespace}:v1:tb:rule:{rule_hash}");
    let hash = digest_hex(&[
        namespace.as_bytes(),
        b"token_bucket",
        rule.as_bytes(),
        subject.as_bytes(),
    ]);
    let prefix = format!("{namespace}:v1:tb:{hash}");
    vec![
        prefix.clone(),
        format!("{prefix}:active"),
        format!("{namespace}:tb:keys"),
        format!("{namespace}:tb:active-count"),
        format!("{namespace}:tb:reservation-seq"),
        format!("{namespace}:tb:active-index"),
        format!("{rule_prefix}:active-count"),
        format!("{rule_prefix}:active-index"),
        format!("{rule_prefix}:keys"),
        format!("{rule_prefix}:balances"),
        format!("{rule_prefix}:remaining-total"),
    ]
}

/// Token-bucket counterpart to [`fixed_sliding_window_keys`].
#[must_use]
pub fn fixed_token_bucket_keys(namespace: &str, rule: &str, subject: &str) -> Vec<String> {
    let mut keys = token_bucket_keys(namespace, rule, subject);
    let rule_hash = digest_hex(&[namespace.as_bytes(), b"token_bucket", rule.as_bytes()]);
    keys.push(format!(
        "{namespace}:v1:tb:rule:{rule_hash}:accounting-config"
    ));
    keys
}

#[must_use]
pub fn plain_sliding_transaction_keys(namespace: &str, rule: &str, subject: &str) -> Vec<String> {
    let id = digest_hex(&[namespace.as_bytes(), rule.as_bytes(), subject.as_bytes()]);
    let prefix = format!("{namespace}:v2:{id}");
    vec![
        format!("{prefix}:u1000:1"),
        format!("{prefix}:r:1"),
        format!("{namespace}:v2:active"),
        format!("{namespace}:v2:keys"),
    ]
}

#[must_use]
pub fn plain_sliding_mget_keys(namespace: &str, rule: &str, subject: &str) -> Vec<String> {
    let id = digest_hex(&[namespace.as_bytes(), rule.as_bytes(), subject.as_bytes()]);
    let prefix = format!("{namespace}:v2:{id}");
    vec![format!("{prefix}:u1000:1"), format!("{prefix}:u1000:2")]
}

#[must_use]
pub fn plain_bucket_transaction_keys(namespace: &str, rule: &str, subject: &str) -> Vec<String> {
    let id = digest_hex(&[
        namespace.as_bytes(),
        b"token_bucket",
        rule.as_bytes(),
        subject.as_bytes(),
    ]);
    let bucket = format!("{namespace}:v2:tb:{id}");
    vec![
        bucket.clone(),
        format!("{bucket}:r:1"),
        format!("{namespace}:v2:tb:active"),
        format!("{namespace}:v2:tb:keys"),
    ]
}

#[must_use]
pub fn tagged_subject_atomic_keys(namespace: &str, rule: &str, subject: &str) -> Vec<String> {
    let id = digest_hex(&[namespace.as_bytes(), rule.as_bytes(), subject.as_bytes()]);
    let prefix = format!("{namespace}:v3:{{{id}}}");
    vec![
        format!("{prefix}:state"),
        format!("{prefix}:active"),
        format!("{prefix}:settled"),
    ]
}

fn hashtag(key: &[u8]) -> &[u8] {
    let Some(open) = key.iter().position(|byte| *byte == b'{') else {
        return key;
    };
    let after = &key[open + 1..];
    let Some(close) = after.iter().position(|byte| *byte == b'}') else {
        return key;
    };
    if close == 0 { key } else { &after[..close] }
}

fn crc16_xmodem(bytes: &[u8]) -> u16 {
    let mut crc = 0_u16;
    for byte in bytes {
        crc ^= u16::from(*byte) << 8;
        for _ in 0..8 {
            crc = if crc & 0x8000 == 0 {
                crc << 1
            } else {
                (crc << 1) ^ 0x1021
            };
        }
    }
    crc
}

#[must_use]
pub fn cluster_slot(key: &str) -> u16 {
    crc16_xmodem(hashtag(key.as_bytes())) % 16_384
}

#[cfg(test)]
mod tests {
    use super::{
        cluster_slot, crc16_xmodem, fixed_sliding_window_keys, fixed_token_bucket_keys,
        plain_bucket_transaction_keys, plain_sliding_transaction_keys, sliding_window_keys,
        tagged_subject_atomic_keys, token_bucket_keys,
    };

    #[test]
    fn crc_matches_xmodem_check_value() {
        assert_eq!(crc16_xmodem(b"123456789"), 0x31c3);
    }

    #[test]
    fn hashtag_controls_slot() {
        assert_eq!(cluster_slot("a:{subject}:1"), cluster_slot("b:{subject}:2"));
        assert_ne!(cluster_slot("a:{one}"), cluster_slot("a:{two}"));
    }

    #[test]
    fn frozen_key_counts_match_source() {
        assert_eq!(sliding_window_keys("ns", "rule", "subject").len(), 12);
        assert_eq!(token_bucket_keys("ns", "rule", "subject").len(), 11);
    }

    #[test]
    fn repaired_key_counts_match_source() {
        assert_eq!(fixed_sliding_window_keys("ns", "rule", "subject").len(), 13);
        assert_eq!(fixed_token_bucket_keys("ns", "rule", "subject").len(), 12);
        assert!(
            fixed_sliding_window_keys("ns", "rule", "subject")[12].ends_with(":accounting-config")
        );
        assert!(
            fixed_token_bucket_keys("ns", "rule", "subject")[11].ends_with(":accounting-config")
        );
    }

    #[test]
    fn frozen_plain_transactions_are_cross_slot() {
        for keys in [
            plain_sliding_transaction_keys("ns", "rule", "subject"),
            plain_bucket_transaction_keys("ns", "rule", "subject"),
        ] {
            let first = cluster_slot(&keys[0]);
            assert!(keys.iter().any(|key| cluster_slot(key) != first));
        }
    }

    #[test]
    fn tagged_subject_groups_are_same_slot_and_subjects_distribute() {
        let alice = tagged_subject_atomic_keys("ns", "rule", "alice");
        assert!(
            alice
                .iter()
                .all(|key| cluster_slot(key) == cluster_slot(&alice[0]))
        );
        let slots = (0..64)
            .map(|index| {
                let keys = tagged_subject_atomic_keys("ns", "rule", &format!("subject-{index}"));
                cluster_slot(&keys[0])
            })
            .collect::<std::collections::BTreeSet<_>>();
        assert!(slots.len() > 1);
    }
}
