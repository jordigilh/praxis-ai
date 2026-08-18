// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Praxis Contributors

//! `cargo xtask lint-extended` -- diff-scoped heuristic checks for common
//! low-quality-code patterns that automated compiler lints can't catch
//! structurally.
//!
//! Clippy already denies the machine-checkable half of this class of issue
//! (`unwrap_used`, `expect_used`, `panic`, `todo`/`unimplemented`,
//! `dead_code`, `missing_docs`, print/`dbg` macros, and more, per this
//! crate's own lint configuration). What lint tooling structurally cannot
//! check is comment *content* and diff-local *repetition* -- two common
//! low-effort-code tells. This checks only lines added or changed versus
//! the diff base, so pre-existing code is never relitigated.
//!
//! Checks (block = fails; warn = printed, does not fail):
//!   - block: leftover `TODO`/`FIXME`/`XXX`/`HACK` markers in comments
//!   - block: commented-out code
//!   - warn: narrating "what the code does" comments
//!   - warn: the same numeric or string literal repeated 3+ times without a named constant
//!   - warn: weak or generic identifier names introduced by a new binding
//!   - warn: new clippy lint suppressions added
//!
//! Diff base resolution: `--base`, else `EXTENDED_LINT_BASE`, else the
//! `GitHub` Actions pull-request base ref, else `origin/main`.

use std::{
    collections::{HashMap, HashSet},
    process::Command,
    sync::LazyLock,
};

use clap::Parser;
use regex::Regex;

// -----------------------------------------------------------------------------
// CLI Arguments
// -----------------------------------------------------------------------------

/// CLI arguments for `cargo xtask lint-extended`.
#[derive(Parser)]
pub(crate) struct Args {
    /// Git ref or SHA to diff against, overriding automatic resolution.
    #[arg(long, value_name = "REF")]
    base: Option<String>,
}

// -----------------------------------------------------------------------------
// Patterns
// -----------------------------------------------------------------------------

/// Matches a leftover `TODO`/`FIXME`/`XXX`/`HACK` marker in a comment.
static TODO_MARKER_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)//.*\b(TODO|FIXME|XXX|HACK)\b").expect("valid regex"));

/// Matches comment text shaped like commented-out Rust code.
static COMMENTED_CODE_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"^//+\s*(let\s+\w|fn\s+\w|if\s*\(|for\s*\(|match\s+\w|return\b|\
          \w+\s*\([^)]*\)\s*;?\s*$|\w+\.\w+\(.*\)\s*;?\s*$|[\w:<>]+\s*=\s*.+;\s*$)",
    )
    .expect("valid regex")
});

/// Matches a new `let`/`fn` binding introducing a weak, generic name.
static WEAK_NAME_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^(let(?:\s+mut)?|fn)\s+(temp|tmp|foo|bar|thing|val|obj|stuff)\b").expect("valid regex")
});

/// Matches a numeric or string literal worth tracking for repetition.
static LIT_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r#"(?:^|[^\w.])(\d{2,}|"[^"]{4,}")(?:$|[^\w])"#).expect("valid regex"));

/// Matches a `const`/`static` declaration line.
static CONST_LINE_RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\b(const|static)\s+\w+").expect("valid regex"));

/// Matches a newly added clippy lint suppression.
static SUPPRESSION_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"#\[(allow|expect)\(clippy::").expect("valid regex"));

/// Matches the start of a file's test module.
static TEST_MODULE_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^(#\[cfg\(test\)\]|mod tests\b)").expect("valid regex"));

/// Matches a unified diff hunk header, capturing the post-diff start line.
static HUNK_RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^@@ -\d+(?:,\d+)? \+(\d+)").expect("valid regex"));

/// Lowercased comment openers indicating "what this does" narration rather
/// than a "why" explanation.
const NARRATING_OPENERS: &[&str] = &[
    "increment",
    "decrement",
    "loop through",
    "iterate over",
    "iterate through",
    "return the",
    "returns the",
    "create a",
    "creates a",
    "initialize",
    "set the",
    "sets the",
    "get the",
    "gets the",
    "parse the",
    "parses the",
    "convert ",
    "converts ",
    "check if",
    "checks if",
    "validate that",
    "validates that",
    "call ",
    "calls ",
    "define ",
    "defines ",
    "import ",
    "imports ",
    "declare ",
    "declares ",
    "instantiate",
    "loop over",
    "append ",
    "appends ",
    "remove ",
    "removes ",
    "add ",
    "adds ",
];

/// Minimum number of repeated-literal occurrences that triggers a warning.
const MIN_LITERAL_REPETITIONS: usize = 3;

// -----------------------------------------------------------------------------
// Entry Point
// -----------------------------------------------------------------------------

/// Run diff-scoped heuristic checks and exit non-zero on blocking findings.
pub(crate) fn run(args: &Args) {
    let diff_base = resolve_diff_base(args.base.as_deref());
    let added = run_diff(&diff_base);
    if added.is_empty() {
        println!("[extended-lint] no added Rust lines vs {diff_base}; nothing to check.");
        return;
    }

    let report = evaluate(&added);
    print_warnings(&report.warnings);

    if report.blocking.is_empty() {
        eprintln!("[extended-lint] no blocking findings.");
        return;
    }
    print_blocking(&report.blocking);
    std::process::exit(1);
}

/// Resolve the diff base: CLI arg, then `EXTENDED_LINT_BASE`, then the
/// `GitHub` Actions pull-request base ref, then `origin/main`.
fn resolve_diff_base(cli_arg: Option<&str>) -> String {
    if let Some(base) = cli_arg {
        return base.to_owned();
    }
    if let Ok(base) = std::env::var("EXTENDED_LINT_BASE") {
        return base;
    }
    if let Ok(base_ref) = std::env::var("GITHUB_BASE_REF") {
        return format!("origin/{base_ref}");
    }
    "origin/main".to_owned()
}

/// Print non-blocking warnings to stderr, if any.
fn print_warnings(warnings: &[String]) {
    if warnings.is_empty() {
        return;
    }
    eprintln!("[extended-lint] warnings (review, does not block):");
    for warning in warnings {
        eprintln!("  - {warning}");
    }
    eprintln!();
}

/// Print blocking findings and the remediation hint to stderr.
fn print_blocking(blocking: &[String]) {
    eprintln!("[extended-lint] BLOCKING findings:");
    for finding in blocking {
        eprintln!("  - {finding}");
    }
    eprintln!();
    eprintln!("[extended-lint] fix the above, or if a match is a false positive, note why in the PR description.");
}

// -----------------------------------------------------------------------------
// Diff Collection
// -----------------------------------------------------------------------------

/// One line added or changed in the diff, at its post-diff location.
struct AddedLine {
    /// Path of the file containing the line, relative to the repo root.
    file: String,
    /// One-based line number in the post-diff version of the file.
    lineno: usize,
    /// The line's content, without the diff `+` prefix.
    content: String,
}

/// Run `git diff` against `diff_base` and collect every added Rust line.
fn run_diff(diff_base: &str) -> Vec<AddedLine> {
    let Ok(output) = Command::new("git")
        .args(["diff", "--unified=0", diff_base, "--", "*.rs"])
        .output()
    else {
        eprintln!("[extended-lint] failed to run git diff against {diff_base}");
        std::process::exit(1);
    };
    if !output.status.success() {
        eprintln!(
            "[extended-lint] git diff against {diff_base} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        std::process::exit(1);
    }

    parse_unified_diff(&String::from_utf8_lossy(&output.stdout))
}

/// Parse unified diff output into the list of added lines it contains.
fn parse_unified_diff(diff: &str) -> Vec<AddedLine> {
    let mut added = Vec::new();
    let mut current_file = String::new();
    let mut new_lineno: usize = 0;

    for line in diff.lines() {
        if let Some(path) = line.strip_prefix("+++ b/") {
            path.clone_into(&mut current_file);
            continue;
        }
        if let Some(caps) = HUNK_RE.captures(line) {
            new_lineno = caps.get(1).and_then(|m| m.as_str().parse().ok()).unwrap_or(0);
            continue;
        }
        if line.starts_with("+++") || line.starts_with("---") {
            continue;
        }
        if let Some(content) = line.strip_prefix('+') {
            added.push(AddedLine {
                file: current_file.clone(),
                lineno: new_lineno,
                content: content.to_owned(),
            });
            new_lineno += 1;
        } else if !line.starts_with('-') {
            new_lineno += 1;
        }
    }
    added
}

// -----------------------------------------------------------------------------
// Checks
// -----------------------------------------------------------------------------

/// Accumulated findings from evaluating every added line.
#[derive(Default)]
struct Report {
    /// Findings that fail the check.
    blocking: Vec<String>,
    /// Findings that are printed but do not fail the check.
    warnings: Vec<String>,
}

/// Per-`(file, literal)` occurrence sites recorded as `(line text, lineno)`.
type LiteralSites = HashMap<(String, String), Vec<(String, usize)>>;

/// Per-file set of literals already declared as a named constant.
type ConstDeclared = HashMap<String, HashSet<String>>;

/// Evaluate every added line and return the combined blocking/warning report.
fn evaluate(added: &[AddedLine]) -> Report {
    let mut report = Report::default();
    let mut literal_sites: LiteralSites = HashMap::new();
    let mut const_declared: ConstDeclared = HashMap::new();

    for line in added {
        check_line(line, &mut report, &mut literal_sites, &mut const_declared);
    }

    report
        .warnings
        .extend(repeated_literal_warnings(&literal_sites, &const_declared));
    report
}

/// Run every per-line check against one added line.
fn check_line(
    line: &AddedLine,
    report: &mut Report,
    literal_sites: &mut LiteralSites,
    const_declared: &mut ConstDeclared,
) {
    let stripped = line.content.trim();
    let comment = comment_text(&line.content);

    check_todo_marker(line, stripped, comment, &mut report.blocking);
    check_commented_code(line, stripped, comment, &mut report.blocking);
    check_narrating_comment(line, stripped, comment, &mut report.warnings);
    check_weak_name(line, stripped, &mut report.warnings);
    check_suppression(line, stripped, &mut report.warnings);
    record_literal_occurrences(line, stripped, literal_sites, const_declared);
}

/// Extract a line's trailing `//` comment text, if any.
fn comment_text(content: &str) -> Option<&str> {
    content.find("//").and_then(|index| content.get(index..)).map(str::trim)
}

/// Whether a `//`-prefixed comment is a doc comment (`///` or `//!`).
fn is_doc_comment(comment: &str) -> bool {
    comment.starts_with("///") || comment.starts_with("//!")
}

/// Block leftover `TODO`/`FIXME`/`XXX`/`HACK` markers in a comment.
fn check_todo_marker(line: &AddedLine, stripped: &str, comment: Option<&str>, blocking: &mut Vec<String>) {
    let Some(comment) = comment else { return };
    if TODO_MARKER_RE.is_match(comment) {
        blocking.push(format!(
            "{}:{}: leftover TODO/FIXME/XXX/HACK marker: {stripped:?}",
            line.file, line.lineno
        ));
    }
}

/// Block comment text shaped like commented-out Rust code, excluding doc
/// comments.
fn check_commented_code(line: &AddedLine, stripped: &str, comment: Option<&str>, blocking: &mut Vec<String>) {
    let Some(comment) = comment else { return };
    if is_doc_comment(comment) {
        return;
    }
    if COMMENTED_CODE_RE.is_match(comment) {
        blocking.push(format!(
            "{}:{}: looks like commented-out code: {stripped:?}",
            line.file, line.lineno
        ));
    }
}

/// Warn on comments narrating "what" the next line does rather than "why".
fn check_narrating_comment(line: &AddedLine, stripped: &str, comment: Option<&str>, warnings: &mut Vec<String>) {
    let Some(comment) = comment else { return };
    if !comment.starts_with("//") || is_doc_comment(comment) {
        return;
    }
    let body = comment.trim_start_matches('/').trim().to_lowercase();
    if NARRATING_OPENERS.iter().any(|opener| body.starts_with(opener)) {
        warnings.push(format!(
            "{}:{}: narrating 'what' comment, prefer self-explanatory code or a doc comment on why: {stripped:?}",
            line.file, line.lineno
        ));
    }
}

/// Warn on a new `let`/`fn` binding introducing a weak, generic name.
fn check_weak_name(line: &AddedLine, stripped: &str, warnings: &mut Vec<String>) {
    let Some(caps) = WEAK_NAME_RE.captures(stripped) else {
        return;
    };
    let Some(name) = caps.get(2) else { return };
    warnings.push(format!(
        "{}:{}: weak/generic identifier name {:?}: {stripped:?}",
        line.file,
        line.lineno,
        name.as_str()
    ));
}

/// Warn on a newly added clippy lint suppression.
fn check_suppression(line: &AddedLine, stripped: &str, warnings: &mut Vec<String>) {
    if SUPPRESSION_RE.is_match(stripped) {
        warnings.push(format!(
            "{}:{}: new clippy suppression added, double-check the reason: {stripped:?}",
            line.file, line.lineno
        ));
    }
}

/// Record a line's literals for later repetition analysis, and record any
/// literal declared as a named constant on this line.
fn record_literal_occurrences(
    line: &AddedLine,
    stripped: &str,
    literal_sites: &mut LiteralSites,
    const_declared: &mut ConstDeclared,
) {
    if CONST_LINE_RE.is_match(stripped) {
        for literal in literals_in(stripped) {
            const_declared.entry(line.file.clone()).or_default().insert(literal);
        }
    }

    let before_tests = line.lineno < test_module_start_line(&line.file);
    if before_tests && !stripped.starts_with("#[") {
        for literal in literals_in(stripped) {
            literal_sites
                .entry((line.file.clone(), literal))
                .or_default()
                .push((stripped.to_owned(), line.lineno));
        }
    }
}

/// Extract every literal captured by [`LIT_RE`] from a line.
fn literals_in(stripped: &str) -> Vec<String> {
    LIT_RE
        .captures_iter(stripped)
        .filter_map(|caps| caps.get(1).map(|m| m.as_str().to_owned()))
        .collect()
}

/// Return the one-based line number where `file`'s test module begins, or
/// `usize::MAX` if the file has no test module or cannot be read.
fn test_module_start_line(file: &str) -> usize {
    let Ok(text) = std::fs::read_to_string(file) else {
        return usize::MAX;
    };
    text.lines()
        .position(|line| TEST_MODULE_RE.is_match(line))
        .map_or(usize::MAX, |index| index + 1)
}

/// Warn about literals repeated at least [`MIN_LITERAL_REPETITIONS`] times
/// in one file's added lines without a matching named constant.
fn repeated_literal_warnings(literal_sites: &LiteralSites, const_declared: &ConstDeclared) -> Vec<String> {
    let mut warnings = Vec::new();
    for ((file, literal), sites) in literal_sites {
        let declared = const_declared
            .get(file)
            .is_some_and(|literals| literals.contains(literal));
        if sites.len() < MIN_LITERAL_REPETITIONS || declared {
            continue;
        }
        warnings.push(repeated_literal_warning(file, literal, sites));
    }
    warnings
}

/// Format one repeated-literal warning message.
fn repeated_literal_warning(file: &str, literal: &str, sites: &[(String, usize)]) -> String {
    let lines: Vec<String> = sites.iter().map(|(_, lineno)| lineno.to_string()).collect();
    format!(
        "{file}: literal {literal} repeated {count}x at lines {joined} without a named constant -- consider hoisting it",
        count = sites.len(),
        joined = lines.join(", "),
    )
}

// -----------------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------------

#[cfg(test)]
#[expect(clippy::allow_attributes, reason = "blanket test suppressions")]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing, reason = "tests")]
mod tests {
    use super::*;

    #[test]
    fn detects_todo_marker() {
        assert!(TODO_MARKER_RE.is_match("// TODO: fix this later"));
        assert!(!TODO_MARKER_RE.is_match("// this is fine"));
    }

    #[test]
    fn detects_commented_out_code_but_not_doc_comments() {
        assert!(COMMENTED_CODE_RE.is_match("// let x = compute();"));
        assert!(!COMMENTED_CODE_RE.is_match("/// Returns the computed value."));
    }

    #[test]
    fn detects_weak_names() {
        let caps = WEAK_NAME_RE.captures("let temp = 5;").expect("should match");
        assert_eq!(caps.get(2).map(|m| m.as_str()), Some("temp"));
        assert!(WEAK_NAME_RE.captures("let value = 5;").is_none());
    }

    #[test]
    fn detects_narrating_comment_openers() {
        assert!(
            NARRATING_OPENERS
                .iter()
                .any(|opener| "increment the counter by one".starts_with(opener))
        );
        assert!(
            !NARRATING_OPENERS
                .iter()
                .any(|opener| "guards against a torn write".starts_with(opener))
        );
    }

    #[test]
    fn parses_added_lines_from_unified_diff() {
        let diff = concat!(
            "diff --git a/src/lib.rs b/src/lib.rs\n",
            "--- a/src/lib.rs\n",
            "+++ b/src/lib.rs\n",
            "@@ -1,2 +1,3 @@\n",
            " unchanged\n",
            "-removed\n",
            "+added one\n",
            "+added two\n",
        );
        let added = parse_unified_diff(diff);
        assert_eq!(added.len(), 2);
        assert_eq!(added[0].file, "src/lib.rs");
        assert_eq!(added[0].lineno, 2);
        assert_eq!(added[0].content, "added one");
        assert_eq!(added[1].lineno, 3);
    }

    #[test]
    fn flags_blocking_todo_via_evaluate() {
        let added = vec![AddedLine {
            file: "src/lib.rs".to_owned(),
            lineno: 10,
            content: "    // TODO: fix this".to_owned(),
        }];
        let report = evaluate(&added);
        assert_eq!(report.blocking.len(), 1);
        assert!(report.blocking[0].contains("TODO"));
    }

    #[test]
    fn warns_on_repeated_literal_without_constant() {
        let added = vec![
            AddedLine {
                file: "src/lib.rs".to_owned(),
                lineno: 1,
                content: "let a = 4242;".to_owned(),
            },
            AddedLine {
                file: "src/lib.rs".to_owned(),
                lineno: 2,
                content: "let b = 4242;".to_owned(),
            },
            AddedLine {
                file: "src/lib.rs".to_owned(),
                lineno: 3,
                content: "let c = 4242;".to_owned(),
            },
        ];
        let report = evaluate(&added);
        assert!(report.warnings.iter().any(|w| w.contains("4242")));
    }

    #[test]
    fn does_not_warn_when_literal_declared_as_constant() {
        let added = vec![
            AddedLine {
                file: "src/lib.rs".to_owned(),
                lineno: 1,
                content: "const LIMIT: u32 = 4242;".to_owned(),
            },
            AddedLine {
                file: "src/lib.rs".to_owned(),
                lineno: 2,
                content: "let a = 4242;".to_owned(),
            },
            AddedLine {
                file: "src/lib.rs".to_owned(),
                lineno: 3,
                content: "let b = 4242;".to_owned(),
            },
            AddedLine {
                file: "src/lib.rs".to_owned(),
                lineno: 4,
                content: "let c = 4242;".to_owned(),
            },
        ];
        let report = evaluate(&added);
        assert!(!report.warnings.iter().any(|w| w.contains("4242")));
    }
}
