mod cluster_negative;
mod fixed;
mod key_schema;
mod runtime;
mod sentinel;
mod static_audit;

use std::{fs, path::PathBuf};

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use serde::Serialize;

#[derive(Debug, Parser)]
#[command(about = "Local Praxis Redis/Valkey datastore qualification spike")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    StaticAudit {
        #[arg(long)]
        output: PathBuf,
    },
    Standalone {
        #[arg(long)]
        url: String,
        #[arg(long)]
        product: String,
        #[arg(long)]
        expected_version: String,
        #[arg(long)]
        output: PathBuf,
        #[arg(long, default_value_t = false)]
        allow_destructive_isolated: bool,
    },
    FixedStandalone {
        #[arg(long)]
        url: String,
        #[arg(long)]
        product: String,
        #[arg(long)]
        expected_version: String,
        #[arg(long)]
        output: PathBuf,
        #[arg(long, default_value_t = false)]
        allow_destructive_isolated: bool,
    },
    Sentinel {
        #[arg(long = "sentinel", required = true)]
        sentinels: Vec<String>,
        #[arg(long = "node", required = true)]
        nodes: Vec<String>,
        #[arg(long)]
        service_name: String,
        #[arg(long, value_enum)]
        invocation: fixed::Invocation,
        #[arg(long)]
        product: String,
        #[arg(long)]
        expected_version: String,
        #[arg(long)]
        output: PathBuf,
        #[arg(long, default_value_t = false)]
        allow_destructive_isolated: bool,
    },
    ClusterNegative {
        #[arg(long = "seed", required = true)]
        seeds: Vec<String>,
        #[arg(long)]
        product: String,
        #[arg(long)]
        expected_version: String,
        #[arg(long)]
        output: PathBuf,
        #[arg(long, default_value_t = false)]
        allow_destructive_isolated: bool,
    },
}

fn write_json(path: &PathBuf, value: &impl Serialize) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    }
    let json = serde_json::to_vec_pretty(value)?;
    fs::write(path, [json.as_slice(), b"\n"].concat())
        .with_context(|| format!("write {}", path.display()))?;
    println!("{}", path.display());
    Ok(())
}

#[tokio::main]
#[allow(clippy::too_many_lines)]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::StaticAudit { output } => write_json(&output, &static_audit::run()),
        Command::Standalone {
            url,
            product,
            expected_version,
            output,
            allow_destructive_isolated,
        } => {
            if !allow_destructive_isolated {
                bail!(
                    "standalone qualification uses FLUSHDB and SCRIPT FLUSH; pass --allow-destructive-isolated only for an isolated spike container"
                );
            }
            let report = runtime::run_standalone(&url, &product, &expected_version).await?;
            let passed = report.passed();
            write_json(&output, &report)?;
            if !passed {
                bail!(
                    "standalone qualification produced failing assertions; inspect {}",
                    output.display()
                );
            }
            Ok(())
        }
        Command::FixedStandalone {
            url,
            product,
            expected_version,
            output,
            allow_destructive_isolated,
        } => {
            if !allow_destructive_isolated {
                bail!(
                    "fixed standalone qualification uses FLUSHDB and SCRIPT FLUSH; pass --allow-destructive-isolated only for an isolated spike container"
                );
            }
            let report = fixed::run_standalone(&url, &product, &expected_version).await?;
            let passed = report.passed();
            write_json(&output, &report)?;
            if !passed {
                bail!(
                    "fixed standalone qualification produced failing assertions; inspect {}",
                    output.display()
                );
            }
            Ok(())
        }
        Command::Sentinel {
            sentinels,
            nodes,
            service_name,
            invocation,
            product,
            expected_version,
            output,
            allow_destructive_isolated,
        } => {
            if !allow_destructive_isolated {
                bail!(
                    "Sentinel qualification flushes state, changes replication-health settings, stops one Sentinel, and fails over data nodes; pass --allow-destructive-isolated only for an isolated spike topology"
                );
            }
            let report = sentinel::run(
                sentinels,
                nodes,
                &service_name,
                invocation,
                &product,
                &expected_version,
            )
            .await?;
            let passed = report.passed();
            write_json(&output, &report)?;
            if !passed {
                bail!(
                    "Sentinel qualification produced failing assertions; inspect {}",
                    output.display()
                );
            }
            Ok(())
        }
        Command::ClusterNegative {
            seeds,
            product,
            expected_version,
            output,
            allow_destructive_isolated,
        } => {
            if !allow_destructive_isolated {
                bail!(
                    "the Cluster negative control uses FLUSHALL and SCRIPT FLUSH; pass --allow-destructive-isolated only for isolated spike containers"
                );
            }
            let report = cluster_negative::run(seeds, &product, &expected_version).await?;
            let passed = report.passed();
            write_json(&output, &report)?;
            if !passed {
                bail!(
                    "Cluster negative control produced an unexpected result; inspect {}",
                    output.display()
                );
            }
            Ok(())
        }
    }
}
