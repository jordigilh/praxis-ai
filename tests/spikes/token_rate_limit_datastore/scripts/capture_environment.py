#!/usr/bin/env python3
"""Capture a reproducible, hostname-free manifest for local functional runs."""

from __future__ import annotations

import hashlib
import json
import os
import platform
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "evidence" / "environment.json"
REDIS_IMAGE = (
    "docker.io/library/redis:8.10.2-alpine@"
    "sha256:3811787313eba226a2ef38658c6ccb91cd5e110edc89c37767de373120a0e5a0"
)
VALKEY_DECLARED_IMAGE = (
    "registry.access.redhat.com/rhel10/valkey-8@"
    "sha256:5929be16ac020c4851d8dc1f5ead6d48c6da066a9a74349711186925ebd553b9"
)
VALKEY_EXECUTED_IMAGE = (
    "registry.redhat.io/rhel10/valkey-8@"
    "sha256:5929be16ac020c4851d8dc1f5ead6d48c6da066a9a74349711186925ebd553b9"
)


def command(*args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        args,
        check=check,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=30,
    )


def version(*args: str) -> str:
    return command(*args).stdout.strip()


def selected_podman_info() -> dict[str, Any]:
    payload = json.loads(command("podman", "info", "--format", "json").stdout)
    host = payload["host"]
    version_info = payload.get("version", {})
    return {
        "architecture": host.get("arch"),
        "kernel": host.get("kernel"),
        "cpus": host.get("cpus"),
        "memory_bytes": host.get("memTotal"),
        "distribution": host.get("distribution"),
        "cgroup_version": host.get("cgroupVersion"),
        "network_backend": host.get("networkBackend"),
        "oci_runtime": (host.get("ociRuntime") or {}).get("name"),
        "podman_version": version_info.get("Version") or version_info.get("version"),
    }


def selected_image_inspect(reference: str) -> dict[str, Any]:
    result = command("podman", "image", "inspect", reference, check=False)
    if result.returncode != 0:
        return {
            "requested_reference": reference,
            "available": False,
            "error": result.stderr.strip(),
        }
    image = json.loads(result.stdout)[0]
    return {
        "requested_reference": reference,
        "available": True,
        "image_id": image.get("Id"),
        "requested_manifest_digest": image.get("Digest"),
        "repo_digests": image.get("RepoDigests", []),
        "architecture": image.get("Architecture"),
        "os": image.get("Os"),
        "created": image.get("Created"),
        "manifest_type": image.get("ManifestType"),
        "version_label": (image.get("Annotations") or {}).get(
            "org.opencontainers.image.version"
        ),
    }


def harness_files() -> list[Path]:
    files = [
        ROOT / "Cargo.toml",
        ROOT / "Cargo.lock",
        ROOT / "README.md",
    ]
    for directory in [ROOT / "src", ROOT / "scripts", ROOT / "fixtures" / "source"]:
        files.extend(path for path in directory.rglob("*") if path.is_file())
    return sorted(set(files))


def harness_digest() -> tuple[str, list[dict[str, Any]]]:
    tree = hashlib.sha256()
    records = []
    for path in harness_files():
        relative = path.relative_to(ROOT).as_posix()
        payload = path.read_bytes()
        digest = hashlib.sha256(payload).hexdigest()
        tree.update(relative.encode())
        tree.update(b"\0")
        tree.update(payload)
        tree.update(b"\0")
        records.append({"path": relative, "bytes": len(payload), "sha256": digest})
    return tree.hexdigest(), records


def main() -> None:
    digest, files = harness_digest()
    manifest = {
        "schema_version": 1,
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "purpose": "local functional evidence only; no dedicated-hardware performance claim",
        "host": {
            "system": platform.system(),
            "release": platform.release(),
            "architecture": platform.machine(),
            "logical_cpus": os.cpu_count(),
            "hostname_retained": False,
        },
        "container_vm": selected_podman_info(),
        "toolchain": {
            "rustc": version("rustc", "--version"),
            "cargo": version("cargo", "--version"),
            "podman": version("podman", "--version"),
            "python": platform.python_version(),
        },
        "harness": {
            "version_control": "local_unpublished_directory",
            "tree_sha256": digest,
            "files": files,
        },
        "resolved_dependency": {
            "crate": "redis",
            "version": "1.7.0",
            "checksum": "2acbc41a996f7652b2ddd9dfd98cc4ff602cfd742ae35382f07f608405ab50ed",
            "features": ["tokio-comp", "sentinel", "cluster-async", "script"],
        },
        "images": {
            "redis": selected_image_inspect(REDIS_IMAGE),
            "red_hat_valkey": {
                **selected_image_inspect(VALKEY_EXECUTED_IMAGE),
                "original_declared_reference": VALKEY_DECLARED_IMAGE,
                "registry_correction": "terms-gated rhel10 repository is served from authenticated registry.redhat.io; manifest digest unchanged",
            },
        },
        "preflight_registry_diagnostics": {
            "declared_registry": "runtime/valkey-preflight-declared-registry.txt",
            "stale_default_auth": "runtime/valkey-preflight-default-auth.txt",
        },
    }
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    print(OUTPUT)


if __name__ == "__main__":
    main()
