#!/usr/bin/env python3
"""Fetch immutable source fixtures used by the local qualification spike."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "fixtures" / "source"
BASELINE = "56af0e37e4bda2f06c7e33546db66a8d01fce0b7"
FIXED = "b6fe4704af72c05167a280d5070a291997d2085d"
PLAIN = "3ad8d12d6ad7014f7f632ba3468f74f8de13ce77"

SOURCES = {
    "baseline/Cargo.toml": f"https://raw.githubusercontent.com/praxis-proxy/ai/{BASELINE}/Cargo.toml",
    "baseline/Cargo.lock": f"https://raw.githubusercontent.com/praxis-proxy/ai/{BASELINE}/Cargo.lock",
    "baseline/backend.rs": f"https://raw.githubusercontent.com/praxis-proxy/ai/{BASELINE}/filters/src/token_rate_limit/backend.rs",
    "baseline/config.rs": f"https://raw.githubusercontent.com/praxis-proxy/ai/{BASELINE}/filters/src/token_rate_limit/config.rs",
    "baseline/mod.rs": f"https://raw.githubusercontent.com/praxis-proxy/ai/{BASELINE}/filters/src/token_rate_limit/mod.rs",
    "baseline/lua/sliding_window_reserve.lua": f"https://raw.githubusercontent.com/praxis-proxy/ai/{BASELINE}/filters/src/token_rate_limit/lua/sliding_window_reserve.lua",
    "baseline/lua/sliding_window_reconcile.lua": f"https://raw.githubusercontent.com/praxis-proxy/ai/{BASELINE}/filters/src/token_rate_limit/lua/sliding_window_reconcile.lua",
    "baseline/lua/token_bucket_reserve.lua": f"https://raw.githubusercontent.com/praxis-proxy/ai/{BASELINE}/filters/src/token_rate_limit/lua/token_bucket_reserve.lua",
    "baseline/lua/token_bucket_reconcile.lua": f"https://raw.githubusercontent.com/praxis-proxy/ai/{BASELINE}/filters/src/token_rate_limit/lua/token_bucket_reconcile.lua",
    "fixed/Cargo.toml": f"https://raw.githubusercontent.com/jordigilh/praxis-ai/{FIXED}/Cargo.toml",
    "fixed/Cargo.lock": f"https://raw.githubusercontent.com/jordigilh/praxis-ai/{FIXED}/Cargo.lock",
    "fixed/backend.rs": f"https://raw.githubusercontent.com/jordigilh/praxis-ai/{FIXED}/filters/src/token_rate_limit/backend.rs",
    "fixed/config.rs": f"https://raw.githubusercontent.com/jordigilh/praxis-ai/{FIXED}/filters/src/token_rate_limit/config.rs",
    "fixed/mod.rs": f"https://raw.githubusercontent.com/jordigilh/praxis-ai/{FIXED}/filters/src/token_rate_limit/mod.rs",
    "fixed/lua/sliding_window_reserve.lua": f"https://raw.githubusercontent.com/jordigilh/praxis-ai/{FIXED}/filters/src/token_rate_limit/lua/sliding_window_reserve.lua",
    "fixed/lua/sliding_window_reconcile.lua": f"https://raw.githubusercontent.com/jordigilh/praxis-ai/{FIXED}/filters/src/token_rate_limit/lua/sliding_window_reconcile.lua",
    "fixed/lua/token_bucket_reserve.lua": f"https://raw.githubusercontent.com/jordigilh/praxis-ai/{FIXED}/filters/src/token_rate_limit/lua/token_bucket_reserve.lua",
    "fixed/lua/token_bucket_reconcile.lua": f"https://raw.githubusercontent.com/jordigilh/praxis-ai/{FIXED}/filters/src/token_rate_limit/lua/token_bucket_reconcile.lua",
    "plain/connection.rs": f"https://raw.githubusercontent.com/szedan-rh/ai/{PLAIN}/filters/src/token_rate_limit/valkey/connection.rs",
    "plain/mod.rs": f"https://raw.githubusercontent.com/szedan-rh/ai/{PLAIN}/filters/src/token_rate_limit/valkey/mod.rs",
    "plain/sliding_window.rs": f"https://raw.githubusercontent.com/szedan-rh/ai/{PLAIN}/filters/src/token_rate_limit/valkey/sliding_window.rs",
    "plain/token_bucket.rs": f"https://raw.githubusercontent.com/szedan-rh/ai/{PLAIN}/filters/src/token_rate_limit/valkey/token_bucket.rs",
}


def fetch(url: str) -> bytes:
    request = Request(url, headers={"User-Agent": "praxis-local-datastore-spike"})
    with urlopen(request, timeout=30) as response:
        if response.status != 200:
            raise RuntimeError(f"GET {url}: HTTP {response.status}")
        return response.read()


def main() -> None:
    records = []
    for relative, url in sorted(SOURCES.items()):
        payload = fetch(url)
        destination = FIXTURES / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(payload)
        records.append(
            {
                "path": relative,
                "url": url,
                "bytes": len(payload),
                "sha256": hashlib.sha256(payload).hexdigest(),
            }
        )

    manifest = {
        "schema_version": 1,
        "baseline_commit": BASELINE,
        "fixed_candidate_commit": FIXED,
        "plain_candidate_commit": PLAIN,
        "sources": records,
    }
    output = FIXTURES / "manifest.json"
    output.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    print(output)


if __name__ == "__main__":
    main()
