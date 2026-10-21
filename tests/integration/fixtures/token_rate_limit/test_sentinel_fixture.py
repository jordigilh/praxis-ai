#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Praxis Contributors

"""Controlled lifecycle tests: no containers or registry credentials required."""

import os
from pathlib import Path
import selectors
import signal
import subprocess
import tempfile
import unittest


FIXTURE = Path(__file__).with_name("start-sentinel-topology.sh")
FAKE_ENGINE = """#!/usr/bin/env bash
set -eu
case "$1" in
  rm) printf '%s\\n' "$*" >> "$FIXTURE_ENGINE_LOG" ;;
  exec)
    [[ "$2" != -i ]] || IFS= read -r password
    case "$*" in
      *get-master-addr-by-name*) printf '127.0.0.1\\n26379\\n' ;;
      *'INFO replication'*) printf 'master_repl_offset:42\\r\\nmaster_link_status:up\\r\\n' ;;
      *'INFO server'*) printf 'redis_version:8.10.2\\r\\n' ;;
    esac
    ;;
esac
"""


class SentinelFixtureLifecycle(unittest.TestCase):
    def run_fixture(self, stop_signal=None):
        with tempfile.TemporaryDirectory(prefix="sentinel-lifecycle-") as directory:
            root = Path(directory)
            engine = root / "fake-engine"
            engine.write_text(FAKE_ENGINE)
            engine.chmod(0o700)
            log = root / "engine.log"
            env_file = root / "fixture.env"
            env = os.environ.copy()
            env.update(CONTAINER_ENGINE=str(engine), FIXTURE_ENGINE_LOG=str(log))
            args = [
                "bash", str(FIXTURE), "--product", "redis", "--image",
                "controlled-fixture@sha256:0000", "--env-file", str(env_file),
            ]
            if stop_signal is None:
                args += ["--", "sh", "-c", "exit 7"]
            process = subprocess.Popen(
                args, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, start_new_session=True
            )
            try:
                if stop_signal is not None:
                    with selectors.DefaultSelector() as selector:
                        selector.register(process.stdout, selectors.EVENT_READ)
                        while True:
                            self.assertTrue(selector.select(timeout=10), "fixture must become ready within 10s")
                            line = process.stdout.readline()
                            self.assertTrue(line, "fixture must stay alive until signalled")
                            if "Sentinel topology ready:" in line:
                                break
                    self.assertTrue(env_file.exists(), "ready fixture must retain its environment file")
                    process.send_signal(stop_signal)
                _, stderr = process.communicate(timeout=10)
            finally:
                # Kill only this test's process group if a failing fixture left
                # a child holding its output pipes open.
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                process.communicate()
            expected = 7 if stop_signal is None else 128 + stop_signal
            self.assertEqual(process.returncode, expected, f"fixture exit status must propagate: {stderr}")
            removed = log.read_text().splitlines()
            self.assertEqual(len(removed), 5, "cleanup must remove each container exactly once")
            self.assertEqual(len(set(removed)), 5, "cleanup must not repeat container removal")
            self.assertFalse(env_file.exists(), "cleanup must remove the environment file")

    def test_term_exits_and_cleans_once(self):
        self.run_fixture(signal.SIGTERM)

    def test_int_exits_and_cleans_once(self):
        self.run_fixture(signal.SIGINT)

    def test_child_failure_exits_and_cleans_once(self):
        self.run_fixture()


if __name__ == "__main__":
    unittest.main()
