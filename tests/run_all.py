#!/usr/bin/env -S python3 -I
"""Run every regression suite in sequence; exit non-zero if any fails.

Suites:
  * build_fixture / verify_independent / run_reports - original diagnostics;
  * build_repair_fixture + test_repair - local repair core invariants;
  * test_repair_shapes - integer keys, multi-column groups, chained aggs.
"""

import os
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

STEPS = [
    ["tests/build_fixture.py"],
    ["tests/verify_independent.py"],
    ["tests/run_reports.py"],
    ["tests/build_repair_fixture.py"],
    ["tests/test_repair.py"],
    ["tests/test_repair_shapes.py"],
    ["tests/test_repair_subscription.py"],
]


def main() -> int:
    for step in STEPS:
        print(f"\n=== {' '.join(step)} ===", flush=True)
        result = subprocess.run(
            [sys.executable, "-I", *step], cwd=ROOT)
        if result.returncode != 0:
            print(f"FAILED: {' '.join(step)}", file=sys.stderr)
            return result.returncode
    print("\nALL SUITES PASSED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
