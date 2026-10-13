#!/usr/bin/env python3
"""Alternate two equally configured benchmark builds and report medians."""

import argparse
import json
import re
import statistics
import subprocess
import tempfile
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("baseline", type=Path)
    parser.add_argument("candidate", type=Path)
    parser.add_argument("--rounds", type=int, default=5)
    parser.add_argument("--scale", type=int, default=2)
    parser.add_argument("--cases", nargs="+", default=[
        "parse", "chunked", "encoding", "sse", "websocket", "streaming",
        "keep-alive", "8-conn", "64-conn", "static", "echo", "routing"])
    parser.add_argument("--output", type=Path, help="Save all measurements and raw output as JSON")
    args = parser.parse_args()
    if args.rounds < 1 or args.scale < 1:
        parser.error("rounds and scale must be positive")
    pattern = re.compile(
        r"^(.*?)\s+(\d+) ops/s\s+wall\s+([\d.]+) us\s+"
        r"user\s+([\d.]+) us\s+sys\s+([\d.]+) us$", re.MULTILINE)
    records = []
    for case in args.cases:
        # Warm both executables before collecting results for this workload.
        for executable in (args.baseline, args.candidate):
            subprocess.run([str(executable.resolve()), "1", case],
                           check=True, capture_output=True, text=True, timeout=300)
        for index in range(args.rounds):
            order = [("baseline", args.baseline), ("candidate", args.candidate)]
            if index % 2:
                order.reverse()
            for label, executable in order:
                result = subprocess.run(
                    [str(executable.resolve()), str(args.scale), case],
                    check=True, capture_output=True, text=True, timeout=300)
                matches = pattern.findall(result.stdout)
                if not matches:
                    raise RuntimeError(f"No measurements for {case}: {result.stdout}")
                for name, ops, wall, user, system in matches:
                    records.append(dict(case=case, round=index, build=label, name=name.strip(),
                                        ops=float(ops), wall=float(wall), user=float(user),
                                        system=float(system), stdout=result.stdout))
            print(f"{case}: {index + 1}/{args.rounds}", flush=True)
        if args.output:
            # Keep earlier completed cases intact even if a later save fails
            # (for example, because the filesystem runs out of space).
            temporary = None
            try:
                with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", delete=False,
                                                 dir=args.output.resolve().parent) as output:
                    temporary = Path(output.name)
                    json.dump(records, output, indent=2)
                    output.write("\n")
                temporary.replace(args.output)
            finally:
                if temporary is not None:
                    temporary.unlink(missing_ok=True)
    print("\n| Case | Baseline ops/s | Candidate ops/s | Throughput | User CPU |")
    print("|---|---:|---:|---:|---:|")
    for name in dict.fromkeys(record["name"] for record in records):
        medians = {}
        for build in ("baseline", "candidate"):
            matching = [record for record in records if record["name"] == name and record["build"] == build]
            medians[build] = {metric: statistics.median(record[metric] for record in matching)
                              for metric in ("ops", "user")}
        old, new = medians["baseline"], medians["candidate"]
        cpu = f"{(new['user'] / old['user'] - 1) * 100:+.1f}%" if old["user"] else "n/a"
        print(f"| {name} | {old['ops']:.0f} | {new['ops']:.0f} | "
              f"{(new['ops'] / old['ops'] - 1) * 100:+.1f}% | {cpu} |")


if __name__ == "__main__":
    main()
