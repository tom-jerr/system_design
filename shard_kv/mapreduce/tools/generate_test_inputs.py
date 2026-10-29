#!/usr/bin/env python3
"""Generate deterministic multi-file MapReduce input for manual load checks."""

import argparse
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--outdir", type=Path, required=True)
    parser.add_argument("--files", type=int, default=4)
    parser.add_argument("--lines", type=int, default=20_000, help="lines per file")
    args = parser.parse_args()
    if args.files < 1 or args.lines < 1:
        parser.error("--files and --lines must be positive")

    args.outdir.mkdir(parents=True, exist_ok=True)
    for file_id in range(args.files):
        path = args.outdir / f"bulk-{file_id:02}.txt"
        with path.open("w", encoding="utf-8", newline="\n") as output:
            for line_id in range(args.lines):
                output.write(
                    f"rust common common shard{file_id} term{line_id % 257} payload\n"
                )
        print(f"{path} ({args.lines} lines)")


if __name__ == "__main__":
    main()
