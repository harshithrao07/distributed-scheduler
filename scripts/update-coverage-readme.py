#!/usr/bin/env python3
"""Regenerate the Coverage subsection of README.md from target/site/jacoco/jacoco.csv.

Reads the JaCoCo CSV, computes total + per-package instruction coverage, and
rewrites the block between the COVERAGE-START and COVERAGE-END markers in
README.md. Exits non-zero (without modifying the README) if the markers are
missing or the CSV is unavailable.
"""

from __future__ import annotations

import csv
import sys
from collections import defaultdict
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
CSV_PATH = REPO_ROOT / "target" / "site" / "jacoco" / "jacoco.csv"


def find_readme() -> Path | None:
    for name in ("Readme.md", "README.md", "readme.md"):
        candidate = REPO_ROOT / name
        if candidate.exists():
            return candidate
    return None


README_PATH = find_readme()
START_MARKER = "<!-- COVERAGE-START -->"
END_MARKER = "<!-- COVERAGE-END -->"

PACKAGE_LABELS = {
    "com.job.scheduler.scheduler": "`scheduler` (due-job, watchdogs, DLQ)",
    "com.job.scheduler.producers": "`producers`",
    "com.job.scheduler.enums": "`enums`",
    "com.job.scheduler.monitoring.events": "`monitoring.events`",
    "com.job.scheduler.dto.payload": "`dto.payload`",
    "com.job.scheduler.utility": "`utility`",
    "com.job.scheduler.handlers": "`handlers`",
    "com.job.scheduler.dto": "`dto`",
    "com.job.scheduler.controller": "`controller`",
    "com.job.scheduler.monitoring": "`monitoring`",
    "com.job.scheduler.exception": "`exception`",
    "com.job.scheduler.service": "`service` (job lifecycle, worker, locks)",
    "com.job.scheduler.config": "`config`",
    "com.job.scheduler.consumers": "`consumers`",
    "com.job.scheduler": "`com.job.scheduler` (root)",
}


def pct(covered: int, total: int) -> str:
    if total == 0:
        return "n/a"
    return f"{round(covered * 100 / total)}%"


def main() -> int:
    if not CSV_PATH.exists():
        print(f"error: jacoco csv not found at {CSV_PATH}", file=sys.stderr)
        return 1
    if README_PATH is None:
        print(f"error: README not found under {REPO_ROOT}", file=sys.stderr)
        return 1

    totals = defaultdict(int)
    packages: dict[str, dict[str, int]] = defaultdict(
        lambda: {"instr_missed": 0, "instr_covered": 0}
    )

    with CSV_PATH.open(newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            instr_missed = int(row["INSTRUCTION_MISSED"])
            instr_covered = int(row["INSTRUCTION_COVERED"])
            branch_missed = int(row["BRANCH_MISSED"])
            branch_covered = int(row["BRANCH_COVERED"])
            line_missed = int(row["LINE_MISSED"])
            line_covered = int(row["LINE_COVERED"])
            method_missed = int(row["METHOD_MISSED"])
            method_covered = int(row["METHOD_COVERED"])

            totals["instr_missed"] += instr_missed
            totals["instr_covered"] += instr_covered
            totals["branch_missed"] += branch_missed
            totals["branch_covered"] += branch_covered
            totals["line_missed"] += line_missed
            totals["line_covered"] += line_covered
            totals["method_missed"] += method_missed
            totals["method_covered"] += method_covered
            totals["class_total"] += 1
            if instr_covered > 0:
                totals["class_covered"] += 1

            pkg = row["PACKAGE"]
            packages[pkg]["instr_missed"] += instr_missed
            packages[pkg]["instr_covered"] += instr_covered

    instr_total = totals["instr_missed"] + totals["instr_covered"]
    branch_total = totals["branch_missed"] + totals["branch_covered"]
    line_total = totals["line_missed"] + totals["line_covered"]
    method_total = totals["method_missed"] + totals["method_covered"]

    summary_rows = [
        ("Instructions", totals["instr_covered"], instr_total),
        ("Branches", totals["branch_covered"], branch_total),
        ("Lines", totals["line_covered"], line_total),
        ("Methods", totals["method_covered"], method_total),
        ("Classes", totals["class_covered"], totals["class_total"]),
    ]

    sorted_packages = sorted(
        packages.items(),
        key=lambda kv: (
            -((kv[1]["instr_covered"]) / max(1, kv[1]["instr_covered"] + kv[1]["instr_missed"])),
            kv[0],
        ),
    )

    lines: list[str] = []
    lines.append(START_MARKER)
    lines.append("")
    lines.append("Latest JaCoCo run across the full Testcontainers-backed suite:")
    lines.append("")
    lines.append("| Metric | Covered | Total | Coverage |")
    lines.append("|---|---:|---:|---:|")
    for name, covered, total in summary_rows:
        lines.append(f"| {name} | {covered:,} | {total:,} | {pct(covered, total)} |")
    lines.append("")
    lines.append("Per-package instruction coverage:")
    lines.append("")
    lines.append("| Package | Coverage |")
    lines.append("|---|---:|")
    for pkg, vals in sorted_packages:
        label = PACKAGE_LABELS.get(pkg, f"`{pkg}`")
        covered = vals["instr_covered"]
        total = covered + vals["instr_missed"]
        lines.append(f"| {label} | {pct(covered, total)} |")
    lines.append("")
    lines.append("Open `target/site/jacoco/index.html` after `mvn test` for the drill-down view.")
    lines.append("")
    lines.append(END_MARKER)
    new_block = "\n".join(lines)

    readme = README_PATH.read_text(encoding="utf-8")
    start = readme.find(START_MARKER)
    end = readme.find(END_MARKER)
    if start == -1 or end == -1 or end < start:
        print(
            f"error: could not find {START_MARKER} / {END_MARKER} markers in README",
            file=sys.stderr,
        )
        return 1

    end += len(END_MARKER)
    updated = readme[:start] + new_block + readme[end:]
    if updated != readme:
        README_PATH.write_text(updated, encoding="utf-8")
        print("README coverage section updated.")
    else:
        print("README coverage section already up to date.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
