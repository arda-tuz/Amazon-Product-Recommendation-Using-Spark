#!/usr/bin/env python3
"""Merge pytest JUnit fragments without discarding testcase evidence."""

from __future__ import annotations

import argparse
import copy
import xml.etree.ElementTree as ET
from pathlib import Path


COUNTERS = ("tests", "failures", "errors", "skipped")


def merge(
    output: Path, inputs: list[Path], *, allow_failures: bool = False
) -> None:
    root = ET.Element("testsuites")
    totals = {name: 0 for name in COUNTERS}
    for path in inputs:
        parsed = ET.parse(path).getroot()
        suites = [parsed] if parsed.tag == "testsuite" else list(parsed.findall("testsuite"))
        if not suites:
            raise ValueError(f"no testsuite found in {path}")
        for suite in suites:
            for name in COUNTERS:
                totals[name] += int(suite.attrib.get(name, "0"))
            root.append(copy.deepcopy(suite))
    root.attrib.update({name: str(value) for name, value in totals.items()})
    if totals["tests"] <= 0:
        raise RuntimeError(f"refusing to merge empty JUnit evidence: {totals}")
    if not allow_failures and (totals["failures"] or totals["errors"]):
        raise RuntimeError(f"refusing to merge non-passing JUnit evidence: {totals}")
    output.parent.mkdir(parents=True, exist_ok=True)
    ET.ElementTree(root).write(output, encoding="utf-8", xml_declaration=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--allow-failures",
        action="store_true",
        help="preserve failing/error suites in the merged output",
    )
    parser.add_argument("output", type=Path)
    parser.add_argument("inputs", nargs="+", type=Path)
    args = parser.parse_args()
    merge(args.output, args.inputs, allow_failures=args.allow_failures)


if __name__ == "__main__":
    main()
