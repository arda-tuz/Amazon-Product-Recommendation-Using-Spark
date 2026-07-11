"""Single command interface for the strict G0-G12 pipeline."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
import traceback
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any, Iterable

if __package__ in (None, ""):  # support direct spark-submit execution
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from amazon_recommender.core.config import ProjectConfig, load_config
from amazon_recommender.core.gates import GATES, GateBlocked, GateStore
from amazon_recommender.core.logging import configure_logging
from amazon_recommender.core.manifest import atomic_write_json, build_manifest, utc_now
from amazon_recommender.core.paths import RunPaths
from amazon_recommender.gate_handlers import HANDLERS


DEFAULT_RUN_ID = "run-20260711T030500Z-60013511"
ALIASES: dict[str, tuple[str, ...]] = {
    "smoke": ("G2", "G3"),
    "etl": ("G4", "G5"),
    "train": ("G6", "G7", "G8"),
    "evaluate": ("G9",),
    "performance": ("G11",),
    "all": GATES,
}


def _junit_summary(path: Path) -> dict[str, Any]:
    root = ET.parse(path).getroot()
    suites = [root] if root.tag == "testsuite" else list(root.findall("testsuite"))
    summary = {
        field: sum(int(suite.attrib.get(field, "0")) for suite in suites)
        for field in ("tests", "failures", "errors", "skipped")
    }
    summary["path"] = str(path.resolve())
    if summary["tests"] <= 0 or summary["failures"] or summary["errors"]:
        raise RuntimeError(f"JUnit evidence is not passing: {summary}")
    return summary


def _context(args: argparse.Namespace) -> tuple[ProjectConfig, RunPaths, GateStore]:
    root = Path(args.project_root).resolve()
    config_path = Path(args.config)
    if not config_path.is_absolute():
        config_path = root / config_path
    config = load_config(config_path, project_root=root)
    paths = RunPaths.create(root, config.resolve("paths", "artifacts"), args.run_id)
    paths.ensure_control_dirs()
    store = GateStore(
        paths=paths,
        config_sha256=config.sha256,
        source_sha256=config.get("source", "sha256"),
    )
    return config, paths, store


def _record_attempt(paths: RunPaths, gate: str, payload: dict[str, Any]) -> Path:
    path = paths.manifests / "attempts" / f"{gate}-{utc_now().replace(':', '')}.json"
    atomic_write_json(path, payload)
    return path


def run_gate(args: argparse.Namespace, gate: str) -> int:
    started_at = utc_now()
    started_clock = time.perf_counter()
    config, paths, store = _context(args)
    logger = configure_logging(paths.logs / "pipeline.jsonl", verbose=args.verbose)
    if store.passed(gate) and not args.force:
        if gate == "G12":
            from amazon_recommender.phases.g12 import finalize_g12_delivery

            try:
                finalize_g12_delivery(paths, store.path(gate))
            except Exception as error:
                logger.exception(
                    "G12 delivery finalization failed",
                    extra={"run_id": paths.run_id, "gate": gate},
                )
                print(
                    json.dumps(
                        {
                            "gate": gate,
                            "status": "failed",
                            "reason": str(error),
                            "canonical_manifest_preserved": True,
                        }
                    ),
                    file=sys.stderr,
                )
                return 1
        print(json.dumps({"gate": gate, "status": "passed", "reused": True}))
        return 0
    try:
        previous = store.require_prerequisites(gate)
        if gate == "G0":
            raise GateBlocked(
                "G0 must be run with scripts/g0_smoke.py; no passed evidence exists"
            )
        if gate == "G1":
            if args.evidence_file is None:
                raise GateBlocked("G1 requires --evidence-file with passing JUnit XML")
            evidence = {
                "config": {
                    "path": str(config.path),
                    "sha256": config.sha256,
                    "schema_version": config.get("schema_version"),
                },
                "pytest": _junit_summary(Path(args.evidence_file)),
                "commands": sorted((*ALIASES, "gate", "dashboard", "status")),
                "manifest_chain": "strict",
            }
        else:
            handler = HANDLERS.get(gate)
            if handler is None:
                raise GateBlocked(f"{gate} handler is not implemented yet")
            evidence = handler(config, paths, args.evidence_file)
        finished_at = utc_now()
        manifest = build_manifest(
            gate=gate,
            run_id=paths.run_id,
            status="passed",
            config_sha256=config.sha256,
            source_sha256=config.get("source", "sha256"),
            previous_evidence=previous,
            evidence=evidence,
            started_at=started_at,
            finished_at=finished_at,
            duration_seconds=time.perf_counter() - started_clock,
        )
        atomic_write_json(store.path(gate), manifest)
        if gate == "G12":
            from amazon_recommender.phases.g12 import finalize_g12_delivery

            try:
                finalize_g12_delivery(paths, store.path(gate))
            except Exception:
                # A handled finalization failure must not leave a canonical
                # passed manifest unless success was already committed. A
                # process crash is recovered by the reuse branch above.
                if not (paths.run / "delivery" / "_SUCCESS.json").is_file():
                    store.path(gate).unlink(missing_ok=True)
                raise
        logger.info("gate passed", extra={"run_id": paths.run_id, "gate": gate})
        print(json.dumps(manifest, indent=2, ensure_ascii=False))
        return 0
    except GateBlocked as error:
        finished_at = utc_now()
        payload = build_manifest(
            gate=gate,
            run_id=paths.run_id,
            status="blocked",
            config_sha256=config.sha256,
            source_sha256=config.get("source", "sha256"),
            previous_evidence={},
            evidence={},
            error={"type": type(error).__name__, "message": str(error)},
            started_at=started_at,
            finished_at=finished_at,
            duration_seconds=time.perf_counter() - started_clock,
        )
        attempt = _record_attempt(paths, gate, payload)
        logger.warning(str(error), extra={"run_id": paths.run_id, "gate": gate})
        print(
            json.dumps(
                {
                    "status": "blocked",
                    "gate": gate,
                    "reason": str(error),
                    "evidence": str(attempt),
                }
            ),
            file=sys.stderr,
        )
        return 3
    except Exception as error:
        finished_at = utc_now()
        payload = build_manifest(
            gate=gate,
            run_id=paths.run_id,
            status="failed",
            config_sha256=config.sha256,
            source_sha256=config.get("source", "sha256"),
            previous_evidence={},
            evidence={},
            error={
                "type": type(error).__name__,
                "message": str(error),
                "traceback": traceback.format_exc(),
            },
            started_at=started_at,
            finished_at=finished_at,
            duration_seconds=time.perf_counter() - started_clock,
        )
        attempt = _record_attempt(paths, gate, payload)
        logger.exception("gate failed", extra={"run_id": paths.run_id, "gate": gate})
        print(
            json.dumps(
                {
                    "status": "failed",
                    "gate": gate,
                    "reason": str(error),
                    "evidence": str(attempt),
                }
            ),
            file=sys.stderr,
        )
        return 1


def run_sequence(args: argparse.Namespace, gates: Iterable[str]) -> int:
    for gate in gates:
        result = run_gate(args, gate)
        if result:
            return result
    return 0


def show_status(args: argparse.Namespace) -> int:
    _, _, store = _context(args)
    rows = []
    for gate in GATES:
        path = store.path(gate)
        rows.append(
            {
                "gate": gate,
                "status": store.read(gate)["status"] if path.exists() else "missing",
            }
        )
    print(json.dumps(rows, indent=2))
    return 0


def serve_dashboard(args: argparse.Namespace) -> int:
    _, paths, store = _context(args)
    store.require_prerequisites("G11")
    if not store.passed("G10"):
        raise GateBlocked("dashboard requires G10=passed")
    app = paths.project_root / "app" / "Home.py"
    if not app.exists():
        raise GateBlocked(f"Streamlit application is missing: {app}")
    return subprocess.call(
        [sys.executable, "-m", "streamlit", "run", str(app)], cwd=paths.project_root
    )


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(prog="amazon-rec", description=__doc__)
    result.add_argument(
        "--project-root", default=os.environ.get("AMAZON_REC_ROOT", Path.cwd())
    )
    result.add_argument("--config", default="configs/project.yaml")
    result.add_argument(
        "--run-id", default=os.environ.get("AMAZON_REC_RUN_ID", DEFAULT_RUN_ID)
    )
    result.add_argument("--verbose", action="store_true")
    result.add_argument("--force", action="store_true")
    subcommands = result.add_subparsers(dest="command", required=True)
    gate = subcommands.add_parser("gate", help="run one strict phase gate")
    gate.add_argument("gate", choices=GATES)
    gate.add_argument("--evidence-file", type=Path)
    for command in ALIASES:
        subcommands.add_parser(command)
    subcommands.add_parser("dashboard")
    subcommands.add_parser("status")
    return result


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    if args.command == "gate":
        return run_gate(args, args.gate)
    if args.command in ALIASES:
        return run_sequence(args, ALIASES[args.command])
    if args.command == "status":
        return show_status(args)
    if args.command == "dashboard":
        try:
            return serve_dashboard(args)
        except GateBlocked as error:
            print(
                json.dumps({"status": "blocked", "reason": str(error)}), file=sys.stderr
            )
            return 3
    raise AssertionError(args.command)


if __name__ == "__main__":
    raise SystemExit(main())
