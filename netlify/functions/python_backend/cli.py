#!/usr/bin/env python3
"""Command-line interface to expose the legacy Python attendance logic."""
from __future__ import annotations

import argparse
import base64
import json
import sys
import traceback
from pathlib import Path
from typing import Any, Dict

CURRENT_DIR = Path(__file__).resolve().parent
if str(CURRENT_DIR) not in sys.path:
    sys.path.insert(0, str(CURRENT_DIR))

import logic  # type: ignore  # noqa: E402


def _read_bytes(path: str | None) -> bytes | None:
    if not path:
        return None
    with open(path, "rb") as f:
        return f.read()


def handle_process(args: argparse.Namespace) -> Dict[str, Any]:
    zoom_bytes = _read_bytes(args.zoom)
    if zoom_bytes is None:
        raise ValueError("Zoom CSV path is required")
    roster_bytes = _read_bytes(args.roster)
    try:
        params = json.loads(args.params_json or "{}")
    except Exception:
        params = {}
    try:
        exemptions = json.loads(args.exemptions_json or "{}")
    except Exception:
        exemptions = {}
    data, meta = logic.process_request(zoom_bytes, roster_bytes, params, exemptions)
    payload = {
        "ok": True,
        "meta": meta,
        "data": base64.b64encode(data).decode("ascii"),
    }
    return payload


def handle_keys(args: argparse.Namespace) -> Dict[str, Any]:
    zoom_bytes = _read_bytes(args.zoom)
    if zoom_bytes is None:
        raise ValueError("Zoom CSV path is required")
    items = logic.extract_keys_from_bytes(zoom_bytes)
    return {"ok": True, "items": items}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Zoom attendance CLI adapter")
    subparsers = parser.add_subparsers(dest="command", required=True)

    p_process = subparsers.add_parser("process", help="Process Zoom attendance CSV")
    p_process.add_argument("--zoom", required=True, help="Path to Zoom CSV file")
    p_process.add_argument("--roster", help="Path to roster file (optional)")
    p_process.add_argument("--params-json", default="{}", help="JSON blob of parameters")
    p_process.add_argument(
        "--exemptions-json", default="{}", help="JSON blob of exemption flags"
    )

    p_keys = subparsers.add_parser("keys", help="Extract exemption keys from Zoom CSV")
    p_keys.add_argument("--zoom", required=True, help="Path to Zoom CSV file")

    args = parser.parse_args(argv)

    try:
        if args.command == "process":
            payload = handle_process(args)
        elif args.command == "keys":
            payload = handle_keys(args)
        else:
            raise ValueError(f"Unsupported command: {args.command}")
        print(json.dumps(payload))
        return 0
    except Exception as exc:  # pragma: no cover - best effort error reporting
        err_payload = {
            "ok": False,
            "error": str(exc),
            "traceback": traceback.format_exc(),
        }
        print(json.dumps(err_payload))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
