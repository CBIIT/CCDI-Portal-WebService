#!/usr/bin/env python3
"""
Emit OpenSearch 2.x create-index JSON bodies from CCDI Inventory Dataloader
es_indices_*.yml (mappings only; no settings, no Neo4j cypher).

Typical regeneration (from repo root, with dataloader checked out as a sibling):

  python3 scripts/generate-integration-opensearch-mappings.py \\
    --yaml ../CCDI-Inventory-Dataloader/config/es_indices_ccdi_model.yml \\
    --out src/test/resources/opensearch/mappings

Requires: pip install pyyaml
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--yaml",
        type=Path,
        required=True,
        help="Path to es_indices_ccdi_model.yml (or compatible Indices: schema).",
    )
    parser.add_argument(
        "--out",
        type=Path,
        required=True,
        help="Directory to write <index_name>.json create-index bodies.",
    )
    args = parser.parse_args()

    try:
        import yaml  # type: ignore
    except ImportError:
        print("Missing dependency: pip install pyyaml", file=sys.stderr)
        sys.exit(1)

    with args.yaml.open(encoding="utf-8") as handle:
        data = yaml.safe_load(handle)

    indices = data.get("Indices") or []
    args.out.mkdir(parents=True, exist_ok=True)

    written: list[str] = []
    for item in indices:
        name = item.get("index_name")
        mapping = item.get("mapping")
        if not name or not mapping:
            continue
        body = {"mappings": {"properties": mapping}}
        out_file = args.out / f"{name}.json"
        out_file.write_text(json.dumps(body, indent=2) + "\n", encoding="utf-8")
        written.append(name)

    print(f"Wrote {len(written)} mapping files to {args.out}")
    for n in written:
        print(f"  - {n}.json")


if __name__ == "__main__":
    main()
