from __future__ import annotations

import argparse
import json
from pathlib import Path

from ldaca_wordflow.main import app

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUTPUT = REPO_ROOT / "frontend" / "openapi" / "ldaca-wordflow.openapi.json"


def export_openapi(output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(app.openapi(), indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export the LDaCA Wordflow FastAPI OpenAPI schema."
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help=f"Path to write the OpenAPI schema. Defaults to {DEFAULT_OUTPUT}.",
    )
    args = parser.parse_args()
    export_openapi(args.output)


if __name__ == "__main__":
    main()
