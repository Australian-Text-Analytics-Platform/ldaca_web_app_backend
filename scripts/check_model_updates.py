"""Optional release-time check for the pinned topic-modelling embedder.

Compares the SHA pinned in `worker_tasks_topic._TOPIC_EMBEDDER_REVISION`
against the current `main` revision on HuggingFace Hub. If a newer revision
is available, prompts the developer whether to update the pin in source.

The default answer is **no**: the script only rewrites source files if you
explicitly type `y`. Intended to be run by hand before tagging a new LDaCA
release, not on every commit.

Usage:
    python backend/scripts/check_model_updates.py
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

REPO_ID = "sentence-transformers/all-MiniLM-L6-v2"
PINNED_FILE = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "ldaca_web_app"
    / "core"
    / "worker_tasks_topic.py"
)
PIN_PATTERN = re.compile(
    r'(_TOPIC_EMBEDDER_REVISION\s*=\s*")([0-9a-f]+)(")'
)


def read_pinned_sha() -> str:
    text = PINNED_FILE.read_text()
    match = PIN_PATTERN.search(text)
    if not match:
        sys.exit(f"Could not find _TOPIC_EMBEDDER_REVISION in {PINNED_FILE}")
    return match.group(2)


def write_pinned_sha(new_sha: str) -> None:
    text = PINNED_FILE.read_text()
    new_text, n = PIN_PATTERN.subn(rf"\g<1>{new_sha}\g<3>", text)
    if n != 1:
        sys.exit(f"Expected exactly one match in {PINNED_FILE}, got {n}")
    PINNED_FILE.write_text(new_text)


def main() -> int:
    try:
        from huggingface_hub import HfApi
    except ImportError:
        sys.exit(
            "huggingface_hub is required: install it in this environment first"
        )

    pinned = read_pinned_sha()
    print(f"Pinned revision:  {pinned}")

    info = HfApi().model_info(REPO_ID)
    latest = info.sha
    print(f"Latest on HF Hub: {latest}")
    last_modified = getattr(info, "last_modified", None)
    if last_modified:
        print(f"Last modified:    {last_modified}")

    if pinned == latest:
        print("\nUp to date — no action needed.")
        return 0

    print("\nA newer upstream revision is available.")
    answer = input("Update pinned revision in source? [y/N]: ").strip().lower()
    if answer != "y":
        print("No changes made.")
        return 0

    write_pinned_sha(latest)
    print(f"\nUpdated _TOPIC_EMBEDDER_REVISION to {latest} in:\n  {PINNED_FILE}")
    print("Review the diff with `git diff` and commit when ready.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
