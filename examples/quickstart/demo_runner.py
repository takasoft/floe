"""End-to-end Floe watcher demo for the README GIF.

Starts the Rich dashboard, then fires `append_defect.py` twice in a
background thread so the watcher detects two distinct upstream changes
and the dashboard visibly refreshes the affected DITs.

Used by `examples/quickstart/demo.tape` to generate the README GIF via
VHS (https://github.com/charmbracelet/vhs).

Prerequisite: the catalog must already be bootstrapped:
    python seed_sources.py && floe apply
"""

from __future__ import annotations

import subprocess
import sys
import threading
import time
from pathlib import Path

HERE = Path(__file__).parent

from floe.dashboard import run_dashboard  # noqa: E402
from floe.pipeline import Pipeline  # noqa: E402
from floe.watcher import WatchConfig, Watcher  # noqa: E402

CONFIG_PATH = HERE / "floe.yaml"


def _append_defect() -> None:
    subprocess.run(
        [sys.executable, "append_defect.py"],
        cwd=str(HERE),
        check=True,
        capture_output=True,
    )


def trigger_appends() -> None:
    """Drive the demo with two append cycles, spaced out so the dashboard
    visibly settles between them."""
    time.sleep(7)  # let the dashboard come up and show "Waiting..."
    _append_defect()
    time.sleep(9)  # let the first refresh play out
    _append_defect()


def main() -> None:
    threading.Thread(target=trigger_appends, daemon=True).start()

    pipeline = Pipeline.from_config(CONFIG_PATH)
    watcher = Watcher(
        pipeline,
        WatchConfig(
            poll_interval_seconds=3,
            quiet_period_seconds=1,
            max_iterations=9,  # ~27s of polling, ends ~30s after start
        ),
    )
    run_dashboard(pipeline, watcher, poll_interval=3)


if __name__ == "__main__":
    main()
