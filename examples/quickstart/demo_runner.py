"""End-to-end Floe watcher demo for the README GIF.

Starts the Rich dashboard, then fires append scripts in a background thread
so the watcher detects two distinct upstream changes and the dashboard
visibly refreshes the affected DITs.

The two appends exercise different slices of the DAG on purpose:

  1. ``append_delivery.py`` adds a row to ``bronze.raw_deliveries``, which
     cascades through every silver and gold DIT downstream of it:
     ``silver.deliveries_enriched``, ``gold.da_daily_performance``,
     ``gold.station_daily_summary``, and ``gold.delivery_quality_daily``.
  2. ``append_defect.py`` adds a row to ``bronze.delivery_defects``, which
     only ``gold.delivery_quality_daily`` depends on — so the watcher
     refreshes that one DIT.

Used by ``examples/quickstart/demo.tape`` to generate the README GIF via
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


def _run_append(script: str) -> None:
    subprocess.run(
        [sys.executable, script],
        cwd=str(HERE),
        check=True,
        capture_output=True,
    )


def trigger_appends() -> None:
    """Drive the demo with two append cycles, spaced out so the dashboard
    visibly settles between them."""
    time.sleep(7)  # let the dashboard come up and show "Waiting..."
    _run_append("append_delivery.py")  # cascade: bronze → silver → gold
    time.sleep(11)  # let the multi-hop cascade play out
    _run_append("append_defect.py")  # narrower: only gold.delivery_quality_daily


def main() -> None:
    threading.Thread(target=trigger_appends, daemon=True).start()

    pipeline = Pipeline.from_config(CONFIG_PATH)
    watcher = Watcher(
        pipeline,
        WatchConfig(
            poll_interval_seconds=3,
            quiet_period_seconds=1,
            max_iterations=10,  # ~30s of polling
        ),
    )
    run_dashboard(pipeline, watcher, poll_interval=3)


if __name__ == "__main__":
    main()
