"""Native (no-Docker) end-to-end performance harness for Floe.

Seeds a large synthetic dataset into an isolated local SQLite catalog + on-disk
warehouse, then runs the real Floe refresh path (PyIceberg scan -> DuckDB SQL ->
Iceberg overwrite) for every dynamic table in topological order, timing each hop.

This is the local counterpart to the containerized benchmark (see README.md):
it isolates Floe's compute from object-store and Postgres latency so the numbers
reflect the engine itself.

Usage (from the repo root, inside the project venv):

    BENCH_DELIVERIES=2000000 python benchmarks/bench_native.py

Env knobs:
    BENCH_DELIVERIES  number of synthetic delivery rows to generate (default 2,000,000)
    BENCH_WORKDIR     reuse a specific scratch dir instead of a temp dir
    BENCH_KEEP=1      keep the scratch catalog + warehouse after the run

The synthetic data shape is controlled by the seeder's own knobs:
    FLOE_SEED_DAYS / FLOE_SEED_DAS / FLOE_SEED_ROUTES /
    FLOE_SEED_SAFETY_PCT / FLOE_SEED_DEFECT_PCT
"""

from __future__ import annotations

import os
import shutil
import sys
import tempfile
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))
sys.path.insert(0, str(REPO_ROOT / "examples" / "quickstart"))


def peak_rss_mb() -> float:
    """Best-effort peak resident set size of this process, in MiB.

    Uses the platform's native accounting so it captures DuckDB/Arrow C++
    allocations, not just the Python heap. Returns 0.0 if it cannot be determined.
    """
    if sys.platform == "win32":
        import ctypes
        import ctypes.wintypes

        class PMC(ctypes.Structure):
            _fields_ = [
                ("cb", ctypes.wintypes.DWORD),
                ("PageFaultCount", ctypes.wintypes.DWORD),
                ("PeakWorkingSetSize", ctypes.c_size_t),
                ("WorkingSetSize", ctypes.c_size_t),
                ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
                ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                ("PagefileUsage", ctypes.c_size_t),
                ("PeakPagefileUsage", ctypes.c_size_t),
            ]

        pmc = PMC()
        pmc.cb = ctypes.sizeof(PMC)
        k = ctypes.windll.kernel32
        k.GetCurrentProcess.restype = ctypes.c_void_p
        psapi = ctypes.windll.psapi
        psapi.GetProcessMemoryInfo.argtypes = [
            ctypes.c_void_p,
            ctypes.POINTER(PMC),
            ctypes.wintypes.DWORD,
        ]
        psapi.GetProcessMemoryInfo.restype = ctypes.wintypes.BOOL
        if psapi.GetProcessMemoryInfo(k.GetCurrentProcess(), ctypes.byref(pmc), pmc.cb):
            return pmc.PeakWorkingSetSize / 1024 / 1024
        return 0.0

    # Unix: ru_maxrss is KiB on Linux, bytes on macOS.
    import resource

    maxrss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    divisor = 1024 * 1024 if sys.platform == "darwin" else 1024
    return maxrss / divisor


def fmt(n: float) -> str:
    return f"{n:,.0f}"


def main() -> None:
    n = int(os.environ.get("BENCH_DELIVERIES", "2000000"))
    workdir = Path(os.environ.get("BENCH_WORKDIR") or tempfile.mkdtemp(prefix="floe_bench_"))
    workdir.mkdir(parents=True, exist_ok=True)
    cat = workdir / "catalog.db"
    wh = workdir / "warehouse"

    os.environ["FLOE_CATALOG_URI"] = f"sqlite:///{cat.as_posix()}"
    os.environ["FLOE_WAREHOUSE"] = str(wh)
    os.environ["FLOE_SEED_DELIVERIES"] = str(n)

    cfg = REPO_ROOT / "examples" / "quickstart" / "floe.yaml"

    print("== Floe native benchmark ==")
    print(f"deliveries target : {fmt(n)}")
    print(f"workdir           : {workdir}")
    print(f"catalog           : {os.environ['FLOE_CATALOG_URI']}")
    print(f"warehouse         : {wh}")
    print()

    import seed_sources  # noqa: E402  (examples/quickstart on sys.path)

    t0 = time.perf_counter()
    seed_sources.main()
    t_seed = time.perf_counter() - t0
    print(f"\nseed time         : {t_seed:7.2f}s  ({fmt(n / t_seed)} rows/s into Iceberg)\n")

    from floe.pipeline import Pipeline  # noqa: E402

    p = Pipeline.from_config(cfg)
    order = p.planner.topological_order()

    print(f"{'dynamic table':<32}{'rows':>14}{'secs':>9}{'rows/s':>14}")
    print("-" * 69)
    t1 = time.perf_counter()
    for name in order:
        s = time.perf_counter()
        r = p.executor.refresh(p.dits[name])
        dt = time.perf_counter() - s
        rate = r.rows_written / dt if dt > 0 else 0
        tag = "  (skipped)" if r.skipped else ""
        print(f"{name:<32}{fmt(r.rows_written):>14}{dt:>9.2f}{fmt(rate):>14}{tag}")
    t_apply = time.perf_counter() - t1
    print("-" * 69)
    print(f"{'TOTAL apply':<32}{'':>14}{t_apply:>9.2f}")

    print()
    print(f"end-to-end (seed+apply): {t_seed + t_apply:7.2f}s")
    print(f"peak resident set      : {peak_rss_mb():,.0f} MiB")

    if not os.environ.get("BENCH_KEEP"):
        shutil.rmtree(workdir, ignore_errors=True)
        print(f"cleaned up {workdir}")
    else:
        print(f"kept {workdir}")


if __name__ == "__main__":
    main()
