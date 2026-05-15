# VHS recording for the Floe watcher dashboard.
#
# Renders to examples/quickstart/demo.gif. Run from the repo root with:
#   vhs examples/quickstart/demo.tape
#
# Prereq (Linux/macOS): VHS, ttyd, ffmpeg installed; quickstart catalog bootstrapped:
#   python -m venv .venv && source .venv/bin/activate
#   pip install -e ".[dev]"
#   cd examples/quickstart && python seed_sources.py && floe apply
#
# Note: record on Linux/macOS — VHS on Windows cannot activate the venv mid-session.

Output examples/quickstart/demo.gif

Set FontSize 13
Set Width 1400
Set Height 1050
Set Theme "Catppuccin Mocha"
Set Padding 20
Set PlaybackSpeed 1.0
Set TypingSpeed 60ms
Set Shell "bash"

Type "source .venv/bin/activate && cd examples/quickstart"
Enter
Sleep 500ms

Type "python demo_runner.py"
Enter

# Let the full demo play out:
#   - dashboard spins up (~1s)
#   - first append (delivery) fires at ~7s
#   - first cascade visible at ~10s (silver + multiple golds refresh)
#   - second append (defect) fires at ~18s
#   - second refresh visible at ~21s (only gold.delivery_quality_daily)
#   - max_iterations=10 ends watcher at ~30s
Sleep 35s
