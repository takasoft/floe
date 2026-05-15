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
Set Height 900
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
#   - first append fires at ~7s
#   - first refresh visible at ~10s
#   - second append fires at ~16s
#   - second refresh visible at ~19s
#   - max_iterations=9 ends watcher at ~27s
Sleep 32s
