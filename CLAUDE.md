# miniserve-ai: working rules for Claude Code

## Build & test (run before every commit)
cmake --build build -j && ctest --test-dir build --output-on-failure
PYTHONPATH="build/python:." pytest -q

## Conventions
- Canonical repo is ~/miniserve-ai (never a Desktop copy). Check `pwd` before running commands.
- One feature per branch (feat/*, bench/*, docs/*); open a PR with `gh pr create`. Never commit to main.
- Every feature ships with: tests, a docs/devlog.md entry (Done / Decisions / Problems / Next),
  and README Features/Limitations updated in the same PR.
- Benchmarks: publish only measured facts. Report the load generator's ceiling next to any req/s claim.
- Python owns payloads/futures; the C++ queue holds opaque integer IDs only. Registry access and
  future completion stay on the event-loop thread.

## Owner's learning rule
Before writing a non-trivial feature, use plan mode and wait for Owen to approve the design.
Do not implement design decisions Owen has not explained back.
