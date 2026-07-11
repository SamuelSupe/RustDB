# Packet: ci_docs

Objective: add platform-neutral quality gates, hosted CI where appropriate,
and concise acceptance documentation.

Ownership: `scripts/ci/**`, `.github/workflows/**`, and
`docs/acceptance.md`. Do not edit engine or benchmark implementation files.

Verification: shell syntax checks, configuration validation, and a local CI
entrypoint smoke run in OrbStack.
