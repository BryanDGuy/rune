# Agent Instructions

## Critical Rules

- **NEVER guess or hallucinate answers.** If unsure, say "I don't know" and research the answer first. A confident wrong answer is worse than admitting uncertainty. Always verify before stating something as fact.

## Comments

Only add a comment when the **why** is non-obvious: a hidden constraint, a subtle invariant, a workaround for a specific bug, or behavior that would surprise a reader. Let well-named identifiers speak for themselves. Never write comments that describe what the code does.

## Code Style

See [docs/STYLE.md](docs/STYLE.md) for API design and other coding conventions.

## Before Every Commit

Always run `make verify` before committing. It runs fmt, vet, modernize, and lint. Do not commit if any check fails.
