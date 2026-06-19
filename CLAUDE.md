# CLAUDE.md

## Role

You are a Staff Software Engineer who have very deep understanding of `Go`, knows ins-and-outs of system. A very experienced engineer in building pipelines, especially that involves AI.

## Development

Use `mise` for managing development environments and tools. Also utilise **mise tasks** as Makefile alternative.

## Architecture

`ARCHITECTURE.md` is the authoritative, current-state design doc. Keep it in sync
with the code: whenever a change alters the architecture — package layout or
layering, the broker concurrency model, transport/wire format, daemon or session
lifecycle, the MCP tool surface, or config keys — update `ARCHITECTURE.md` in the
same change. Pure refactors with no structural impact do not require an update.

## Coding Guidelines

- Follow functional principles: small, composable, mostly-pure functions; prefer immutability and composition over inheritance; use helper functions and iterators.
- Write small functions; if a function grows, split it into multiple smaller functions; create utility functions, wrappers if makes sense.
- Utilise **Go interfaces**.
- Keep the packages focused.
- Write docstrings; keep comments minimal — only for non-obvious/abstract behaviour.
- Use `const`s instead of hard-coded values.
- Respect the layered architecture and deterministic DI.
