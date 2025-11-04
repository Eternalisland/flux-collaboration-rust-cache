# Repository Guidelines

## Project Structure & Module Organization
The crate entry point lives in `src/lib.rs`, with focused modules such as `cache.rs` and `disk_storage.rs` for tiered memory/disk flows, and JNI adapters under `src/jni_*.rs`. Performance experiments reside alongside (`benchmark_runner.rs`, `optimized_mmap_storage.rs`), so keep experimental work isolated behind `cfg(test)` or feature flags when merging. Integration and perf regressions are exercised through `tests/`, while runnable scenarios and API demonstrations live in `examples/`. Reference `docs/` for configuration notes and `benchdata/` for large fixture payloads used by the mmap benchmarks.

## Build, Test, and Development Commands
- `cargo build --all-targets` — compile the library and JNI artifacts.
- `cargo test` — execute unit tests and integration suites under `tests/`.
- `cargo fmt --all` — apply canonical formatting; pair with `-- --check` in CI mode.
- `cargo clippy --all-targets --all-features -- -D warnings` — lint with the project’s strict warning policy.
- `./build.sh` (or `build.bat` on Windows) — reproduce the standard toolchain bootstrap with fmt + clippy before packaging.

## Coding Style & Naming Conventions
Adhere to rustfmt defaults (4-space indentation, trailing commas). Public APIs use `snake_case` for functions and modules, `CamelCase` for types/traits, and `SCREAMING_SNAKE_CASE` for constants. Keep JNI bridge modules prefixed `jni_bridge_` and suffix platform specializations (`_high_perf`, `_flatbuffers`) to match existing files. Run `cargo fmt` before review; surface Clippy suppressions inline with a short rationale.

## Testing Guidelines
Prefer focused unit tests colocated with their module via `#[cfg(test)]`, and use the `tests/` directory for integration or performance harnesses (`cargo test --test performance_test -- --nocapture` shows throughput prints). When adding large fixtures, store them under `benchdata/` and gate long-running benchmarks with `#[ignore]` so CI stays fast. Document expected metrics deltas in PRs when touching performance-sensitive code.

## Commit & Pull Request Guidelines
Match the existing history convention: start commit subjects with a datestamp (`YYYYMMDD`), contributor tag, and a bracketed category (e.g., `【功能完善】`), followed by a concise summary. PRs must describe scope, highlight configuration changes, list the commands you ran, and attach benchmark snippets or screenshots when tuning performance.

## Security & Configuration Tips
Respect the pinned toolchain in `rust-toolchain.toml`; do not upgrade it without coordinating CI changes. Secrets or disk paths consumed by JNI layers belong in environment variables documented in `docs/configuration.md`, never hard-coded. Use tempfile directories (`std::env::temp_dir()`) for experimental disk writes to avoid polluting shared volumes.
