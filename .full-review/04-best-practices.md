# Phase 4: Best Practices & Standards

## Framework & Language Findings

### Critical

1. **Indicator Protocol declares `extend_realtime` but daily indicators lack it** (also flagged in Phase 1)
   - Multiple classes don't satisfy the `Indicator` protocol. Code works via `hasattr` duck-typing.
   - **Fix**: Split into `Indicator` and `RealtimeIndicator` protocols.

2. **`CandleStoreTest.get` references undefined variables** (also flagged in Phase 1)
   - Scanner fixture params `start`/`end` vs body `start_date`/`end_date`. `NameError` at runtime.

3. **`SmallCapUpScanner.scan` uses wrong loop variable** (also flagged in Phase 1)
   - `shift_indicator` always references the last element from a prior loop.

### High

4. **No linter, formatter, or type checker configured**
   - `pyproject.toml` has zero tooling config (no ruff, mypy, pyright, flake8). No `.pre-commit-config.yaml`.
   - **Fix**: Add `[tool.ruff]` and `[tool.mypy]` sections. Add pre-commit hooks.

5. **`hasattr` for protocol conformance instead of `isinstance`** (`service.py:97`)
   - Defeats static analysis and is fragile.
   - **Fix**: Use `isinstance(ind, RealtimeIndicator)` after protocol split.

6. **Truthiness check conflates `0` and `None`** (`candle.py:642`)
   - `if atr_val and ...` evaluates `False` when `atr_val` is `0.0`.
   - **Fix**: `if atr_val is not None and ...`

7. **`ApplicationRegistry` global service locator** (all indicator/scanner classes)
   - Hidden dependencies, fragile testing, prevents multiple service instances.
   - **Fix**: Constructor-inject `CandleStore` and `Cache` explicitly.

8. **~2000 lines of duplicated scanner code** (gap.py, parabolic.py, range_gap.py)
   - Up/Down pairs ~95% identical. Changes must be replicated across all.
   - **Fix**: Extract direction-parameterized base class.

9. **`DailyIndicatorCache` thundering herd** (`service.py:319-324`)
   - All concurrent calls see stale date and issue `_load()` simultaneously.
   - **Fix**: Double-checked locking with `asyncio.Lock`.

### Medium

10. **Legacy `Dict`/`List`/`Union` from `typing`** — use Python 3.11+ builtins (`dict`, `list`, `|`).
11. **Missing return type annotations** — `type()` and `column_name()` across all indicator/scanner classes.
12. **Inconsistent `Annotated[..., Depends()]` usage** — REST endpoints use it, WebSocket endpoints don't.
13. **CSV endpoint returns plain `str`** — should use `Response(media_type="text/csv")`.
14. **`logging.basicConfig()` in service module** — overrides app-level YAML logging config.
15. **`assert` used for runtime validation** — stripped with `-O`. Use `if`/`raise ValueError`.
16. **Missing `pytest-asyncio auto` mode** — every test needs `@pytest.mark.asyncio` manually.
17. **Three-layer concurrency in `scan_all`** — async -> thread -> multiprocess -> async. Complex and fragile.
18. **`str, Enum` instead of `StrEnum`** — already used elsewhere in codebase (`CumulativeOperation`).

### Low

19. **Exact-pinned `pandas = "2.3.0"`** — blocks patch/security updates. Use `^2.3.0`.
20. **`CandleCol` not using `Final` annotations** — allows accidental mutation at runtime.
21. **Duplicate `self._id` assignment** (`smallcap.py:41,44`).
22. **Unused imports** in multiple files — ruff would catch these automatically.
23. **Wrong class name in error message** (`parabolic.py:481`) — "Up" referenced in "Down" scanner.
24. **`ContextVar` used as global singleton** — semantically misleading; `functools.lru_cache` more idiomatic.
25. **Commented-out code blocks** in scanner files — should rely on git history.

---

## CI/CD & DevOps Findings

### Critical

1. **No CI/CD pipeline exists**
   - No GitHub Actions, no GitLab CI, nothing. The only test runner is a manual `make test`. Code can be pushed to `main` with failing tests.
   - **Fix**: Create a GitHub Actions workflow running `pytest` + linting on push/PR to `main`.

### High

2. **No linting or static analysis** — no ruff/mypy/flake8 in toolchain or CI.
3. **No deployment automation** — deployment is SSH + `git pull` + `poetry install` + `systemctl restart`. No rollback mechanism.
4. **`logging.basicConfig` overrides structured logging** — called in service modules, overrides YAML config.
5. **No application metrics** — zero Prometheus/StatsD/OpenTelemetry instrumentation. No way to monitor WebSocket connections, throughput, cache hit rates, error rates.
6. **No health check endpoint** — no way to determine if app can reach Dragonfly/NATS. Process can be running but deadlocked.
7. **`multiprocessing.Pool` recreated per `scan_all` request** — fork overhead on every call. No concurrency control.

### Medium

8. **SIGUSR2 in `ExecStop` not handled by Python/uvicorn** — systemd sends SIGUSR2 (NATS Lame Duck Mode signal, not Python), then SIGKILL after 2.5 min. Every stop is a hard kill.
   - **Fix**: Change to `SIGTERM` or remove `ExecStop` (systemd sends SIGTERM by default).
9. **NATS connection has no reconnection logic** — no `reconnected_cb`, `disconnected_cb`, `max_reconnect_attempts`. Silent data loss on connection drops.
10. **New daily caching script has no systemd unit or scheduler** — must be run manually.
11. **Incomplete infrastructure coverage** — docker-compose only covers Redis. NATS not configured anywhere. Dragonfly socket path mismatch between systemd unit and `config.py` default.
12. **Logging config uses relative file path** (`logging.yaml`) — fails if CWD isn't project root.
13. **Missing env vars crash with unhelpful `KeyError`** — 7 mandatory vars use `os.environ[...]` with no descriptive error.
14. **No Dockerfile** — environment parity not guaranteed between dev and prod.

### Low

15. **Hardcoded paths/user in systemd units** — `/home/mmoheeka/Projects/fastscanner`, not reusable.
16. **`.env.example` out of sync with `config.py`** — missing several configurable parameters.
17. **DragonflyCache has no connection timeouts** — operations block indefinitely if Dragonfly is unresponsive.
