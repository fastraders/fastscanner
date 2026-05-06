"""
Benchmark news filter prompts against all symbols in headlines.csv.

Runs both old_prompt.txt and new_prompt.txt through Codex for every symbol,
then prints a side-by-side comparison showing what each prompt kept or dropped.

Usage:
    python -m fastscanner.adapters.cmd.run_news_filter
"""

import asyncio
import csv
import json
import re
import shutil
import sys
import textwrap
from pathlib import Path

from fastscanner.pkg.observability import init_metrics

NEWS_DIR = Path(__file__).parent / "news"
HEADLINES_CSV = NEWS_DIR / "headlines.csv"
OLD_PROMPT_FILE = NEWS_DIR / "old_prompt.txt"
NEW_PROMPT_FILE = NEWS_DIR / "new_prompt.txt"

CODEX_TIMEOUT = 90.0
CONFIDENCE_THRESHOLD = 50
CONCURRENCY = 3


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------


def _load_headlines() -> dict[str, list[str]]:
    """Return {symbol: [headline, ...]} from headlines.csv."""
    by_symbol: dict[str, list[str]] = {}
    with HEADLINES_CSV.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            sym = row["symbol"].strip().upper()
            title = row["headline"].strip()
            if sym and title:
                by_symbol.setdefault(sym, []).append(title)
    return by_symbol


# ---------------------------------------------------------------------------
# Codex
# ---------------------------------------------------------------------------


async def _run_codex(prompt: str) -> str:
    proc = await asyncio.create_subprocess_exec(
        "codex",
        "exec",
        "--sandbox",
        "read-only",
        "-c",
        'web_search="disabled"',
        "-c",
        "features.shell_tool=false",
        prompt,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout_b, stderr_b = await asyncio.wait_for(
            proc.communicate(), timeout=CODEX_TIMEOUT
        )
    except asyncio.TimeoutError:
        proc.kill()
        await proc.wait()
        raise RuntimeError(f"codex timed out after {CODEX_TIMEOUT}s")

    if proc.returncode != 0:
        err = stderr_b.decode(errors="replace")[:300]
        raise RuntimeError(f"codex exited {proc.returncode}: {err}")

    return stdout_b.decode(errors="replace")


def _parse_response(text: str, n: int) -> list[dict]:
    match = re.search(r"\[\s*(?:\{.*?\}\s*,?\s*)*\]", text, re.DOTALL)
    if not match:
        raise RuntimeError(f"No JSON array in codex output: {text[:300]!r}")
    raw = json.loads(match.group(0))
    if not isinstance(raw, list):
        raise RuntimeError(f"Codex output is not a list: {raw!r}")

    valid: list[dict] = []
    for entry in raw:
        if not isinstance(entry, dict):
            continue
        idx = entry.get("idx")
        conf = entry.get("confidence")
        if not isinstance(idx, int) or not isinstance(conf, (int, float)):
            continue
        if not (0 <= idx < n) or not (0 <= conf <= 100):
            continue
        valid.append({"idx": idx, "confidence": int(conf)})

    if not valid:
        raise RuntimeError(f"No valid entries in codex output: {raw!r}")
    return valid


async def _score_symbol(
    sem: asyncio.Semaphore,
    symbol: str,
    headlines: list[str],
    prompt_template: str,
    prompt_label: str,
) -> tuple[str, str, list[dict]]:
    """Return (symbol, prompt_label, scored_entries)."""
    items = [{"idx": i, "title": h} for i, h in enumerate(headlines)]
    prompt = prompt_template.format(
        symbol=symbol,
        headlines=json.dumps(items, ensure_ascii=False),
    )
    async with sem:
        print(f"  [{prompt_label}] {symbol} ({len(headlines)} headlines)...")
        scores = _parse_response(await _run_codex(prompt), len(headlines))

    score_map = {s["idx"]: s["confidence"] for s in scores}
    entries = [
        {
            "idx": i,
            "title": h,
            "confidence": score_map.get(i, -1),
            "kept": score_map.get(i, -1) > CONFIDENCE_THRESHOLD,
        }
        for i, h in enumerate(headlines)
    ]
    return symbol, prompt_label, entries


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------

HEADLINE_COL = 60


def _print_report(
    results: dict[str, dict[str, list[dict]]],
    symbols: list[str],
) -> None:
    sep = "=" * 72
    thin = "-" * 72

    total_old_kept = 0
    total_new_kept = 0
    symbols_changed = 0
    total_sym_diff = 0

    print(f"\n{sep}")
    print("  PER-SYMBOL COMPARISON  (only symbols with differences)")
    print(sep)

    for sym in symbols:
        sym_results = results.get(sym, {})
        old_entries = sym_results.get("old", [])
        new_entries = sym_results.get("new", [])

        if not old_entries or not new_entries:
            continue

        old_kept = {e["title"] for e in old_entries if e["kept"]}
        new_kept = {e["title"] for e in new_entries if e["kept"]}
        total_old_kept += len(old_kept)
        total_new_kept += len(new_kept)

        only_old = old_kept - new_kept
        only_new = new_kept - old_kept
        sym_diff = len(only_old) + len(only_new)
        total_sym_diff += sym_diff

        if sym_diff > 0:
            symbols_changed += 1
            old_conf = {e["title"]: e["confidence"] for e in old_entries}
            new_conf = {e["title"]: e["confidence"] for e in new_entries}

            print(f"\n  {sym}  (diff: {sym_diff})")
            print(thin)

            if only_old:
                print(
                    f"  REMOVED by new prompt ({len(only_old)}) — kept before, now dropped:"
                )
                for t in sorted(only_old):
                    oc = old_conf.get(t, -1)
                    nc = new_conf.get(t, -1)
                    nc_label = f"{nc:3d}%" if nc >= 0 else " n/a"
                    print(f"    [was {oc:3d}% → now {nc_label}]  {t}")

            if only_new:
                print(
                    f"  ADDED by new prompt ({len(only_new)}) — dropped before, now kept:"
                )
                for t in sorted(only_new):
                    oc = old_conf.get(t, -1)
                    nc = new_conf.get(t, -1)
                    oc_label = f"{oc:3d}%" if oc >= 0 else " n/a"
                    print(f"    [was {oc_label} → now {nc:3d}%]  {t}")

    print(f"\n{sep}")
    print("  FULL RESULTS PER SYMBOL  (all kept headlines)")
    print(sep)

    for sym in symbols:
        sym_results = results.get(sym, {})
        old_entries = sym_results.get("old", [])
        new_entries = sym_results.get("new", [])

        if not old_entries or not new_entries:
            continue

        old_kept = {e["title"] for e in old_entries if e["kept"]}
        new_kept = {e["title"] for e in new_entries if e["kept"]}

        if not old_kept and not new_kept:
            continue

        old_conf = {e["title"]: e["confidence"] for e in old_entries}
        new_conf = {e["title"]: e["confidence"] for e in new_entries}
        all_kept = old_kept | new_kept

        print(f"\n  {sym}")
        print(thin)
        print(f"  {'HEADLINE':<60}  OLD    NEW")
        print(thin)
        for t in sorted(all_kept):
            oc = old_conf.get(t, -1)
            nc = new_conf.get(t, -1)
            oc_label = f"{oc:3d}%" if oc >= 0 else " n/a"
            nc_label = f"{nc:3d}%" if nc >= 0 else " n/a"
            kept_old = "✓" if t in old_kept else "✗"
            kept_new = "✓" if t in new_kept else "✗"
            lines = textwrap.wrap(t, width=HEADLINE_COL)
            print(
                f"  {lines[0]:<{HEADLINE_COL}}  {kept_old} {oc_label}  {kept_new} {nc_label}"
            )
            for line in lines[1:]:
                print(f"  {line}")

    total_symbols = len([s for s in symbols if results.get(s)])
    print(f"\n{sep}")
    print("  SUMMARY")
    print(sep)
    print(f"  Symbols processed     : {total_symbols}")
    print(f"  Symbols with changes  : {symbols_changed}")
    print(f"  Total kept (old)      : {total_old_kept}")
    print(f"  Total kept (new)      : {total_new_kept}")
    delta = total_new_kept - total_old_kept
    delta_sign = "+" if delta >= 0 else ""
    print(f"  Net change            : {delta_sign}{delta}")
    print(f"  Total symmetric diff  : {total_sym_diff}  (lower = more similar)")
    print(f"{sep}\n")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


async def _main() -> None:
    init_metrics(role="news_filter")
    if shutil.which("codex") is None:
        print("Error: codex CLI not found in PATH", file=sys.stderr)
        sys.exit(1)

    if not HEADLINES_CSV.exists():
        print(f"Error: headlines file not found: {HEADLINES_CSV}", file=sys.stderr)
        sys.exit(1)

    old_template = OLD_PROMPT_FILE.read_text(encoding="utf-8")
    new_template = NEW_PROMPT_FILE.read_text(encoding="utf-8")

    by_symbol = _load_headlines()
    symbols = sorted(by_symbol)
    total_headlines = sum(len(v) for v in by_symbol.values())

    print(f"\nLoaded {total_headlines} headlines across {len(symbols)} symbols")
    print(f"Confidence threshold: >{CONFIDENCE_THRESHOLD}")
    print(f"Concurrency: {CONCURRENCY} parallel Codex calls\n")

    sem = asyncio.Semaphore(CONCURRENCY)

    print("Running OLD prompt...")
    old_tasks = [
        _score_symbol(sem, sym, by_symbol[sym], old_template, "old") for sym in symbols
    ]
    old_results_raw = await asyncio.gather(*old_tasks, return_exceptions=True)

    print("\nRunning NEW prompt...")
    new_tasks = [
        _score_symbol(sem, sym, by_symbol[sym], new_template, "new") for sym in symbols
    ]
    new_results_raw = await asyncio.gather(*new_tasks, return_exceptions=True)

    results: dict[str, dict[str, list[dict]]] = {}

    for res in old_results_raw:
        if isinstance(res, BaseException):
            print(f"  [old] ERROR: {res}", file=sys.stderr)
            continue
        sym, label, entries = res
        results.setdefault(sym, {})[label] = entries

    for res in new_results_raw:
        if isinstance(res, BaseException):
            print(f"  [new] ERROR: {res}", file=sys.stderr)
            continue
        sym, label, entries = res
        results.setdefault(sym, {})[label] = entries

    _print_report(results, symbols)


if __name__ == "__main__":
    asyncio.run(_main())
