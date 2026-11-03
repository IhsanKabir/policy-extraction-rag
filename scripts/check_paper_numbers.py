"""Trace every number in the paper to a committed results file (2026-11-03).

Extracts each number from paper/main.tex (outside the bibliography, labels,
references and file paths) and looks for it among the values in results/*.json,
in the renderings the paper uses (x100 with one decimal, raw with one or two
decimals, whole numbers). A number found nowhere is printed with its context.
Each such number must either be corrected in the paper or listed in
paper/number_allowlist.txt with the reason it is not in a results file (a
count from the data section, a difference computed in the text, a date...).

This catches transcription errors, not wrong interpretations: a sentence can
quote a correct number and still claim the wrong thing, so the paper still
needs reading.

Usage:
    python scripts/check_paper_numbers.py [--tex paper/main.tex]
"""
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

NUMBER = re.compile(r"(?<![\w.])(\d{1,3}(?:,\d{3})+|\d+)(?:\.(\d+))?(?![\w])")
STRIP = [
    re.compile(r"(?<!\\)%.*"),                                   # comments
    re.compile(r"\\begin\{thebibliography\}.*", re.S),          # references
    re.compile(r"\\(?:ref|label|cite[pt]?|citealp|url|path|res|texttt|usetikzlibrary|usepackage)\{[^}]*\}"),
    re.compile(r"\\(?:documentclass|setlength|tabcolsep)\S*"),
    re.compile(r"\{[pl]\{[\d.]+cm\}"), re.compile(r"[\d.]+(?:cm|pt|em)\b"),
    re.compile(r"\b\d{4}-\d{2}-\d{2}(?:/\d{2})?\b"),             # dates
    re.compile(r"\b(?:Cat|Section|Sections|Table|Figure|Step)~?\s*\d+(?:\.\d+)?"),
]


def result_values(results_dir: Path) -> set[str]:
    """Every number in the results files, in the renderings the paper uses."""
    out: set[str] = set()

    def walk(x) -> None:
        if isinstance(x, bool):
            return
        if isinstance(x, (int, float)):
            for v in (x, abs(x)):
                out.update({f"{v:.1f}", f"{v:.2f}", f"{100 * v:.1f}", f"{100 * v:.0f}", f"{v:.0f}"})
                if float(v).is_integer():
                    out.add(str(int(v)))
        elif isinstance(x, dict):
            for k, v in x.items():
                walk(v)
                walk_key(k)
        elif isinstance(x, list):
            for v in x:
                walk(v)

    def walk_key(k: str) -> None:  # keys such as k in retrieval results ("5", "10")
        if re.fullmatch(r"\d+", k):
            out.add(k)

    for path in sorted(results_dir.glob("*.json")):
        walk(json.loads(path.read_text(encoding="utf-8")))
    return out


def paper_numbers(tex: str) -> list[tuple[str, int, str]]:
    """(number as written, line number, context) for every number in the text."""
    for pattern in STRIP:
        tex = pattern.sub(lambda m: re.sub(r"[^\n]", " ", m.group(0)), tex)
    found = []
    for lineno, line in enumerate(tex.splitlines(), 1):
        for m in NUMBER.finditer(line):
            number = m.group(1).replace(",", "") + (f".{m.group(2)}" if m.group(2) else "")
            found.append((number, lineno, line[max(0, m.start() - 50):m.end() + 30].strip()))
    return found


def load_allowlist(path: Path) -> dict[str, str]:
    allow = {}
    for line in path.read_text(encoding="utf-8").splitlines() if path.exists() else []:
        if line.strip() and not line.startswith("#"):
            number, _, reason = line.partition(" ")
            allow[number] = reason.strip()
    return allow


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--tex", type=Path, default=Path("paper/main.tex"))
    ap.add_argument("--results", type=Path, default=Path("results"))
    ap.add_argument("--allowlist", type=Path, default=Path("paper/number_allowlist.txt"))
    args = ap.parse_args()

    known = result_values(args.results)
    allow = load_allowlist(args.allowlist)
    numbers = paper_numbers(args.tex.read_text(encoding="utf-8"))
    untraced = [(n, ln, ctx) for n, ln, ctx in numbers if n not in known and n not in allow]
    unused = sorted(set(allow) - {n for n, _, _ in numbers})
    for n, ln, ctx in untraced:
        print(f"  line {ln:4d}  {n:>8s}  ... {ctx}")
    for n in unused:
        print(f"  allowlist entry not in the paper: {n}")
    traced = len(numbers) - len(untraced)
    print(f"{len(numbers)} numbers: {traced} traced to results or allowlisted, {len(untraced)} untraced")
    return 1 if untraced else 0


if __name__ == "__main__":
    raise SystemExit(main())
