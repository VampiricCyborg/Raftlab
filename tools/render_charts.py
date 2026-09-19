"""Render the README's benchmark charts (light and dark) from fresh benchmark runs.

    uv sync --group docs
    uv run python tools/render_charts.py

Writes docs/media/bench-{election,commit,availability}-{light,dark}.png. The
numbers come from the same functions as benchmarks/*.py, with the same seeds,
so the charts always agree with the README tables.

Colours: the dataviz reference palette. Its first three categorical slots are
validated all-pairs for colour-vision deficiency in both modes; every chart
also direct-labels its series, and the README keeps the tables as the
accessible text view.
"""

from __future__ import annotations

import statistics
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.lines import Line2D  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "benchmarks"))

import bench_availability  # noqa: E402
import bench_commit  # noqa: E402
import bench_election  # noqa: E402
from _common import pct  # noqa: E402

OUT = ROOT / "docs" / "media"

THEMES = {
    "light": {
        "surface": "#fcfcfb", "ink": "#0b0b0b", "ink2": "#52514e", "muted": "#898781",
        "grid": "#e1e0d9", "axis": "#c3c2b7",
        "series": ["#2a78d6", "#eb6834", "#1baf7a"],
    },
    "dark": {
        "surface": "#1a1a19", "ink": "#ffffff", "ink2": "#c3c2b7", "muted": "#898781",
        "grid": "#2c2c2a", "axis": "#383835",
        "series": ["#3987e5", "#d95926", "#199e70"],
    },
}


def style(theme: dict) -> None:
    plt.rcParams.update({
        "figure.facecolor": theme["surface"],
        "axes.facecolor": theme["surface"],
        "savefig.facecolor": theme["surface"],
        "font.family": "DejaVu Sans",
        "font.size": 11,
        "text.color": theme["ink"],
        "axes.labelcolor": theme["ink2"],
        "axes.edgecolor": theme["axis"],
        "axes.linewidth": 0.8,
        "axes.spines.top": False,
        "axes.spines.right": False,
        "axes.spines.left": False,
        "axes.grid": True,
        "axes.grid.axis": "y",
        "grid.color": theme["grid"],
        "grid.linewidth": 0.8,
        "xtick.color": theme["muted"],
        "ytick.color": theme["muted"],
        "xtick.labelcolor": theme["ink2"],
        "ytick.labelcolor": theme["ink2"],
        "ytick.major.size": 0,
        "legend.frameon": False,
        "legend.labelcolor": theme["ink2"],
    })


def title(fig, theme, headline: str, sub: str) -> None:
    fig.text(0.02, 0.965, headline, fontsize=14, fontweight="bold", color=theme["ink"], va="top")
    fig.text(0.02, 0.895, sub, fontsize=10.5, color=theme["ink2"], va="top")


def line(ax, theme, x, y, i: int, label: str, dy: float = 0) -> None:
    """A 2px series line with ringed markers and a direct label at its end."""
    color = theme["series"][i]
    ax.plot(x, y, color=color, linewidth=2, marker="o", markersize=7,
            markeredgecolor=theme["surface"], markeredgewidth=2, zorder=3)
    ax.annotate(label, (x[-1], y[-1]), xytext=(8, dy), textcoords="offset points",
                va="center", fontsize=10, color=theme["ink2"])


def legend(ax, theme, items: list[tuple[int, str]], **kw) -> None:
    """Solid-line legend handles (the markers' surface ring would read as dashes)."""
    handles = [Line2D([], [], color=theme["series"][i], linewidth=2, marker="o", markersize=6)
               for i, _ in items]
    ax.legend(handles, [label for _, label in items], fontsize=9.5, handlelength=1.8, **kw)


def save(fig, name: str, theme_name: str) -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    path = OUT / f"bench-{name}-{theme_name}.png"
    fig.savefig(path, dpi=160)
    plt.close(fig)
    print(path.relative_to(ROOT))


# --- data ----------------------------------------------------------------------


def election_data():
    rows = []
    for jitter in bench_election.JITTERS:
        results = [bench_election.first_leader(s, jitter) for s in bench_election.SEEDS]
        elected = [r for r in results if r is not None]
        ticks = [t for t, _ in elected]
        split = sum(term > 1 for _, term in elected) + len(results) - len(elected)
        rows.append({
            "jitter": jitter,
            "split_pct": 100 * split / len(results),
            "median": statistics.median(ticks) if ticks else None,
            "p95": pct(ticks, 95) if ticks else None,
        })
    return rows


def commit_data():
    rows = []
    for drop in bench_commit.DROP_RATES:
        lat, terms, ticks = [], 0, 0
        for seed in bench_commit.SEEDS:
            l, _, tm, tk = bench_commit.run(seed, drop)
            lat += l
            terms += tm
            ticks += tk
        rows.append({"drop": drop, "median": statistics.median(lat), "p95": pct(lat, 95),
                     "max": max(lat), "elections": 1000 * terms / ticks})
    return rows


def availability_data():
    out = {}
    for every in bench_availability.KILL_EVERY:
        out[every] = []
        for n in bench_availability.SIZES:
            res = [bench_availability.run(s, n, every) for s in bench_availability.SEEDS]
            out[every].append(100 * statistics.mean(a for a, _, _ in res))
    return out


# --- charts --------------------------------------------------------------------


def chart_election(rows, theme, theme_name):
    style(theme)
    fig, (left, right) = plt.subplots(1, 2, figsize=(10, 4.4), gridspec_kw={"wspace": 0.28})
    fig.subplots_adjust(left=0.07, right=0.93, top=0.74, bottom=0.16)
    title(fig, theme, "Randomized timeouts are what make elections converge",
          "5 nodes, cold start, 100 seeds per setting. Timeout drawn from [10, 10 + jitter] ticks.")
    labels = [str(r["jitter"]) for r in rows]
    x = list(range(len(rows)))

    left.bar(x, [r["split_pct"] for r in rows], width=0.55, color=theme["series"][0], zorder=3)
    for xi, r in zip(x, rows):
        if r["split_pct"] > 0:
            left.annotate(f"{r['split_pct']:.0f}%", (xi, r["split_pct"]), xytext=(0, 4),
                          textcoords="offset points", ha="center", fontsize=9.5, color=theme["ink2"])
    left.set_title("Runs with a split vote", loc="left", fontsize=11.5, color=theme["ink"], pad=10)
    left.set_ylim(0, 112)
    left.set_yticks([0, 25, 50, 75, 100], ["0%", "25%", "50%", "75%", "100%"])
    left.set_xticks(x, labels)
    left.set_xlabel("timeout jitter (ticks)")
    left.annotate("no jitter: every run\nsplits forever", (0.28, 62), xytext=(22, 0),
                  textcoords="offset points", va="center", fontsize=9.5, color=theme["ink2"],
                  arrowprops={"arrowstyle": "-", "color": theme["muted"], "lw": 0.8})

    elected = [(xi, r) for xi, r in zip(x, rows) if r["median"] is not None]
    xs = [xi for xi, _ in elected]
    line(right, theme, xs, [r["p95"] for _, r in elected], 1, "p95")
    line(right, theme, xs, [r["median"] for _, r in elected], 0, "median")
    right.set_title("Ticks until a leader is elected", loc="left", fontsize=11.5, color=theme["ink"], pad=10)
    right.set_xticks(x, labels)
    right.set_xlim(-0.4, len(x) - 0.2)
    right.set_ylim(0, max(r["p95"] for _, r in elected) * 1.25)
    right.set_xlabel("timeout jitter (ticks)")
    legend(right, theme, [(1, "p95"), (0, "median")], loc="upper left", ncols=2)
    right.annotate("never", (0, 1.5), ha="center", fontsize=9, color=theme["muted"])
    save(fig, "election", theme_name)


def chart_commit(rows, theme, theme_name):
    style(theme)
    fig, (left, right) = plt.subplots(1, 2, figsize=(10, 4.4), gridspec_kw={"wspace": 0.28})
    fig.subplots_adjust(left=0.07, right=0.93, top=0.74, bottom=0.16)
    title(fig, theme, "Message loss barely slows commits, until it starts deposing leaders",
          "5 nodes, delay 1–3 ticks, 100 seeds × 20 sequential writes with a retrying client.")
    x = list(range(len(rows)))
    labels = [f"{r['drop']:.0%}" for r in rows]

    line(left, theme, x, [r["max"] for r in rows], 1, "max")
    line(left, theme, x, [r["p95"] for r in rows], 2, "p95", dy=7)
    line(left, theme, x, [r["median"] for r in rows], 0, "median", dy=-7)
    left.set_title("Ticks from submit to commit", loc="left", fontsize=11.5, color=theme["ink"], pad=10)
    left.set_xticks(x, labels)
    left.set_xlim(-0.3, len(x) - 0.4)
    left.set_ylim(0, max(r["max"] for r in rows) * 1.15)
    left.set_xlabel("message drop rate")
    legend(left, theme, [(1, "max"), (2, "p95"), (0, "median")], loc="upper left")

    right.bar(x, [r["elections"] for r in rows], width=0.55, color=theme["series"][0], zorder=3)
    for xi, r in zip(x, rows):
        right.annotate(f"{r['elections']:.1f}" if r["elections"] else "0", (xi, r["elections"]), xytext=(0, 4),
                       textcoords="offset points", ha="center", fontsize=9.5, color=theme["ink2"])
    right.set_title("Leader elections per 1000 ticks", loc="left", fontsize=11.5, color=theme["ink"], pad=10)
    right.set_xticks(x, labels)
    right.set_ylim(0, max(max(r["elections"] for r in rows) * 1.3, 1))
    right.set_xlabel("message drop rate")
    save(fig, "commit", theme_name)


def chart_availability(data, theme, theme_name):
    style(theme)
    fig, ax = plt.subplots(figsize=(10, 4.4))
    fig.subplots_adjust(left=0.07, right=0.84, top=0.74, bottom=0.16)
    title(fig, theme, "Bigger clusters stay available: the leader is a smaller target",
          "One node killed every K ticks (the previous victim restarts). 50 seeds × 3000 ticks each.")
    sizes = bench_availability.SIZES
    for i, every in enumerate(sorted(data)):
        line(ax, theme, sizes, data[every], i, f"kill every {every} ticks")
    legend(ax, theme, [(i, f"kill every {k} ticks") for i, k in enumerate(sorted(data))],
           loc="lower right", bbox_to_anchor=(1.0, 0.02))
    ax.set_title("Ticks with a stable leader", loc="left", fontsize=11.5, color=theme["ink"], pad=10)
    ax.set_xticks(sizes, [f"{n} nodes" for n in sizes])
    ax.set_xlim(2.7, 7.3)
    ax.set_ylim(84, 100)
    ax.set_yticks([85, 90, 95, 100], ["85%", "90%", "95%", "100%"])
    save(fig, "availability", theme_name)


def main() -> None:
    election = election_data()
    commit = commit_data()
    availability = availability_data()
    for name, theme in THEMES.items():
        chart_election(election, theme, name)
        chart_commit(commit, theme, name)
        chart_availability(availability, theme, name)


if __name__ == "__main__":
    main()
