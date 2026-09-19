"""Render the README's animated GIFs from real, seeded simulation runs.

    uv sync --group docs
    uv run python tools/render_gifs.py

Writes docs/media/{demo,chaos}-{light,dark}.gif. Nothing here is hand-drawn
or staged: every frame is a snapshot of the actual cluster (roles, terms,
logs, commit points, in-flight messages, cut links) taken after a simulator
step, and the captions are the demo's own narration. Re-running produces
the same files.
"""

from __future__ import annotations

import io
import math
import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path

import matplotlib
from PIL import Image, ImageDraw, ImageFont

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import demo  # noqa: E402
from raftlab import Cluster, Role  # noqa: E402
from raftlab.chaos import Chaos  # noqa: E402
from raftlab.messages import AppendEntriesReq, AppendEntriesResp, RequestVoteReq  # noqa: E402

OUT = ROOT / "docs" / "media"
W, H = 960, 600
SS = 2  # supersampling factor for antialiased shapes
FONT_DIR = Path(matplotlib.get_data_path()) / "fonts" / "ttf"

# Palette: the dataviz reference palette (categorical slots validated for
# CVD separation in both modes), plus its chrome/ink tokens.
THEMES = {
    "light": {
        "surface": "#fcfcfb", "ink": "#0b0b0b", "ink2": "#52514e", "muted": "#898781",
        "hairline": "#e1e0d9", "axis": "#c3c2b7", "leader": "#2a78d6",
        "candidate": "#eda100", "vote": "#eb6834", "append": "#2a78d6",
        "critical": "#d03b3b", "good": "#0ca30c", "on_accent": "#ffffff",
        "cut": "#f1c4c4",
    },
    "dark": {
        "surface": "#1a1a19", "ink": "#ffffff", "ink2": "#c3c2b7", "muted": "#898781",
        "hairline": "#2c2c2a", "axis": "#383835", "leader": "#3987e5",
        "candidate": "#c98500", "vote": "#d95926", "append": "#3987e5",
        "critical": "#d03b3b", "good": "#0ca30c", "on_accent": "#ffffff",
        "cut": "#5a2a29",
    },
}


# --- recording -----------------------------------------------------------------


@dataclass(frozen=True)
class NodeSnap:
    id: int
    role: Role
    term: int
    crashed: bool
    log: tuple[tuple[int, str], ...]
    commit_index: int


@dataclass(frozen=True)
class MsgSnap:
    src: int
    dst: int
    kind: str  # "vote", "append", "heartbeat", "reply"
    sent_at: int
    deliver_at: int


@dataclass(frozen=True)
class Snap:
    now: int
    nodes: tuple[NodeSnap, ...]
    msgs: tuple[MsgSnap, ...]
    blocked: frozenset[tuple[int, int]]
    checks: int


def _kind(msg) -> str:
    if isinstance(msg, RequestVoteReq):
        return "vote"
    if isinstance(msg, AppendEntriesReq):
        return "append" if msg.entries else "heartbeat"
    if isinstance(msg, AppendEntriesResp):
        return "reply"
    return "vote_reply"


class Recorder:
    """A cluster observer: snapshot everything visible after each step."""

    def __init__(self) -> None:
        self.snaps: list[Snap] = []

    def __call__(self, cluster: Cluster) -> None:
        nodes = tuple(
            NodeSnap(
                n.id, n.role, n.current_term, n.id in cluster.crashed,
                tuple((e.term, e.command) for e in n.log.entries()), n.commit_index,
            )
            for n in cluster.nodes.values()
        )
        msgs = tuple(
            MsgSnap(e.src, e.dst, _kind(e.msg), e.sent_at, e.deliver_at)
            for e in cluster.network.pending()
        )
        checks = cluster.checkers[0].steps if cluster.checkers else 0
        snap = Snap(cluster.now, nodes, msgs, cluster.network.blocked_links, checks)
        if self.snaps and self.snaps[-1].now == snap.now:
            self.snaps[-1] = snap  # keep the latest view of a tick
        else:
            self.snaps.append(snap)


# --- drawing -------------------------------------------------------------------


_fonts: dict[tuple[int, bool, bool], ImageFont.FreeTypeFont] = {}


def font(size: int, bold: bool = False, mono: bool = False) -> ImageFont.FreeTypeFont:
    key = (size, bold, mono)
    if key not in _fonts:
        name = "DejaVuSans" + ("Mono" if mono else "") + ("-Bold" if bold else "") + ".ttf"
        _fonts[key] = ImageFont.truetype(str(FONT_DIR / name), size * SS)
    return _fonts[key]


class Canvas:
    """ImageDraw wrapper in logical pixels (drawn at SS x, downsampled once)."""

    def __init__(self, theme: dict[str, str]) -> None:
        self.t = theme
        self.img = Image.new("RGB", (W * SS, H * SS), theme["surface"])
        self.d = ImageDraw.Draw(self.img)

    def text(self, xy, s, size=14, fill="ink", bold=False, mono=False, anchor="la"):
        self.d.text((xy[0] * SS, xy[1] * SS), s, font=font(size, bold, mono),
                    fill=self.t.get(fill, fill), anchor=anchor)

    def text_width(self, s, size=14, bold=False, mono=False) -> float:
        return self.d.textlength(s, font=font(size, bold, mono)) / SS

    def circle(self, c, r, fill=None, outline=None, width=1):
        x, y = c
        self.d.ellipse(
            [(x - r) * SS, (y - r) * SS, (x + r) * SS, (y + r) * SS],
            fill=self.t.get(fill, fill) if fill else None,
            outline=self.t.get(outline, outline) if outline else None,
            width=int(width * SS),
        )

    def line(self, a, b, fill="hairline", width=1):
        self.d.line([a[0] * SS, a[1] * SS, b[0] * SS, b[1] * SS],
                    fill=self.t.get(fill, fill), width=int(width * SS))

    def rect(self, box, fill=None, outline=None, width=1, radius=4):
        x0, y0, x1, y1 = box
        self.d.rounded_rectangle(
            [x0 * SS, y0 * SS, x1 * SS, y1 * SS], radius=radius * SS,
            fill=self.t.get(fill, fill) if fill else None,
            outline=self.t.get(outline, outline) if outline else None,
            width=int(width * SS),
        )

    def finish(self) -> Image.Image:
        return self.img.resize((W, H), Image.LANCZOS)


CENTER, RADIUS, NODE_R = (235, 300), 160, 40


def node_xy(i: int, n: int) -> tuple[float, float]:
    a = math.radians(-90 + 360 * i / n)
    return CENTER[0] + RADIUS * math.cos(a), CENTER[1] + RADIUS * math.sin(a)


def short(command: str) -> str:
    if command == "NOOP":
        return "noop"
    return re.sub(r"=\d+\.", "=", command.removeprefix("SET "))  # chaos values are seed.tick


def draw_frame(theme, title, snap: Snap, frac: float, captions: list[str], footer: str | None) -> Image.Image:
    cv = Canvas(theme)
    n = len(snap.nodes)
    pos = [node_xy(i, n) for i in range(n)]

    # Header.
    cv.text((28, 22), "RaftLab", size=22, bold=True)
    cv.text((28 + cv.text_width("RaftLab", 22, bold=True) + 12, 29), title, size=15, fill="ink2")
    cv.text((W - 28, 22), f"t = {snap.now:03d}", size=22, bold=True, mono=True, anchor="ra")
    cv.line((28, 64), (W - 28, 64), fill="hairline")

    # Network mesh: a hairline per link; cut links are removed and marked.
    for a in range(n):
        for b in range(a + 1, n):
            cut = (a, b) in snap.blocked or (b, a) in snap.blocked
            if not cut:
                cv.line(pos[a], pos[b], fill="hairline", width=1.5)
    for a in range(n):
        for b in range(a + 1, n):
            if (a, b) in snap.blocked or (b, a) in snap.blocked:
                cv.line(pos[a], pos[b], fill="cut", width=1.5)
                mx, my = (pos[a][0] + pos[b][0]) / 2, (pos[a][1] + pos[b][1]) / 2
                cv.circle((mx, my), 9, fill="surface", outline="critical", width=1.5)
                cv.text((mx, my + 1), "✗", size=11, fill="critical", bold=True, anchor="mm")

    # Messages in flight, interpolated along their link.
    for m in snap.msgs:
        span = max(1, m.deliver_at - m.sent_at)
        p = (snap.now + frac - m.sent_at) / span
        if not 0 <= p <= 1:
            continue
        (x0, y0), (x1, y1) = pos[m.src], pos[m.dst]
        # Start and end on the node rims, not their centres.
        dx, dy = x1 - x0, y1 - y0
        dist = math.hypot(dx, dy) or 1
        ux, uy = dx / dist, dy / dist
        sx, sy = x0 + ux * NODE_R, y0 + uy * NODE_R
        ex, ey = x1 - ux * NODE_R, y1 - uy * NODE_R
        c = (sx + (ex - sx) * p, sy + (ey - sy) * p)
        if m.kind == "vote":
            cv.circle(c, 6, fill="vote")
        elif m.kind == "append":
            cv.circle(c, 7, fill="append")
        elif m.kind == "heartbeat":
            cv.circle(c, 4, fill="append")
        elif m.kind == "vote_reply":
            cv.circle(c, 4, outline="vote", width=2)
        else:
            cv.circle(c, 4, outline="append", width=2)

    # Nodes.
    for node, c in zip(snap.nodes, pos):
        if node.crashed:
            cv.circle(c, NODE_R, fill="surface", outline="critical", width=3)
            label, label_fill, name_fill = "CRASHED", "critical", "muted"
        elif node.role is Role.LEADER:
            cv.circle(c, NODE_R, fill="leader")
            label, label_fill, name_fill = "LEADER", "leader", "on_accent"
        elif node.role is Role.CANDIDATE:
            cv.circle(c, NODE_R, fill="surface", outline="candidate", width=4)
            label, label_fill, name_fill = "candidate", "candidate", "ink"
        else:
            cv.circle(c, NODE_R, fill="surface", outline="axis", width=2.5)
            label, label_fill, name_fill = "follower", "muted", "ink"
        cv.text((c[0], c[1] - 8), f"node{node.id}", size=14, bold=True, fill=name_fill, anchor="mm")
        cv.text((c[0], c[1] + 11), f"term {node.term}", size=12,
                fill="on_accent" if node.role is Role.LEADER and not node.crashed else "ink2",
                anchor="mm")
        cv.text((c[0], c[1] + NODE_R + 13), label, size=12, bold=True, fill=label_fill, anchor="mm")

    # Logs panel.
    lx, ly = 470, 92
    cv.text((lx, ly), "Replicated logs", size=15, bold=True)
    cv.text((W - 28, ly + 2), "index →", size=12, fill="muted", anchor="ra")
    bw, bh, gap = 56, 30, 6
    for row, node in enumerate(snap.nodes):
        y = ly + 34 + row * 52
        cv.text((lx, y + bh / 2), f"node{node.id}", size=13, bold=True,
                fill="muted" if node.crashed else "ink", anchor="lm")
        entries = list(enumerate(node.log, start=1))
        fits = int((W - 28 - (lx + 70) + gap) // (bw + gap))
        if len(entries) > fits:
            fits = int((W - 28 - (lx + 70 + 34) + gap) // (bw + gap))
        hidden = max(0, len(entries) - fits)
        x = lx + 70
        if hidden:
            cv.text((x, y + bh / 2), f"+{hidden}", size=12, fill="muted", anchor="lm")
            x += 34
        for index, (term, command) in entries[hidden:]:
            committed = index <= node.commit_index
            box = (x, y, x + bw, y + bh)
            if committed:
                cv.rect(box, fill="leader", radius=5)
                fg = "on_accent"
            else:
                cv.rect(box, fill="surface", outline="leader", width=2, radius=5)
                fg = "ink"
            cv.text((x + bw / 2, y + 12), short(command), size=11, bold=True, fill=fg, anchor="mm")
            cv.text((x + bw / 2, y + 24), f"t{term}", size=9, fill=fg, anchor="mm")
            x += bw + gap
        if not entries:
            cv.text((x, y + bh / 2), "empty", size=12, fill="muted", anchor="lm")

    # Legend.
    gy = ly + 34 + len(snap.nodes) * 52 + 4
    lgx = lx
    cv.rect((lgx, gy, lgx + 16, gy + 12), fill="leader", radius=3)
    cv.text((lgx + 22, gy + 6), "committed", size=12, fill="ink2", anchor="lm")
    lgx += 22 + cv.text_width("committed", 12) + 18
    cv.rect((lgx, gy, lgx + 16, gy + 12), fill="surface", outline="leader", width=2, radius=3)
    cv.text((lgx + 22, gy + 6), "not committed", size=12, fill="ink2", anchor="lm")
    lgx += 22 + cv.text_width("not committed", 12) + 26
    cv.circle((lgx + 6, gy + 6), 6, fill="append")
    cv.text((lgx + 16, gy + 6), "AppendEntries", size=12, fill="ink2", anchor="lm")
    lgx += 16 + cv.text_width("AppendEntries", 12) + 18
    cv.circle((lgx + 6, gy + 6), 6, fill="vote")
    cv.text((lgx + 16, gy + 6), "RequestVote", size=12, fill="ink2", anchor="lm")

    # Narration.
    cv.line((28, 492), (W - 28, 492), fill="hairline")
    shown = captions[-3:]
    for i, line in enumerate(shown):
        newest = i == len(shown) - 1
        size = 15 if newest else 13
        while cv.text_width(line, size, bold=newest) > W - 56 and len(line) > 4:
            line = line[:-2].rstrip() + "…"
        cv.text((28, 506 + i * 24), line, size=size, bold=newest,
                fill="ink" if newest else "muted")
    if footer:
        cv.text((W - 28, H - 14), footer, size=12, fill="good", bold=True, anchor="rs")
    return cv.finish()


# --- assembling ----------------------------------------------------------------


@dataclass
class Clip:
    title: str
    snaps: list[Snap]
    captions: dict[int, list[str]]  # tick -> caption lines that appear at it
    sub: int  # frames per tick
    ms: int  # duration of each frame
    hold_ms: int  # extra time on frames where a caption appears
    footer: str  # format string with {checks}


def render(clip: Clip, theme_name: str, path: Path) -> None:
    theme = THEMES[theme_name]
    frames: list[Image.Image] = []
    durations: list[int] = []
    shown: list[str] = []
    for snap in clip.snaps:
        new = clip.captions.get(snap.now, [])
        for line in new:
            shown.append(line)
            frames.append(draw_frame(theme, clip.title, snap, 0.0, shown, clip.footer.format(checks=snap.checks)))
            durations.append(clip.hold_ms)
        for k in range(clip.sub):
            frac = (k + 1) / clip.sub
            frames.append(draw_frame(theme, clip.title, snap, frac, shown, clip.footer.format(checks=snap.checks)))
            durations.append(clip.ms)
    durations[-1] = 4000  # rest on the final state before looping

    # One shared palette for every frame, so colours don't shimmer.
    picks = frames[:: max(1, len(frames) // 12)]
    sample = Image.new("RGB", (W, H * len(picks) + 40), theme["surface"])
    for i, f in enumerate(picks):
        sample.paste(f, (0, H * i))
    swatch = ImageDraw.Draw(sample)
    colours = sorted(set(theme.values()))
    for i, c in enumerate(colours):  # guarantee every token survives exactly
        swatch.rectangle([i * 40, H * len(picks), i * 40 + 39, H * len(picks) + 39], fill=c)
    palette = sample.quantize(colors=255, method=Image.Quantize.MEDIANCUT)
    quantized = [f.quantize(palette=palette, dither=Image.Dither.NONE) for f in frames]
    path.parent.mkdir(parents=True, exist_ok=True)
    quantized[0].save(path, save_all=True, append_images=quantized[1:], duration=durations,
                      loop=0, optimize=True, disposal=1)
    print(f"{path.relative_to(ROOT)}: {len(frames)} frames, {os.path.getsize(path) / 1e6:.2f} MB")


def demo_clip() -> Clip:
    rec = Recorder()
    out = io.StringIO()
    demo.run_demo(demo.SEED, out, observers=[rec])
    captions: dict[int, list[str]] = {}
    for line in out.getvalue().splitlines():
        m = re.match(r"t=(\d+)\s+(.*)", line)
        captions.setdefault(int(m[1]), []).append(line)
    return Clip("split brain, and why it's harmless", rec.snaps, captions,
                sub=3, ms=90, hold_ms=1400,
                footer="✓ I1–I5 checked after every step ({checks})")


CHAOS_SEED = 47  # 30% loss, partitions, isolations, crashes, log conflicts


def chaos_clip() -> Clip:
    chaos = Chaos(CHAOS_SEED)
    loss = chaos.cluster.network.drop_probability
    rec = Recorder()
    rec(chaos.cluster)
    chaos.cluster.checkers.append(rec)
    chaos.run(160)
    chaos.recover()

    narrator = demo.Narrator(chaos.cluster, io.StringIO())
    captions: dict[int, list[str]] = {}
    for e in chaos.cluster.trace:
        text = narrator.render(e.node, e.text)
        if text is None and e.node is not None and e.text in ("CRASHED",):
            text = f"node{e.node} CRASHED"
        elif text is None and e.node is not None and e.text.startswith("restarted"):
            text = f"node{e.node} restarted (term, vote and log survive)"
        elif text is None and e.node is None and e.text.startswith(("ISOLATE", "REJOIN")):
            text = "⚡ " + e.text
        if text and "discarded" in text:
            head, _, dropped = text.partition("truncated, ")
            count = dropped.removesuffix(" discarded").count(",") + 1
            text = f"{head}truncated, {count} uncommitted entr{'y' if count == 1 else 'ies'} discarded"
        if text:
            captions.setdefault(e.now, []).append(f"t={e.now:03d}  {text}")
    last = chaos.cluster.now
    captions.setdefault(last, []).append(f"t={last:03d}  ✓ healed: every node holds the same log, nothing committed was lost")
    return Clip(f"chaos: crashes, partitions, {loss:.0%} message loss (seed {CHAOS_SEED})", rec.snaps, captions,
                sub=2, ms=70, hold_ms=500,
                footer="✓ I1–I5 held after every one of {checks} steps")


def main() -> None:
    for name, clip in (("demo", demo_clip()), ("chaos", chaos_clip())):
        for theme in ("light", "dark"):
            render(clip, theme, OUT / f"{name}-{theme}.gif")


if __name__ == "__main__":
    main()
