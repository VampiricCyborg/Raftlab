"""The demo is deterministic, and the README shows exactly what it prints."""

import io
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import demo  # noqa: E402


def demo_output(seed: int = demo.SEED) -> str:
    out = io.StringIO()
    demo.run_demo(seed, out)
    return out.getvalue()


def test_demo_is_deterministic():
    assert demo_output() == demo_output()


def test_readme_transcript_matches_demo_byte_for_byte():
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    m = re.search(r"<!-- demo:start -->\n```text\n(.*?)```\n<!-- demo:end -->", readme, re.S)
    assert m, "README is missing the demo transcript block"
    assert m[1] == demo_output(), "README transcript is stale: rerun `python demo.py`"


def test_demo_story_holds_for_other_seeds():
    for seed in range(10):
        text = demo_output(seed)
        assert "NOT committed" in text
        assert "steps down to FOLLOWER" in text
        assert "SET x=42 discarded" in text
        assert "agree: x=99" in text
