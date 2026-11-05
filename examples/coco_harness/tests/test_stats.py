from __future__ import annotations

from coco_harness.tools.stats import Bucket, bottom_k, format_range, widen_count


def test_widen_count_log2_buckets() -> None:
    assert widen_count(0) == (0, 0)
    assert widen_count(1) == (1, 2)
    assert widen_count(5) == (4, 8)
    assert widen_count(1024) == (1024, 2048)
    assert widen_count(2047) == (1024, 2048)


def test_widen_count_hysteresis_keeps_previous_bucket_near_a_boundary() -> None:
    prev = Bucket(1024, 2048)
    assert widen_count(1000, prev) == prev  # within 10% below the lower bound
    assert widen_count(2100, prev) == prev  # within 10% above the upper bound
    assert widen_count(800, prev) == (512, 1024)  # far enough: move
    assert widen_count(5, Bucket(4, 8)) == (4, 8)


def test_format_range() -> None:
    assert format_range(Bucket(0, 0)) == "0"
    assert format_range(Bucket(4, 8)) == "4–8"
    assert format_range(Bucket(1024, 2048)) == "1k–2k"  # log₂ buckets render with k/M
    assert format_range(Bucket(4096, 8192)) == "4k–8k"
    assert format_range(Bucket(1 << 20, 1 << 21)) == "1M–2M"
    assert format_range(Bucket(1000, 2000)) == "1k–2k"
    assert format_range(Bucket(512, 1024)) == "512–1k"


def test_bottom_k_sample_barely_moves_when_the_population_grows() -> None:
    keys = [f"docs/{i}.md" for i in range(200)]
    before = set(bottom_k(keys, 5))
    after = set(bottom_k([*keys, "docs/new.md"], 5))
    assert len(before) == 5
    assert len(before - after) <= 1  # at most one slot changed
    assert bottom_k(keys, 0) == []
    assert bottom_k(keys, 5) == bottom_k(list(reversed(keys)), 5)  # order independent
