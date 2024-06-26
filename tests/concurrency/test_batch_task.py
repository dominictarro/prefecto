"""
Unit tests for the `concurrency` module.

"""

from __future__ import annotations

import pytest
from prefect import flow, task, unmapped

from prefecto.concurrency.batch_task import BatchTask


@task
def add(a, b):
    """Add two numbers."""
    return a + b


@task
def add_many(a, b, c, d):
    """Add many numbers."""
    return a + b + c + d


class TestBatchTask:
    """Unit tests for `BatchTask`."""

    def test_make_batches(self):
        """Test `_make_batches`."""
        batches = BatchTask(add, 3)._make_batches(a=[1, 2, 3, 4, 5], b=[2, 3, 4, 5, 6])
        assert batches == [{"a": [1, 2, 3], "b": [2, 3, 4]}, {"a": [4, 5], "b": [5, 6]}]

    def test_make_batches_with_unmapped(self):
        """Test `_make_batches` with one or more unmapped arguments."""
        batches = BatchTask(add, 3)._make_batches(a=unmapped(1), b=[2, 3, 4, 5, 6])
        assert batches == [
            {"a": unmapped(1), "b": [2, 3, 4]},
            {"a": unmapped(1), "b": [5, 6]},
        ]

        batches = BatchTask(add_many, 3)._make_batches(
            a=unmapped(1), b=[2, 3, 4, 5, 6], c=unmapped(0), d=[4, 5, 6, 7, 8]
        )
        assert batches == [
            {"a": unmapped(1), "b": [2, 3, 4], "c": unmapped(0), "d": [4, 5, 6]},
            {"a": unmapped(1), "b": [5, 6], "c": unmapped(0), "d": [7, 8]},
        ]

        with pytest.raises(
            ValueError, match="Must provide at least one non-unmapped iterable."
        ):
            BatchTask(add, 3)._make_batches(a=unmapped(1), b=unmapped(2))

    @pytest.mark.parametrize(
        "a,b,expectation",
        [
            ([1, 2, 3, 4, 5], [2, 3, 4, 5, 6], [3, 5, 7, 9, 11]),
            ([1, 2], [2, 3], [3, 5]),
            ([], [], []),
            (unmapped(1), [2, 3, 4, 5, 6], [3, 4, 5, 6, 7]),
        ],
    )
    def test_map(self, a: list[int], b: list[int], expectation: list[int], harness):
        """Test `BatchTask.map`."""

        @task
        def realize(futures: list[int]):
            """Converts futures to their values."""
            return futures

        @flow
        def test() -> list[int]:
            """Test flow."""
            futures = BatchTask(add, 3).map(a, b)
            return realize(futures)

        result = test()
        assert result == expectation

    def test_map_with_kill_switch(self, harness):
        """Test `BatchTask.map` with a kill switch."""
        from prefecto.concurrency.kill_switch import CountSwitch, KillSwitchError

        @flow
        def test() -> list[int]:
            """Test flow."""
            bt = BatchTask(add, 3, CountSwitch(2))
            bt.map([1, 2, 3, 4, 5, 6, 7, 8, 9], ["x", 1, 1, "y", 1, 1, 1, 1, 1])

        with pytest.raises(KillSwitchError) as exc:
            test()
            assert isinstance(exc.value.ks, CountSwitch)
            assert exc.value.ks._current_count == 2
            assert exc.value.ks._max_count == 2

    def test_map_with_kill_switch_within_batch(self, harness):
        """Test `BatchTask.map` with a kill switch."""
        from prefecto.concurrency.kill_switch import CountSwitch, KillSwitchError

        @flow
        def test() -> list[int]:
            """Test flow."""
            bt = BatchTask(add, 4, CountSwitch(2))
            bt.map([1, 2, 3, 4, 5], ["x", 1, "y", 1, 1])

        with pytest.raises(KillSwitchError) as exc:
            test()
            assert isinstance(exc.value.ks, CountSwitch)
            assert exc.value.ks._current_count == 2
            assert exc.value.ks._max_count == 2
