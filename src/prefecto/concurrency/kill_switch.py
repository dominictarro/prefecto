"""
Kill switch logic classes for stopping the execution of a
[`BatchTask`][src.prefecto.concurrency.batch_task.BatchTask].

"""

import abc

from prefect.states import State


class KillSwitchError(Exception):
    """Error raised when a kill switch is activated."""

    def __init__(self, message: str, ks: "KillSwitch"):
        super().__init__(message)
        self.ks = ks


class KillSwitch(abc.ABC):
    """Abstract base class for a kill switch.

    Each invocation of the `should_flip_switch` method should advance the state of the
    kill switch and return if the kill switch should be activated. The
    `raise_if_triggered` method should raise a `KillSwitchError` if the kill switch has
    been activated.

    ```python

    class AnyFailedSwitch(KillSwitch):
        def should_flip_switch(self, state: State) -> bool:
            return state.is_failed() or state.is_crashed()

        def raise_if_triggered(self, state: State):
            if self.should_flip_switch(state):
                raise KillSwitchError("Failed task detected.", self)
    ```
    """

    @abc.abstractmethod
    def should_flip_switch(self, state: State) -> bool:
        """Check if this state should flip the kill switch.

        Returns:
            `True` if the kill switch should be activated, `False` otherwise.

        """

    @abc.abstractmethod
    def raise_if_triggered(self, state: State):
        """Check a state and raise a `KillSwitchError` if the kill switch has been activated.

        Raises:
            KillSwitchError: If the kill switch has been activated.

        """


class AnyFailedSwitch(KillSwitch):
    """A kill switch that activates if any task fails."""

    def should_flip_switch(self, state: State) -> bool:
        """Check if the state is failed or crashed."""
        return state.is_failed() or state.is_crashed()

    def raise_if_triggered(self, state: State):
        """Raise a `KillSwitchError` if the state is failed or crashed."""
        if self.should_flip_switch(state):
            raise KillSwitchError("Failed task detected.", self)


class CountSwitch(KillSwitch):
    """A kill switch that activates after a certain number of tasks fail.

    Args:
        count (int): The number of failed or crashed states that should trigger the kill
            switch.

    """

    def __init__(self, max_count: int):
        self.max_count = max_count
        self._current_count = 0

    def should_flip_switch(self, state: State) -> bool:
        """Increment the count if the state is failed or crashed and return if the count exceeds
        the maximum.
        """
        if state.is_failed() or state.is_crashed():
            self._current_count += 1
        return self._current_count >= self.max_count

    def raise_if_triggered(self, state: State):
        """Raise a `KillSwitchError` if the count exceeds the maximum."""
        if self.should_flip_switch(state):
            raise KillSwitchError(f"{self.max_count} failed tasks detected.", self)


class RateSwitch(KillSwitch):
    """A kill switch that activates after the failure rate exceeds a certain threshold.
    Requires a minimum number of states to sample.

    Args:
        min_sample (int): The minimum number of states to sample.
        max_fail_rate (float): The maximum frequency of failed or crashed states.

    """

    def __init__(self, min_sample: int, max_fail_rate: float):
        self.min_sample = min_sample
        self.max_fail_rate = max_fail_rate
        self._current_count = 0
        self._failed_count = 0

    def should_flip_switch(self, state: State) -> bool:
        """Increment the count if the state is failed or crashed and return if the failure rate
        equals or exceeds the max rate.
        """
        self._current_count += 1
        if state.is_failed() or state.is_crashed():
            self._failed_count += 1
        return (
            self._current_count >= self.min_sample
            and self._failed_count / self._current_count >= self.max_fail_rate
        )

    def raise_if_triggered(self, state: State):
        """Raise a `KillSwitchError` if the failure rate equals or exceeds the maximum rate."""
        if self.should_flip_switch(state):
            raise KillSwitchError(
                f"Failure rate exceeded {self.max_fail_rate} after {self.min_sample} samples.",
                self,
            )
