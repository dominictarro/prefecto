"""
Module to improve Prefect concurrency operations.
"""

from __future__ import annotations

from typing import TypeVar

from prefect import unmapped
from prefect.futures import PrefectFuture
from prefect.tasks import Task
from prefect.utilities.callables import get_call_parameters
from typing_extensions import ParamSpec

from prefecto import logging, states
from prefecto.concurrency.kill_switch import KillSwitch

T = TypeVar("T")  # Generic type var for capturing the inner return type of async funcs
R = TypeVar("R")  # The return type of the user's function
P = ParamSpec("P")  # The parameters of the task
P_i = ParamSpec("P_i")  # The type of each parameter

MapArgument = list[P_i] | unmapped[P_i]
Batch = dict[str, MapArgument]


class BatchTask:
    """Wraps a `Task` to perform `Task.map` in batches, reducing the number of
    concurrent tasks, mellowing Prefect API requests, and allowing for faster
    failure detection.

    ```python
    from prefect import flow, task
    from prefecto.concurrency import BatchTask

    @task
    def add(a, b):
        return a + b

    @flow
    def my_flow():
        batch_add = BatchTask(add, 2)
        return batch_add.map([1,2,3,4], [2,3,4,5])

    ```

    The `kill_switch` argument can be used to stop the execution of the task
    after a certain condition is met.

    ```python
    from prefect import flow, task
    from prefecto.concurrency import BatchTask, AnyFailedSwitch

    @task
    def add(a, b):
        return a + b

    @flow
    def my_flow():
        batch_add = BatchTask(add, 2, kill_switch=AnyFailedSwitch())
        # This will error on the first batch and stop the execution.
        return batch_add.map([1,2,3,4], [1,'2',3,4])

    ```

    See [kill switches][src.prefecto.concurrency.kill_switch] for more information.

    """

    def __init__(
        self, task: Task[P, R], size: int, kill_switch: KillSwitch | None = None
    ):
        """Wrap the `task` to be executed in batches of `size`.

        Args:

                task (Task): The task to wrap.
                size (int): The size of the batches to perform `Task.map` on.
                kill_switch (KillSwitch, optional): A kill switch to stop the execution of the task
                    after a certain condition is met.
        """
        self.task: Task = task
        self.size: int = size

        if kill_switch is not None and not isinstance(kill_switch, KillSwitch):
            raise TypeError(
                f"Expected 'kill_switch' to be a subclass of 'KillSwitch', got {type(kill_switch)}."
            )
        self._kill_switch = kill_switch

    def _make_batches(self, **params: MapArgument) -> list[Batch]:
        """Create batches of arguments to pass to the `Task.map` calls.

        Args:
            **params (MapArgument): Keyword arguments where each value is an
            iterable of equal length or an `unmapped` object. Should be at least
            one non-`unmapped` argument.

        Returns:
            (list[dict[str, list[P_i] | unmapped[P_i]]]): A list of dictionaries where each
            dictionary has the same keys as the provided keyword arguments.
            The values of the dictionaries are lists with lengths no greater
            than `BatchTask.size`.

        Examples:

            ```python
            BatchTask(task, 3)._make_batches(a=[1,2,3,4,5], b=[2,3,4,5,6])
            ```
            ```json
            [
                {"a": [1,2,3], "b": [2,3,4]},
                {"a": [4,5], "b": [4,5,6]}
            ]
            ```
        """
        parameters = sorted(params.keys())
        if len(parameters) == 0:
            raise ValueError("Must provide at least one iterable.")

        # Validate all are iterables
        for k in parameters:
            if not hasattr(params[k], "__iter__"):
                raise ValueError(f"Expected '{k}' to be an iterable.")

        # Get the first non-unmapped iterable
        length: int
        for j, name in enumerate(parameters):
            if not isinstance(params[name], unmapped):
                length = len(params[name])
                break
        else:
            # Logically, at least one iterable must be provided
            raise ValueError("Must provide at least one non-unmapped iterable.")

        # Assure all of equal length
        if len(parameters[j:]) > 1:
            for k in parameters[j + 1 :]:
                if not isinstance(params[k], unmapped) and not len(params[k]) == length:
                    raise ValueError(
                        f"Expected all iterables to be of length {length} like "
                        f"'{parameters[j]}'. '{k}' is length {len(params[k])}."
                    )

        batches = []

        i = 0
        for i in range(length // self.size):
            batch = {p: [] for p in parameters}
            for p in parameters:
                if isinstance(params[p], unmapped):
                    # Pass the unmapped argument to be handled by the task
                    batch[p] = params[p]
                else:
                    batch[p] = params[p][i * self.size : (i + 1) * self.size]
            batches.append(batch)

        # Add the remainder if there is one
        if length % self.size != 0:
            batch = {p: [] for p in parameters}
            for p in parameters:
                if isinstance(params[p], unmapped):
                    # Pass the unmapped argument to be handled by the task
                    batch[p] = params[p]
                else:
                    batch[p] = params[p][(length // self.size) * self.size :]
            batches.append(batch)

        return batches

    def map(self, *args: MapArgument, **kwds: MapArgument) -> list[PrefectFuture]:
        """Perform a `Task.map` operation in batches of the keyword arguments. The
        arguments must be iterables of equal length.

        Args:

            *args: Positional arguments to pass to the task.
            **kwds: Keyword arguments to pass to the task.

        Returns:
            A list of futures for each batch.

        Examples:

            ```python
            from prefect import flow, task
            from prefecto.concurrency import BatchTask

            @task
            def add(a, b):
                return a + b

            @flow
            def my_flow():
                batch_add = BatchTask(add, 2)
                return batch_add.map([1,2,3,4], [2,3,4,5])

            print(my_flow())
            ```

            ```log
            $ python my_flow.py
            01:31:51.012 | INFO    | prefect.engine - Created flow run 'beryl-moth' for flow 'test'
            01:31:52.238 | DEBUG   | Flow run 'beryl-moth' - Mapping 'add' batch 1 of 2.
            01:31:52.239 | INFO    | Flow run 'beryl-moth' - Created task run 'add-0' for task 'add'
            01:31:52.240 | INFO    | Flow run 'beryl-moth' - Submitted task run 'add-0' for execution.
            01:31:52.253 | INFO    | Flow run 'beryl-moth' - Created task run 'add-1' for task 'add'
            01:31:52.254 | INFO    | Flow run 'beryl-moth' - Submitted task run 'add-1' for execution.
            01:31:52.259 | DEBUG   | Flow run 'beryl-moth' - Mapping 'add' batch 2 of 2.
            01:31:52.258 | INFO    | Flow run 'beryl-moth' - Created task run 'add-3' for task 'add'
            01:31:52.258 | INFO    | Flow run 'beryl-moth' - Submitted task run 'add-3' for execution.
            01:31:52.260 | INFO    | Flow run 'beryl-moth' - Created task run 'add-2' for task 'add'
            01:31:52.261 | INFO    | Flow run 'beryl-moth' - Submitted task run 'add-2' for execution.
            01:31:52.675 | INFO    | Task run 'add-1' - Finished in state Completed()
            01:31:52.770 | INFO    | Task run 'add-0' - Finished in state Completed()
            01:31:52.885 | INFO    | Task run 'add-2' - Finished in state Completed()
            01:31:53.075 | INFO    | Task run 'add-3' - Finished in state Completed()
            01:31:53.979 | INFO    | Flow run 'beryl-moth' - Finished in state Completed()
            ```

            ```json
            [3, 5, 7, 9]
            ```
        """
        parameters = get_call_parameters(self.task.fn, args, kwds, apply_defaults=False)
        batches = self._make_batches(**parameters)

        return self._map(batches)

    def _map(self, batches: list[Batch]) -> list[PrefectFuture]:
        """Applies `Task.map` to each batch.

        Args:
            batches (list[Batch]): Batches of arguments to pass to
            `Task.map`.

        Returns:
            A list of futures for each batch.
        """
        if len(batches) == 0:
            return []
        logger = logging.get_prefect_or_default_logger()
        results: list[PrefectFuture] = []
        for i, batch in enumerate(batches[:-1]):
            logger.debug(f"Mapping {self.task.name} batch {i+1} of {len(batches)}.")
            # Map the batch
            futures = self.task.map(**batch)
            results.extend(futures)
            # Poll futures to ensure they are not active.
            is_processing: bool = True
            while is_processing:
                # If any future is still processing, loop and poll again.
                # If all futures are terminal, break while loop and continue
                if all(states.is_terminal(f.get_state()) for f in futures):
                    is_processing = False

            if self._kill_switch is not None:
                for f in futures:
                    self._kill_switch.raise_if_triggered(f.get_state())

        # Map the last batch
        logger.debug(
            f"Mapping {self.task.name} batch {len(batches)} of {len(batches)}."
        )
        results.extend(self.task.map(**batches[-1]))
        return results
