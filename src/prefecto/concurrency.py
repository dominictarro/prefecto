"""
Tools to improve Prefect concurrently.

"""
from typing import Any, TypeVar

from prefect.futures import PrefectFuture
from prefect.tasks import Task
from prefect.utilities.callables import get_call_parameters
from typing_extensions import ParamSpec

from . import logging, states

T = TypeVar("T")  # Generic type var for capturing the inner return type of async funcs
R = TypeVar("R")  # The return type of the user's function
P = ParamSpec("P")  # The parameters of the task


class BatchTask:
    """Wraps a `Task` to perform `Task.map` in batches."""

    def __init__(self, task: Task[P, R], size: int):
        """Create a `BatchTask` to wrap a `Task` and perform `Task.map` in batches.

        Parameters
        ----------
        task : Task[P, R]
            The task to wrap.
        size : int
            The size of the batches to perform `Task.map` on.

        Examples
        --------
        >>> from prefect import task
        >>> from prefecto.concurrency import BatchTask
        >>> @task
        ... def add(a, b):
        ...     return a + b
        >>> batch_add = BatchTask(add, 3)
        >>> batch_add.map([1,2,3,4,5], [2,3,4,5,6])
        [
            PrefectFuture<Success: 3>,
            PrefectFuture<Success: 5>,
            PrefectFuture<Success: 7>,
            PrefectFuture<Success: 9>,
            PrefectFuture<Success: 11>
        ]
        """
        self.task: Task = task
        self.size: int = size

    def _make_batches(self, **params) -> list[dict[str, list[Any]]]:
        """Create batches of arguments to pass to the `Task.map` calls.

        Parameters
        ----------
        **params
            Keyword arguments where each value is an iterable of equal length. Should
            be at least one keyword argument.

        Returns
        -------
        list[dict[str, list[Any]]]
            A list of dictionaries where each dictionary has the same keys as the
            provided keyword arguments. The values of the dictionaries are lists with
            lengths no greater than `BatchTask.size`.

        Examples
        --------

        >>> BatchTask(task, 3)._make_batches(a=[1,2,3,4,5], b=[2,3,4,5,6])
        [
            {"a": [1,2,3], "b": [2,3,4]},
            {"a": [4,5], "b": [4,5,6]}
        ]
        """
        parameters = sorted(params.keys())
        if len(parameters) == 0:
            raise ValueError("Must provide at least one iterable.")

        # Validate all are iterables
        for k in parameters:
            if not hasattr(params[k], "__iter__"):
                raise ValueError(f"Expected '{k}' to be an iterable.")

        length = len(params[parameters[0]])

        # Assure all of equal length
        if len(parameters) > 1:
            for k in parameters[1:]:
                if not len(params[k]) == length:
                    raise ValueError(
                        f"Expected all iterables to be of length {length} like "
                        f"'{parameters[0]}'. '{k}' is length {len(params[k])}."
                    )

        batches = []

        for i in range(length // self.size):
            batch = {p: [] for p in parameters}
            for p in parameters:
                batch[p] = params[p][i * self.size : (i + 1) * self.size]
            batches.append(batch)

        # Add the remainder if there is one
        if length % self.size != 0:
            batch = {p: [] for p in parameters}
            for p in parameters:
                batch[p] = params[p][(i + 1) * self.size :]
            batches.append(batch)

        return batches

    def map(self, *args, **kwds) -> list[PrefectFuture]:
        """Perform a `Task.map` operation in batches of the keyword arguments. The
        arguments must be iterables of equal length.

        Parameters
        ----------
        *args
            Positional arguments to pass to the task.
        **kwds
            Keyword arguments to pass to the task.

        Returns
        -------
        list[PrefectFuture]
            A list of futures for each batch.
        """
        parameters = get_call_parameters(self.task.fn, args, kwds, apply_defaults=False)
        batches = self._make_batches(**parameters)

        return self._map(batches)

    def _map(self, batches: list[dict[str, list[Any]]]) -> list[PrefectFuture]:
        """Applies `Task.map` to each batch.

        Args:
            batches (list[dict[str, list[Any]]]): _description_

        Returns:
            list[PrefectFuture]: _description_
        """
        logger = logging.get_prefect_or_default_logger()
        results: list[PrefectFuture] = []
        for i, batch in enumerate(batches[:-1]):
            logger.debug(f"Mapping {self.task.name} batch {i+1} of {len(batches)}.")
            # Map the batch
            futures = self.task.map(**batch)
            results.extend(futures)
            # Poll futures to ensure they are not active.
            is_processing: bool = False
            while is_processing:
                for f in futures:
                    if not states.is_terminal(f.get_state()):
                        # If any future is still processing, break and poll again.
                        is_processing = True
                        break
                else:
                    is_processing = False

        # Map the last batch
        logger.debug(
            f"Mapping {self.task.name} batch {len(batches)} of {len(batches)}."
        )
        results.extend(self.task.map(**batches[-1]))
        return results
