"""
Tools to extend Prefect file system behavior.
"""
from __future__ import annotations

from pathlib import Path
from typing import TypeVar

from prefect import Task
from prefect.filesystems import ReadableFileSystem, WritableFileSystem
from typing_extensions import ParamSpec

from .logging import get_prefect_or_default_logger

FileSystem = ReadableFileSystem | WritableFileSystem

P = ParamSpec("P")
R = TypeVar("R")


def create_child(
    fs: FileSystem, path: str | Path | None = None, suffix: str | None = None
) -> FileSystem:
    """
    Create a child file system from a parent file system. If `path` is given, the
    child file system will have its `basepath` set to the resolved path. If `suffix`
    is given, it will be appended to the child block's `_block_document_name`.

    The FileSystem class must have

    1. a `basepath` or `bucket_folder` attribute and __init__ parameter
    1. a `_resolve_path` method to resolve the path.

    `path`. It must also have a `_resolve_path` method to resolve the path.

    Args:
        fs (FileSystem): the parent file system
        path (str | Path): the path to the child file system
        suffix (str, optional): the suffix to append to the child block's
        `_block_document_name`. Defaults to None.

    Returns:
        FileSystem: the child file system with a resolved `basepath`

    Examples:

        Basic usage:

        ```python
        from prefect.filesystems import LocalFileSystem
        from prefecto.filesystems import create_child

        fs = LocalFileSystem(basepath="base_folder")

        child = create_child(fs, path="child_folder")

        print(child.basepath)
        ```
        ```
        base_folder/child_folder
        ```

        With `prefect-aws`:

        ```python
        from prefect_aws import S3Bucket
        from prefecto.filesystems import create_child

        fs = S3Bucket(bucket_folder="base_folder")

        child = create_child(fs, path="child_folder")

        print(child.bucket_folder)
        ```
        ```
        base_folder/child_folder
        ```

    """
    to_update = {}
    if suffix is not None:
        # or "" in case the block is not registered with a name
        to_update["_block_document_name"] = (fs._block_document_name or "") + suffix
    else:
        to_update["_is_anonymous"] = True

    if path is not None:
        # Get the variable to set the basepath from
        for path_var in ["bucket_folder", "basepath"]:
            if hasattr(fs, path_var):
                break
        else:
            raise ValueError(f"No variable to resolve path from for file system: {fs}")

        # Get the path resolution function
        for resolver_func_name in ["_resolve_path"]:
            if hasattr(fs, resolver_func_name):
                break
        else:
            raise ValueError(f"No path resolution function for file system: {fs}")

        # Resolve the path
        path: Path | str = getattr(fs, resolver_func_name)(path)
        to_update[path_var] = str(path)

    return fs.copy(
        update=to_update,
        exclude={"_block_document_id"},
    )


def task_persistence_subfolder(
    fs: FileSystem,
    path: str | None = None,
    suffix: str | None = None,
):
    """Decorator to set the `task.result_storage` attribute to a child file system of
    `fs`.

    *This method does not alter the `result_serializer` or `persist_result` attributes.*

    Args:
        fs (FileSystem): the file system to create a child file system from
        path (str, optional): the path to the child file system. Defaults to None.
        suffix (str, optional): the suffix to append to the child block's `_block_document_name`. Defaults to None.

    Returns:
        Callable: the decorator to apply to a task

    Examples:

        Basic usage:

        ```python
        from prefect import task
        from prefect.filesystems import LocalFileSystem
        from prefecto.filesystems import task_persistence_subfolder

        fs = LocalFileSystem(basepath="base_folder/")

        @task_persistence_subfolder(fs)
        @task(persist_result=True)
        def persisted_task():
            ...

        print(persisted_task.result_storage.basepath)
        ```
        ```
        base_folder/persisted_task
        ```

        Custom path:

        ```python
        from prefect import task
        from prefect.filesystems import LocalFileSystem
        from prefecto.filesystems import task_persistence_subfolder

        fs = LocalFileSystem(basepath="base_folder/")

        @task_persistence_subfolder(fs, path="custom_persisted_task")
        @task(persist_result=True)
        def persisted_task():
            ...

        print(persisted_task.result_storage.basepath)
        ```
        ```
        base_folder/custom_persisted_task
        ```

        With `prefect-aws`:

        ```python
        from prefect import task
        from prefect_aws import S3Bucket
        from symph.prefect_utils.blocks import task_persistence_subfolder

        bucket: S3Bucket = S3Bucket(bucket_folder="base_folder/")

        @task_persistence_subfolder(bucket)
        @task(persist_result=True)
        def persisted_task(data: dict) -> dict:
            '''A task whose return value is persisted in a subfolder of `bucket`.
            '''
            ...

        print(persisted_task.result_storage.basepath)
        ```
        ```
        base_folder/persisted_task
        ```
    """

    def decorator(task: Task[P, R]) -> Task[P, R]:
        """Decorates the task to assign a new `task.result_storage`."""
        logger = get_prefect_or_default_logger()
        task.result_storage = create_child(
            fs,
            path or task.fn.__name__,
            # Replace to conform with Prefect's naming conventions
            suffix or "-" + task.fn.__name__.replace("_", "-"),
        )
        logger.debug(
            "Task %s is being persisted to storage %s",
            task.name,
            task.result_storage,
        )
        return task

    return decorator
