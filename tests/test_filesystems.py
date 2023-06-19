"""
Unit tests for the `filesystems` module.

"""
from __future__ import annotations

import pytest
from prefect import task
from prefect.filesystems import LocalFileSystem

from prefecto.filesystems import create_child, task_persistence_subfolder


@pytest.fixture
def fs(basepath: str | None):
    """Returns a `LocalFileSystem`."""
    return LocalFileSystem(basepath=basepath)


@pytest.mark.parametrize(
    "basepath,expected_basepath",
    [
        [None, "child"],
        ["parent_folder", "parent_folder/child"],
    ],
)
def test_create_child(fs: LocalFileSystem, expected_basepath: str | None):
    """Tests `create_child`."""
    child: LocalFileSystem = create_child(fs, "child")
    assert child.basepath.endswith(expected_basepath)


@pytest.mark.parametrize(
    "basepath,path,expected_basepath",
    [
        [None, None, "persisted_task"],
        [None, "persisted_task_custom_folder", "persisted_task_custom_folder"],
        [
            "parent_folder",
            "persisted_task_custom_folder",
            "parent_folder/persisted_task_custom_folder",
        ],
    ],
)
def test_task_persistence_subfolder(
    fs: LocalFileSystem, path: str, expected_basepath: str | None
):
    """Tests `task_persistence_subfolder`."""

    @task_persistence_subfolder(fs, path=path)
    @task(persist_result=True)
    def persisted_task():
        """A dummy task."""
        ...

    assert persisted_task.result_storage.basepath.endswith(expected_basepath)
