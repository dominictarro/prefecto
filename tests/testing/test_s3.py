"""
Tests the `s3` testing fixtures module.
"""
import contextlib
import tempfile
from pathlib import Path

import pytest
from _pytest.fixtures import SubRequest

from prefecto.testing.s3 import mock_bucket_factory


@pytest.fixture(scope="module")
def tempdir():
    """Creates a temporary directory."""
    with tempfile.TemporaryDirectory() as tempdir:
        yield Path(tempdir)


@pytest.fixture
def export_bucket_manager(request: SubRequest, tempdir):
    """A fixture that returns a context manager to create a mock S3 bucket and export
    contents upon exit.

    Args:
        request (SubRequest): The pytest request object. Used to access the `param`.
        tempdir: The tempdir fixture.

    `request.param` is a tuple of the following:

    - request.param[0] (str): The name of the bucket (bucket_name).
    - request.param[1] (list[str] | None): The keys to export (keys).
    - request.param[2] (Any): A flag to use None as the export directory. Causes
        no objects to be exported from the bucket. If empty, bucket uses the
        tempdir fixture.

    """
    bucket_name = request.param[0]
    keys = request.param[1]
    export_path = tempdir if len(request.param) < 3 else None

    return contextlib.contextmanager(
        mock_bucket_factory(bucket_name=bucket_name, export_path=export_path, keys=keys)
    )()


@pytest.mark.parametrize(
    ["export_bucket_manager", "expected_paths"],
    [
        # passing export_bucket_manager parameters as a tuple
        [("test-bucket", ["test-key-1", "test-key-2"], None), []],  # no export
        [
            ("test-bucket", None),
            ["test-bucket/test-key-1", "test-bucket/test-key-2"],
        ],  # export all
        [("test-bucket", ["test-key-1"]), ["test-bucket/test-key-1"]],  # export one
        [("test-bucket", ["test-key-2"]), ["test-bucket/test-key-2"]],  # export one
    ],
    indirect=["export_bucket_manager"],
)
def test_mock_bucket_export(export_bucket_manager, expected_paths: list[str], tempdir):
    """Tests the `make_mock_bucket_fixture` fixture."""
    with export_bucket_manager as bucket:
        bucket.put_object(Key="test-key-1", Body=b"test value 1")
        bucket.put_object(Key="test-key-2", Body=b"test value 2")

    for path in expected_paths:
        path = tempdir / path
        assert path.is_file()
