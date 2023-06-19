"""
Module for pytest fixtures.
"""
from itertools import repeat
from multiprocessing.pool import ThreadPool
from pathlib import Path
from typing import Callable, Generator

import boto3
import pytest
from _pytest.fixtures import FixtureFunction
from moto import mock_s3


@pytest.fixture
def s3_mock():
    """Fixture to create a mock S3 bucket with `moto`."""
    with mock_s3():
        yield


def _export(obj, path: Path):
    """Exports the S3 Object's contents to the path."""
    key_path: Path = path / obj.key
    key_path.parent.mkdir(exist_ok=True, parents=True)

    with open(key_path, "wb") as f:
        f.write(obj.get()["Body"].read())


def mock_bucket_factory(
    bucket_name: str,
    export_path: str | Path | None = None,
    keys: list[str] | None = None,
    processes: int = 3,
    chunksize: int = 5,
) -> Callable[[], Generator["boto3.resources.factory.s3.Bucket", None, None]]:
    """Creates a factory function to create S3 Bucket mocks. If given an export
    path, the mock bucket will export its contents during teardown.

    Args:
        bucket_name (str): The name of the bucket.
        export_path (str | Path | None, optional): The path to export the bucket contents to.
        keys (list[str] | None, optional): The keys to export. If None, all keys will be exported.
        processes (int, optional): The number of threads to use for exporting. Defaults to 3.
        chunksize (int, optional): The chunksize to use for exporting. Defaults to 5.

    Returns:
        Callable[[], Generator["boto3.resources.factory.s3.Bucket", None, None]]: A factory function

    Examples:

        You can use the factory function to mock a bucket in your tests without exporting its contents:

        ```python
        from prefecto.testing.s3 import mock_bucket_factory

        my_bucket = mock_bucket_factory("my-bucket")

        def test_my_flow(my_bucket):
            # Run your flow
            ...
        ```

        Or you can create a fixture that exports the bucket's contents to a directory:
        Useful for auditing folder structures and file contents after local tests.

        ```python
        from prefecto.testing.s3 import mock_bucket_factory

        my_exported_bucket = mock_bucket_factory("my-bucket", export_path="path/to/export/dir")

        def test_my_flow(my_exported_bucket):
            # Run your flow
            ...
        ```

    """

    def mock_bucket():
        """Creates a mock S3 bucket with `moto`. If given an export path, the mock bucket
        will export its contents during teardown.
        """
        try:
            s3 = boto3.resource("s3")
            bucket = s3.Bucket(bucket_name)
            bucket.create()
            yield bucket
        finally:
            if export_path is not None:

                if keys is None:
                    objects = bucket.objects.all()
                else:
                    objects = (bucket.Object(key) for key in keys)

                # Resolve the export path and create the bucket directory
                bucket_path = Path(export_path) / bucket.name
                bucket_path.mkdir(parents=True, exist_ok=True)

                if processes > 1:
                    # Export the objects
                    with ThreadPool(processes) as pool:
                        pool.starmap(
                            _export,
                            zip(objects, repeat(bucket_path)),
                            chunksize=chunksize,
                        )
                else:
                    map(_export, objects, repeat(bucket_path))

    return mock_bucket
