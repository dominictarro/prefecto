"""
Tests the `s3` testing fixtures module.
"""
import tempfile
from pathlib import Path
from typing import Any

import pytest

from prefecto.testing.s3 import mock_bucket


@pytest.mark.parametrize(
    "files, bucket_name, keys, export_path, expected_paths, unexpected_paths",
    [
        # no export
        [
            ["test-key-1", "subfolder/test-key-2"],
            "test-bucket",
            ["test-key-1", "subfolder/test-key-2"],
            None,
            [],
            ["test-bucket/test-key-1", "test-bucket/subfolder/test-key-2"],
        ],
        # export all
        [
            ["test-key-1", "subfolder/test-key-2"],
            "test-bucket-2",
            None,
            "tempdir",
            ["test-bucket-2/test-key-1", "test-bucket-2/subfolder/test-key-2"],
            [],
        ],
        # export select
        [
            ["test-key-1", "subfolder/test-key-2"],
            "test-bucket-3",
            ["test-key-1"],
            "tempdir",
            ["test-bucket-3/test-key-1"],
            ["test-bucket-3/subfolder/test-key-2"],
        ],
    ],
)
def test_mock_bucket_export(
    files: list[str],
    bucket_name: str,
    keys: list[str] | None,
    export_path: Any | None,
    expected_paths: list[str],
    unexpected_paths: list[str],
):
    """Tests the `mock_bucket` export behavior.

    Args:
        files (list[str]): The files to put in the bucket.
        bucket_name (str): The name of the bucket.
        keys (list[str]): The keys to export.
        export_path (str | Path | None): The path to export the bucket contents to.
        expected_paths (list[str]): The expected paths of the exported files.
        unexpected_paths (list[str]): The unexpected paths of the exported files.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)

        if export_path is not None:
            export_path = tmpdir

        with mock_bucket(bucket_name, export_path=export_path, keys=keys) as bucket:
            for file in files:
                bucket.put_object(Key=file, Body=b"test value")

        # check that the expected files were exported
        for path in expected_paths:
            path = tmpdir / path
            assert path.is_file()

        # check that unexpected files were not exported
        for path in unexpected_paths:
            path = tmpdir / path
            assert not path.is_file()
