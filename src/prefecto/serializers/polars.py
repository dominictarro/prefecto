"""
Serializer classes for Prefect.
"""
from __future__ import annotations

from typing import Any, Literal

try:
    import polars as pl
except ImportError:
    raise ImportError(
        "Polars is required for the polars serializer.\nInstall"
        " with `pip install polars`."
    )

from .core import ExtendedSerializer, Method


class Parquet(Method):
    """Method for reading and writing Parquet files."""

    discriminator: str = "polars.parquet"
    __read__ = pl.read_parquet
    __write__ = pl.DataFrame.write_parquet


class CSV(Method):
    """Method for reading and writing CSV files."""

    discriminator: str = "polars.csv"
    default_write_kwargs: dict[str, Any] = {}
    __read__ = pl.read_csv
    __write__ = pl.DataFrame.write_csv


class JSON(Method):
    """Method for reading and writing JSON files."""

    discriminator: str = "polars.json"
    __read__ = pl.read_json
    __write__ = pl.DataFrame.write_json


class NDJSON(Method):
    """Method for reading and writing NDJSON files."""

    discriminator: str = "polars.ndjson"
    __read__ = pl.read_ndjson
    __write__ = pl.DataFrame.write_ndjson


class TSV(Method):
    """Method for reading and writing TSV files."""

    discriminator: str = "polars.tsv"
    default_read_kwargs: dict[str, Any] = {"separator": "\t"}
    default_write_kwargs: dict[str, Any] = {"separator": "\t"}
    __read__ = pl.read_csv
    __write__ = pl.DataFrame.write_csv


class Excel(Method):
    """Method for reading and writing Excel files."""

    discriminator: str = "polars.excel"
    __read__ = pl.read_excel
    __write__ = pl.DataFrame.write_excel


@ExtendedSerializer.register
class PolarsSerializer(ExtendedSerializer):
    """Serializer for `polars.DataFrame` objects.

    Args:
        method (str, optional): The method to use for reading and writing.
            Must be a registered `Method`. Defaults to "polars.tsv".
        read_kwargs (dict[str, Any], optional): Keyword arguments for the read
            method. Overrides default arguments for the method.
        write_kwargs (dict[str, Any], optional): Keyword arguments for the
            write method. Overrides default arguments for the method.

    Examples:
        Simple read and write.

        ```python
        import polars as pl
        from prefecto.serializers.polars import PolarsSerializer
        df = pl.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        blob = PolarsSerializer().dumps(df)
        print(blob)
        df2 = PolarsSerializer().loads(blob)
        assert df2.frame_equal(df)
        ```

        ```text
        True
        ```

        Using a different method.

        ```python
        blob = PolarsSerializer(method="polars.csv").dumps(df)
        df2 = PolarsSerializer(method="polars.csv").loads(blob)
        assert df2.frame_equal(df)
        ```

        ```text
        True
        ```

        Using custom read and write kwargs.

        ```python
        blob = PolarsSerializer(write_kwargs={"use_pyarrow": True}).dumps(df)
        df2 = PolarsSerializer(read_kwargs={"use_pyarrow": True}).loads(blob)
        assert df2.frame_equal(df)
        ```

    """

    type: Literal["polars"] = "polars"
    method = "polars.parquet"
