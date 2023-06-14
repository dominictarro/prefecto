"""
Serializer classes for Prefect.
"""
from typing import Any, Literal

try:
    import polars as pl
except ImportError:
    raise ImportError(
        "Polars is required for the polars serializer.\nInstall"
        " with `pip install polars`."
    )

from .core import ExtendedSerializer, Method, method


@method
class Parquet(Method):
    """Method for reading and writing Parquet files."""

    discriminator: str = "polars.parquet"
    __read__ = pl.read_parquet
    __write__ = pl.DataFrame.write_parquet


@method
class CSV(Method):
    """Method for reading and writing CSV files."""

    discriminator: str = "polars.csv"
    default_write_kwargs: dict[str, Any] = {}
    __read__ = pl.read_csv
    __write__ = pl.DataFrame.write_csv


@method
class JSON(Method):
    """Method for reading and writing JSON files."""

    discriminator: str = "polars.json"
    __read__ = pl.read_json
    __write__ = pl.DataFrame.write_json


@method
class NDJSON(Method):
    """Method for reading and writing NDJSON files."""

    discriminator: str = "polars.ndjson"
    __read__ = pl.read_ndjson
    __write__ = pl.DataFrame.write_ndjson


@method
class TSV(Method):
    """Method for reading and writing TSV files."""

    discriminator: str = "polars.tsv"
    default_read_kwargs: dict[str, Any] = {"separator": "\t"}
    default_write_kwargs: dict[str, Any] = {"separator": "\t"}
    __read__ = pl.read_csv
    __write__ = pl.DataFrame.write_csv


@method
class Excel(Method):
    """Method for reading and writing Excel files."""

    discriminator: str = "polars.excel"
    __read__ = pl.read_excel
    __write__ = pl.DataFrame.write_excel


@ExtendedSerializer.register
class PolarsSerializer(ExtendedSerializer):
    """Serializer for `polars.DataFrame` objects.

    Parameters
    ----------
    method : str
        The method to use for reading and writing. Must be a registered
        `Method`. Defaults to "polars.tsv".
    read_kwargs : dict[str, Any], optional
        Keyword arguments for the read method. Overrides default arguments for
        the method.
    write_kwargs : dict[str, Any], optional
        Keyword arguments for the write method. Overrides default arguments
        for the method.

    Examples
    --------
    Simple read and write.
    >>> import polars as pl
    >>> from prefecto.serializers.polars import PolarsSerializer
    >>> df = pl.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    >>> blob = PolarsSerializer().dumps(df)
    >>> blob
    b'a\\tb\\n1\\t4\\n2\\t5\\n3\\t6\\n'
    >>> df2 = PolarsSerializer().loads(blob)
    >>> df2.frame_equal(df)
    True

    Using a different method.
    >>> blob = PolarsSerializer(method="polars.csv").dumps(df)
    >>> blob
    b'a,b\\n1,4\\n2,5\\n3,6\\n'
    >>> df2 = PolarsSerializer(method="polars.csv").loads(blob)
    >>> df2.frame_equal(df)
    True

    Using custom read and write kwargs.
    >>> blob = PolarsSerializer(write_kwargs={"index": True}).dumps(df)
    >>> blob
    b'index\\ta\\tb\\n0\\t1\\t4\\n1\\t2\\t5\\n2\\t3\\t6\\n'
    >>> df2 = PolarsSerializer(read_kwargs={"index_col": 0}).loads(blob)
    >>> df2.frame_equal(df)
    True
    """

    type: Literal["polars"] = "polars"
    method = "polars.parquet"
