"""
Pandas IO `Method`s and `ExtendedSerializer`.
"""
from typing import Any, Literal

try:
    import pandas as pd
except ImportError:
    raise ImportError(
        "Pandas is required for the pandas serializer.\nInstall"
        " with `pip install pandas`."
    )

from .core import ExtendedSerializer, Method


class Parquet(Method):
    """Method for reading and writing Parquet files."""

    discriminator: str = "pandas.parquet"
    __read__ = pd.read_parquet
    __write__ = pd.DataFrame.to_parquet


class CSV(Method):
    """Method for reading and writing CSV files."""

    discriminator: str = "pandas.csv"
    default_write_kwargs: dict[str, Any] = {"index": False}
    __read__ = pd.read_csv
    __write__ = pd.DataFrame.to_csv


class JSON(Method):
    """Method for reading and writing JSON files."""

    discriminator: str = "pandas.json"
    __read__ = pd.read_json
    __write__ = pd.DataFrame.to_json


class JSONL(Method):
    """Method for reading and writing JSONL files."""

    discriminator: str = "pandas.jsonl"
    default_read_kwargs: dict[str, Any] = {"lines": True, "orient": "records"}
    default_write_kwargs: dict[str, Any] = {"lines": True, "orient": "records"}
    __read__ = pd.read_json
    __write__ = pd.DataFrame.to_json


class Feather(Method):
    """Method for reading and writing Feather files."""

    discriminator: str = "pandas.feather"
    __read__ = pd.read_feather
    __write__ = pd.DataFrame.to_feather


class Pickle(Method):
    """Method for reading and writing Pickle files."""

    discriminator: str = "pandas.pickle"
    __read__ = pd.read_pickle
    __write__ = pd.DataFrame.to_pickle


class TSV(Method):
    """Method for reading and writing TSV files."""

    discriminator: str = "pandas.tsv"
    default_read_kwargs: dict[str, Any] = {"sep": "\t"}
    default_write_kwargs: dict[str, Any] = {"sep": "\t", "index": False}
    __read__ = pd.read_table
    __write__ = pd.DataFrame.to_csv


class Excel(Method):
    """Method for reading and writing Excel files."""

    discriminator: str = "pandas.excel"
    default_write_kwargs: dict[str, Any] = {"index": False}
    __read__ = pd.read_excel
    __write__ = pd.DataFrame.to_excel


@ExtendedSerializer.register
class PandasSerializer(ExtendedSerializer):
    """Serializer for `pandas.DataFrame` objects.

    Parameters
    ----------
    method : str
        The method to use for reading and writing. Must be a registered
        `Method`. Defaults to "pandas.tsv".
    read_kwargs : dict[str, Any], optional
        Keyword arguments for the read method. Overrides default arguments for
        the method.
    write_kwargs : dict[str, Any], optional
        Keyword arguments for the write method. Overrides default arguments
        for the method.

    Examples
    --------
    Simple read and write.
    >>> import pandas as pd
    >>> from prefecto.serializers.pandas import PandasSerializer
    >>> df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    >>> blob = PandasSerializer().dumps(df)
    >>> blob
    b'a\\tb\\n1\\t4\\n2\\t5\\n3\\t6\\n'
    >>> df2 = PandasSerializer().loads(blob)
    >>> df2.equals(df)
    True

    Using a different method.
    >>> blob = PandasSerializer(method="pandas.csv").dumps(df)
    >>> blob
    b'a,b\\n1,4\\n2,5\\n3,6\\n'
    >>> df2 = PandasSerializer(method="pandas.csv").loads(blob)
    >>> df2.equals(df)
    True

    Using custom read and write kwargs.
    >>> blob = PandasSerializer(write_kwargs={"index": True}).dumps(df)
    >>> blob
    b'index\\ta\\tb\\n0\\t1\\t4\\n1\\t2\\t5\\n2\\t3\\t6\\n'
    >>> df2 = PandasSerializer(read_kwargs={"index_col": 0}).loads(blob)
    >>> df2.equals(df)
    True
    """

    type: Literal["pandas"] = "pandas"
    method = "pandas.tsv"
