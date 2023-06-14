"""
Tests for the `serializers.pandas` module.
"""
import pandas as pd
import pytest

from prefecto.serializers import pandas as pds


@pytest.fixture
def df():
    """Returns a simple DataFrame."""
    return pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})


def test_parquet(df: pd.DataFrame):
    """Tests the parquet method."""
    s = pds.PandasSerializer(method=pds.Parquet.discriminator)
    assert s.get_method() == pds.Parquet

    blob = s.dumps(df)
    df2 = s.loads(blob)
    assert df.equals(df2)


def test_csv(df: pd.DataFrame):
    """Tests the csv method."""
    s = pds.PandasSerializer(method=pds.CSV.discriminator)
    assert s.get_method() == pds.CSV

    blob = s.dumps(df)
    df2 = s.loads(blob)
    assert df.equals(df2)


def test_json(df: pd.DataFrame):
    """Tests the json method."""
    s = pds.PandasSerializer(method=pds.JSON.discriminator)
    assert s.get_method() == pds.JSON

    blob = s.dumps(df)
    df2 = s.loads(blob)
    assert df.equals(df2)


def test_jsonl(df: pd.DataFrame):
    """Tests the jsonl method."""
    s = pds.PandasSerializer(method=pds.JSONL.discriminator)
    assert s.get_method() == pds.JSONL

    blob = s.dumps(df)
    df2 = s.loads(blob)
    assert df.equals(df2)


def test_feather(df: pd.DataFrame):
    """Tests the feather method."""
    s = pds.PandasSerializer(method=pds.Feather.discriminator)
    assert s.get_method() == pds.Feather

    blob = s.dumps(df)
    df2 = s.loads(blob)
    assert df.equals(df2)


def test_pickle(df: pd.DataFrame):
    """Tests the pickle method."""
    s = pds.PandasSerializer(method=pds.Pickle.discriminator)
    assert s.get_method() == pds.Pickle

    blob = s.dumps(df)
    df2 = s.loads(blob)
    assert df.equals(df2)


def test_tsv(df: pd.DataFrame):
    """Tests the tsv method."""
    s = pds.PandasSerializer(method=pds.TSV.discriminator)
    assert s.get_method() == pds.TSV

    blob = s.dumps(df)
    df2 = s.loads(blob)
    assert df.equals(df2)


def test_excel(df: pd.DataFrame):
    """Tests the excel method."""
    s = pds.PandasSerializer(method=pds.Excel.discriminator)
    assert s.get_method() == pds.Excel
    blob = s.dumps(df)
    df2 = s.loads(blob)
    assert df.equals(df2)
