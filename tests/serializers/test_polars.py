"""
Tests for the `serializers.polars` module.
"""
import polars as pl
import pytest

from prefecto.serializers import polars as pls


@pytest.fixture
def df():
    """Returns a simple DataFrame."""
    return pl.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})


def test_parquet(df: pl.DataFrame):
    """Tests the parquet method."""
    s = pls.PolarsSerializer(method=pls.Parquet.discriminator)
    assert s.get_method() == pls.Parquet

    blob = s.dumps(df)
    df2 = s.loads(blob)
    assert df.frame_equal(df2)


def test_csv(df: pl.DataFrame):
    """Tests the csv method."""
    s = pls.PolarsSerializer(method=pls.CSV.discriminator)
    assert s.get_method() == pls.CSV

    blob = s.dumps(df)
    df2 = s.loads(blob)
    assert df.frame_equal(df2)


def test_json(df: pl.DataFrame):
    """Tests the json method."""
    s = pls.PolarsSerializer(method=pls.JSON.discriminator)
    assert s.get_method() == pls.JSON

    blob = s.dumps(df)
    df2 = s.loads(blob)
    assert df.frame_equal(df2)


def test_ndjson(df: pl.DataFrame):
    """Tests the jsonl method."""
    s = pls.PolarsSerializer(method=pls.NDJSON.discriminator)
    assert s.get_method() == pls.NDJSON

    blob = s.dumps(df)
    df2 = s.loads(blob)
    assert df.frame_equal(df2)


def test_tsv(df: pl.DataFrame):
    """Tests the tsv method."""
    s = pls.PolarsSerializer(method=pls.TSV.discriminator)
    assert s.get_method() == pls.TSV

    blob = s.dumps(df)
    df2 = s.loads(blob)
    assert df.frame_equal(df2)


def test_excel(df: pl.DataFrame):
    """Tests the excel method."""
    s = pls.PolarsSerializer(method=pls.Excel.discriminator)
    assert s.get_method() == pls.Excel
    blob = s.dumps(df)
    df2 = s.loads(blob)
    assert df.frame_equal(df2)
