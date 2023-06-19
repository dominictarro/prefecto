# Pandas

## PandasSerializer

Type: `pandas`

::: src.prefecto.serializers.pandas.PandasSerializer

## Methods

`Method` classes for serializing `pandas.DataFrame`.

| Discriminator | Default Read | Default Write |
| --- | --- | --- |
| `pandas.csv` | `{"index": False}` | `{"index": False}` |
| `pandas.excel` | None | None |
| `pandas.feather` | None | None |
| `pandas.json` | None | None |
| `pandas.jsonl` | None | None |
| `pandas.parquet` | None | None |
| `pandas.pickle` | None | None |
| `pandas.tsv` | `{"sep": "\t", "index": False}` | `{"sep": "\t", "index": False}` |
