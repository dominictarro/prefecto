# Polars

Serialization module for for the `polars.DataFrame` type.

## PolarsSerializer

Type: `polars`

::: src.prefecto.serializers.polars.PolarsSerializer

## Methods

`Method` classes for serializing `polars.DataFrame`.

| Discriminator | Default Read | Default Write |
| --- | --- | --- |
| `polars.csv` | None | None |
| `polars.excel` | None | None |
| `polars.json` | None | None |
| `polars.ndjson` | None | None |
| `polars.parquet` | None | None |
| `polars.tsv` | `{"separator": "\t"}` | `{"separator": "\t"}` |
