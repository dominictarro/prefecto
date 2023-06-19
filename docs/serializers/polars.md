# Polars

Serialization module for for the `polars.DataFrame` type.

## PolarsSerializer

Type: `polars`

::: src.prefecto.serializers.polars.PolarsSerializer

## Methods

`Method` classes for serializing `polars.DataFrame`.

### CSV

Discriminator: `polars.csv`

::: src.prefecto.serializers.polars.CSV
    options:
        show_source: true

### Excel

Discriminator: `polars.excel`

::: src.prefecto.serializers.polars.Excel
    options:
        show_source: true

### JSON

Discriminator: `polars.json`

::: src.prefecto.serializers.polars.JSON
    options:
        show_source: true

### NDJSON

Discriminator: `polars.ndjson`

::: src.prefecto.serializers.polars.NDJSON
    options:
        show_source: true

### Parquet

Discriminator: `polars.parquet`

::: src.prefecto.serializers.polars.Parquet
    options:
        show_source: true

### TSV

Discriminator: `polars.tsv`

::: src.prefecto.serializers.polars.TSV
    options:
        show_source: true
