# Pandas

## PandasSerializer

Type: `pandas`

::: src.prefecto.serializers.pandas.PandasSerializer

## Methods

`Method` classes for serializing `pandas.DataFrame`.

### CSV

Discriminator: `pandas.csv`

::: src.prefecto.serializers.pandas.CSV
    options:
        show_source: true

### Excel

Discriminator: `pandas.excel`

::: src.prefecto.serializers.pandas.Excel
    options:
        show_source: true

### Feather

Discriminator: `pandas.feather`

::: src.prefecto.serializers.pandas.Feather
    options:
        show_source: true

### JSON

Discriminator: `pandas.json`

::: src.prefecto.serializers.pandas.JSON
    options:
        show_source: true

### JSONL

Discriminator: `pandas.jsonl`

::: src.prefecto.serializers.pandas.JSONL
    options:
        show_source: true

### Parquet

Discriminator: `pandas.parquet`

::: src.prefecto.serializers.pandas.Parquet
    options:
        show_source: true

### Pickle

Discriminator: `pandas.pickle`

::: src.prefecto.serializers.pandas.Pickle
    options:
        show_source: true

### TSV

Discriminator: `pandas.tsv`

::: src.prefecto.serializers.pandas.TSV
    options:
        show_source: true
