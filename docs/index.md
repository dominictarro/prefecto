# Prefecto

<p align="center">
    <img src="./assets/Monkeywrench-Data-Pipeline.png">
    <br>
    <a href="https://pypi.python.org/pypi/prefecto/" alt="PyPI version">
        <img alt="PyPI" src="https://img.shields.io/pypi/v/prefecto?color=fe7200&labelColor=eeeeee"></a>
    <a href="https://github.com/dominictarro/prefecto/" alt="Stars">
        <img src="https://img.shields.io/github/stars/dominictarro/prefecto?color=fe7200&labelColor=eeeeee" /></a>
    <a href="https://github.com/dominictarro/prefecto/pulse" alt="Activity">
        <img src="https://img.shields.io/github/commit-activity/m/dominictarro/prefecto?color=fe7200&labelColor=eeeeee" /></a>
</p>

Prefecto is a collection of tools to extend and augment [Prefect](https://www.prefect.io/) capabilities.

## Getting Started

Install Prefecto with

```bash
pip install prefecto
```

*Prefecto is only tested with Python 3.10 and higher. It may work with older versions, but it is not guaranteed.*

Break up `Task.map` into [batches](./batch_task.md#batchtask).

```python
from prefect import flow, task
from prefecto.concurrency import BatchTask

@task
def task(x):
    print(x)
    return x

@flow
def flow():
    results = BatchTask(task, size=100).map(range(1000))

```

Standard serializers for [`pandas.DataFrame`](./serializers/pandas/methods.md) and [`polars.DataFrame`](./serializers/polars/methods.md).

```python
import polars as pl
from prefect import flow, task
from prefecto.serialization.polars import PolarsSerializer

@task(serializer=PolarsSerializer(method="polars.parquet"), persist_result=True, cache_result_in_memory=False)
def parquet_task(df: pl.DataFrame) -> pl.DataFrame:
    ...

@task(serializer=PolarsSerializer(method="polars.csv"), persist_result=True, cache_result_in_memory=False)
def csv_task(df: pl.DataFrame) -> pl.DataFrame:
    ...

@flow
def flow():
    df = pl.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    df = parquet_task(df)
    df = csv_task(df)
    return df
```
