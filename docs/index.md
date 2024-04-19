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

Break `Task.map` into smaller concurrent [batches](./concurrency/batch_task.md).

```python
from prefect import flow, task
from prefecto.concurrency import BatchTask, CountSwitch

@task
def my_task(x):
    if x % 100 == 0:
        raise ValueError(f"{x} is divisible by 100.")
    return x

@flow
def my_flow():
    results = BatchTask(
        my_task,
        size=100,
        kill_switch=CountSwitch(15),
    ).map(range(1000))

```

Simply and efficiently define and load [blocks](./blocks.md) for dev/test/prod environments.

```bash
# .env
BLOCKS_PASSWORD_BLOCK=secret-password-test
```

```python
from prefect.blocks.system import Secret
from prefecto.blocks import lazy_load
from pydantic_settings import BaseSettings, SettingsConfigDict


class Blocks(BaseSettings):
    """Class for lazy loading Prefect Blocks."""

    model_config = SettingsConfigDict(env_prefix="BLOCKS_")

    # Define the block name variables
    password_block: str = "secret-password"

    @property
    @lazy_load("password_block")
    def password(self) -> Secret:
        """The password block."""

blocks = Blocks()
password = blocks.password
print(password.password_block)
# secret-password-test
print(password.get())
# my-secret-test-password$123
```
