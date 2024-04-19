"""
A module for lazy loading Prefect Blocks.

The `@lazy_load` decorator is centered around a model to attach blocks to. Each block
is defined with two parts: a block name and a block property. The block name is a
string that identifies the block. The block property is a property that loads the block
on first access. The block property should be decorated with the `@lazy_load` decorator,
and the block's type should be specified in the property annotations.

Example:

```python
from prefect.blocks.system import Secret
from prefecto.blocks import lazy_load


class Blocks:
    \"""Class for lazy loading Prefect Blocks.\"""

    # Define the block name variables
    password_block: str = "secret-password"

    @property
    @lazy_load("password_block")
    def password(self) -> Secret:
        \"""The password block.\"""

blocks = Blocks()
password = blocks.password
print(password)
# Secret(value=SecretStr('**********'))
print(password.get())
# my-secret-password$123
```

This technique is useful for preventing the blocks from loading until they are actually
needed. This can be important during unit testing, where connections to the Prefect
server may not exist. The alternative would be to call `block.load()` directly, which
would load the block every time it is called and could lead to performance issues.

The loader can be integrated with `pydantic-settings` to load block names from
environment variables. This can be achieved by setting the block name variables
as class variables with default values set to the environment variables.

Example:

```python
from pydantic_settings import BaseSettings, SettingsConfigDict
from prefect.blocks.system import Secret
from prefecto.blocks import lazy_load


class Blocks(BaseSettings):
    \"""Class for lazy loading Prefect Blocks.\"""

    model_config = SettingsConfigDict(env_prefix="BLOCKS_")

    # Define the block name variables
    password_block: str = "secret-password"

    @property
    @lazy_load("password_block")
    def password(self) -> Secret:
        \"""The password block.\"""

```

"""

import asyncio
import functools
import types
from typing import Union, get_args, get_origin

from prefect._internal.concurrency.api import from_sync
from prefect.blocks.abstract import Block
from prefect.utilities.asyncutils import sync_compatible


@sync_compatible
async def load_block(block_type: type[Block], block_name: str) -> Block:
    """Load a block.

    Args:
        block_type (type[Block]): The block type.
        block_name (str): The block name.
    """
    # Sometimes when calling this load func in an async context,
    # block_type.load() will return a coroutine object instead of the block.
    # This is a workaround to handle that case.
    block = block_type.load(block_name)
    return (await block) if asyncio.iscoroutine(block) else block


def lazy_load(varname: str):
    """Decorator for lazy loading a block.

    Args:
        varname (str): The variable name of the block name var.

    Returns:
        The decorator for the loader.

    Example:

    Specify the block name, then create a decorated property to load the block on
    first access.

    ```python
    class MyClass:
        block_name = "block_name"

        @property
        @lazy_load("block_name")
        def block(self) -> BlockType:
            \"""The block.\"""
    ```
    """

    def decorator(func):
        """Decorator for the loader."""

        block_type = func.__annotations__["return"]

        if (get_origin(block_type) is Union) or isinstance(block_type, types.UnionType):
            raise TypeError(
                f"Only one block type can be specified for a block property. Received: {get_args(block_type)}",
            )
        elif isinstance(block_type, type) and not issubclass(block_type, Block):
            raise TypeError(
                f"The block type must be a subclass of Block. Received: {block_type}",
            )

        block_varname = f"_{func.__name__}_block"

        @functools.wraps(func)
        def lazy_property(self):
            """Lazy property loader for a block."""
            block = getattr(self, block_varname, None)
            if not block:
                block = load_block(block_type, getattr(self, varname))
                if asyncio.iscoroutine(block):
                    # This is a workaround for a case where the block is first accessed
                    # within an asynchronous flow. In that case, the @sync_compatible
                    # decorator will return a coroutine object instead of the block.
                    # This will run the coroutine in the loop thread to get the block.
                    block = from_sync.call_in_loop_thread(lambda: block)
                setattr(self, block_varname, block)
            return block

        lazy_property.varname = varname
        lazy_property.block_type = block_type
        lazy_property.block_varname = block_varname

        return lazy_property

    return decorator
