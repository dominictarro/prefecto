import re
from typing import Union
from unittest.mock import patch

import pytest
from prefect.blocks.system import Secret

from prefecto.blocks import lazy_load, load_block


def test_load_block():

    with patch(
        "prefect.blocks.system.Secret.load", return_value=Secret(value="abc-123")
    ):
        block = load_block(Secret, "block")
        assert isinstance(block, Secret)
        assert block.get() == "abc-123"


def test_lazy_load():

    with patch(
        "prefect.blocks.system.Secret.load", return_value=Secret(value="abc-123")
    ):

        class Blocks:
            password = "block"

            @property
            @lazy_load("password")
            def pw(self) -> Secret:
                return load_block(Secret, getattr(self, "password"))

        blocks = Blocks()
        assert isinstance(blocks.pw, Secret)
        assert blocks.pw.get() == "abc-123"


def test_lazy_load_fail_on_union_return():

    with pytest.raises(
        TypeError,
        match=re.escape(
            "Only one block type can be specified for a block property. Received: (<class 'prefect.blocks.system.Secret'>, <class 'str'>)",
        ),
    ):

        class _:
            password = "block"

            @property
            @lazy_load("password")
            def pw(self) -> Secret | str:
                return load_block(Secret, getattr(self, "password"))

    with pytest.raises(
        TypeError,
        match=re.escape(
            "Only one block type can be specified for a block property. Received: (<class 'prefect.blocks.system.Secret'>, <class 'str'>)",
        ),
    ):

        class _:
            password = "block"

            @property
            @lazy_load("password")
            def pw(self) -> Union[Secret, str]:
                return load_block(Secret, getattr(self, "password"))


@pytest.mark.asyncio
async def test_lazy_load_run_coro_return_val():
    with patch(
        "prefect.blocks.system.Secret.load", return_value=Secret(value="abc-123")
    ):

        class Blocks:
            password = "block"

            @property
            @lazy_load("password")
            def pw(self) -> Secret:
                return load_block(Secret, getattr(self, "password"))

        blocks = Blocks()
        assert isinstance(blocks.pw, Secret)
        assert blocks.pw.get() == "abc-123"
