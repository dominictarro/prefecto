"""
Tests for the `serializers.core` module.
"""
import io
from typing import Any

import pytest
from _pytest.monkeypatch import MonkeyPatch

from prefecto.serializers import core


def dummyread() -> str:
    """Returns "abc"."""
    return "abc"


def dummywrite() -> bytes:
    """Returns b"abc"."""
    return b"abc"


@pytest.fixture
def method_registry(monkeypatch: MonkeyPatch):
    """Fixture to clear the method registry."""
    try:
        __registry__ = core.__registry__
        monkeypatch.setattr(core, "__registry__", __registry__.copy())
        yield
    finally:
        monkeypatch.setattr(core, "__registry__", __registry__)


def test_method(method_registry):
    """Tests the method decorator."""

    class Method(core.Method):
        """Dummy method."""

        discriminator: str = "test.method"
        default_read_kwargs: dict[str, Any] = {}
        default_write_kwargs: dict[str, Any] = {}
        __read__ = dummyread
        __write__ = dummywrite

    assert core.get_method("test.method") == Method
    assert Method.read() == "abc"
    assert Method.write() == b"abc"


def test_method_missing_attr(method_registry):
    """Tests the method decorator with missing attributes."""
    with pytest.raises(AssertionError):

        class _(core.Method):
            """Dummy method."""

            __read__ = dummyread
            __write__ = dummywrite

    with pytest.raises(AssertionError):

        class _(core.Method):
            """Dummy method."""

            discriminator: str = "test.method.a"
            __write__ = dummywrite

    with pytest.raises(AssertionError):

        class _(core.Method):
            """Dummy method."""

            discriminator: str = "test.method.b"
            default_read_kwargs: dict[str, Any] = {}
            __read__ = dummyread


def test_extended_serializer_basic(method_registry):
    """Tests the serializer class with simple inputs."""

    class _(core.Method):
        """Dummy method."""

        discriminator: str = "test"
        default_read_kwargs: dict[str, Any] = {}
        default_write_kwargs: dict[str, Any] = {}

        def __read__(buff: io.BytesIO) -> str:
            """Reads the string from the buffer."""
            return buff.read().decode()

        def __write__(value: str, buff: io.BytesIO) -> None:
            """Writes the string to the buffer."""
            buff.write(value.encode())

    s = core.ExtendedSerializer(method="test")
    string = "abc"
    assert s.dumps(string) == b"abc"
    assert s.loads(b"abc") == "abc"
