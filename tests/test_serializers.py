"""Tests for the serializers library.
"""
import io
from typing import Any

import pytest
from _pytest.monkeypatch import MonkeyPatch

from prefecto.serializers import core


class TestCore:
    """Tests for the core module."""

    @staticmethod
    def dummyread() -> str:
        """Returns "abc"."""
        return "abc"

    @staticmethod
    def dummywrite() -> bytes:
        """Returns b"abc"."""
        return b"abc"

    @pytest.fixture
    def method_registry(self, monkeypatch: MonkeyPatch):
        """Fixture to clear the method registry."""
        try:
            __registry__ = core.__registry__
            monkeypatch.setattr(core, "__registry__", __registry__.copy())
            yield
        finally:
            monkeypatch.setattr(core, "__registry__", __registry__)

    def test_method(self, method_registry):
        """Tests the method decorator."""

        @core.method
        class Method(core.Method):
            """Dummy method."""

            discriminator: str = "test.method"
            default_read_kwargs: dict[str, Any] = {}
            default_write_kwargs: dict[str, Any] = {}
            __read__ = self.dummyread
            __write__ = self.dummywrite

        assert core.__registry__ == {"test.method": Method}
        assert core.get_method("test.method") == Method
        assert Method.read() == "abc"
        assert Method.write() == b"abc"

    def test_method_missing_attr(self, method_registry):
        """Tests the method decorator with missing attributes."""
        with pytest.raises(AttributeError):

            @core.method
            class _(core.Method):
                """Dummy method."""

                __read__ = self.dummyread
                __write__ = self.dummywrite

        with pytest.raises(AttributeError):

            @core.method
            class _(core.Method):
                """Dummy method."""

                discriminator: str = "test.method.a"
                __write__ = self.dummywrite

        with pytest.raises(AttributeError):

            @core.method
            class _(core.Method):
                """Dummy method."""

                discriminator: str = "test.method.b"
                default_read_kwargs: dict[str, Any] = {}
                __read__ = self.dummyread

    def test_extended_serializer_basic(self, method_registry):
        """Tests the serializer class with simple inputs."""

        @core.method
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
