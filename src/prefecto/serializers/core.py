"""
Module for the Method class, its factory wrapper, and the ExtendedSerializer.
"""
from __future__ import annotations

import functools
import io
from typing import Any, Callable, Generic, Literal, TypeVar

from prefect.serializers import Serializer
from prefect.utilities.callables import get_call_parameters
from typing_extensions import ParamSpec

P1 = ParamSpec("P1")  # The parameters of the read function
R1 = TypeVar("R1")  # The return type of the read function
P2 = ParamSpec("P2")  # The parameters of the write function
R2 = TypeVar("R2")  # The return type of the write function


__registry__: dict[str, "Method"] = {}


class MethodMeta(type):
    """Metaclass for the `Method` class.

    This metaclass wraps the `read` and `write` methods to apply default keyword
    arguments. It also registers the class in the `__registry__` dictionary.
    """

    @staticmethod
    def read_wrapper(cls, *args: P1.args, **kwargs: P1.kwargs) -> R1:
        """Reads the object."""
        parameters = get_call_parameters(
            cls.__read__,
            args,
            {**cls.default_read_kwargs, **kwargs},
            apply_defaults=False,
        )
        return cls.__read__(**parameters)

    @staticmethod
    def write_wrapper(cls, *args: P2.args, **kwargs: P2.kwargs) -> R2:
        """Writes the object."""
        parameters = get_call_parameters(
            cls.__write__,
            args,
            {**cls.default_write_kwargs, **kwargs},
            apply_defaults=False,
        )
        return cls.__write__(**parameters)

    def __new__(mcs, name, bases, attrs):
        # Don't do anything for the base class. This should only be called when
        # creating a subclass.
        if not bases:
            return super().__new__(mcs, name, bases, attrs)
        assert "discriminator" in attrs, "Must define a discriminator."
        assert (
            attrs["discriminator"] not in __registry__
        ), f"Duplicate method discriminator: '{attrs['discriminator']}'"

        # Set default keyword arguments to empty dicts if not provided.
        attrs["default_read_kwargs"] = attrs.get("default_read_kwargs", {})
        attrs["default_write_kwargs"] = attrs.get("default_write_kwargs", {})

        # Wrap the read and write functions to apply default keyword arguments.
        assert "__read__" in attrs, "Must define a read function."
        attrs["read"] = classmethod(
            functools.update_wrapper(mcs.read_wrapper, attrs["__read__"])
        )
        assert "__write__" in attrs, "Must define a write function."
        attrs["write"] = classmethod(
            functools.update_wrapper(mcs.write_wrapper, attrs["__write__"])
        )
        cls = super().__new__(mcs, name, bases, attrs)

        # Register the constructed class
        __registry__[attrs["discriminator"]] = cls
        return cls


class Method(metaclass=MethodMeta):
    """A method for reading and writing a type. To be subclassed for access via
    its discriminator.

    Args:
        discriminator (str): The discriminator for the method. This must be globally
            unique.
        __read__ (Callable): The function to read the object.
        __write__ (Callable): The function to write the object.
        default_read_kwargs (dict[str, Any], optional): Default keyword arguments for
            the read function. Must accept a `BytesIO` object as the first argument.
        default_write_kwargs (dict[str, Any], optional): Default keyword arguments for
            the write function. Must accept the object to serialize as the first
            argument and a `BytesIO` object as the second argument.

    Examples:

        ```python
        from io import IOBase as IO
        from prefecto.serializers import Method

        def read(io: IO, encoding: str = 'utf-8') -> str:
            return io.read().decode(encoding)

        def write(value: str, io: IO, encoding: str = 'utf-8') -> None:
            io.write(value.encode(encoding))

        class Utf8(Method):
            discriminator = "utf8"
            __read__ = read
            __write__ = write

        class Latin1(Method):
            discriminator = "latin1"
            __read__ = read
            __write__ = write
            default_read_kwargs = {"encoding": "latin1"}
            default_write_kwargs = {"encoding": "latin1"}
        ```

    """

    discriminator: str
    __read__: Callable[P1, R1]
    __write__: Callable[P2, R2]
    default_read_kwargs: dict[str, Any]
    default_write_kwargs: dict[str, Any]

    def __init_subclass__(cls) -> None:
        cls.__metaclass__ = MethodMeta
        return super().__init_subclass__()

    @classmethod
    def read(cls, *args: P1.args, **kwargs: P1.kwargs) -> R1:
        """Reads the object."""
        parameters = get_call_parameters(
            cls.__read__,
            args,
            {**cls.default_read_kwargs, **kwargs},
            apply_defaults=False,
        )
        return cls.__read__(**parameters)

    @classmethod
    def write(cls, *args: P2.args, **kwargs: P2.kwargs) -> R2:
        """Writes the object."""
        parameters = get_call_parameters(
            cls.__write__,
            args,
            {**cls.default_write_kwargs, **kwargs},
            apply_defaults=False,
        )
        return cls.__write__(**parameters)


def get_method(discriminator: str) -> Method:
    """Gets the `Method` by its discriminator.

    Args:
        discriminator (str): The discriminator for the method.

    Raises:
        KeyError: If the discriminator is not registered.

    Returns:
        Method: The method.
    """
    return __registry__[discriminator]


@Serializer.register
class ExtendedSerializer(Serializer):
    """Extends the `Serializer` class to allow for custom serializers to be registered
    with their own methods for reading and writing. Good for complex types with
    standard read and write methods.

    Args:
        method (str): The method to use for reading and writing. Must be a registered
            `Method`.
        read_kwargs (dict[str, Any], optional): Keyword arguments for the read method.
            Overrides default arguments for the method.
        write_kwargs (dict[str, Any], optional): Keyword arguments for the write
            method. Overrides default arguments for the method.

    Examples:

        ```python
        from prefecto.serializers import ExtendedSerializer, Method, get_method

        def read(io: IO, encoding: str = 'utf-8') -> str:
            return io.read().decode(encoding)

        def write(value: str, io: IO, encoding: str = 'utf-8') -> None:
            io.write(value.encode(encoding))

        class Utf8(Method):
            discriminator = "utf8"
            __read__ = read
            __write__ = write

        class Latin1(Method):
            discriminator = "latin1"
            __read__ = read
            __write__ = write
            default_read_kwargs = {"encoding": "latin1"}
            default_write_kwargs = {"encoding": "latin1"}

        ExtendedSerializer("utf8").dumps("Hello, world!")
        ```
        ```text
        b'Hello, world!'
        ```

    """

    method: str
    read_kwargs: dict[str, Any] | None = None
    write_kwargs: dict[str, Any] | None = None
    type: Literal["ext"] = "ext"

    def get_method(self) -> Method:
        """Gets the `Method` to read and write objects with."""
        return get_method(self.method)

    def dumps(self, value: Any) -> bytes:
        """Serialize the object with `Method.write`.

        Args:
            value (Any): The object to serialize.

        Returns:
            bytes: The serialized object.
        """
        method = get_method(self.method)
        with io.BytesIO() as buffer:
            method.write(value, buffer, **(self.write_kwargs or {}))
            return buffer.getvalue()

    def loads(self, value: bytes) -> Any:
        """Deserialize the object with `Method.read`.

        Args:
            value (bytes): The serialized object.

        Returns:
            Any: The deserialized object.
        """
        method = get_method(self.method)
        with io.BytesIO(value) as buffer:
            return method.read(buffer, **(self.read_kwargs or {}))
