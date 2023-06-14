"""
Module for the Method class, its factory wrapper, and the ExtendedSerializer.
"""
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


class Method(Generic[P1, R1, P2, R2]):
    """A method for reading and writing a type.

    Parameters
    ----------
    discriminator : str
        The discriminator for the method.
    read : Callable
        The function to read the object.
    write : Callable
        The function to write the object.
    default_read_kwargs : dict[str, Any], optional
        Default keyword arguments for the read function.
    default_write_kwargs : dict[str, Any], optional
        Default keyword arguments for the write function.

    Notes
    -----
    The `read` method must take an `IO` object as the first argument and the
    `write` method must take an object as the first argument and an `IO` object
    as the second argument.

    """

    discriminator: str
    __read__: Callable[P1, R1]
    __write__: Callable[P2, R2]
    default_read_kwargs: dict[str, Any]
    default_write_kwargs: dict[str, Any]

    @classmethod
    def read(cls, *args: P1.args, **kwargs: P1.kwargs) -> R1:
        """Reads the object."""
        return cls.__read__(*args, **kwargs)

    @classmethod
    def write(cls, *args: P2.args, **kwargs: P2.kwargs) -> R2:
        """Writes the object."""
        return cls.__write__(*args, **kwargs)


__registry__: dict[str, Method] = {}


def get_method(discriminator: str) -> Method:
    """Gets the method for the discriminator.

    Parameters
    ----------
    discriminator : str
        The discriminator for the method.

    Returns
    -------
    Callable
        The method.
    """
    return __registry__[discriminator]


def method(cls):
    """Class decorator to register a method for reading and writing a type. Wraps the
    read and write functions to apply the default keyword arguments.

    Parameters
    ----------
    cls : Method
        The method to register.

    Returns
    -------
    Method
        The registered method.
    """
    assert (
        cls.discriminator not in __registry__
    ), f"Duplicate method discriminator: '{cls.discriminator}'"
    __registry__[cls.discriminator] = cls

    # Get optional default keyword arguments
    cls.default_read_kwargs = getattr(cls, "default_read_kwargs", {})
    cls.default_write_kwargs = getattr(cls, "default_write_kwargs", {})

    read = cls.__read__

    @functools.wraps(read)
    def _read(*args, **kwargs):
        parameters = get_call_parameters(
            read, args, {**cls.default_read_kwargs, **kwargs}, apply_defaults=False
        )
        return read(**parameters)

    cls.read = _read

    write = cls.__write__

    @functools.wraps(write)
    def _write(*args: P2.args, **kwargs: P2.kwargs):
        parameters = get_call_parameters(
            write, args, {**cls.default_write_kwargs, **kwargs}, apply_defaults=False
        )
        return write(**parameters)

    cls.write = _write

    return cls


@Serializer.register
class ExtendedSerializer(Serializer):
    """Extends the `Serializer` class to allow for custom serializers to be registered
    with their own methods for reading and writing. Good for complex types with
    standard read and write methods.

    Parameters
    ----------
    method : str
        The method to use for reading and writing. Must be a registered `Method`.
    read_kwargs : dict[str, Any], optional
        Keyword arguments for the read method. Overrides default arguments for the method.
    write_kwargs : dict[str, Any], optional
        Keyword arguments for the write method. Overrides default arguments for the method.
    """

    method: str
    read_kwargs: dict[str, Any] | None = None
    write_kwargs: dict[str, Any] | None = None
    type: Literal["ext"] = "ext"

    def get_method(self) -> Method:
        """The method to use for reading and writing."""
        return get_method(self.method)

    def dumps(self, value: Any) -> bytes:
        """Serialize the object with `Method.write`."""
        method = get_method(self.method)
        with io.BytesIO() as buffer:
            method.write(value, buffer, **(self.write_kwargs or {}))
            return buffer.getvalue()

    def loads(self, value: bytes) -> Any:
        """Deserialize the object with `Method.read`."""
        method = get_method(self.method)
        with io.BytesIO(value) as buffer:
            return method.read(buffer, **(self.read_kwargs or {}))
