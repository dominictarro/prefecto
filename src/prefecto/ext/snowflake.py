"""
Opinionated Snowflake cursor for logging Snowflake execution calls with parameter obfuscation options.

```python
from snowflake.connector import connect
from prefecto.ext.snowflake import LogCursor

with connect(...).cursor(LogCursor) as cursor:
    cursor.execute(
        "SELECT * FROM table WHERE id = %(id)s AND secret = %(secret)s",
        params={"id": 123, "secret": "shhh"},
        obfuscate_params=["secret"],
        command_id="secret-selection",
    )
```

```
INFO - [secret-selection] Beginning command.
DEBUG - [secret-selection] Executing command:
SELECT * FROM table WHERE id = 123 AND secret = ****
INFO - [secret-selection] Command executed successfully.
```

"""

from __future__ import annotations

import logging
import sys
from typing import IO, Any, Sequence

import coolname
from snowflake.connector.cursor import SnowflakeCursor as _SnowflakeCursor
from snowflake.connector.file_transfer_agent import SnowflakeProgressPercentage

__python_version__ = sys.version_info


class CommandLogAdapter(logging.LoggerAdapter):
    """A logging adapter that prepends the command ID to the log messages."""

    def process(self, msg: str, kwargs: dict[str, Any]) -> tuple[str, dict[str, Any]]:
        return f"[{self.extra['command_id']}] {msg}", kwargs


def _obfuscate_params(
    params: dict[str, Any], obfuscate_params: bool | str | list[str]
) -> dict[str, Any]:
    """Obfuscates the parameters in the dictionary.

    Args:
        params (dict[str, Any]): The parameters to obfuscate.
        obfuscate_params (bool | str | list[str]): Whether to obfuscate the parameters.
            If `True`, obfuscates all parameters. If a string or list of strings, obfuscates only the specified parameters.

    Returns:
        dict[str, Any]: The obfuscated parameters.

    Example:

        ```python
        _obfuscate_params({"id": 123, "secret": "shhh"}, True)
        # {"id": "****", "secret": "****"}
        ```

    """
    if obfuscate_params is True:
        return {k: "****" for k in params}
    elif isinstance(obfuscate_params, str):
        return {k: "****" if k == obfuscate_params else v for k, v in params.items()}
    elif isinstance(obfuscate_params, list):
        return {k: "****" if k in obfuscate_params else v for k, v in params.items()}
    return params


def _execute(
    c: _SnowflakeCursor,
    command: str,
    *,
    kwargs: dict[str, Any] | None = None,
    logger: logging.Logger | None = None,
    command_id: str | None = None,
    obfuscate_params: bool | str | list[str] = False,
    level: int | str = logging.DEBUG,
) -> _SnowflakeCursor | dict[str, Any] | None:
    """Logs the command and executes it with the Snowflake cursor.

    Args:
        c (SnowflakeCursor): The Snowflake cursor or connection to use.
        command (str): The command to execute.
        kwargs (dict[str, Any] | None, optional): The kwargs to pass to the `c.execute`. Defaults to None.
        logger (logging.Logger | None): The logger to use.
        command_id (str, optional): The ID to use in the logs. Generates a random one if not provided. Defaults to None.
        obfuscate_params (bool | str | list[str], optional): Whether to obfuscate the parameters in the logs.
            If `True`, obfuscates all parameters. If a string or list of strings, obfuscates only the specified parameters.
            Defaults to False.
        level (int | str, optional): The logging level to use. Defaults to logging.DEBUG.

    Returns:
        Any: The result of the `c.execute`.

    Example:

        ```python
        import logging
        from snowflake.connector import connect
        from prefecto.ext.snowflake import _execute

        conn = connect(...)
        logger = logging.getLogger(__name__)

        _execute(conn.cursor(), "SELECT * FROM table", logger=logger)
        ```

        Produces logs like

        ```txt
        INFO - [rustic-human] Beginning command.
        DEBUG - [rustic-human] Executing command:
        SELECT * FROM table
        INFO - [rustic-human] Command executed successfully.
        ```

        And tracebacks like

        ```txt
        ERROR - [rustic-human] Command failed.
        Traceback (most recent call last):
        ...
        snowflake.connector.errors.ProgrammingError: ...
        [rustic-human] Command failed
        SELECT * FROM table
        ```

        You can also obfuscate parameters:

        ```python
        _execute(conn.cursor(), "SELECT * FROM table WHERE id = %(id)s", kwargs=dict(params={"id": 123}), obfuscate_params=True)
        _execute(conn.cursor(), "SELECT * FROM table WHERE id = %(id)s AND %(secret)s", kwargs=dict(params={"id": 123, "secret": "shhh"}), obfuscate_params=["secret"])
        ```

        ```txt
        INFO - [delightful-octopus] Beginning command.
        DEBUG - [delightful-octopus] Executing command:
        SELECT * FROM table WHERE id = ****
        INFO - [delightful-octopus] Command executed successfully.
        INFO - [super-athlete] Beginning command.
        DEBUG - [super-athlete] Executing command:
        SELECT * FROM table WHERE id = 123 AND secret = ****
        INFO - [super-athlete] Command executed successfully.
        ```

    """
    kwargs = kwargs or {}
    level_int = logging._nameToLevel.get(level) if isinstance(level, str) else level
    assert isinstance(
        level_int, int
    ), f"Provided logging level {level} didn't convert to int ({level_int}, {type(level_int).__name__})"

    command_id = command_id or "-".join(coolname.generate(2))
    logger = CommandLogAdapter(
        logger or logging.getLogger(__name__), extra={"command_id": command_id}
    )

    # Obfuscate parameters if requested
    formatted_command_string = command % _obfuscate_params(
        kwargs.get("params", {}), obfuscate_params
    )

    logger.info("Beginning command.")
    try:
        logger.log(
            level_int,
            f"Executing command:\n{formatted_command_string}",
        )
        result = c.execute(command, **kwargs)
    except Exception as e:
        if hasattr(e, "add_note") and callable(e.add_note):
            e.add_note(f"Command failed\n{formatted_command_string}")
        logger.error("Command failed.", exc_info=e, stack_info=True)
        raise e
    logger.info("Command executed successfully.")
    return result


def execute(
    c: _SnowflakeCursor,
    command: str,
    params: Sequence[Any] | dict[Any, Any] | None = None,
    _bind_stage: str | None = None,
    timeout: int | None = None,
    _exec_async: bool = False,
    _no_retry: bool = False,
    _do_reset: bool = True,
    _put_callback: SnowflakeProgressPercentage = None,
    _put_azure_callback: SnowflakeProgressPercentage = None,
    _put_callback_output_stream: IO[str] = sys.stdout,
    _get_callback: SnowflakeProgressPercentage = None,
    _get_azure_callback: SnowflakeProgressPercentage = None,
    _get_callback_output_stream: IO[str] = sys.stdout,
    _show_progress_bar: bool = True,
    _statement_params: dict[str, str] | None = None,
    _is_internal: bool = False,
    _describe_only: bool = False,
    _no_results: bool = False,
    _is_put_get: bool | None = None,
    _raise_put_get_error: bool = True,
    _force_put_overwrite: bool = False,
    file_stream: IO[bytes] | None = None,
    *,
    logger: logging.Logger | None = None,
    command_id: str | None = None,
    obfuscate_params: bool | str | list[str] = False,
    level: int | str = logging.DEBUG,
) -> _SnowflakeCursor | dict[str, Any] | None:
    """Executes a command on the Snowflake connection after logging it.

    Args:
        c (SnowflakeCursor): The Snowflake cursor or connection to use.
        command (str): The command to execute.
        params (Sequence[Any] | dict[Any, Any] | None, optional): The parameters to pass to the `c.execute`. Defaults to None.
        _bind_stage (str | None, optional): The stage to bind. Defaults to None.
        timeout (int | None, optional): The timeout to use. Defaults to None.
        _exec_async (bool, optional): Whether to execute asynchronously. Defaults to False.
        _no_retry (bool, optional): Whether to retry on failure. Defaults to False.
        _do_reset (bool, optional): Whether to reset the connection. Defaults to True.
        _put_callback (SnowflakeProgressPercentage, optional): The callback for PUT operations. Defaults to None.
        _put_azure_callback (SnowflakeProgressPercentage, optional): The callback for Azure PUT operations. Defaults to None.
        _put_callback_output_stream (IO[str], optional): The output stream for PUT callbacks. Defaults to sys.stdout.
        _get_callback (SnowflakeProgressPercentage, optional): The callback for GET operations. Defaults to None.
        _get_azure_callback (SnowflakeProgressPercentage, optional): The callback for Azure GET operations. Defaults to None.
        _get_callback_output_stream (IO[str], optional): The output stream for GET callbacks. Defaults to sys.stdout.
        _show_progress_bar (bool, optional): Whether to show a progress bar. Defaults to True.
        _statement_params (dict[str, str] | None, optional): The statement parameters. Defaults to None.
        _is_internal (bool, optional): Whether the command is internal. Defaults to False.
        _describe_only (bool, optional): Whether to describe only. Defaults to False.
        _no_results (bool, optional): Whether to return no results. Defaults to False.
        _is_put_get (bool | None, optional): Whether the command is a PUT or GET operation. Defaults to None.
        _raise_put_get_error (bool, optional): Whether to raise an error on PUT or GET failure. Defaults to True.
        _force_put_overwrite (bool, optional): Whether to force PUT overwrite. Defaults to False.
        file_stream (IO[bytes] | None, optional): The file stream to use. Defaults to None.
        logger (logging.Logger | None): The logger to use.
        command_id (str, optional): The ID to use in the logs. Generates a random one if not provided. Defaults to None.
        obfuscate_params (bool | str | list[str], optional): Whether to obfuscate the parameters in the logs.
            If `True`, obfuscates all parameters. If a string or list of strings, obfuscates only the specified parameters.
            Defaults to False.
        level (int | str, optional): The logging level to use. Defaults to logging.DEBUG.

    Returns:
        SnowflakeCursor | dict[str, Any] | None: The result of the `c.execute`.

    Example:

        ```python
        import logging
        from snowflake.connector import connect
        from prefecto.ext.snowflake import execute

        c = connect(...).cursor()
        execute(c, "SELECT * FROM table")
        ```

        Produces logs like

        ```txt
        INFO - [rustic-human] Beginning command.
        DEBUG - [rustic-human] Executing command:
        SELECT * FROM table
        INFO - [rustic-human] Command executed successfully.
        ```

        And tracebacks like

        ```txt
        ERROR - [rustic-human] Command failed.
        Traceback (most recent call last):
        ...
        snowflake.connector.errors.ProgrammingError: ...
        [rustic-human] Command failed
        SELECT * FROM table
        ```

        You can also obfuscate parameters:

        ```python
        execute(c, "SELECT * FROM table WHERE id = %(id)s", params={"id": 123}, obfuscate_params=True)
        execute(c, "SELECT * FROM table WHERE id = %(id)s AND %(secret)s", params={"id": 123, "secret": "shhh"}, obfuscate_params=["secret"])
        ```

        ```txt
        INFO - [delightful-octopus] Beginning command.
        DEBUG - [delightful-octopus] Executing command:
        SELECT * FROM table WHERE id = ****
        INFO - [delightful-octopus] Command executed successfully.
        INFO - [super-athlete] Beginning command.
        DEBUG - [super-athlete] Executing command:
        SELECT * FROM table WHERE id = 123 AND secret = ****
        INFO - [super-athlete] Command executed successfully.
        ```

    """

    return _execute(
        c,
        command,
        kwargs={
            "params": params,
            "_bind_stage": _bind_stage,
            "timeout": timeout,
            "_exec_async": _exec_async,
            "_no_retry": _no_retry,
            "_do_reset": _do_reset,
            "_put_callback": _put_callback,
            "_put_azure_callback": _put_azure_callback,
            "_put_callback_output_stream": _put_callback_output_stream,
            "_get_callback": _get_callback,
            "_get_azure_callback": _get_azure_callback,
            "_get_callback_output_stream": _get_callback_output_stream,
            "_show_progress_bar": _show_progress_bar,
            "_statement_params": _statement_params,
            "_is_internal": _is_internal,
            "_describe_only": _describe_only,
            "_no_results": _no_results,
            "_is_put_get": _is_put_get,
            "_raise_put_get_error": _raise_put_get_error,
            "_force_put_overwrite": _force_put_overwrite,
            "file_stream": file_stream,
        },
        logger=logger,
        command_id=command_id,
        obfuscate_params=obfuscate_params,
        level=level,
    )


class LogCursor(_SnowflakeCursor):
    """A Snowflake cursor that logs command executions. The `execute` method has additional parameters for altering log behavior.

    Example:

        ```python
        from snowflake.connector import connect
        from prefecto.ext.snowflake import LogCursor

        c = connect(...).cursor(LogCursor)
        c: LogCursor
        c.execute("SELECT * FROM table")
        ```

        Produces logs like

        ```txt
        INFO - [rustic-human] Beginning command.
        DEBUG - [rustic-human] Executing command:
        SELECT * FROM table
        INFO - [rustic-human] Command executed successfully.
        ```
    """

    def execute(
        self,
        command: str,
        params: Sequence[Any] | dict[Any, Any] | None = None,
        _bind_stage: str | None = None,
        timeout: int | None = None,
        _exec_async: bool = False,
        _no_retry: bool = False,
        _do_reset: bool = True,
        _put_callback: SnowflakeProgressPercentage = None,
        _put_azure_callback: SnowflakeProgressPercentage = None,
        _put_callback_output_stream: IO[str] = sys.stdout,
        _get_callback: SnowflakeProgressPercentage = None,
        _get_azure_callback: SnowflakeProgressPercentage = None,
        _get_callback_output_stream: IO[str] = sys.stdout,
        _show_progress_bar: bool = True,
        _statement_params: dict[str, str] | None = None,
        _is_internal: bool = False,
        _describe_only: bool = False,
        _no_results: bool = False,
        _is_put_get: bool | None = None,
        _raise_put_get_error: bool = True,
        _force_put_overwrite: bool = False,
        file_stream: IO[bytes] | None = None,
        *,
        logger: logging.Logger | None = None,
        command_id: str | None = None,
        obfuscate_params: bool | str | list[str] = False,
        level: int | str = logging.DEBUG,
    ) -> _SnowflakeCursor | dict[str, Any] | None:
        """Executes a command on the Snowflake connection after logging it.

        Args:
            command (str): The command to execute.
            params (Sequence[Any] | dict[Any, Any] | None, optional): The parameters to pass to the `c.execute`. Defaults to None.
            _bind_stage (str | None, optional): The stage to bind. Defaults to None.
            timeout (int | None, optional): The timeout to use. Defaults to None.
            _exec_async (bool, optional): Whether to execute asynchronously. Defaults to False.
            _no_retry (bool, optional): Whether to retry on failure. Defaults to False.
            _do_reset (bool, optional): Whether to reset the connection. Defaults to True.
            _put_callback (SnowflakeProgressPercentage, optional): The callback for PUT operations. Defaults to None.
            _put_azure_callback (SnowflakeProgressPercentage, optional): The callback for Azure PUT operations. Defaults to None.
            _put_callback_output_stream (IO[str], optional): The output stream for PUT callbacks. Defaults to sys.stdout.
            _get_callback (SnowflakeProgressPercentage, optional): The callback for GET operations. Defaults to None.
            _get_azure_callback (SnowflakeProgressPercentage, optional): The callback for Azure GET operations. Defaults to None.
            _get_callback_output_stream (IO[str], optional): The output stream for GET callbacks. Defaults to sys.stdout.
            _show_progress_bar (bool, optional): Whether to show a progress bar. Defaults to True.
            _statement_params (dict[str, str] | None, optional): The statement parameters. Defaults to None.
            _is_internal (bool, optional): Whether the command is internal. Defaults to False.
            _describe_only (bool, optional): Whether to describe only. Defaults to False.
            _no_results (bool, optional): Whether to return no results. Defaults to False.
            _is_put_get (bool | None, optional): Whether the command is a PUT or GET operation. Defaults to None.
            _raise_put_get_error (bool, optional): Whether to raise an error on PUT or GET failure. Defaults to True.
            _force_put_overwrite (bool, optional): Whether to force PUT overwrite. Defaults to False.
            file_stream (IO[bytes] | None, optional): The file stream to use. Defaults to None.
            logger (logging.Logger | None): The logger to use.
            command_id (str, optional): The ID to use in the logs. Generates a random one if not provided. Defaults to None.
            obfuscate_params (bool | str | list[str], optional): Whether to obfuscate the parameters in the logs.
                If `True`, obfuscates all parameters. If a string or list of strings, obfuscates only the specified parameters.
                Defaults to False.
            level (int | str, optional): The logging level to use. Defaults to logging.DEBUG.

        Returns:
            SnowflakeCursor | dict[str, Any] | None: The result of the `c.execute`.

        Example:

            ```python
            import logging
            from snowflake.connector import connect
            from prefecto.ext.snowflake import LogCursor

            c = connect(...).cursor(LogCursor)
            c.execute("SELECT * FROM table")
            ```

            Produces logs like

            ```txt
            INFO - [rustic-human] Beginning command.
            DEBUG - [rustic-human] Executing command:
            SELECT * FROM table
            INFO - [rustic-human] Command executed successfully.
            ```

            And tracebacks like

            ```txt
            ERROR - [rustic-human] Command failed.
            Traceback (most recent call last):
            ...
            snowflake.connector.errors.ProgrammingError: ...
            [rustic-human] Command failed
            SELECT * FROM table
            ```

            You can also obfuscate parameters:

            ```python
            c.execute("SELECT * FROM table WHERE id = %(id)s", params={"id": 123}, obfuscate_params=True)
            c.execute("SELECT * FROM table WHERE id = %(id)s AND %(secret)s", params={"id": 123, "secret": "shhh"}, obfuscate_params=["secret"])
            ```

            ```txt
            INFO - [delightful-octopus] Beginning command.
            DEBUG - [delightful-octopus] Executing command:
            SELECT * FROM table WHERE id = ****
            INFO - [delightful-octopus] Command executed successfully.
            INFO - [super-athlete] Beginning command.
            DEBUG - [super-athlete] Executing command:
            SELECT * FROM table WHERE id = 123 AND secret = ****
            INFO - [super-athlete] Command executed successfully.
            ```

        """

        return _execute(
            super(),
            command,
            kwargs={
                "params": params,
                "_bind_stage": _bind_stage,
                "timeout": timeout,
                "_exec_async": _exec_async,
                "_no_retry": _no_retry,
                "_do_reset": _do_reset,
                "_put_callback": _put_callback,
                "_put_azure_callback": _put_azure_callback,
                "_put_callback_output_stream": _put_callback_output_stream,
                "_get_callback": _get_callback,
                "_get_azure_callback": _get_azure_callback,
                "_get_callback_output_stream": _get_callback_output_stream,
                "_show_progress_bar": _show_progress_bar,
                "_statement_params": _statement_params,
                "_is_internal": _is_internal,
                "_describe_only": _describe_only,
                "_no_results": _no_results,
                "_is_put_get": _is_put_get,
                "_raise_put_get_error": _raise_put_get_error,
                "_force_put_overwrite": _force_put_overwrite,
                "file_stream": file_stream,
            },
            logger=logger,
            command_id=command_id,
            obfuscate_params=obfuscate_params,
            level=level,
        )
