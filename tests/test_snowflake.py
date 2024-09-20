import logging
from unittest.mock import MagicMock

import pytest

from prefecto.ext.snowflake import (
    PrefectoSnowflakeCursor,
    CommandLogAdapter,
    _execute,
    _obfuscate_params,
)


@pytest.fixture
def mock_cursor():
    return MagicMock(spec=PrefectoSnowflakeCursor)


def test_obfuscate_params():
    params = {"id": 123, "secret": "shhh"}
    assert _obfuscate_params(params, True) == {"id": "****", "secret": "****"}
    assert _obfuscate_params(params, "secret") == {"id": 123, "secret": "****"}
    assert _obfuscate_params(params, ["id", "secret"]) == {
        "id": "****",
        "secret": "****",
    }
    assert _obfuscate_params(params, False) == params


def test_command_log_adapter(caplog: pytest.LogCaptureFixture):
    logger = logging.getLogger("test")
    adapter = CommandLogAdapter(logger, {"command_id": "12345"})
    with caplog.at_level(logging.INFO):
        adapter.info("Test message")
        assert "[12345] " in caplog.text


@pytest.mark.parametrize(
    "level, execute_log_is_expected",
    [
        (logging.DEBUG, True),
        (logging.INFO, False),
    ],
)
def test_execute_log_level_adjustment(
    level, execute_log_is_expected, mock_cursor, caplog: pytest.LogCaptureFixture
):
    logger = logging.getLogger("test")
    with caplog.at_level(level):
        _execute(
            mock_cursor,
            "SELECT * FROM table",
            logger=logger,
            command_id="12345",
            level=logging.DEBUG,
        )
        assert "[12345] Beginning command." in caplog.text
        if execute_log_is_expected:
            assert "[12345] Executing command:\n" + "SELECT * FROM table" in caplog.text
        else:
            assert (
                "[12345] Executing command:\n" + "SELECT * FROM table" not in caplog.text
            )
        assert "[12345] Command executed successfully." in caplog.text


@pytest.mark.parametrize(
    "command, params, masked_params, expected_command_log",
    [
        (
            "SELECT * FROM table",
            {},
            None,
            "SELECT * FROM table",
        ),
        (
            "SELECT * FROM table WHERE id = %(id)s",
            {"id": 123},
            False,
            "SELECT * FROM table WHERE id = 123",
        ),
        (
            "SELECT * FROM table WHERE id = %(id)s",
            {"id": 123},
            True,
            "SELECT * FROM table WHERE id = ****",
        ),
        (
            "SELECT * FROM table WHERE id = %(id)s AND secret = %(secret)s",
            {"id": 123, "secret": "shhh"},
            ["secret"],
            "SELECT * FROM table WHERE id = 123 AND secret = ****",
        ),
    ],
)
def test_execute_parameter_obfuscation(
    mock_cursor,
    caplog: pytest.LogCaptureFixture,
    command,
    params,
    masked_params,
    expected_command_log,
):
    logger = logging.getLogger("test")
    with caplog.at_level(logging.DEBUG):
        _execute(
            mock_cursor,
            command,
            kwargs=dict(params=params),
            logger=logger,
            command_id="12345",
            obfuscate_params=masked_params,
        )
        assert expected_command_log in caplog.text
