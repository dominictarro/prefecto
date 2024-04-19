"""
Tools to improve Prefect states.

"""

from __future__ import annotations

from prefect import states


def is_terminal(state: states.State) -> bool:
    """Return True if the state is terminal. Terminal states are:

    - Cancelled
    - Completed
    - Crashed
    - Failed
    """
    TERMINALS = [
        state.is_cancelled,
        state.is_completed,
        state.is_crashed,
        state.is_failed,
    ]
    for terminal in TERMINALS:
        if terminal():
            return True
    return False
