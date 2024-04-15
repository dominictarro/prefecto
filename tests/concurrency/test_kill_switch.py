import pytest
from prefect import states

from prefecto.concurrency.kill_switch import (
    AnyFailedSwitch,
    CountSwitch,
    KillSwitchError,
    RateSwitch,
)


@pytest.mark.asyncio
async def test_any_kill_switch():
    with pytest.raises(KillSwitchError):
        AnyFailedSwitch().raise_if_triggered(states.Failed())


@pytest.mark.asyncio
async def test_count_kill_switch():
    ks = CountSwitch(2)
    ks.raise_if_triggered(states.Failed())
    ks.raise_if_triggered(states.Completed())
    with pytest.raises(KillSwitchError):
        ks.raise_if_triggered(states.Failed())


@pytest.mark.asyncio
async def test_rate_kill_switch():
    ks = RateSwitch(3, 0.5)
    ks.raise_if_triggered(states.Completed())
    ks.raise_if_triggered(states.Failed())
    with pytest.raises(KillSwitchError):
        ks.raise_if_triggered(states.Crashed())
