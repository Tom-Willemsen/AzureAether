import asyncio

from ibex_bluesky_core.devices.dae import Dae
from ophyd_async.core import AsyncStatus


class SimpleDae(Dae):
    @AsyncStatus.wrap
    async def trigger(self) -> None:
        await self.begin_run.trigger(wait=True, timeout=None)
        await asyncio.sleep(3)
        await self.end_run.trigger(wait=True, timeout=None)
