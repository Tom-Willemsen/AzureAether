import asyncio
from typing import Dict

from bluesky.protocols import Reading
from event_model import DataKey
from ibex_bluesky_core.devices.dae import Dae
from ophyd_async.core import AsyncStatus


class SimpleDae(Dae):
    @AsyncStatus.wrap
    async def trigger(self) -> None:
        await self.begin_run.trigger(wait=True, timeout=None)
        await asyncio.sleep(3)
        await self.end_run.trigger(wait=True, timeout=None)

    async def read(self) -> dict[str, Reading]:
        good_uah_reading = await self.good_uah.read()
        return {
            self.name: {
                "value": good_uah_reading[self.good_uah.name]["value"],
                "timestamp": good_uah_reading[self.good_uah.name]["timestamp"],
            }
        }

    async def describe(self) -> Dict[str, DataKey]:
        return {self.name: {"dtype": "number", "shape": [], "source": self.good_uah.source}}
