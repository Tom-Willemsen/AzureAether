import asyncio
import random
import time

import bluesky.plans as bp
from bluesky.callbacks import LiveTable
from bluesky.protocols import Hints, Reading, Triggerable
from event_model import DataKey
from ibex_bluesky_core.devices import get_pv_prefix
from ibex_bluesky_core.devices.block import BlockRw, block_rw
from ibex_bluesky_core.devices.dae import Dae
from ophyd_async.core import AsyncStageable, AsyncStatus, StandardReadable
from ophyd_async.plan_stubs import ensure_connected


class Polarization(StandardReadable, Triggerable, AsyncStageable):
    def __init__(self, prefix: str, name: str = "", flipper_block: str = "flipper"):
        self._pol_up_run: int | None = None
        self._pol_up_intensity: float | None = None
        self._pol_down_run: int | None = None
        self._pol_down_intensity: float | None = None

        self.dae = Dae(prefix)
        self.flipper = BlockRw(int, prefix, flipper_block)

        super().__init__(name=name)

    async def _measure_one_pol(self) -> (int, float):
        await self.dae.begin_run.trigger()
        await asyncio.sleep(1)
        await self.dae.end_run.trigger()

        return await asyncio.gather(self.dae.good_uah.get_value(), self.dae.good_uah.get_value())

    @AsyncStatus.wrap
    async def trigger(self):
        await self.flipper.set(0)
        # Measure "up"
        self._pol_up_run, self._pol_up_intensity = await self._measure_one_pol()
        self._pol_up_intensity *= random.random()

        await self.flipper.set(1)
        # Measure "down"
        self._pol_down_run, self._pol_down_intensity = await self._measure_one_pol()
        self._pol_down_intensity *= random.random()

    async def read(self) -> dict[str, Reading]:
        assert self._pol_up_run is not None, "read() called before trigger(), no UP run"
        assert self._pol_down_run is not None, "read() called before trigger(), no DOWN run"
        return {
            self.name: {
                "value": (self._pol_up_intensity - self._pol_down_intensity)
                / (self._pol_up_intensity + self._pol_down_intensity),
                "timestamp": time.time(),
            },
            self.name + "-up": {
                "value": self._pol_up_run,
                "timestamp": time.time(),
            },
            self.name + "-down": {
                "value": self._pol_down_run,
                "timestamp": time.time(),
            },
        }

    async def describe(self) -> dict[str, DataKey]:
        return {
            self.name: {
                "dtype": "number",
                "shape": [],
                "source": self.dae.good_uah.source,
                "precision": 6,
            },
            self.name + "-up": {
                "dtype": "number",
                "shape": [],
                "source": self.dae.good_uah.source,
            },
            self.name + "-down": {
                "dtype": "number",
                "shape": [],
                "source": self.dae.good_uah.source,
            },
        }

    @property
    def hints(self) -> Hints:
        return {"fields": [self.name]}


def pol_scan(block_name: str, *, start: float, stop: float, num: int):
    block = block_rw(float, block_name)
    det = Polarization(get_pv_prefix(), name="pol")
    yield from ensure_connected(block, det)
    yield from bp.scan([det], block, start, stop, num)


if __name__ == "__main__":
    from ibex_bluesky_core.run_engine import get_run_engine

    RE = get_run_engine()
    RE(pol_scan("mot", start=0, stop=10, num=6), LiveTable(["mot", "pol-up", "pol-down", "pol"]))
