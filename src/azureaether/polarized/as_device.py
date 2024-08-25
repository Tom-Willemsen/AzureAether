import asyncio

import bluesky.plans as bp
from bluesky.callbacks import LiveTable
from bluesky.protocols import Triggerable
from ibex_bluesky_core.devices import get_pv_prefix
from ibex_bluesky_core.devices.block import BlockRw, block_rw
from ibex_bluesky_core.devices.dae import Dae
from ophyd_async.core import (
    AsyncStatus,
    HintedSignal,
    StandardReadable,
    soft_signal_rw,
)
from ophyd_async.plan_stubs import ensure_connected


class Polarization(StandardReadable, Triggerable):
    def __init__(self, prefix: str, name: str = "", flipper_block: str = "flipper"):
        self.dae = Dae(prefix)
        self.flipper = BlockRw(int, prefix, flipper_block)

        with self.add_children_as_readables(HintedSignal):
            self.polarization = soft_signal_rw(float, 0.0, precision=6)

        with self.add_children_as_readables():
            self.up_run = soft_signal_rw(int, 0)
            self.down_run = soft_signal_rw(int, 0)
            self.up = soft_signal_rw(float, 0.0)
            self.down = soft_signal_rw(float, 0.0)

        super().__init__(name=name)
        self.polarization.set_name(name)

    async def _measure_one_pol(self) -> (int, float):
        await self.dae.begin_run.trigger()
        await asyncio.sleep(1)
        await self.dae.end_run.trigger()

        return await asyncio.gather(self.dae.good_uah.get_value(), self.dae.good_uah.get_value())

    @AsyncStatus.wrap
    async def trigger(self) -> None:
        # Measure "up"
        await self.flipper.set(0)
        up_run, up_intensity = await self._measure_one_pol()

        # Measure "down"
        await self.flipper.set(1)
        down_run, down_intensity = await self._measure_one_pol()

        await asyncio.gather(
            self.up_run.set(up_run),
            self.up.set(up_intensity),
            self.down_run.set(down_run),
            self.down.set(down_intensity),
            self.polarization.set(
                (up_intensity - down_intensity) / (up_intensity + down_intensity),
            ),
        )


def pol_scan(block_name: str, *, start: float, stop: float, num: int):
    block = block_rw(float, block_name)
    det = Polarization(get_pv_prefix(), name="pol")
    yield from ensure_connected(block, det)
    yield from bp.scan([det], block, start, stop, num)


if __name__ == "__main__":
    from ibex_bluesky_core.run_engine import get_run_engine

    RE = get_run_engine()
    RE(pol_scan("mot", start=0, stop=10, num=6), LiveTable(["mot", "pol-up", "pol-down", "pol"]))
