import time
from typing import Dict

from event_model import DataKey
from ibex_bluesky_core.devices.dae import Dae
from bluesky.protocols import Hints, Reading, HasHints
from ibex_bluesky_core.devices.block import block_rw, BlockRw
from ibex_bluesky_core.devices import get_pv_prefix
from bluesky.callbacks import LiveTable
from ophyd_async.core import AsyncStatus, StandardReadable
import asyncio
from ophyd_async.plan_stubs import ensure_connected
import bluesky.plans as bp
import bluesky.plan_stubs as bps


def pol_scan(block_name: str, *, start: float, stop: float, num: int):
    block = block_rw(float, block_name)
    det = Dae(get_pv_prefix())
    yield from ensure_connected(block, det)

    def polarisation_read(dae):
        yield from bps.create()

        yield from bps.trigger(dae.begin_run, wait=True)
        yield from bps.sleep(1)
        yield from bps.trigger(dae.end_run, wait=True)
        rd1 = yield from bps.read(dae)

        # ... yield from set flippers just like in the device example...

        yield from bps.trigger(dae.begin_run, wait=True)
        yield from bps.sleep(1)
        yield from bps.trigger(dae.end_run, wait=True)
        rd2 = yield from bps.read(dae)

        # This doesn't actually work - some magic required to ensure readings above don't clash?
        # But maybe the device approach is better anyway
        # I don't want to mess with low-level details of the bundler...

        yield from bps.save()

    def per_step(detectors, step, pos_cache):
        for k, v in step.items():
            yield from bps.mv(k, v, group="moves")
        yield from bps.wait("moves")

        assert len(detectors) == 1
        return (yield from polarisation_read(detectors[0]))

    yield from bp.scan([det], block, start, stop, num, per_step=per_step)


if __name__ == "__main__":
    from ibex_bluesky_core.run_engine import get_run_engine
    RE = get_run_engine()
    RE(pol_scan("mot", start=0, stop=10, num=6), LiveTable(["mot", "pol-up", "pol-down", "pol"]))