import asyncio
from typing import Dict

import bluesky.plan_stubs as bps
import bluesky.plans as bp
from bluesky.callbacks import LiveTable
from bluesky.protocols import HasHints, HasName, Hints, Readable, Reading, Stageable, Triggerable
from event_model import DataKey
from ibex_bluesky_core.devices import get_pv_prefix
from ibex_bluesky_core.devices.block import block_rw
from ophyd_async.core import AsyncStatus, Device
from ophyd_async.plan_stubs import ensure_connected

from azureaether.simpledae import SimpleDae


class Normalized(Device, Readable, HasHints, HasName, Stageable, Triggerable):
    def __init__(
        self,
        name,
        numerator: Readable,
        denominator: Readable,
        numerator_uncertainty: Readable | None = None,
        denominator_uncertainty: Readable | None = None,
    ):
        self._name = name
        self._num_sig = numerator
        self._denom_sig = denominator
        self._num_uncert = numerator_uncertainty
        self._denom_uncert = denominator_uncertainty

        super().__init__(name)

    def set_name(self, name: str):
        pass

    @property
    def name(self) -> str:
        """Return the name of the Device"""
        return self._name

    async def read(self) -> dict[str, Reading]:
        num, denom = await asyncio.gather(self._num_sig.read(), self._denom_sig.read())

        result = {
            self.name: {
                "value": num[self._num_sig.name]["value"] / denom[self._denom_sig.name]["value"],
                "timestamp": num[self._num_sig.name]["timestamp"],
            }
        }
        if self._num_uncert is not None or self._denom_uncert is not None:
            result[self.name + "-uncertainty"] = {...}  # dict as above but for uncertainty
        return result

    async def describe(self) -> Dict[str, DataKey]:
        num_descriptor = await self._num_sig.describe()
        return {
            self.name: {
                "dtype": "number",
                "shape": [],
                "source": num_descriptor[self._num_sig.name]["source"],
            }
        }

    @property
    def hints(self) -> Hints:
        return {"fields": [self.name]}

    @AsyncStatus.wrap
    async def _noop(self) -> None:
        pass

    def stage(self, *args, **kwargs):
        if hasattr(self._num_sig, "stage"):
            return self._num_sig.stage(*args, **kwargs)
        else:
            return self._noop()

    def unstage(self, *args, **kwargs):
        if hasattr(self._num_sig, "unstage"):
            return self._num_sig.unstage(*args, **kwargs)
        else:
            return self._noop()

    def trigger(self, *args, **kwargs):
        if hasattr(self._num_sig, "trigger"):
            return self._num_sig.trigger(*args, **kwargs)
        else:
            return self._noop()


def plan():
    dae = SimpleDae(get_pv_prefix())
    mot = block_rw(float, "mot")
    normalized_counts = Normalized("norm_counts", dae, mot)
    yield from ensure_connected(dae, mot)
    yield from bp.scan([normalized_counts], mot, 1, 2, 3, per_step=bps.one_nd_step)


if __name__ == "__main__":
    from ibex_bluesky_core.run_engine import get_run_engine

    RE = get_run_engine()
    RE(plan(), LiveTable(["mot", "norm_counts"]))
