import asyncio
import math
from typing import TypeVar

import bluesky.plans as bp
import numpy as np
import scipp as sc
from bluesky.callbacks import LiveTable
from ibex_bluesky_core.devices import get_pv_prefix
from ibex_bluesky_core.devices.block import block_rw
from ophyd_async.core import (
    AsyncStatus,
    DeviceVector,
    HintedSignal,
    StandardReadable,
    soft_signal_rw,
)
from ophyd_async.epics.signal import epics_signal_r
from ophyd_async.plan_stubs import ensure_connected

from azureaether.simpledae import SimpleDae

T = TypeVar("T")


def plan():
    mot = block_rw(float, "mot")

    class Spectrum(StandardReadable):
        def __init__(self, spec_num: int, name=""):
            self.y = epics_signal_r(
                np.typing.NDArray[np.float32], f"TE:NDW2922:DAE:SPEC:1:{spec_num}:Y"
            )
            self.x = epics_signal_r(
                np.typing.NDArray[np.float32], f"TE:NDW2922:DAE:SPEC:1:{spec_num}:X"
            )
            super().__init__(name=name)

    class DaeWithUncertainty(SimpleDae):
        def __init__(self, prefix: str, name: str):
            self.nspec = 250
            self.spec = DeviceVector({i: Spectrum(spec_num=i) for i in range(1, self.nspec + 1)})

            with self.add_children_as_readables(HintedSignal):
                self.val = soft_signal_rw(float, 0.0)
            with self.add_children_as_readables():
                self.err = soft_signal_rw(float, 0.0)

            super().__init__(prefix, name)

        @AsyncStatus.wrap
        async def trigger(self) -> None:
            await super().trigger()
            await self._normalize_trigger()

        async def _normalize_trigger(self) -> None:
            # Read all spectra
            spectra = await asyncio.gather(*(spec.y.get_value() for spec in self.spec.values()))

            # Run normalization in a separate thread so that we don't block the main event loop
            # if it takes a while.
            val, err = await asyncio.to_thread(self._normalize, spectra)

            await asyncio.gather(
                self.val.set(val, wait=True, timeout=None),
                self.err.set(err, wait=True, timeout=None),
            )

        def _normalize(self, values):
            data = sc.concat(
                [sc.array(dims=["tof"], values=v, variances=v) for v in values],
                dim="spectrum",
            )

            monitors = data["spectrum", 0:10]
            detectors = data["spectrum", 10 : self.nspec + 1]

            result = sc.sum(detectors) / sc.sum(monitors)
            return float(result.value), float(math.sqrt(result.variance))

    dae = DaeWithUncertainty(get_pv_prefix(), "DAE")

    yield from ensure_connected(dae, mot)
    yield from bp.scan([dae], mot, 1, 3, 5)


if __name__ == "__main__":
    from ibex_bluesky_core.run_engine import get_run_engine

    RE = get_run_engine()
    RE(
        plan(),
        [
            LiveTable(
                [
                    "mot",
                    "DAE-val",
                    "DAE-err",
                ]
            ),
        ],
    )
