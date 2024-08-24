import math
from typing import TypeVar

import bluesky.plans as bp
import numpy as np
import scipp as sc
from bluesky.callbacks import LiveTable
from ibex_bluesky_core.devices import get_pv_prefix
from ibex_bluesky_core.devices.block import block_rw
from ophyd_async.core import DeviceVector, HintedSignal, StandardReadable
from ophyd_async.epics.signal import epics_signal_r
from ophyd_async.plan_stubs import ensure_connected

from azureaether.haven_derived_signal import derived_signal_r
from azureaether.simpledae import SimpleDae

T = TypeVar("T")


def plan():
    dae = SimpleDae(get_pv_prefix())
    mot = block_rw(float, "mot")

    class DaeWithUncertainty(StandardReadable):
        def __init__(self, name):
            self.nspec = 250
            self.spec = DeviceVector(
                {
                    i: epics_signal_r(np.typing.NDArray[np.float32], f"TE:NDW2922:DAE:SPEC:1:{i}:Y")
                    for i in range(1, self.nspec + 1)
                }
            )

            with self.add_children_as_readables(HintedSignal.uncached):
                self.norm = derived_signal_r(
                    np.typing.NDArray[np.float64],
                    [0.0, 0.0],
                    derived_from={f"spec_{i}": self.spec[i] for i in range(1, self.nspec + 1)},
                    inverse=self._normalize,
                    units="",
                    precision=5,
                    monitor_and_cache=False
                )
                self.val = derived_signal_r(
                    float,
                    initial_value=0.0,
                    derived_from={"norm": self.norm},
                    inverse=lambda values, *, norm: values[norm][0],
                    monitor_and_cache=False
                )
                self.err = derived_signal_r(
                    float,
                    initial_value=0.0,
                    derived_from={"norm": self.norm},
                    inverse=lambda values, *, norm: values[norm][1],
                    monitor_and_cache=False
                )
            super().__init__(name)
            self.norm.set_name(name)

        def _normalize(self, values, **kwds):
            data = sc.concat(
                [sc.array(dims=["tof"], values=v, variances=v) for v in values.values()],
                dim="spectrum",
            )

            monitors = data["spectrum", 0:10]
            detectors = data["spectrum", 10:self.nspec]

            result = sc.sum(detectors) / sc.sum(monitors)
            return [float(result.value), float(math.sqrt(result.variance))]

    dae_err = DaeWithUncertainty("DaeWithUncertainty")

    yield from ensure_connected(dae, mot, dae_err)
    yield from bp.scan([dae, dae_err], mot, 1, 3, 5)


if __name__ == "__main__":
    from ibex_bluesky_core.run_engine import get_run_engine

    RE = get_run_engine()
    RE(
        plan(),
        [
            LiveTable(
                [
                    "mot",
                    "DAE",
                    "DaeWithUncertainty-val",
                    "DaeWithUncertainty-err",
                ]
            ),
        ],
    )
