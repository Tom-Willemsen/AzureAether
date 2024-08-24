from typing import TypeVar

import bluesky.plans as bp
from bluesky.callbacks import LiveTable
from ibex_bluesky_core.devices import get_pv_prefix
from ibex_bluesky_core.devices.block import block_rw
from ophyd_async.plan_stubs import ensure_connected

from azureaether.haven_derived_signal import derived_signal_rw
from azureaether.simpledae import SimpleDae

T = TypeVar("T")


def plan():
    dae = SimpleDae(get_pv_prefix())
    mot = block_rw(float, "mot")

    def normalize(values, *, dae, mot):
        try:
            return values[dae] / values[mot]
        except ArithmeticError:
            return 0

    normalized_counts = derived_signal_rw(
        float,
        0.0,
        derived_from={"dae": dae.good_uah, "mot": mot.readback},
        forward=None,
        inverse=normalize,
        units="",
        precision=5,
        name="norm_counts",
    )

    yield from ensure_connected(dae, mot, normalized_counts)
    yield from bp.scan([dae, normalized_counts], mot, 1, 3, 5)


if __name__ == "__main__":
    from ibex_bluesky_core.run_engine import get_run_engine

    RE = get_run_engine()
    RE(plan(), LiveTable(["mot", "DAE", "norm_counts"]))
