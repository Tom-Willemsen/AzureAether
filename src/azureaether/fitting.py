import sys

import bluesky.plans as bp
import lmfit
import numpy as np
from bluesky.callbacks import LiveFit, LiveFitPlot, LivePlot
from ibex_bluesky_core.devices.block import BlockWriteConfig, block_rw
from ophyd_async.plan_stubs import ensure_connected

sys.path.append(r"c:\instrument\apps\python3\lib\site-packages")
import matplotlib
import matplotlib.pyplot as plt

matplotlib.use("module://genie_python.matplotlib_backend.ibex_websocket_backend")
import genie_python.matplotlib_backend.ibex_websocket_backend as _mpl_backend

_mpl_backend.set_up_plot_default(
    is_primary=True, should_open_ibex_window_on_show=True, max_figures=3
)


def plan():
    p3 = block_rw(float, "p3")
    mot = block_rw(float, "mot", write_config=BlockWriteConfig(settle_time_s=2))
    yield from ensure_connected(p3, mot)
    yield from bp.adaptive_scan(
        [p3], "p3", mot, start=50, stop=100, min_step=0.1, max_step=5, target_delta=4, backstep=True
    )


if __name__ == "__main__":
    from ibex_bluesky_core.run_engine import get_run_engine

    RE = get_run_engine()

    def gaussian(x, amp, sigma, x0):
        return amp * np.exp(-((x - x0) ** 2) / (2 * sigma**2))

    model = lmfit.Model(gaussian)

    init_guess = {
        "amp": lmfit.Parameter("A", 50, min=1),
        "sigma": lmfit.Parameter("sigma", min=1, max=20),
        "x0": lmfit.Parameter("x0", 80, min=70, max=90),
    }

    lf = LiveFit(model, "p3", {"x": "mot"}, init_guess=init_guess)

    fig, ax = plt.subplots()
    lfp = LiveFitPlot(lf, ax=ax, color="r")
    lp = LivePlot("p3", "mot", ax=ax, marker="o", linestyle="none")
    RE(plan(), [lp, lfp, lambda *a: plt.show()])

    print(lf.result.values)

    input()
