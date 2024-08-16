import bluesky.plan_stubs as bps
import bluesky.plans as bp
from bluesky.callbacks import LiveTable
from ibex_bluesky_core.devices import get_pv_prefix
from ibex_bluesky_core.devices.block import block_rw
from ibex_bluesky_core.devices.dae import Dae
from ophyd_async.core import soft_signal_rw
from ophyd_async.plan_stubs import ensure_connected


def pol_scan(block_name: str, *, start: float, stop: float, num: int):
    block = block_rw(float, block_name)
    dae = Dae(get_pv_prefix())

    _pol = soft_signal_rw(float, 0.0, "pol")
    _pol_up = soft_signal_rw(float, 0.0, "pol-up")
    _pol_down = soft_signal_rw(float, 0.0, "pol-down")

    flipper = block_rw(int, "flipper")

    yield from ensure_connected(block, dae, flipper, _pol, _pol_up, _pol_down)

    def polarisation_read():
        yield from bps.mv(flipper, 0)

        yield from bps.trigger(dae.begin_run, wait=True)
        yield from bps.sleep(1)
        yield from bps.trigger(dae.end_run, wait=True)
        pol_up = yield from bps.rd(dae.good_uah)

        yield from bps.mv(flipper, 1)

        yield from bps.trigger(dae.begin_run, wait=True)
        yield from bps.sleep(1)
        yield from bps.trigger(dae.end_run, wait=True)
        pol_down = yield from bps.rd(dae.good_uah)

        assert isinstance(pol_up, float)
        assert isinstance(pol_down, float)
        polarization = (pol_up - pol_down) / (pol_up + pol_down)

        yield from bps.mv(_pol, polarization, _pol_up, pol_up, _pol_down, pol_down)

        yield from bps.create()
        yield from bps.read(block)
        yield from bps.read(_pol)
        yield from bps.read(_pol_up)
        yield from bps.read(_pol_down)
        yield from bps.save()

    def per_step(detectors, step, pos_cache):
        for k, v in step.items():
            yield from bps.mv(k, v, group="moves")
        yield from bps.wait("moves")

        assert len(detectors) == 1
        return (yield from polarisation_read())

    yield from bp.scan([_pol], block, start, stop, num, per_step=per_step)


if __name__ == "__main__":
    from ibex_bluesky_core.run_engine import get_run_engine

    RE = get_run_engine()
    RE(pol_scan("mot", start=0, stop=10, num=6), LiveTable(["mot", "pol-up", "pol-down", "pol"]))
