import asyncio

import bluesky.plan_stubs as bps
import bluesky.plans as bp
from bluesky.callbacks import LiveTable
from bluesky.protocols import Triggerable
from ibex_bluesky_core.devices import get_pv_prefix
from ibex_bluesky_core.devices.block import block_rw
from ibex_bluesky_core.devices.dae.dae import Dae
from ibex_bluesky_core.devices.dae.dae_controls import BeginRunExBits
from ibex_bluesky_core.devices.dae.dae_spectra import DaeSpectra
from ibex_bluesky_core.run_engine import get_run_engine
from ophyd_async.core import (
    AsyncStageable,
    AsyncStatus,
    Device,
    StandardReadable,
    soft_signal_r_and_setter,
    wait_for_value,
)
from ophyd_async.plan_stubs import ensure_connected

# TODO:
# - Make this whole thing more pythonic, this is basically some horribly verbose java masquerading
#   in python syntax.
# - Things that have stage/unstage/trigger methods but don't follow the Stageable and Triggerable
#   interfaces probably aren't ideal. But I like the idea that it's easy to tell when they'll be
#   called. Find some better names.


class ProvidesExtraReadables:
    def additional_readable_signals(self, dae: "SimpleDae") -> list[Device]:
        """
        Define signals that this strategy considers "important", and will be added to the dae's
        default-read signals and made available by read() on the DAE object.
        """
        return []


class Controller(ProvidesExtraReadables):
    async def trigger_start(self, dae: "SimpleDae") -> None:
        """Start counting"""

    async def trigger_end(self, dae: "SimpleDae") -> None:
        """Stop counting"""

    async def stage(self, dae: "SimpleDae") -> None:
        """Pre-scan setup"""

    async def unstage(self, dae: "SimpleDae") -> None:
        """Post-scan teardown"""


class PeriodPerPointController(Controller):
    def __init__(self, save_run: bool):
        self._save_run = save_run
        self._current_period = 0

    async def trigger_start(self, dae: "SimpleDae") -> None:
        self._current_period += 1
        await dae.period_num.set(self._current_period, wait=True, timeout=None)

        # Ensure frame counters have had a chance to reset to zero for the new period.
        # TODO: is there a nicer way to do this?
        # Something to do with https://github.com/ISISComputingGroup/IBEX/issues/8499 probably.
        await wait_for_value(dae.period.good_frames, 0, timeout=10)
        await wait_for_value(dae.period.raw_frames, 0, timeout=10)

        await dae.controls.resume_run.trigger(wait=True, timeout=None)

    async def trigger_end(self, dae: "SimpleDae") -> None:
        await dae.controls.pause_run.trigger(wait=True, timeout=None)

    async def stage(self, dae: "SimpleDae") -> None:
        self._current_period = 0
        await dae.controls.begin_run_ex.set(BeginRunExBits.BEGIN_PAUSED)

    async def unstage(self, dae: "SimpleDae") -> None:
        if self._save_run:
            await dae.controls.end_run.trigger(wait=True, timeout=None)
        else:
            await dae.controls.abort_run.trigger(wait=True, timeout=None)

    def additional_readable_signals(self, dae: "SimpleDae") -> list[Device]:
        return [dae.period_num]


class Waiter(ProvidesExtraReadables):
    async def wait(self, dae: "SimpleDae"):
        pass


class PeriodGoodFramesWaiter(Waiter):
    def __init__(self, frames):
        # TODO: Maybe make this a soft signal and/or make it adjustable via "Preparable" so it can
        # be changed from plan level?
        self._frames = frames

    async def wait(self, dae: "SimpleDae"):
        await wait_for_value(dae.period.good_frames, lambda v: v >= self._frames, timeout=None)

    def additional_readable_signals(self, dae: "SimpleDae") -> list[Device]:
        return [dae.period.good_frames]


class Reducer(ProvidesExtraReadables):
    async def trigger(self, dae: "SimpleDae"):
        pass


class SingleSpectrumByPeriodGoodFramesReducer(Reducer, StandardReadable):
    def __init__(self, dae_prefix: str, spectrum):
        self.spec = DaeSpectra(dae_prefix=dae_prefix, spectra=spectrum, period=0)

        self.raw_counts, self.raw_counts_setter = soft_signal_r_and_setter(float, 0)
        self.intensity, self.intensity_setter = soft_signal_r_and_setter(float, 0, precision=6)
        super().__init__(name="")

    async def trigger(self, dae: "SimpleDae"):
        spec, good_frames = await asyncio.gather(
            self.spec.read_counts(),
            dae.period.good_frames.get_value(),
        )
        counts = float(spec.sum())
        self.raw_counts_setter(counts)
        self.intensity_setter(counts / good_frames)

    def additional_readable_signals(self, dae: "SimpleDae") -> list[Device]:
        return [
            self.raw_counts,
            self.intensity,
        ]


class SimpleDae(Dae, Triggerable, AsyncStageable):
    """
    Configurable DAE with pluggable strategies for data collection, waiting, and reduction.

    Should cover many simple DAE use-cases, but for complex use-cases a custom Dae subclass
    may still be required to give maximum flexibility.
    """

    def __init__(
        self,
        *,
        prefix: str,
        name: str,
        controller: Controller,
        waiter: Waiter,
        reducer: Reducer,
    ):
        self.prefix = prefix
        self.controller = controller
        self.waiter = waiter
        self.reducer = reducer

        # controller, waiter and reducer may be Devices (but they don't have to be),
        # so can define their own signals. Do __init__ after that so that those signals
        # are usable.
        super().__init__(prefix=prefix, name=name)

        # TODO - do we want something to be "hinted" such that just rd'ing 'DAE' works?
        # TODO - do we want to name some signal as this device's name? How to choose...?

        extra_readables = set()
        for strat in [self.controller, self.waiter, self.reducer]:
            for sig in strat.additional_readable_signals(self):
                extra_readables.add(sig)

        self.add_readables(devices=list(extra_readables))

    @AsyncStatus.wrap
    async def trigger(self) -> None:
        await self.controller.trigger_start(self)
        await self.waiter.wait(self)
        await self.controller.trigger_end(self)
        await self.reducer.trigger(self)

    @AsyncStatus.wrap
    async def stage(self) -> None:
        await self.controller.stage(self)

    @AsyncStatus.wrap
    async def unstage(self) -> None:
        await self.controller.unstage(self)


def set_and_check_exact(signal, value):
    # TODO: instead of doing this at the plan level it would be better to do it at the device level.
    # Get set() on the common dae signals which should be "exactly" settable to do this themselves.
    # Should in principle be easy enough with a thin wrapper...
    yield from bps.abs_set(signal, value, wait=True)
    actual = yield from bps.rd(signal)
    if value != actual:
        raise IOError(f"Signal {signal.name} failed to set to value {value} (actual: {actual})")


def plan():
    mot = block_rw(float, "mot")

    # Don't love having to have both, come up with something better.
    pv_prefix = get_pv_prefix()
    dae_prefix = pv_prefix + "DAE:"

    dae = SimpleDae(
        prefix=pv_prefix,
        name="DAE",
        controller=PeriodPerPointController(save_run=False),
        waiter=PeriodGoodFramesWaiter(frames=200),
        reducer=SingleSpectrumByPeriodGoodFramesReducer(dae_prefix=dae_prefix, spectrum=1),
    )

    # Can support renaming any signal to something more scientist-friendly than the defaults, at
    # plan level. I *think* this is right, the exact interpretation of each signal may depend on
    # how it's being used.
    dae.reducer.intensity.set_name("my_intensity")

    yield from ensure_connected(dae, mot)
    num_points = 15

    # TODO: should this be the responsibility of the controller instead? If so
    # how does it get passed into the controller - via prepare() or similar?
    yield from set_and_check_exact(dae.number_of_periods, num_points)

    yield from bp.scan([dae], mot, 1, 3, num=num_points)


if __name__ == "__main__":
    RE = get_run_engine()
    RE(
        plan(),
        [
            LiveTable(
                [
                    "mot",
                    "DAE-period_num",
                    "DAE-period-good_frames",
                    "DAE-reducer-raw_counts",
                    "my_intensity",
                ]
            ),
        ],
    )
