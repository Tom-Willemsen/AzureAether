import asyncio
from typing import Callable, Dict, Mapping, Sequence, TypeVar

import bluesky.plans as bp
from bluesky.callbacks import LiveTable
from bluesky.protocols import HasHints, HasName, Hints, Readable, Reading, Triggerable
from event_model import DataKey
from ibex_bluesky_core.devices import get_pv_prefix
from ibex_bluesky_core.devices.block import block_rw
from ophyd_async.core import AsyncStatus
from ophyd_async.core._protocol import AsyncStageable
from ophyd_async.plan_stubs import ensure_connected

from azureaether.simpledae import SimpleDae

T = TypeVar("T")


class Derived(Readable, HasHints, HasName, AsyncStageable, Triggerable):
    parent = None

    def __init__(
        self,
        name,
        *,
        inputs: Sequence[Readable],
        derive_value: Callable,
        derive_extra: Mapping[str, Callable] | None = None,
        trigger: Sequence[Triggerable] | None = None,
        stage: Sequence[AsyncStageable] | None = None,
    ):
        """
        A derived readable. When read, it gets a value by reading all of the underlying inputs
        and applying the derive_value function to them.
        """
        self._name = name

        if len(inputs) == 0:
            raise ValueError("Need at least one input to a derived signal")
        self._inputs = inputs
        self._derive_value = derive_value
        self._derive_extra = derive_extra or {}

        self._to_trigger = trigger or []
        self._to_stage = stage or []

    @property
    def name(self) -> str:
        return self._name

    async def read(self) -> dict[str, Reading]:
        readings = await asyncio.gather(*[dev.read() for dev in self._inputs])
        values = [reading[dev.name]["value"] for reading, dev in zip(readings, self._inputs)]

        # Timestamp of a derived signal is the timestamp of the most-recently updated source signal
        timestamp = max(
            [reading[dev.name]["timestamp"] for reading, dev in zip(readings, self._inputs)]
        )

        reading = {
            self.name: {
                "value": self._derive_value(*values),
                "timestamp": timestamp,
            }
        }
        for name, func in self._derive_extra.items():
            reading[f"{self.name}-{name}"] = {"value": func(*values), "timestamp": timestamp}
        return reading

    async def describe(self) -> Dict[str, DataKey]:
        _number_descriptor = {
            "dtype": "number",
            "shape": [],
            "source": f"derived[{','.join(d.name for d in self._inputs)}]",
        }
        descriptor = {
            self.name: _number_descriptor,
        }
        for name in self._derive_extra:
            descriptor[f"{self.name}-{name}"] = _number_descriptor
        return descriptor

    @property
    def hints(self) -> Hints:
        return {"fields": [self.name]}

    @AsyncStatus.wrap
    async def trigger(self) -> None:
        for t in self._to_trigger:
            await t.trigger()

    @AsyncStatus.wrap
    async def stage(self) -> None:
        for t in self._to_stage:
            await t.stage()

    @AsyncStatus.wrap
    async def unstage(self) -> None:
        for t in self._to_stage:
            await t.unstage()


def plan():
    dae = SimpleDae(get_pv_prefix())
    mot = block_rw(float, "mot")
    normalized_counts = Derived(
        "norm_counts",
        inputs=[dae, mot],
        derive_value=lambda d, m: d / m,
        derive_extra={
            "rawdae": lambda d, m: d,
            "rawmot": lambda d, m: m,
        },
        trigger=[dae],
        stage=[dae],
    )
    yield from ensure_connected(dae, mot)
    yield from bp.scan([normalized_counts], mot, 1, 2, 3)


if __name__ == "__main__":
    from ibex_bluesky_core.run_engine import get_run_engine

    RE = get_run_engine()
    RE(plan(), print)
    RE(plan(), LiveTable(["mot", "norm_counts", "norm_counts-rawdae"]))
