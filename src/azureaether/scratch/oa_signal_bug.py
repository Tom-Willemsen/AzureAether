from bluesky.plan_stubs import close_run, open_run, trigger_and_read
from bluesky.plans import scan
from bluesky.run_engine import RunEngine
from ophyd_async.epics.signal import epics_signal_r, epics_signal_rw
from ophyd_async.plan_stubs import ensure_connected

RE = RunEngine()


def plan():
    sig1 = epics_signal_r(float, "TE:NDW2922:PARS:USER:R0", name="sig1")
    sig2 = epics_signal_rw(float, "TE:NDW2922:PARS:USER:R1", name="sig2")
    yield from ensure_connected(sig1, sig2)
    yield from scan([sig1], sig2, 0, 1, 2)


def simpler_plan():
    sig1 = epics_signal_r(float, "TE:NDW2922:PARS:USER:R0", name="sig1")
    sig2 = epics_signal_r(float, "TE:NDW2922:PARS:USER:R1", name="sig2")
    yield from ensure_connected(sig1, sig2)
    yield from open_run()
    yield from trigger_and_read([sig1, sig2])
    yield from close_run()


RE(plan(), print)
