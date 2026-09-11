"""Public SDR++ RigCTL application-adapter seam.

The implementation remains Qt-free in :mod:`freqinout.core.sdrpp_rigctl_receiver`.
This narrow module keeps receiver application bridges alongside the existing
radio-interface clients while exposing the clear SDR++ name used in setup and
tests.
"""

from __future__ import annotations

import socket
import time
from typing import Callable

from freqinout.core.receiver_control import ReceiverIdentity
from freqinout.core.sdrpp_rigctl_receiver import (
    SDRPP_RIGCTL_DEFAULT_PORT,
    SdrppRigctlReceiverControl,
)


class SDRPlusPlusRigctlAdapter(SdrppRigctlReceiverControl):
    """Receive-only adapter for SDR++'s selected RigCTL VFO.

    ``timeout_s`` bounds each short-lived socket exchange; the caller's
    absolute deadline can only shorten that interval.
    """

    def __init__(
        self,
        *,
        host: str,
        port: int = SDRPP_RIGCTL_DEFAULT_PORT,
        identity: ReceiverIdentity,
        timeout_s: float = 0.75,
        socket_factory: Callable[..., socket.socket] = socket.create_connection,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        super().__init__(
            identity,
            host=host,
            port=port,
            timeout_s=timeout_s,
            socket_factory=socket_factory,
            monotonic=monotonic,
        )


__all__ = ["SDRPlusPlusRigctlAdapter"]
