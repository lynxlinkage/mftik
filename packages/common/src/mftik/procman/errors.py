"""Values this layer already refuses.

``NotImplementedError("IF-03")`` is the stub for behaviour B3 implements.
These exceptions are for inputs that are already illegal: a worker id that
cannot live under ``run/``, a spec §4.3 cannot describe, a state-machine
edge the diagram does not have, or a frame that is not the protocol.
"""


class ProcmanError(Exception):
    """Base for values this layer refuses."""


class InvalidWorkerId(ProcmanError):
    """The id cannot be a relative path under ``${WORK_DIR}/run``."""


class InvalidWorkerSpec(ProcmanError):
    """The spec's fields contradict §4.3."""


class InvalidTransition(ProcmanError):
    """The state machine has no such edge (§4.3)."""


class MessageError(ProcmanError):
    """A frame is not the protocol, or an exit record contradicts itself."""
