"""The Python SDK for writing a sima program.

A program plugs into sima by speaking two small protocols over its own stdin
and stdout; ``docs/protocol.md`` in the sima repository is that contract, and
this package is one client of it. The Rust SDK, ``sima-api``, is the other.

A whole program is three things:

- a :class:`Domain`, which answers everything one format id binds — its
  environment, its devices, the translation of its ``[search.params]`` section,
  and the :class:`Executor` that evaluates a candidate;
- one or more :class:`Generator` objects, each a way of choosing candidates for
  that format;
- a call to :func:`serve`, which reads the role from the process arguments and
  answers whichever conversation the search opened.

``examples/stepper-py/`` in the sima repository is a complete program written
against this package.
"""

from .encode import Dec, Enc, EncodingError
from .frame import MAX_PAYLOAD, TransportError, read_frame, write_frame
from .model import (
    Artifact,
    Checkpoint,
    Completed,
    DeviceBinding,
    DeviceInfo,
    DeviceType,
    Domain,
    Environment,
    EnvironmentComponent,
    ExecutionContext,
    Executor,
    Failed,
    Generator,
    NoCheckpoint,
    Outcome,
    Rejected,
    Spec,
    Stats,
    TaskInput,
)
from .serve import PROTOCOL_VERSION, serve

#: Artifact name under which a segmented executor commits its continuation
#: state. The next segment receives that artifact's bytes as its input state,
#: so the chain walks committed state hop by hop.
STATE_ARTIFACT = "state"

__all__ = [
    "Artifact",
    "Checkpoint",
    "Completed",
    "Dec",
    "DeviceBinding",
    "DeviceInfo",
    "DeviceType",
    "Domain",
    "Enc",
    "EncodingError",
    "Environment",
    "EnvironmentComponent",
    "ExecutionContext",
    "Executor",
    "Failed",
    "Generator",
    "MAX_PAYLOAD",
    "NoCheckpoint",
    "Outcome",
    "PROTOCOL_VERSION",
    "Rejected",
    "STATE_ARTIFACT",
    "Spec",
    "Stats",
    "TaskInput",
    "TransportError",
    "read_frame",
    "serve",
    "write_frame",
]
