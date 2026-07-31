"""The vocabulary a program exchanges with sima, and the contracts it implements.

Two groups live here. The **values** are what messages carry: identities, the
environment, a candidate spec, a task's inputs, and the outcome of an attempt.
The **contracts** are the four abstract bases a program fills in — a
:class:`Domain` and its :class:`Generator` answer what a format binds, the
:class:`Executor` the domain builds evaluates one candidate, and the
:class:`Checkpoint` handle is the resume channel it evaluates under.

Only the values that are hashed carry a canonical encoding: an
:class:`Environment` and a :class:`Spec`. Everything else is either message
layout, which belongs to the protocol, or observational, which is never hashed
at all.
"""

from __future__ import annotations

import re
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import IntEnum

from .encode import Dec, Enc, EncodingError

__all__ = [
    "Artifact",
    "Checkpoint",
    "Completed",
    "DeviceBinding",
    "DeviceInfo",
    "DeviceType",
    "Domain",
    "Environment",
    "EnvironmentComponent",
    "ExecutionContext",
    "Executor",
    "Failed",
    "Generator",
    "NoCheckpoint",
    "Outcome",
    "Rejected",
    "Spec",
    "Stats",
    "TaskInput",
    "validate_device_class",
    "validate_name",
]

#: Domain tag opening a canonical :class:`Spec` encoding.
TAG_SPEC = "sima.spec.v1"
#: Domain tag opening a canonical :class:`Environment` encoding.
TAG_ENVIRONMENT = "sima.environment.v1"

#: Arm byte marking a component carrying a version string.
_ARM_VERSION = 0
#: Arm byte marking a component carrying a digest.
_ARM_DIGEST = 1

_NAME = re.compile(r"^[a-z0-9._-]{1,64}$")
_DEVICE_CLASS = re.compile(r"^[a-z0-9._:-]{1,64}$")


def validate_name(name: str) -> str:
    """The name, refused when it is outside the shared name rule.

    Format ids, generator ids, environment component names, and artifact names
    are 1 to 64 bytes of ``[a-z0-9._-]``. Lowercase-only keeps one spelling per
    identity.
    """
    if not _NAME.match(name):
        raise ValueError(f"name {name!r} is not 1 to 64 bytes of [a-z0-9._-]")
    return name


def validate_device_class(clazz: str) -> str:
    """The device class, refused when it is outside the class rule.

    A class is 1 to 64 bytes of ``[a-z0-9._:-]`` — the colon is what separates
    it from the shared name rule, so a class minted from configuration-space
    identifiers is spelled as its backend mints it.
    """
    if not _DEVICE_CLASS.match(clazz):
        raise ValueError(f"device class {clazz!r} is not 1 to 64 bytes of [a-z0-9._:-]")
    return clazz


class DeviceType(IntEnum):
    """The category of a device, as its wire tag."""

    DISCRETE = 0
    INTEGRATED = 1
    VIRTUAL = 2
    CPU = 3
    OTHER = 4


@dataclass(frozen=True)
class DeviceInfo:
    """One device a format's work can run on, as the program enumerates it."""

    #: The device class. Named ``clazz`` because ``class`` is a keyword.
    clazz: str
    #: The device's name, as the execution backend reports it.
    name: str
    device_type: DeviceType
    #: The position within the class, in the backend's enumeration order.
    member: int

    def __post_init__(self) -> None:
        validate_device_class(self.clazz)


@dataclass(frozen=True)
class DeviceBinding:
    """The device a worker's executor is to be built for: a class and a member."""

    clazz: str
    member: int

    def __post_init__(self) -> None:
        validate_device_class(self.clazz)


@dataclass(frozen=True)
class Spec:
    """One candidate: the format it belongs to and its opaque bytes."""

    format: str
    candidate: bytes

    def __post_init__(self) -> None:
        validate_name(self.format)

    def encode(self, enc: Enc) -> None:
        """Appends the tagged canonical form: tag, format, candidate bytes."""
        enc.str(TAG_SPEC).str(self.format).bytes(self.candidate)


@dataclass(frozen=True)
class EnvironmentComponent:
    """A named environment component, carrying a version string or a digest.

    Exactly one of the two is given. A version is an engine or executor
    identity constant; a digest is the content hash of a build input the
    results depend on, as its 32 raw bytes.
    """

    name: str
    version: str | None = None
    digest: bytes | None = None

    def __post_init__(self) -> None:
        validate_name(self.name)
        if (self.version is None) == (self.digest is None):
            raise ValueError(
                f"environment component {self.name!r} carries a version or a digest, not both"
            )
        if self.version is not None and not self.version:
            raise ValueError(f"environment component {self.name!r} has an empty version string")

    def encode(self, enc: Enc) -> None:
        """Appends the component: name, arm byte, then the arm's payload."""
        enc.str(self.name)
        if self.version is not None:
            enc.u8(_ARM_VERSION).str(self.version)
        else:
            enc.u8(_ARM_DIGEST).hash(self.digest or b"")


@dataclass(frozen=True)
class Environment:
    """What a format's results depend on: a non-empty list of components.

    Components are held sorted by unique name, so equal environments have equal
    bytes regardless of the order they were built in. Content-derived values
    only: anything machine-derived — hostname, device, driver, path, time — is
    journal metadata and never a component, because two machines with equal
    environments must produce equal results.
    """

    components: tuple[EnvironmentComponent, ...]

    def __post_init__(self) -> None:
        ordered = tuple(sorted(self.components, key=lambda c: c.name))
        if not ordered:
            raise ValueError("environment must have at least one component")
        for earlier, later in zip(ordered, ordered[1:]):
            if earlier.name == later.name:
                raise ValueError(f"duplicate environment component name {later.name!r}")
        # Sorting is what makes equal environments equal bytes, so the sorted
        # order replaces whatever the caller passed.
        object.__setattr__(self, "components", ordered)

    def encode(self, enc: Enc) -> None:
        """Appends the tagged canonical form: tag, count, components in order."""
        enc.str(TAG_ENVIRONMENT).u64(len(self.components))
        for component in self.components:
            component.encode(enc)

    @staticmethod
    def decode(dec: Dec) -> Environment:
        """Reads a canonical form written by :meth:`encode`."""
        tag = dec.str()
        if tag != TAG_ENVIRONMENT:
            raise EncodingError(f"domain tag mismatch: expected {TAG_ENVIRONMENT!r}, found {tag!r}")
        count = dec.u64()
        components = []
        for _ in range(count):
            name = dec.str()
            arm = dec.u8()
            if arm == _ARM_VERSION:
                components.append(EnvironmentComponent(name, version=dec.str()))
            elif arm == _ARM_DIGEST:
                components.append(EnvironmentComponent(name, digest=dec.hash()))
            else:
                raise EncodingError(f"invalid environment value arm byte {arm}, expected 0 or 1")
        return Environment(tuple(components))


@dataclass(frozen=True)
class Artifact:
    """A named blob an attempt commits. Its bytes are a pure function of the
    task's identity-bearing inputs."""

    name: str
    bytes: bytes

    def __post_init__(self) -> None:
        validate_name(self.name)


@dataclass(frozen=True)
class Stats:
    """Observational statistics: named scalars plus an opaque family blob.

    Observational only — never identity-bearing — so they may reflect the
    execution context. A non-finite scalar says the candidate diverged.
    """

    scalars: tuple[tuple[str, float], ...] = ()
    blob: bytes = b""


@dataclass(frozen=True)
class Completed:
    """The candidate evaluated: the committed artifacts and the attempt's stats."""

    artifacts: tuple[Artifact, ...] = ()
    stats: Stats = field(default_factory=Stats)


@dataclass(frozen=True)
class Failed:
    """A transient failure, which sima may retry. The reason is observational."""

    reason: str
    stats: Stats = field(default_factory=Stats)


@dataclass(frozen=True)
class Rejected:
    """A definitive failure: the candidate cannot produce a result, and the task
    is never retried. The reason is observational."""

    reason: str
    stats: Stats = field(default_factory=Stats)


#: What one evaluation attempt returns.
Outcome = Completed | Failed | Rejected


@dataclass(frozen=True)
class TaskInput:
    """The identity-bearing inputs of one evaluation.

    Every field here determines the task's key and its committed artifacts:
    the candidate, the run's params bytes, the seed, the environment id this
    task runs under, and — for a segment — the loaded bytes of the state the
    previous segment committed.
    """

    spec: Spec
    params: bytes
    seed: int
    #: The 32-byte environment id, carried and compared, never recomputed.
    environment: bytes
    input_state: bytes | None = None


@dataclass(frozen=True)
class ExecutionContext:
    """The per-attempt facts: visible to the executor, forbidden from
    influencing any committed artifact. They may reach stats and logs, and may
    gate a retryable failure."""

    #: Zero-based attempt number: 0 is the first try.
    attempt: int
    #: The worker slot running this attempt.
    worker: int


class Checkpoint(ABC):
    """The resume channel of one attempt.

    The executor decides what bytes capture its continuation state and when
    offering them is safe; the handle decides whether an offer is written and
    performs all the I/O. An executor never touches a store.
    """

    @abstractmethod
    def resume(self) -> bytes | None:
        """Bytes a previous attempt of this same task saved, if any survive.

        The executor validates them itself and starts fresh when they do not
        apply: a resumed and a fresh evaluation must commit byte-identical
        artifacts.
        """

    @abstractmethod
    def offer(self, produce) -> None:
        """Offers continuation state at a boundary where resuming is safe.

        ``produce`` is a callable returning the state bytes. The handle calls
        it only when it decides to save, so serialization costs nothing when no
        save is due.
        """


class NoCheckpoint(Checkpoint):
    """The inert handle: nothing to resume, offers ignored. What a task that
    does not checkpoint runs under."""

    def resume(self) -> bytes | None:
        return None

    def offer(self, produce) -> None:
        return None


class Executor(ABC):
    """Pure compute over one candidate, built by a :class:`Domain` for a device."""

    @abstractmethod
    def execute(
        self, task: TaskInput, context: ExecutionContext, checkpoint: Checkpoint
    ) -> Outcome:
        """Evaluates one candidate, returning one of the three outcomes.

        Raising instead reports a panic: sima journals a diagnostic and
        rejects the task definitively, so no retry follows.
        """


class Domain(ABC):
    """Everything one format id binds."""

    @abstractmethod
    def format(self) -> str:
        """The format id this domain serves. One program serves one format."""

    @abstractmethod
    def environment(self) -> Environment:
        """The environment this format's results depend on."""

    @abstractmethod
    def enumerate_devices(self) -> list[DeviceInfo]:
        """The devices this format's work can run on; empty when it opens none."""

    @abstractmethod
    def translate_config(self, toml: str, segmented: bool) -> bytes:
        """Translates the run's ``[run.params]`` section into canonical params
        bytes. ``segmented`` says whether the run divides each candidate's
        evaluation into a chain, so a domain can refuse a combination it does
        not support. Raising refuses the configuration, naming what is wrong.
        """

    @abstractmethod
    def executor(self, device: DeviceBinding | None) -> Executor:
        """Builds the executor for ``device``, or for the domain's own default
        selection when the parent named none."""

    def device_desc(self, device: DeviceBinding | None) -> tuple[str, str]:
        """The name and driver version of the device the executor opened, which
        sima journals verbatim. A domain that opens no device answers both
        empty, which is the default here."""
        return ("", "")


class Generator(ABC):
    """One way of choosing candidates for a format. A format has one executor
    and may have many generators."""

    @abstractmethod
    def id(self) -> str:
        """The generator id, which a run names in ``[run.generator]``."""

    @abstractmethod
    def format(self) -> str:
        """The format id of every spec this generator produces."""

    @abstractmethod
    def translate_config(self, toml: str) -> bytes:
        """Translates the ``[run.generator]`` section, minus its ``id`` key,
        into this generator's opaque params blob."""

    @abstractmethod
    def generate(self, root_seed: int, params: bytes) -> list[Spec]:
        """The run's candidates. One root seed always yields the same specs, in
        the same order."""
