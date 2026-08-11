import hashlib
import json
from dataclasses import dataclass


SEED_POLICY_VERSION = "disjoint-namespaces-crn-v1"


@dataclass(frozen=True)
class SeedNamespace:
    name: str
    start: int
    stop_exclusive: int

    def __post_init__(self):
        if not self.name:
            raise ValueError("Seed namespace name cannot be empty.")
        if (
            isinstance(self.start, bool)
            or isinstance(self.stop_exclusive, bool)
            or not isinstance(self.start, int)
            or not isinstance(self.stop_exclusive, int)
            or self.start < 0
            or self.stop_exclusive <= self.start
            or self.stop_exclusive > 2**63
        ):
            raise ValueError("Seed namespace bounds must form a signed-64-bit range.")

    @property
    def size(self):
        return self.stop_exclusive - self.start

    def seeds(self, count, *, offset=0):
        if (
            isinstance(count, bool)
            or not isinstance(count, int)
            or count < 0
            or isinstance(offset, bool)
            or not isinstance(offset, int)
            or offset < 0
        ):
            raise ValueError("Seed count and offset must be non-negative integers.")
        if offset + count > self.size:
            raise ValueError(f"Seed namespace {self.name!r} is exhausted.")
        return tuple(range(self.start + offset, self.start + offset + count))

    def contains(self, seed):
        return self.start <= seed < self.stop_exclusive


@dataclass(frozen=True)
class SeedPolicy:
    version: str
    namespaces: tuple[SeedNamespace, ...]

    def __post_init__(self):
        if not self.version:
            raise ValueError("Seed policy version cannot be empty.")
        if not self.namespaces:
            raise ValueError("At least one seed namespace is required.")
        names = tuple(namespace.name for namespace in self.namespaces)
        if len(set(names)) != len(names):
            raise ValueError("Seed namespace names must be unique.")
        ordered = sorted(self.namespaces, key=lambda item: item.start)
        for left, right in zip(ordered, ordered[1:]):
            if left.stop_exclusive > right.start:
                raise ValueError(
                    f"Seed namespaces {left.name!r} and {right.name!r} overlap."
                )

    def namespace(self, name):
        try:
            return next(item for item in self.namespaces if item.name == name)
        except StopIteration as error:
            raise KeyError(f"Unknown seed namespace {name!r}.") from error

    def seeds(self, name, count, *, offset=0):
        return self.namespace(name).seeds(count, offset=offset)

    def deterministic_seed(self, name, *identity):
        """Map an identity reproducibly into one dedicated namespace."""
        namespace = self.namespace(name)
        encoded = json.dumps(
            identity,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode("utf-8")
        digest = hashlib.sha256(encoded).digest()
        offset = int.from_bytes(digest[:8], "big") % namespace.size
        return namespace.start + offset

    def to_dict(self):
        return {
            "version": self.version,
            "namespaces": [
                {
                    "name": namespace.name,
                    "start": namespace.start,
                    "stop_exclusive": namespace.stop_exclusive,
                }
                for namespace in self.namespaces
            ],
        }

    @classmethod
    def from_dict(cls, value):
        return cls(
            version=str(value["version"]),
            namespaces=tuple(
                SeedNamespace(
                    name=str(item["name"]),
                    start=int(item["start"]),
                    stop_exclusive=int(item["stop_exclusive"]),
                )
                for item in value["namespaces"]
            ),
        )


def default_seed_policy():
    return SeedPolicy(
        version=SEED_POLICY_VERSION,
        namespaces=(
            SeedNamespace("warmup_calibration", 100_000, 1_000_000),
            SeedNamespace("warmup_validation", 1_000_000, 10_000_000),
            SeedNamespace("measurement_calibration", 10_000_000, 100_000_000),
            SeedNamespace("optimization", 100_000_000, 200_000_000),
            SeedNamespace("bootstrap", 200_000_000, 1_000_000_000),
            SeedNamespace("final_validation", 1_000_000_000, 1_100_000_000),
        ),
    )
