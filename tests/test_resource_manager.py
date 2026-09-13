from __future__ import annotations

import unittest
from dataclasses import dataclass

from _resource.driver import RequirementAcquireFailed, RequirementAcquireSucceeded
from _resource.manager import ResolvedResourceProvider
from _resource.result import ResourceAcquireFailed


@dataclass(frozen=True)
class Requirement:
    name: str


class RequirementsResolver:
    def resolve(self, request: str) -> tuple[Requirement, ...]:
        return (Requirement("first"), Requirement("second"))


class PartialFailureDriver:
    def acquire(self, requirement: Requirement):
        if requirement.name == "first":
            return RequirementAcquireSucceeded(("r1",))
        return RequirementAcquireFailed(RuntimeError("second failed"), ("r2-partial",))

    def cleanup(self, resources: tuple[str, ...]) -> None:
        raise AssertionError("acquire must not roll back resources")


class ResolvedResourceProviderTests(unittest.TestCase):
    def test_partial_failure_contains_prior_and_current_partial_resources(self) -> None:
        provider = ResolvedResourceProvider(RequirementsResolver(), PartialFailureDriver())

        result = provider.acquire("request-a")

        self.assertIsInstance(result, ResourceAcquireFailed)
        self.assertEqual(result.resources, ("r1", "r2-partial"))


if __name__ == "__main__":
    unittest.main()
