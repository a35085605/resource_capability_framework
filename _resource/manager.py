from __future__ import annotations

from typing import Generic, Protocol, TypeVar

from _resource.driver import (
    PhysicalAcquired,
    PhysicalFailed,
    PhysicalResourceSet,
    ResourceDriver,
)
from _resource.result import ResourceAcquireResult, ResourceFailed, ResourceReady


RequestT = TypeVar("RequestT")
RequirementT = TypeVar("RequirementT")
PhysicalResourceT = TypeVar("PhysicalResourceT")


class ResourceRequirementsModel(Protocol[RequestT, RequirementT]):
    """Resolve only the physical-resource requirements for one Request."""

    def requirements(self, request: RequestT) -> tuple[RequirementT, ...]: ...


class ResourceManagement(Protocol[RequestT, PhysicalResourceT]):
    """Managed-facing boundary for synchronous physical-resource I/O."""

    def acquire(self, request: RequestT) -> ResourceAcquireResult[PhysicalResourceT]: ...

    def release(self, resources: PhysicalResourceSet[PhysicalResourceT]) -> None: ...


class ResourceManager(Generic[RequestT, RequirementT, PhysicalResourceT]):
    """Acquire and clean up physical resources without owning lifecycle state."""

    def __init__(
        self,
        requirements_model: ResourceRequirementsModel[RequestT, RequirementT],
        driver: ResourceDriver[RequirementT, PhysicalResourceT],
    ) -> None:
        self._requirements_model = requirements_model
        self._driver = driver

    @property
    def requirements_model(self) -> ResourceRequirementsModel[RequestT, RequirementT]:
        return self._requirements_model

    @property
    def driver(self) -> ResourceDriver[RequirementT, PhysicalResourceT]:
        return self._driver

    def acquire(self, request: RequestT) -> ResourceAcquireResult[PhysicalResourceT]:
        """Synchronously acquire all requirements and retain partial resources on failure."""

        if request is None:
            raise TypeError("request cannot be None")

        try:
            requirements = self._requirements_model.requirements(request)
        except BaseException as exc:
            return ResourceFailed(exc, ())

        if not isinstance(requirements, tuple):
            return ResourceFailed(
                TypeError("ResourceRequirementsModel.requirements() must return a tuple"),
                (),
            )

        resources: PhysicalResourceSet[PhysicalResourceT] = ()
        for requirement in requirements:
            try:
                outcome = self._driver.acquire(requirement)
            except BaseException as exc:
                return ResourceFailed(exc, resources)

            if not isinstance(outcome, (PhysicalAcquired, PhysicalFailed)):
                return ResourceFailed(
                    TypeError("ResourceDriver.acquire() must return a PhysicalAcquireOutcome"),
                    resources,
                )
            if not isinstance(outcome.resources, tuple):
                return ResourceFailed(
                    TypeError("physical outcome resources must be a PhysicalResourceSet tuple"),
                    resources,
                )

            resources += outcome.resources
            if isinstance(outcome, PhysicalFailed):
                if not isinstance(outcome.error, Exception):
                    return ResourceFailed(
                        TypeError("PhysicalFailed.error must be an Exception"),
                        resources,
                    )
                return ResourceFailed(outcome.error, resources)

        return ResourceReady(resources)

    def release(self, resources: PhysicalResourceSet[PhysicalResourceT]) -> None:
        """Synchronously clean up resources; an empty resource set is already clean."""

        if not isinstance(resources, tuple):
            raise TypeError("resources must be a PhysicalResourceSet tuple")
        if resources:
            self._driver.cleanup(resources)


__all__ = [
    "ResourceManagement",
    "ResourceManager",
    "ResourceRequirementsModel",
]
