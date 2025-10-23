"""
This module provides APIs to manage the setup of flows.
"""

from . import setting
from . import _engine  # type: ignore
from .runtime import execution_context


class SetupChangeBundle:
    """
    This class represents a bundle of setup changes.
    """

    _engine_bundle: _engine.SetupChangeBundle

    def __init__(self, _engine_bundle: _engine.SetupChangeBundle):
        self._engine_bundle = _engine_bundle

    def __str__(self) -> str:
        desc, _ = execution_context.run(self._engine_bundle.describe_async())
        return desc  # type: ignore

    def __repr__(self) -> str:
        return self.__str__()

    def apply(self, report_to_stdout: bool = False) -> None:
        """
        Apply the setup changes.
        """
        execution_context.run(self.apply_async(report_to_stdout=report_to_stdout))

    async def apply_async(self, report_to_stdout: bool = False) -> None:
        """
        Apply the setup changes. Async version of `apply`.
        """
        await self._engine_bundle.apply_async(report_to_stdout=report_to_stdout)

    def describe(self) -> tuple[str, bool]:
        """
        Describe the setup changes.
        """
        return execution_context.run(self.describe_async())  # type: ignore

    async def describe_async(self) -> tuple[str, bool]:
        """
        Describe the setup changes. Async version of `describe`.
        """
        return await self._engine_bundle.describe_async()  # type: ignore

    def describe_and_apply(self, report_to_stdout: bool = False) -> None:
        """
        Describe the setup changes and apply them if `report_to_stdout` is True.
        Silently apply setup changes otherwise.
        """
        execution_context.run(
            self.describe_and_apply_async(report_to_stdout=report_to_stdout)
        )

    async def describe_and_apply_async(self, *, report_to_stdout: bool = False) -> None:
        """
        Describe the setup changes and apply them if `report_to_stdout` is True.
        Silently apply setup changes otherwise. Async version of `describe_and_apply`.
        """
        if report_to_stdout:
            desc, is_up_to_date = await self.describe_async()
            print("Setup status:\n")
            print(desc)
            if is_up_to_date:
                print("No setup changes to apply.")
                return
        await self.apply_async(report_to_stdout=report_to_stdout)


def flow_names_with_setup() -> list[str]:
    """
    Get the names of all flows that have been setup.
    """
    return execution_context.run(flow_names_with_setup_async())  # type: ignore


async def flow_names_with_setup_async() -> list[str]:
    """
    Get the names of all flows that have been setup. Async version of `flow_names_with_setup`.
    """
    result = []
    all_flow_names = await _engine.flow_names_with_setup_async()
    for name in all_flow_names:
        app_namespace, name = setting.split_app_namespace(name, ".")
        if app_namespace == setting.get_app_namespace():
            result.append(name)
    return result
