from __future__ import annotations

import unittest

from _managed.snapshot import ManagedPhase
from api.adb_server import AdbServerPhase


class AdbServerApiTests(unittest.TestCase):
    def test_adb_server_phase_is_managed_phase_alias(self) -> None:
        self.assertIs(AdbServerPhase, ManagedPhase)
        self.assertIs(AdbServerPhase.CURRENT, ManagedPhase.CURRENT)


if __name__ == "__main__":
    unittest.main()
