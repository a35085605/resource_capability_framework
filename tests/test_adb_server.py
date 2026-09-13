from __future__ import annotations

import subprocess
import sys
import unittest

from _managed.snapshot import LifecyclePhase
from api.adb_server import AdbServerPhase


class AdbServerApiTests(unittest.TestCase):
    def test_adb_server_phase_is_lifecycle_phase_alias(self) -> None:
        self.assertIs(AdbServerPhase, LifecyclePhase)
        self.assertIs(AdbServerPhase.ACTIVE, LifecyclePhase.ACTIVE)
        self.assertEqual(AdbServerPhase.ACTIVE.value, "active")
        self.assertEqual(AdbServerPhase.RELEASE_REQUIRED.value, "release_required")

    def test_importing_adb_api_does_not_load_implementation_modules(self) -> None:
        completed = subprocess.run(
            [
                sys.executable,
                "-c",
                (
                    "import sys; import api.adb_server; "
                    "forbidden = {'_managed.coordinator', '_resource.manager'}; "
                    "raise SystemExit(bool(forbidden.intersection(sys.modules)))"
                ),
            ],
            check=False,
        )

        self.assertEqual(completed.returncode, 0)


if __name__ == "__main__":
    unittest.main()
