from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
import uuid
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[2]
COMPOSE_FILE = PROJECT_ROOT / "docker-compose.contract-test.yaml"
CONTRACT_TIMEOUT_SECONDS = 600


def test_enterprise_contract_suite() -> None:
    """Build the API image and run the Enterprise suite in an isolated Compose project."""
    if shutil.which("docker") is None:
        pytest.fail("Docker CLI with the Compose plugin is required to run the contract suite")

    project_name = f"interfaces-contract-{uuid.uuid4().hex[:10]}"
    command_prefix = [
        "docker",
        "compose",
        "--file",
        str(COMPOSE_FILE),
        "--project-name",
        project_name,
    ]

    env = os.environ.copy()
    configured_license_directory = Path(os.getenv("SPECMATIC_LICENSE_DIR", str(Path.home() / ".specmatic"))).expanduser()
    with tempfile.TemporaryDirectory(prefix="specmatic-license-") as empty_license_directory:
        license_directory = (
            configured_license_directory
            if configured_license_directory.is_dir()
            else Path(empty_license_directory)
        )

        env["SPECMATIC_LICENSE_DIR"] = str(license_directory.resolve())
        try:
            result = subprocess.run(
                [
                    *command_prefix,
                    "up",
                    "--build",
                    "--abort-on-container-exit",
                    "--exit-code-from",
                    "specmatic",
                    "specmatic",
                ],
                cwd=PROJECT_ROOT,
                env=env,
                timeout=CONTRACT_TIMEOUT_SECONDS,
                check=False,
            )
        except subprocess.TimeoutExpired as error:
            pytest.fail(f"Specmatic contract suite exceeded {CONTRACT_TIMEOUT_SECONDS} seconds: {error}")
        finally:
            subprocess.run(
                [*command_prefix, "down", "--volumes", "--remove-orphans", "--timeout", "10"],
                cwd=PROJECT_ROOT,
                env=env,
                capture_output=True,
                text=True,
                check=False,
            )

    assert result.returncode == 0, "Specmatic contract suite failed. See the Compose output above."
