from pathlib import Path

import pytest

_INTEGRATION_DIR = Path(__file__).parent / "integration"


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    for item in items:
        if _INTEGRATION_DIR in item.path.parents:
            item.add_marker(pytest.mark.integration)
