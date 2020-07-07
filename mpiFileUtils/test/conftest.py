import subprocess
from unittest.mock import MagicMock

import pytest


@pytest.fixture
def mock_subprocess():
    mock = MagicMock(spec=subprocess)
    return mock
