import pytest
from unittest.mock import MagicMock
import subprocess


@pytest.fixture
def mock_subprocess():
    mock = MagicMock(spec=subprocess)
    return mock
