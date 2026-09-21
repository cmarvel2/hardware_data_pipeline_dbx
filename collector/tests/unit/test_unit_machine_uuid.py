import sys
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from pytest_mock import MockerFixture

from local_data_collection.utils import machine_uuid

pytestmark = pytest.mark.unit

VALID_GUID = "12345678-1234-5678-1234-567812345678"


@pytest.fixture
def registry(mocker: MockerFixture, monkeypatch: pytest.MonkeyPatch) -> SimpleNamespace:
    fake_registry = SimpleNamespace(
        HKEY_LOCAL_MACHINE=Mock(),
        KEY_READ=Mock(),
        OpenKey=mocker.Mock(),
        QueryValueEx=mocker.Mock(),
        CloseKey=mocker.Mock(),
    )
    monkeypatch.setitem(sys.modules, "winreg", fake_registry)
    return fake_registry


def test_get_windows_uuid_reads_machine_guid_and_returns_canonical_uuid(registry: SimpleNamespace) -> None:
    key_handle = Mock()
    registry.OpenKey.return_value = key_handle
    registry.QueryValueEx.return_value = (f"{{{VALID_GUID.upper()}}}", None)

    result = machine_uuid.get_windows_uuid()

    assert result == VALID_GUID
    registry.QueryValueEx.assert_called_once_with(key_handle, "MachineGuid")
    registry.CloseKey.assert_called_once_with(key_handle)


def test_get_windows_uuid_raises_value_error_for_invalid_registry_value(registry) -> None:
    registry.OpenKey.return_value = Mock()
    registry.QueryValueEx.return_value = ("not-a-uuid", None)

    with pytest.raises(
        ValueError,
        match="badly formed hexadecimal UUID string",
    ):
        machine_uuid.get_windows_uuid()


def test_get_windows_uuid_propagates_registry_read_error_without_closing_key(
    registry,
) -> None:
    key_handle = Mock()
    registry.OpenKey.return_value = key_handle
    registry.QueryValueEx.side_effect = OSError("registry unavailable")

    with pytest.raises(OSError, match="registry unavailable"):
        machine_uuid.get_windows_uuid()

    registry.CloseKey.assert_not_called()
