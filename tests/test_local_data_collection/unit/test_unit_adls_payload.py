from datetime import datetime, timezone
from uuid import UUID

import pytest

from local_data_collection.sensor_polling import adls_payload
from local_data_collection.sensor_polling.models import (
    HardwareDataStorage,
    HardwarePayload,
)

pytestmark = pytest.mark.unit


@pytest.fixture
def payload() -> HardwarePayload:
    return HardwarePayload(
        metadata={"test_id": "unit-test-id"},
        snapshots=[
            HardwareDataStorage(
                timestamp="20260906_154000123",
                hardware_name="Test hardware /test/0",
                sensor_name="Test sensor",
                sensor_type="Temperature",
                sensor_value=42.5,
            ),
        ],
    )


def test_payload_buffer_initializes_metadata_once_and_appends_snapshots(mocker) -> None:
    load_conf = mocker.patch.object(
        adls_payload.config_loader,
        "load_conf",
        return_value={"occt": {"stability_test": {"length_in_minutes": 1}}},
    )
    get_uuid = mocker.patch.object(
        adls_payload.machine_uuid,
        "get_windows_uuid",
        return_value="device-id",
    )
    buffer = adls_payload.UploadMemory()
    cpu_snapshot = HardwareDataStorage(None, "cpu", "load", "Load", 50.0)

    buffer.payload_buffer_add(
        filename="occt_software.yml",
        cpu_payload=[cpu_snapshot],
        gpu_payload=[],
        memory_payload=[],
        test_id="test-id",
    )
    buffer.payload_buffer_add(
        filename="occt_software.yml",
        cpu_payload=[],
        gpu_payload=[],
        memory_payload=[],
        test_id="test-id",
    )

    assert buffer.payload.metadata["device_id"] == "device-id"
    assert buffer.payload.metadata["test_id"] == "test-id"
    assert buffer.payload.snapshots == [cpu_snapshot]
    assert buffer.count == 2
    load_conf.assert_called_once_with("occt_software.yml")
    get_uuid.assert_called_once()


@pytest.mark.parametrize(
    ("snapshot_count", "limit", "expected"),
    [
        (0, 1, False),
        (1, 1, True),
        (2, 1, True),
    ],
)
def test_check_buffer_returns_whether_limit_is_reached(
    snapshot_count: int,
    limit: int,
    expected: bool,
) -> None:
    buffer = adls_payload.UploadMemory()
    buffer.payload.snapshots = [
        HardwareDataStorage(None, "hw", "sensor", "Load", 1.0)
        for _ in range(snapshot_count)
    ]

    assert buffer.check_buffer(limit) is expected


def test_clear_buffer_removes_snapshots_and_resets_count(
    payload: HardwarePayload,
) -> None:
    buffer = adls_payload.UploadMemory()
    buffer.payload = payload
    buffer.count = 1

    buffer.clear_buffer()

    assert buffer.payload.snapshots == []
    assert buffer.count == 0


def test_adls_credentials_returns_test_mapping(mocker) -> None:
    credential = mocker.sentinel.credential
    environment = {
        "AZURE_CLIENT_ID": "client-id",
        "AZURE_TENANT_ID": "tenant-id",
        "AZURE_CLIENT_SECRET": "client-secret",
        "TEST_LANDING_CONTAINER": "test-landing",
        "TEST_AZURE_STORAGE_URL": "https://test.example.net",
    }

    mocker.patch.object(adls_payload, "find_dotenv", return_value=".env")
    mocker.patch.object(adls_payload, "load_dotenv")
    mocker.patch.object(
        adls_payload.os,
        "getenv",
        side_effect=lambda name: environment.get(name),
    )
    client_credential = mocker.patch.object(
        adls_payload,
        "ClientSecretCredential",
        return_value=credential,
    )

    actual_credential, mapping = adls_payload.adls_credentials("test")

    assert actual_credential is credential
    assert mapping == {
        "CONTAINER": "test-landing",
        "STORAGE_URL": "https://test.example.net",
    }
    client_credential.assert_called_once_with(
        client_id="client-id",
        tenant_id="tenant-id",
        client_secret="client-secret",
    )


def test_upload_blob_completes_and_uploads_json(
    mocker,
    payload: HardwarePayload,
) -> None:
    blob_client = mocker.Mock()
    container_client = mocker.Mock()
    container_client.get_blob_client.return_value = blob_client
    service_client = mocker.Mock()
    service_client.get_container_client.return_value = container_client

    mocker.patch.object(
        adls_payload,
        "BlobServiceClient",
        return_value=service_client,
    )
    mocker.patch.object(
        adls_payload.uuid,
        "uuid4",
        return_value=UUID("12345678-1234-5678-1234-567812345678"),
    )

    fixed_time = datetime(2026, 9, 6, 15, 40, 0, tzinfo=timezone.utc)
    mocker.patch.object(adls_payload, "datetime", wraps=datetime)
    adls_payload.datetime.now.return_value = fixed_time

    result = adls_payload.upload_blob(
        credentials=mocker.sentinel.credential,
        env_mapping={
            "CONTAINER": "landing",
            "STORAGE_URL": "https://storage.example.net",
        },
        sensor_buffer=payload,
    )

    expected_blob_name = (
        "ingest_date=2026-09-06/"
        "hardware_sensor_data_20260906_154000_"
        "12345678-1234-5678-1234-567812345678.json"
    )

    assert result is None
    service_client.get_container_client.assert_called_once_with(container="landing")
    container_client.get_blob_client.assert_called_once_with(blob=expected_blob_name)
    blob_client.upload_blob.assert_called_once()


def test_upload_blob_propagates_storage_failure(
    mocker,
    payload: HardwarePayload,
) -> None:
    mocker.patch.object(
        adls_payload,
        "BlobServiceClient",
        side_effect=OSError("network unavailable"),
    )

    with pytest.raises(OSError, match="network unavailable"):
        adls_payload.upload_blob(
            credentials=mocker.sentinel.credential,
            env_mapping={
                "CONTAINER": "landing",
                "STORAGE_URL": "https://storage.example.net",
            },
            sensor_buffer=payload,
        )