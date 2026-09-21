import os
from uuid import uuid4

import pytest
from dotenv import find_dotenv, load_dotenv

from local_data_collection.sensor_polling.adls_payload import (
    adls_credentials,
    upload_blob,
)
from local_data_collection.sensor_polling.models import (
    HardwareDataStorage,
    HardwarePayload,
)

pytestmark = pytest.mark.integration


def require_azure_test_environment() -> None:
    load_dotenv(find_dotenv(), override=False)

    required_variables = (
        "AZURE_CLIENT_ID",
        "AZURE_TENANT_ID",
        "AZURE_CLIENT_SECRET",
        "TEST_LANDING_CONTAINER",
        "TEST_AZURE_STORAGE_URL",
    )
    missing = [name for name in required_variables if not os.getenv(name)]

    if missing:
        pytest.skip(
            "Azure integration credentials are unavailable: " + ", ".join(missing),
        )


def test_upload_blob_completes_for_test_storage_account() -> None:
    require_azure_test_environment()

    credentials, mapping = adls_credentials("test")
    payload = HardwarePayload(
        metadata={"test_id": f"pytest-{uuid4()}"},
        snapshots=[
            HardwareDataStorage(
                timestamp="20260906_154000123",
                hardware_name="Integration hardware /test/0",
                sensor_name="Integration sensor",
                sensor_type="Temperature",
                sensor_value=42.5,
            ),
        ],
    )

    result = upload_blob(
        credentials=credentials,
        env_mapping=mapping,
        sensor_buffer=payload,
    )

    assert result is None
