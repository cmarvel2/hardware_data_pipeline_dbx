import os

import pytest
from dotenv import find_dotenv, load_dotenv

pytestmark = [pytest.mark.e2e, pytest.mark.integration]


def test_main_completes_for_explicitly_enabled_hardware_e2e_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    if os.getenv("RUN_HARDWARE_E2E") != "1":
        pytest.skip("Set RUN_HARDWARE_E2E=1 to intentionally run this test.")

    pytest.importorskip("clr")
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
            "Hardware E2E credentials are unavailable: " + ", ".join(missing),
        )

    monkeypatch.setenv("ENVIRONMENT", "test")

    from local_data_collection.main import main

    main()