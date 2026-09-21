import pytest


@pytest.fixture
def sample_payload_dict() -> dict[str, object]:
    return {
        "metadata": {
            "underlying_system": "LibreHardwareMonitor",
            "testing_software": {
                "occt": {
                    "stability_test": {
                        "length_in_minutes": 1,
                    },
                },
            },
            "device_id": "a3bb189e-8bf9-3888-9912-ace4e6543002",
            "test_id": "c9a646d3-9c61-4eb7-bc51-2d744dc92178",
            "test_date": "20260906_194200",
        },
        "snapshots": [
            {
                "timestamp": "20260906_194200123",
                "hardware_name": "Intel Core i7 /intelcpu/0",
                "sensor_name": "CPU Total",
                "sensor_type": "Load",
                "sensor_value": 45.2,
            },
        ],
    }
