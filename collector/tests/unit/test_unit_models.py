import dataclasses

import pytest
from pydantic import ValidationError

from local_data_collection.sensor_polling.models import HardwareDataStorage, HardwarePayload

pytestmark = pytest.mark.unit


def make_snapshot(**overrides: object) -> HardwareDataStorage:
    snapshot = HardwareDataStorage(
        timestamp="20260906_154000123",
        hardware_name="Intel i7 /intelcpu/0",
        sensor_name="CPU Core #1",
        sensor_type="Temperature",
        sensor_value=42.5,
    )
    return dataclasses.replace(snapshot, **overrides)


def test_hardware_payload_defaults_are_empty_and_not_shared() -> None:
    first = HardwarePayload()
    second = HardwarePayload()

    first.metadata["test_id"] = "first"
    first.snapshots.append(make_snapshot())

    assert second.metadata == {}
    assert second.snapshots == []


def test_hardware_payload_coerces_snapshot_dictionary_to_storage_object() -> None:
    payload = HardwarePayload.model_validate(
        {"snapshots": [dataclasses.asdict(make_snapshot())]},
    )

    assert isinstance(payload.snapshots[0], HardwareDataStorage)
    assert payload.snapshots[0].sensor_value == 42.5


def test_hardware_payload_rejects_non_numeric_sensor_value_from_dictionary() -> None:
    invalid_snapshot = dataclasses.asdict(
        make_snapshot(sensor_value="hot"),
    )

    with pytest.raises(ValidationError):
        HardwarePayload.model_validate(
            {"snapshots": [invalid_snapshot]},
        )


def test_hardware_payload_accepts_prebuilt_storage_instance_without_revalidation() -> None:
    snapshot = make_snapshot(sensor_value="hot")

    payload = HardwarePayload(snapshots=[snapshot])

    assert payload.snapshots[0].sensor_value == "hot"


def test_hardware_payload_round_trips_through_serialized_representation() -> None:
    payload = HardwarePayload(
        metadata={"test_id": "abc-123"},
        snapshots=[make_snapshot()],
    )

    rebuilt = HardwarePayload.model_validate(payload.model_dump())

    assert rebuilt == payload


def test_hardware_data_storage_disallows_new_attributes() -> None:
    snapshot = make_snapshot()

    with pytest.raises(AttributeError):
        object.__setattr__(snapshot, "unexpected", "value")
