import importlib
import re
import sys
from types import ModuleType
from unittest.mock import Mock

import pytest

from local_data_collection.sensor_polling.models import HardwareDataStorage

pytestmark = pytest.mark.unit


class FakeHardwareType:
    Cpu = "cpu"
    Memory = "memory"
    GpuAmd = "gpu_amd"
    GpuNvidia = "gpu_nvidia"


class FakeLibraryComputer:
    def __init__(self) -> None:
        self.IsCpuEnabled = False
        self.IsMotherboardEnabled = False
        self.IsGpuEnabled = False
        self.IsMemoryEnabled = False
        self.opened = False

    def Open(self) -> None:
        self.opened = True


class FakeSensorType:
    def __init__(self, label: str) -> None:
        self.label = label

    def ToString(self) -> str:
        return self.label


class FakeSensor:
    def __init__(
        self,
        name: str = "CPU Core #1",
        sensor_type: str = "Temperature",
        value: float | None = 42.5,
    ) -> None:
        self.Name = name
        self.SensorType = FakeSensorType(sensor_type)
        self.Value = value


class FakeHardware:
    def __init__(
        self,
        name: str,
        identifier: str,
        hardware_type: str,
        sensors: list[FakeSensor],
    ) -> None:
        self.Name = name
        self.Identifier = identifier
        self.HardwareType = hardware_type
        self.Sensors = sensors
        self.update = Mock()

    def Update(self) -> None:
        self.update()


class FakeComputer:
    def __init__(self, hardware: list[FakeHardware]) -> None:
        self.Hardware = hardware


@pytest.fixture
def collectors(monkeypatch: pytest.MonkeyPatch):
    clr_module = ModuleType("clr")
    clr_module.AddReference = Mock()

    hardware_module = ModuleType("LibreHardwareMonitor.Hardware")
    hardware_module.Computer = FakeLibraryComputer
    hardware_module.HardwareType = FakeHardwareType

    parent_module = ModuleType("LibreHardwareMonitor")
    parent_module.Hardware = hardware_module

    monkeypatch.setitem(sys.modules, "clr", clr_module)
    monkeypatch.setitem(sys.modules, "LibreHardwareMonitor", parent_module)
    monkeypatch.setitem(sys.modules, "LibreHardwareMonitor.Hardware", hardware_module)
    monkeypatch.delitem(
        sys.modules,
        "local_data_collection.sensor_polling.hardware_sensor_collectors",
        raising=False,
    )

    return importlib.import_module(
        "local_data_collection.sensor_polling.hardware_sensor_collectors",
    )


def make_hardware(
    hardware_type: str,
    sensors: list[FakeSensor] | None = None,
) -> FakeHardware:
    return FakeHardware(
        name="Intel i7",
        identifier="/intelcpu/0",
        hardware_type=hardware_type,
        sensors=sensors if sensors is not None else [FakeSensor()],
    )


def test_init_computer_enables_requested_components_and_opens_monitor(collectors) -> None:
    computer, hardware_type = collectors.init_computer(
        cpu=True,
        motherboard=True,
        gpu=True,
        memory=True,
    )

    assert computer.IsCpuEnabled is True
    assert computer.IsMotherboardEnabled is True
    assert computer.IsGpuEnabled is True
    assert computer.IsMemoryEnabled is True
    assert computer.opened is True
    assert hardware_type is FakeHardwareType


@pytest.mark.parametrize(
    ("method_name", "matching_type"),
    [
        ("get_cpu_data", FakeHardwareType.Cpu),
        ("get_memory_data", FakeHardwareType.Memory),
        ("get_gpu_data", FakeHardwareType.GpuAmd),
        ("get_gpu_data", FakeHardwareType.GpuNvidia),
    ],
)
def test_collector_returns_one_snapshot_per_matching_sensor(
    collectors,
    method_name: str,
    matching_type: str,
) -> None:
    hardware = make_hardware(
        matching_type,
        sensors=[
            FakeSensor(name="Sensor one"),
            FakeSensor(name="Sensor two"),
        ],
    )

    snapshots = getattr(collectors, method_name)(
        FakeComputer([hardware]),
        FakeHardwareType,
    )

    assert len(snapshots) == 2
    assert all(isinstance(snapshot, HardwareDataStorage) for snapshot in snapshots)
    hardware.update.assert_called_once()


@pytest.mark.parametrize(
    ("method_name", "non_matching_type"),
    [
        ("get_cpu_data", FakeHardwareType.Memory),
        ("get_memory_data", FakeHardwareType.Cpu),
        ("get_gpu_data", FakeHardwareType.Memory),
    ],
)
def test_collector_ignores_non_matching_hardware(
    collectors,
    method_name: str,
    non_matching_type: str,
) -> None:
    hardware = make_hardware(non_matching_type)

    snapshots = getattr(collectors, method_name)(
        FakeComputer([hardware]),
        FakeHardwareType,
    )

    assert snapshots == []
    hardware.update.assert_not_called()


def test_cpu_collector_maps_sensor_fields(collectors) -> None:
    hardware = make_hardware(
        FakeHardwareType.Cpu,
        sensors=[FakeSensor(name="Core #1", sensor_type="Clock", value=3600.0)],
    )

    snapshot = collectors.get_cpu_data(
        FakeComputer([hardware]),
        FakeHardwareType,
    )[0]

    assert snapshot.hardware_name == "Intel i7 /intelcpu/0"
    assert snapshot.sensor_name == "Core #1"
    assert snapshot.sensor_type == "Clock"
    assert snapshot.sensor_value == 3600.0
    assert re.fullmatch(r"\d{8}_\d{9}", snapshot.timestamp)


def test_collector_preserves_missing_sensor_value(collectors) -> None:
    hardware = make_hardware(
        FakeHardwareType.Cpu,
        sensors=[FakeSensor(value=None)],
    )

    snapshot = collectors.get_cpu_data(
        FakeComputer([hardware]),
        FakeHardwareType,
    )[0]

    assert snapshot.sensor_value is None