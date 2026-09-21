from dataclasses import dataclass
from typing import Any

from pydantic import BaseModel


@dataclass(slots=True)
class HardwareDataStorage:
    timestamp: str | None
    hardware_name: str | None
    sensor_name: str | None
    sensor_type: str | None
    sensor_value: int | float | None


class HardwarePayload(BaseModel):
    metadata: dict[str, Any] = {}
    snapshots: list[HardwareDataStorage] = []
