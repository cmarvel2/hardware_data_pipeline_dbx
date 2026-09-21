from dataclasses import dataclass


@dataclass(frozen=True)
class BronzeConfig:
    catalog: str
    bronze_schema: str
    landing_volume: str
    bronze_table: str

    @property
    def volume_path(self) -> str:
        return f"/Volumes/{self.catalog}/{self.bronze_schema}/{self.landing_volume}"

    @property
    def source_path(self) -> str:
        return f"{self.volume_path}/incoming"

    @property
    def pipeline_state_path(self) -> str:
        return f"{self.volume_path}/_pipeline_state/{self.bronze_table}"

    @property
    def schema_location(self) -> str:
        return f"{self.pipeline_state_path}/schema"

    @property
    def checkpoint_location(self) -> str:
        return f"{self.pipeline_state_path}/checkpoint"

    @property
    def target_table(self) -> str:
        return f"{self.catalog}.{self.bronze_schema}.{self.bronze_table}"
