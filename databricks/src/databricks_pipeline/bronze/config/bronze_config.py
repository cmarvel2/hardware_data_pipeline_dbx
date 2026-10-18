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
    def target_table(self) -> str:
        return f"{self.catalog}.{self.bronze_schema}.{self.bronze_table}"
