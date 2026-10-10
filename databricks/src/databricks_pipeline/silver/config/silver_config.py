from dataclasses import dataclass


@dataclass(frozen=True)
class SilverConfig:
    catalog: str
    bronze_schema: str
    bronze_table: str
    silver_schema: str
    silver_table: str
    quarantine_table: str

    @property
    def source_table(self) -> str:
        return f"{self.catalog}.{self.bronze_schema}.{self.bronze_table}"

    @property
    def target_table(self) -> str:
        return f"{self.catalog}.{self.silver_schema}.{self.silver_table}"

    @property
    def quarantine_target_table(self) -> str:
        return f"{self.catalog}.{self.silver_schema}.{self.quarantine_table}"
