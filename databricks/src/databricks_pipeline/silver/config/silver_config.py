from dataclasses import dataclass


@dataclass(frozen=True)
class SilverConfig:
    catalog: str
    bronze_schema: str
    bronze_table: str
    silver_schema: str

    @property
    def source_table(self) -> str:
        return f"{self.catalog}.{self.bronze_schema}.{self.bronze_table}"

    def target_table(self, table_name: str) -> str:
        return f"{self.catalog}.{self.silver_schema}.{table_name}"
