from dataclasses import dataclass


@dataclass
class GoldConfig:
    catalog: str
    silver_schema: str
    gold_schema: str

    def source_table(self, tablename: str) -> str:
        return f"{self.catalog}.{self.silver_schema}.{tablename}"

    def target_table(self, tablename: str) -> str:
        return f"{self.catalog}.{self.gold_schema}.{tablename}"
