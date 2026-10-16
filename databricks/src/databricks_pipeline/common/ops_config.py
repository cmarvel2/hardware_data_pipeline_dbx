from dataclasses import dataclass


@dataclass
class CommonConfig:
    catalog: str
    ops_volume: str
    ops_schema: str
    incoming_quarantine_table: str | None
    outgoing_quarantine_table: str | None

    @property
    def ops_volume_path(self) -> str:
        return f"/Volumes/{self.catalog}/{self.ops_schema}/{self.ops_volume}"

    def pipeline_state_path(self, tablename) -> str:
        return f"{self.ops_volume_path}/_pipeline_state/{tablename}"

    def schema_location(self, tablename) -> str:
        return f"{self.pipeline_state_path(tablename)}/schema"

    def checkpoint_location(self, tablename) -> str:
        return f"{self.pipeline_state_path(tablename)}/checkpoint"

    @property
    def inc_quarantine_target_table(self) -> str:
        return f"{self.catalog}.{self.ops_schema}.{self.incoming_quarantine_table}"

    @property
    def out_quarantine_target_table(self) -> str:
        return f"{self.catalog}.{self.ops_schema}.{self.outgoing_quarantine_table}"
