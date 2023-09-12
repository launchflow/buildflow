import dataclasses
import os
from typing import Optional

from buildflow.config.cloud_provider_config import CloudProvider, CloudProviderConfig
from buildflow.core.types.portable_types import TableName
from buildflow.io.duckdb.duckdb import DuckDBTable
from buildflow.io.gcp.bigquery_table import BigQueryTable
from buildflow.io.primitive import PortablePrimtive, Primitive
from buildflow.io.strategies._strategy import StategyType


@dataclasses.dataclass
class AnalysisTable(PortablePrimtive):
    table_name: Optional[TableName] = None

    def to_cloud_primitive(
        self, cloud_provider_config: CloudProviderConfig, strategy_type: StategyType
    ) -> Primitive:
        if strategy_type not in [StategyType.SINK]:
            raise ValueError(
                f"Unsupported strategy type for AnalysisTable: {strategy_type}"
            )
        # GCP Implementations
        if cloud_provider_config.default_cloud_provider == CloudProvider.GCP:
            return BigQueryTable.from_gcp_options(
                gcp_options=cloud_provider_config.gcp_options,
                table_name=self.table_name,
            )
        # AWS Implementations
        elif cloud_provider_config.default_cloud_provider == CloudProvider.AWS:
            raise NotImplementedError("AWS is not implemented for Table.")
        # Azure Implementations
        elif cloud_provider_config.default_cloud_provider == CloudProvider.AZURE:
            raise NotImplementedError("Azure is not implemented for Table.")
        # Local Implementations
        elif cloud_provider_config.default_cloud_provider == CloudProvider.LOCAL:
            database = os.path.join(os.getcwd(), "buildflow_managed.duckdb")
            return DuckDBTable(database=database, table=self.table_name)
        # Sanity check
        else:
            raise ValueError(
                f"Unknown resource provider: {cloud_provider_config.default_cloud_provider}"  # noqa: E501
            )


@dataclasses.dataclass
class RelationalTable(PortablePrimtive):
    table_name: Optional[TableName] = None
