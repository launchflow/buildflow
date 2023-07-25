import dataclasses
from typing import Optional

from buildflow.config.cloud_provider_config import CloudProvider, CloudProviderConfig
from buildflow.core.io.gcp.bigquery import BigQueryTable
from buildflow.core.io.primitive import PortablePrimtive, Primitive
from buildflow.core.strategies._strategy import StategyType
from buildflow.core.types.portable_types import TableName
from buildflow.core.io.local.file import File


@dataclasses.dataclass
class AnalysisTable(PortablePrimtive):
    table_name: Optional[TableName] = None

    # Pulumi only options
    destroy_protection: bool = True

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
                destroy_protection=self.destroy_protection,
            )
        # AWS Implementations
        elif cloud_provider_config.default_cloud_provider == CloudProvider.AWS:
            raise NotImplementedError("AWS is not implemented for Table.")
        # Azure Implementations
        elif cloud_provider_config.default_cloud_provider == CloudProvider.AZURE:
            raise NotImplementedError("Azure is not implemented for Table.")
        # Local Implementations
        elif cloud_provider_config.default_cloud_provider == CloudProvider.LOCAL:
            # TODO: change this to duckdb
            return File.from_local_options(
                local_options=cloud_provider_config.local_options,
                file_path=self.table_name,
                file_format="parquet",
            )
        # Sanity check
        else:
            raise ValueError(
                f"Unknown resource provider: {cloud_provider_config.default_cloud_provider}"  # noqa: E501
            )


@dataclasses.dataclass
class RelationalTable(PortablePrimtive):
    table_name: Optional[TableName] = None
