import dataclasses
from typing import Optional

from buildflow.config.cloud_provider_config import CloudProvider, CloudProviderConfig
from buildflow.core.io.gcp.bigquery import BigQueryTable
from buildflow.core.io.primitive import PortablePrimtive, Primitive
from buildflow.core.strategies._strategy import StategyType
from buildflow.core.types.portable_types import TableName


@dataclasses.dataclass
class AnalysisTable(PortablePrimtive):
    table_name: Optional[TableName] = None

    def to_cloud_primitive(
        self, cloud_provider_config: CloudProviderConfig, strategy_type: StategyType
    ) -> Primitive:
        # GCP Implementations
        if cloud_provider_config.default_cloud_provider == CloudProvider.GCP:
            if strategy_type == StategyType.SOURCE:
                raise NotImplementedError(
                    "Source strategy is not implemented for AnalysisTable (GCP)."
                )
            elif strategy_type == StategyType.SINK:
                return BigQueryTable.from_gcp_options(
                    gcp_options=cloud_provider_config.gcp_options,
                    table_name=self.table_name,
                )
            else:
                raise ValueError(
                    f"Unsupported strategy type for Topic (GCP): {strategy_type}"
                )
        # AWS Implementations
        elif cloud_provider_config.default_cloud_provider == CloudProvider.AWS:
            raise NotImplementedError("AWS is not implemented for Topic.")
        # Azure Implementations
        elif cloud_provider_config.default_cloud_provider == CloudProvider.AZURE:
            raise NotImplementedError("Azure is not implemented for Topic.")
        # Local Implementations
        elif cloud_provider_config.default_cloud_provider == CloudProvider.LOCAL:
            raise NotImplementedError("Local is not implemented for Topic.")
        # Sanity check
        else:
            raise ValueError(
                f"Unknown resource provider: {cloud_provider_config.default_cloud_provider}"  # noqa: E501
            )


@dataclasses.dataclass
class RelationalTable(PortablePrimtive):
    table_name: Optional[TableName] = None
