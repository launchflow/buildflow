import dataclasses
from typing import Optional

from buildflow.config.cloud_provider_config import GCPOptions
from buildflow.core import utils
from buildflow.core.io.gcp.providers.storage_providers import GCSBucketProvider
from buildflow.core.io.primitive import GCPPrimtive, Primitive
from buildflow.core.types.gcp_types import (GCPProjectID, GCPRegion,
                                            GCSBucketName)
from buildflow.core.types.portable_types import BucketName


@dataclasses.dataclass
class GCSBucket(GCPPrimtive):
    project_id: GCPProjectID
    bucket_name: GCSBucketName
    bucket_region: GCPRegion

    # optional args
    # If true destroy will delete the bucket and all contents. If false
    # destroy will fail if the bucket contains data.
    force_destroy: bool = dataclasses.field(default=False, init=False)

    @classmethod
    def from_gcp_options(
        cls,
        gcp_options: GCPOptions,
        *,
        bucket_name: Optional[BucketName] = None,
    ) -> "GCSBucket":
        project_id = gcp_options.default_project_id
        if project_id is None:
            raise ValueError(
                "No Project ID was provided in the GCP options. Please provide one in "
                "the .buildflow config."
            )
        region = gcp_options.default_region
        if region is None:
            raise ValueError(
                "No Region was provided in the GCP options. Please provide one in "
                "the .buildflow config."
            )
        project_hash = utils.stable_hash(project_id)
        if bucket_name is None:
            bucket_name = f"buildflow_{project_hash[:8]}"
        return cls(
            project_id=project_id,
            bucket_name=bucket_name,
            bucket_region=region,
        )

    def options(
        self, *, managed: bool = False, force_destroy: bool = False
    ) -> Primitive:
        to_ret = super().options(managed)
        to_ret.force_destroy = force_destroy
        return to_ret

    def sink_provider(self):
        # TODO: Add support to supply the source-only options. Maybe add some kind of
        # "inject_options" method for the different provider types.
        # Use a Builder pattern for this.
        return GCSBucketProvider(
            project_id=self.project_id,
            bucket_name=self.bucket_name,
            bucket_region=self.bucket_region,
            force_destroy=self.force_destroy,
        )

    def pulumi_provider(self):
        return GCSBucketProvider(
            project_id=self.project_id,
            bucket_name=self.bucket_name,
            bucket_region=self.bucket_region,
            force_destroy=self.force_destroy,
        )
