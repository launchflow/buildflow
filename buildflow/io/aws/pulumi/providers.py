from typing import Optional

import pulumi_aws

from buildflow.core.types.aws_types import AWSAccountID, AWSRegion


def aws_provider(
    provider_id: str,
    *,
    aws_account_id: Optional[AWSAccountID],
    aws_region: Optional[AWSRegion],
) -> Optional[pulumi_aws.Provider]:
    if aws_account_id is None and aws_region is None:
        return None
    allowed_account_ids = None
    if aws_account_id is not None:
        allowed_account_ids = [aws_account_id]
    # NOTE: ideally we would be able to share providers across resources
    # but pulumi doesn't like it when a provider is created in a different
    # scope then used.
    return pulumi_aws.Provider(
        resource_name=provider_id,
        region=aws_region,
        allowed_account_ids=allowed_account_ids,
    )
