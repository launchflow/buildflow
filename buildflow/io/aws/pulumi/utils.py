# Most of this is based on: https://github.com/link2aws/link2aws.github.io/tree/master

import dataclasses
import logging
from typing import List

_SERVICE_TEMPLATES = {
    "s3": lambda parsed_arn: f"https://s3.{parsed_arn.console}/s3/buckets/{parsed_arn.resource}",
    "sqs": lambda parsed_arn: f"https://{parsed_arn.region}.{parsed_arn.console}/sqs/v2/home?region={parsed_arn.region}#/queues/https%3A%2F%2Fsqs.{parsed_arn.region}.amazonaws.com%2F{parsed_arn.account}%2F{parsed_arn.resource}",
}


@dataclasses.dataclass
class _ParsedARN:
    prefix: str
    partition: str
    service: str
    region: str
    account: str
    resource: str
    resource_type: str
    console: str = dataclasses.field(init=False)

    def __post_init__(self):
        if self.partition == "aws":
            self.console = "console.aws.amazon.com"
        elif self.partition == "aws-us-gov":
            self.console = "console.amazonaws-us-gov.com"
        elif self.partition == "aws-cn":
            self.console = "console.amazonaws.cn"
        else:
            self.console = ""
            raise ValueError(f"Unknown partition: {self.partition}")

    def console_url(self) -> str:
        if not self.console:
            return ""
        if self.service in _SERVICE_TEMPLATES:
            return _SERVICE_TEMPLATES[self.service](self)
        else:
            return ""


def _parse_resource(resource):
    first_separator_index = -1
    for idx, c in enumerate(resource):
        if c in (":", "/"):
            first_separator_index = idx
            break

    if first_separator_index != -1:
        resource_type = resource[:first_separator_index]
        resource = resource[first_separator_index + 1 :]
    else:
        resource_type = None

    return resource_type, resource


def arn_to_cloud_console_url(pulumi_output_arn: List[str]) -> str:
    if len(pulumi_output_arn) != 1:
        raise ValueError(f"Expected exactly one ARN, got {len(pulumi_output_arn)}")
    arn = pulumi_output_arn[0]
    try:
        tokens = arn.split(":", 5)
        prefix = tokens[0]
        partition = tokens[1]
        service = tokens[2]
        region = tokens[3]
        account = tokens[4]
        resource = tokens[5]
        if service in ["s3", "sns", "apigateway", "execute-api"]:
            resource_type = None
        else:
            resource_type, resource = _parse_resource(resource)

        parsed_arn = _ParsedARN(
            prefix=prefix,
            partition=partition,
            service=service,
            region=region,
            account=account,
            resource=resource,
            resource_type=resource_type,
        )
        return parsed_arn.console_url()
    except Exception:
        logging.exception(f"Failed to parse ARN into a console URL: {arn}")
        return ""
