# ⚒️ BuildFlow

![CI](https://github.com/launchflow/buildflow/actions/workflows/python_ci.yaml/badge.svg)
![GCP Tests](https://github.com/launchflow/buildflow/actions/workflows/gcp_integration.yaml/badge.svg)
[![Python version](https://badge.fury.io/py/buildflow.svg)](https://pypi.org/project/buildflow)
[![codecov](https://codecov.io/gh/launchflow/buildflow/branch/main/graph/badge.svg?token=AO0TP8XG7X)](https://codecov.io/gh/launchflow/buildflow)
[![Discord Shield](https://discordapp.com/api/guilds/1082821064180117575/widget.png?style=shield)](https://discordapp.com/invite/wz7fjHyrCA)


## Overview
**BuildFlow**, is an open source framework for building large scale systems using Python. All you need to do is describe where your input is coming from and where your output should be written, and BuildFlow handles the rest. **No configuration outside of the code is required**.

Key Features (all provided out-of-the-box):

- Automatic [resource creation / management](https://www.buildflow.dev/docs/features/resource-creation) (Infrastructure as Code) powered by [Pulumi](https://github.com/pulumi/pulumi)
- Automatic [parallelism & concurrency](https://www.buildflow.dev/docs/features/parallelism) powered by [Ray](https://github.com/ray-project/ray)
- [Dynamic autoscaling](https://www.buildflow.dev/docs/features/autoscaling): scale up during high traffic / reduce costs during low traffic
- [Schema validation](https://www.buildflow.dev/docs/features/schema-validation) powered by Python dataclasses and type hints

## Installation

```bash
pip install buildflow
```

### Extra Dependencies

#### Pulumi Installation

BuildFlow uses Pulumi to manage resources used by your BuildFlow Nodes and Processors. To install Pulumi visit: https://www.pulumi.com/docs/install/

Installing Pulumi unlocks:
- allows BuildFlow to manage resource creation and destruction
- full access to Pulumi API / CLI
- fine-grained control over Pulumi Stacks & Resources


## Quick Links

- **Docs**: https://www.buildflow.dev/docs
- **Walkthroughs**: https://www.buildflow.dev/docs/category/walk-throughs
- **Discord**: https://discordapp.com/invite/wz7fjHyrCA

## Code Health Checks

We use [black](https://github.com/psf/black) and [ruff](https://github.com/charliermarsh/ruff) with [pre-commit](https://pre-commit.com/) hooks to perform health checks.
To setup these locally:

- Clone the repo
- Install the `dev` dependencies like `python -m pip install .[dev]
- Check if pre-commit is installed correctly by running `pre-commit --version`
- Setup pre-commit to run before every commit on staged changes by running `pre-commit install`
- Pre-commit can also be ran manually as `pre-commit run --all-files`
