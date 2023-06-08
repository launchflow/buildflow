# ⚒️ BuildFlow

![CI](https://github.com/launchflow/buildflow/actions/workflows/python_ci.yaml/badge.svg)
![GCP Tests](https://github.com/launchflow/buildflow/actions/workflows/gcp_integration.yaml/badge.svg)
[![Discord Shield](https://discordapp.com/api/guilds/1082821064180117575/widget.png?style=shield)](https://discordapp.com/invite/wz7fjHyrCA)

**BuildFlow**, is an open source framework for building large scale systems using Python. All you need to do is describe where your input is coming from and where your output should be written, and BuildFlow handles the rest. **No configuration outside of the code is required**.

Key Features (all provided out-of-the-box):

- Automatic [resource creation / management](https://www.buildflow.dev/docs/features/resource-creation) (Infrastructure as Code) powered by [Pulumi](https://github.com/pulumi/pulumi)
- Automatic [parallelism & concurrency](https://www.buildflow.dev/docs/features/parallelism) powered by [Ray](https://github.com/ray-project/ray)
- [Dynamic autoscaling](https://www.buildflow.dev/docs/features/autoscaling): scale up during high traffic / reduce costs during low traffic
- [Schema validation](https://www.buildflow.dev/docs/features/schema-validation) powered by Python dataclasses and type hints


## Quick Links

- **Docs**: https://www.buildflow.dev/docs
- **Walkthroughs**: https://www.buildflow.dev/docs/category/walk-throughs
- **Discord**: https://discordapp.com/invite/wz7fjHyrCA
