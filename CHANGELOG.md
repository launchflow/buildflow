# Changelog

## [0.0.14](https://github.com/launchflow/buildflow/compare/v0.0.13...v0.0.14) (2023-04-21)


### Bug Fixes

* add metrics ([506cd2c](https://github.com/launchflow/buildflow/commit/506cd2cb24d4d8ad24afc250227c172bc6e6564a))
* improvements to the autoscaller ([3b8bb29](https://github.com/launchflow/buildflow/commit/3b8bb29f1052122e5c177ec2d8137a4599684f6c))
* set billing project when setting up GCP clients ([a11c8f2](https://github.com/launchflow/buildflow/commit/a11c8f2b0788a1e5f5caa5024cbe2af5429386fc))

## [0.0.13](https://github.com/launchflow/buildflow/compare/v0.0.12...v0.0.13) (2023-04-12)


### Bug Fixes

* drain on sigterm and improve pub/sub backlog ([8ad5848](https://github.com/launchflow/buildflow/commit/8ad584853a82252cb5d2c21c9da5e36803385ae1))

## [0.0.12](https://github.com/launchflow/buildflow/compare/v0.0.11...v0.0.12) (2023-04-07)


### Bug Fixes

* add region to SQS source ([c81f47a](https://github.com/launchflow/buildflow/commit/c81f47ac9017cd376e65ec3d6ef5895b93e57922))

## [0.0.11](https://github.com/launchflow/buildflow/compare/v0.0.10...v0.0.11) (2023-04-06)


### Bug Fixes

* add some usability improvements ([4d8fbbe](https://github.com/launchflow/buildflow/commit/4d8fbbe787e9a50d3b464eac80a9e22f11c03e5d))

## [0.0.10](https://github.com/launchflow/buildflow/compare/v0.0.9...v0.0.10) (2023-03-28)


### Bug Fixes

* add support for SQS as a source ([813740e](https://github.com/launchflow/buildflow/commit/813740ed6068222ddf0adfbde86507bf544dcfce))
* finish implementation of streaming autoscaler ([3243412](https://github.com/launchflow/buildflow/commit/3243412f39800c65c34580e1ff9b4f5bf00e38ce))
* return a single flow results object from flow.run() ([e2b2cde](https://github.com/launchflow/buildflow/commit/e2b2cde207d89d402a780bdcbd78944e7a517426))

## [0.0.9](https://github.com/launchflow/buildflow/compare/v0.0.8...v0.0.9) (2023-03-16)


### Bug Fixes

* error in pubsub walkthrough ([669f417](https://github.com/launchflow/buildflow/commit/669f4175ba3395120ad53b924bb79628f66c4e73))

## [0.0.8](https://github.com/launchflow/buildflow/compare/v0.0.7...v0.0.8) (2023-03-16)


### Bug Fixes

* add support to writing to local parquet files ([d2b844b](https://github.com/launchflow/buildflow/commit/d2b844bf1a750e3284dcbcf3550e78b321d29870))

## [0.0.7](https://github.com/launchflow/buildflow/compare/v0.0.6...v0.0.7) (2023-03-15)


### Bug Fixes

* add better error handling for streaming failutes ([8e10580](https://github.com/launchflow/buildflow/commit/8e10580ca7423aec949c757fedeb3d61b466f1a5))

## [0.0.6](https://github.com/launchflow/buildflow/compare/v0.0.5...v0.0.6) (2023-03-15)


### Bug Fixes

* actually use num replicas ([4761a1f](https://github.com/launchflow/buildflow/commit/4761a1f6341e00147c144745f06ef55cb158eeaa))

## [0.0.5](https://github.com/launchflow/buildflow/compare/v0.0.4...v0.0.5) (2023-03-14)


### Bug Fixes

* add ability to output dataclasses ([4d9509c](https://github.com/launchflow/buildflow/commit/4d9509c00bdbbbacaed37fc8a9386449d421532d))
* add pubsub resource setup ([72d16a3](https://github.com/launchflow/buildflow/commit/72d16a30ce235ee5097276cd161043e553b509d6))
* add resource setup for bigquery ([6a770bd](https://github.com/launchflow/buildflow/commit/6a770bd9e7687f06b4aeb680ef83177e75f334b3))
* add source for ingesting GCS file changes ([6b1a979](https://github.com/launchflow/buildflow/commit/6b1a9798f44318ba3ae6af0dfe38ac00432348f5))
* attempt to gracefully shutdown processors when interupt signal is found ([652463d](https://github.com/launchflow/buildflow/commit/652463d01464a5fc9d08c5856b04ef33f148f46e))
* refactor io into seperate sources and sinks ([1dccb72](https://github.com/launchflow/buildflow/commit/1dccb72ac6e0460c100bf1afa6be753f23587c32))
* support lists of dataclasses and some optimizations ([36ef5eb](https://github.com/launchflow/buildflow/commit/36ef5ebd96844dc580a4f4244680aa83bbd5b0de))

## [0.0.4](https://github.com/launchflow/buildflow/compare/v0.0.3...v0.0.4) (2023-03-09)


### Miscellaneous Chores

* release 0.0.4 ([c7f7549](https://github.com/launchflow/buildflow/commit/c7f75494b6621023b73a81d32221d13fd164efde))

## [0.0.3](https://github.com/launchflow/buildflow/compare/v0.0.1...v0.0.3) (2023-03-03)


### Bug Fixes

* add ability to run more than one flow at a time ([583d499](https://github.com/launchflow/buildflow/commit/583d4994e51c2f5c57a7fadc1ac182b1fdef1089))
* add usage stats to detect how often users are calling flow.run ([1ce6279](https://github.com/launchflow/buildflow/commit/1ce6279d43171413330c1aa00ca032290da47260))
* update API to user a top level flow object ([38bd595](https://github.com/launchflow/buildflow/commit/38bd5952ddf6346f83fc78aa6a1b1585e3eaddc8))
* update description of package ([36fb28c](https://github.com/launchflow/buildflow/commit/36fb28c78a5a7df719b5b50685c50395701b42b0))


### Miscellaneous Chores

* release 0.0.1 ([571bd84](https://github.com/launchflow/buildflow/commit/571bd84fef1a628f6b81bfda8c76fdb8ad579dcb))
* release 0.0.3 ([efd8b43](https://github.com/launchflow/buildflow/commit/efd8b43b5271bcd3ba5011153f92a4677da7644f))

## [0.0.1](https://github.com/launchflow/buildflow/compare/v0.0.1...v0.0.1) (2023-03-03)


### Bug Fixes

* add usage stats to detect how often users are calling flow.run ([1ce6279](https://github.com/launchflow/buildflow/commit/1ce6279d43171413330c1aa00ca032290da47260))
* update API to user a top level flow object ([38bd595](https://github.com/launchflow/buildflow/commit/38bd5952ddf6346f83fc78aa6a1b1585e3eaddc8))
* update description of package ([36fb28c](https://github.com/launchflow/buildflow/commit/36fb28c78a5a7df719b5b50685c50395701b42b0))


### Miscellaneous Chores

* release 0.0.1 ([571bd84](https://github.com/launchflow/buildflow/commit/571bd84fef1a628f6b81bfda8c76fdb8ad579dcb))

## [0.0.1](https://github.com/launchflow/buildflow/compare/v0.0.1...v0.0.1) (2023-02-26)


### Miscellaneous Chores

* release 0.0.1 ([571bd84](https://github.com/launchflow/buildflow/commit/571bd84fef1a628f6b81bfda8c76fdb8ad579dcb))

## [0.0.1](https://github.com/launchflow/buildflow/compare/v0.0.1...v0.0.1) (2023-02-26)


### Miscellaneous Chores

* release 0.0.1 ([571bd84](https://github.com/launchflow/buildflow/commit/571bd84fef1a628f6b81bfda8c76fdb8ad579dcb))

## [0.0.1](https://github.com/launchflow/buildflow/compare/v0.1.1...v0.0.1) (2023-02-26)


### Miscellaneous Chores

* release 0.0.1 ([d184bb9](https://github.com/launchflow/buildflow/commit/d184bb9340e9f1a552b39f877b4f83fdbb6fef04))

## [0.1.1](https://github.com/launchflow/buildflow/compare/v0.1.0...v0.1.1) (2023-02-22)


### Miscellaneous Chores

* release 2.0.0 ([58d9129](https://github.com/launchflow/buildflow/commit/58d91290ae8f0802fe9c686ec940f2b1574d7993))

## 0.1.0 (2023-02-22)


### Bug Fixes

* update duckdb connection ([b0a906e](https://github.com/launchflow/buildflow/commit/b0a906ef7897ca0210243da7346c981b9c445918))
* update error handling for pubsub and concurrency for duckdb ([6eee3ae](https://github.com/launchflow/buildflow/commit/6eee3ae2e60a40c22a7fd517edb387a22d336f58))
