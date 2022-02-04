# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

### Added

- Enabled Prometheus scraping ([#380]).
- ZookeeperZnode.spec.clusterRef.namespace now defaults to .metadata.namespace ([#382]).

### Changed

- Shut down gracefully ([#338]).
- Fixed ACL incompatibility with certain managed K8s providers ([#340]).
- Operator-rs: 0.6.0 -> 0.8.0 ([#352]).
- Cleanup for `ZookeeperZnode` now succeeds if the linked `ZookeeperCluster` was already deleted ([#384]).

[#338]: https://github.com/stackabletech/zookeeper-operator/pull/338
[#340]: https://github.com/stackabletech/zookeeper-operator/pull/340
[#352]: https://github.com/stackabletech/zookeeper-operator/pull/352
[#380]: https://github.com/stackabletech/zookeeper-operator/pull/380
[#382]: https://github.com/stackabletech/zookeeper-operator/pull/382
[#384]: https://github.com/stackabletech/zookeeper-operator/pull/384

## [0.8.0] - 2021-12-22


## [0.7.0] - 2021-12-20


### Changed

- Migrated to StatefulSet rather than direct Pod management ([#263]).
- Migrated to PersistentVolumeClaim rather than manual sticky scheduling ([#263]).

[#263]: https://github.com/stackabletech/zookeeper-operator/pull/263

## [0.6.0] - 2021-12-06

## [0.5.0] - 2021-11-12


### Changed

- `operator-rs` `0.3.0` → `0.4.0` ([#255]).
- Adapted pod image and container command to docker image ([#255]).
- Adapted documentation to represent new workflow with docker images ([#255]).

[#255]: https://github.com/stackabletech/zookeeper-operator/pull/255

## [0.4.1] - 2021-10-27

### Added
- `process_command` to reconcile loop for command handling ([#223]).
- `rust/crd/lib/command.rs` for command CRDs
- Trait implementations for command handling for the cluster and status ([#223]):
  - `HasCurrentCommand` to manipulate the current_command in the status
  - `HasClusterExecutionStatus` to access cluster_execution_status in the status
  - `HasRoleRestartOrder` to determine the restart order of different roles
  - `HasCommands` to provide all supported commands like Restart, Start, Stop ...
  - `CanBeRolling` to perform a rolling restart
  - `HasRoles` to run a command only on a subset of roles
- Generated CRDs for Restart, Start, Stop ([#223]).
- Example custom resources for Restart, Start, Stop ([#223]).

### Changed
- Switched to operator-rs tag 0.3.0 ([#251])
- Use `identity::LabeledPodIdentityFactory` to generate pod ids. ([#217])
- Fix `ZookeeperCluster` conditions overwriting each other ([#228])
- BREAKING: renamed crd/util.rs to crd/discovery.rs ([#230]).

### Fixed
- Fixed a bug where `wait_until_crds_present` only reacted to the main CRD, not the commands ([#251]).
- The ZooKeeper discovery now correctly uses the "client" container port from the pod instead of defaulting to 2181 which will only work if the default port is used ([#230]).

[#251]: https://github.com/stackabletech/zookeeper-operator/pull/251
[#230]: https://github.com/stackabletech/zookeeper-operator/pull/230
[#223]: https://github.com/stackabletech/zookeeper-operator/pull/223
[#217]: https://github.com/stackabletech/zookeeper-operator/pull/217
[#228]: https://github.com/stackabletech/zookeeper-operator/pull/228

## [0.4.0] - 2021-09-21


### Changed
- `kube-rs`: `0.59` → `0.60` ([#214]).
- `k8s-openapi` features: `v1_21` → `v1_22` ([#214]).

[#214]: https://github.com/stackabletech/zookeeper-operator/pull/214

## [0.3.0] - 2021-09-20


### Added
- Added versioning code from operator-rs for up and downgrades ([#210]).
- Added `ProductVersion` to status ([#210]).
- Added `PodToNodeMapping` to status ([#209]).

### Changed
- Using scheduler with history from operator-rs instead of random node selection([#209]).

### Removed
- Code for version handling ([#210]).
- Removed `current_version` and `target_version` from cluster status ([#210]).
- Removed `assign_ids`, `read_pod_information` and anything id related which is now covered by the scheduler ([#209]).
- Removed warning for replicas and node id problems from docs ([#209]).
- Set right yaml indentation for the usage examples in docs ([#209]).


[#209]: https://github.com/stackabletech/zookeeper-operator/pull/209
[#210]: https://github.com/stackabletech/zookeeper-operator/pull/210

## [0.2.0] - 2021-09-14

### Changed
- **Breaking:** Repository structure was changed and the -server crate renamed to -binary. As part of this change the -server suffix was removed from both the package name for os packages and the name of the executable ([#197]).

[#197]: https://github.com/stackabletech/zookeeper-operator/pull/197

## [0.1.0] - 2021.09.07

### Added
- Initial release
