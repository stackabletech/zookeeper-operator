# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

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
- BREAKING: renamed crd/util.rs to crd/discovery.rs ([#230]).
- Use `identity::LabeledPodIdentityFactory` to generate pod ids. ([#217]).

### Fixed
- The ZooKeeper discovery now correctly uses the "client" container port from the pod instead of defaulting to 2181 which will only work if the default port is used ([#230]).

[#230]: https://github.com/stackabletech/zookeeper-operator/pull/230
[#223]: https://github.com/stackabletech/zookeeper-operator/pull/223
[#217]: https://github.com/stackabletech/zookeeper-operator/pull/217

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
