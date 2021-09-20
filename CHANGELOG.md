# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

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
