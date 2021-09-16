# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

## [0.2.0] - 2021-09-14

### Added
- Added versioning code from operator-rs for up and downgrades ([#210]).
- Added `ProductVersion` to status ([#210]).

### Changed
- **Breaking:** Repository structure was changed and the -server crate renamed to -binary. As part of this change the -server suffix was removed from both the package name for os packages and the name of the executable ([#197]).

### Removed
- Code for version handling ([#210]).
- Removed `current_version` and `target_version` from cluster status ([#210]). 

[#197]: https://github.com/stackabletech/zookeeper-operator/pull/197
[#210]: https://github.com/stackabletech/zookeeper-operator/pull/210

## 0.1.0 - 2021.09.07

### Added
- Initial release
