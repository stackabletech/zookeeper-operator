# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

## [23.4.0] - 2023-04-17

### Added

- Generate OLM bundle ([#645]).
- Cluster status conditions ([#658])
- Extend cluster resources for status and cluster operation (paused, stopped) ([#660]).

### Changed

- [BREAKING] Support specifying Service type.
  This enables us to later switch non-breaking to using `ListenerClasses` for the exposure of Services.
  This change is breaking, because - for security reasons - we default to the `cluster-internal` `ListenerClass`.
  If you need your cluster to be accessible from outside of Kubernetes you need to set `clusterConfig.listenerClass`
  to `external-unstable` ([#661]).
- Deploy default and support custom affinities ([#649]).
- Operator-rs: `0.36.0` -> `0.40.2` ([#660], [#663], [#665]).
- Use operator-rs `build_rbac_resources` method ([#665]).

### Fixed

- Bugfix: java heap format ([#651]).
- Fixed operator error when creating the ZNode in a different namespace than the ZookeeperCluster ([#653]).
- Avoid empty log events dated to 1970-01-01 and improve the precision of the
  log event timestamps ([#663]).

[#645]: https://github.com/stackabletech/zookeeper-operator/pull/645
[#649]: https://github.com/stackabletech/zookeeper-operator/pull/649
[#651]: https://github.com/stackabletech/zookeeper-operator/pull/651
[#653]: https://github.com/stackabletech/zookeeper-operator/pull/653
[#658]: https://github.com/stackabletech/zookeeper-operator/pull/658
[#660]: https://github.com/stackabletech/zookeeper-operator/pull/660
[#661]: https://github.com/stackabletech/zookeeper-operator/pull/661
[#663]: https://github.com/stackabletech/zookeeper-operator/pull/663
[#665]: https://github.com/stackabletech/zookeeper-operator/pull/665

## [23.1.0] - 2023-01-23

### Added

- Log aggregation added ([#588]).

[#588]: https://github.com/stackabletech/zookeeper-operator/pull/588

### Changed

- [BREAKING] Use Product image selection instead of version. `spec.version` has been replaced by `spec.image` ([#599]).
- Updated stackable image versions ([#586]).
- Operator-rs: 0.25.3 -> 0.27.1 ([#591]).
- Fixed bug where ZNode ConfigMaps were not created due to labeling issues ([#592]).
- tokio-zookeeper: 0.1.3 -> 0.2.1 ([#593]).
- Don't run init container as root and avoid chmod and chowning ([#603]).
- Fixed the RoleGroup `selector`. It was not used before. ([#611]).
- [BREAKING] Moved `spec.authentication`, `spec.tls` and `spec.logging` to `spec.clusterConfig`. Consolidated sub field names like `tls.client.secretClass` to `tls.serverSecretClass` ([#612]).
- Changes to be compatible with crate2nix ([#647]).

[#586]: https://github.com/stackabletech/zookeeper-operator/pull/586
[#591]: https://github.com/stackabletech/zookeeper-operator/pull/591
[#592]: https://github.com/stackabletech/zookeeper-operator/pull/592
[#593]: https://github.com/stackabletech/zookeeper-operator/pull/593
[#599]: https://github.com/stackabletech/zookeeper-operator/pull/599
[#603]: https://github.com/stackabletech/zookeeper-operator/pull/603
[#611]: https://github.com/stackabletech/zookeeper-operator/pull/611
[#612]: https://github.com/stackabletech/zookeeper-operator/pull/612
[#647]: https://github.com/stackabletech/zookeeper-operator/pull/647

## [0.12.0] - 2022-11-07

### Added

- Default resource requests (memory and cpu) for ZooKeeper pods ([#563]).

### Changed

- Resources associated with rolegroups that have since been removed from the ZookeeperCluster will now be deleted ([#569]).
- Operator-rs: 0.22.0 -> 0.25.3 ([#569]).

[#569]: https://github.com/stackabletech/zookeeper-operator/pull/569
[#563]: https://github.com/stackabletech/zookeeper-operator/pull/563

## [0.11.0] - 2022-09-06

### Changed

- Operator-rs: 0.21.1 -> 0.22.0 ([#516]).
- Include chart name when installing with a custom release name ([#517], [#518]).
- Fixed bug where client TLS could not be disabled ([#529]).
- Switched init container to tools image ([#533]).
- Fixed client authentication. Now only the provided secretClass is eligible. Split up tls (client/quorum) dirs into separate directories and create key/truststores in different directory ([#533]).
- Replaced python image with testing-tools image for integration tests ([#535]).

[#516]: https://github.com/stackabletech/zookeeper-operator/pull/516
[#517]: https://github.com/stackabletech/zookeeper-operator/pull/517
[#518]: https://github.com/stackabletech/zookeeper-operator/pull/518
[#529]: https://github.com/stackabletech/zookeeper-operator/pull/529
[#533]: https://github.com/stackabletech/zookeeper-operator/pull/533
[#535]: https://github.com/stackabletech/zookeeper-operator/pull/535

## [0.10.0] - 2022-06-23

### Added

- Reconciliation errors are now reported as Kubernetes events ([#408]).
- Use cli argument `watch-namespace` / env var `WATCH_NAMESPACE` to specify
  a single namespace to watch ([#434]).
- Support for ZooKeeper 3.8.0 added ([#464]).
- Integration tests for all supported ZooKeeper versions added ([#464]).
- TLS encryption and authentication support for quorum and client ([#479]).
- PVCs for data storage, cpu and memory limits are now configurable ([#490]).
- OpenShift compatibility: use custom service account and cluster role for product pods ([#505]).

### Changed

- Operator-rs: 0.10.0 -> 0.21.1 ([#408], [#431], [#434], [#454], [#479], [#490]).
- [BREAKING] Specifying the product version has been changed to adhere to [ADR018](https://docs.stackable.tech/home/contributor/adr/ADR018-product_image_versioning.html) instead of just specifying the product version you will now have to add the Stackable image version as well, so `version: 3.5.8` becomes (for example) `version: 3.5.8-stackable0.1.0` ([#487])

[#408]: https://github.com/stackabletech/zookeeper-operator/pull/408
[#431]: https://github.com/stackabletech/zookeeper-operator/pull/431
[#434]: https://github.com/stackabletech/zookeeper-operator/pull/434
[#454]: https://github.com/stackabletech/zookeeper-operator/pull/454
[#464]: https://github.com/stackabletech/zookeeper-operator/pull/464
[#479]: https://github.com/stackabletech/zookeeper-operator/pull/479
[#487]: https://github.com/stackabletech/zookeeper-operator/pull/487
[#490]: https://github.com/stackabletech/zookeeper-operator/pull/490
[#505]: https://github.com/stackabletech/zookeeper-operator/pull/505

## [0.9.0] - 2022-02-14

### Added

- Enabled Prometheus scraping ([#380]).
- ZookeeperZnode.spec.clusterRef.namespace now defaults to .metadata.namespace ([#382]).
- PodSecurityContext.fsGroup to allow write access to mounted volumes ([#406]).
- Added `ZOOKEEPER_HOSTS` and `ZOOKEEPER_CHROOT` to discovery config maps,
  for clients that do not support the composite connection string ([#421]).

### Changed

- Shut down gracefully ([#338]).
- Fixed ACL incompatibility with certain managed K8s providers ([#340]).
- Operator-rs: 0.6.0 -> 0.10.0 ([#352], [#383]).
- Cleanup for `ZookeeperZnode` now succeeds if the linked `ZookeeperCluster` was already deleted ([#384]).

[#338]: https://github.com/stackabletech/zookeeper-operator/pull/338
[#340]: https://github.com/stackabletech/zookeeper-operator/pull/340
[#352]: https://github.com/stackabletech/zookeeper-operator/pull/352
[#380]: https://github.com/stackabletech/zookeeper-operator/pull/380
[#382]: https://github.com/stackabletech/zookeeper-operator/pull/382
[#383]: https://github.com/stackabletech/zookeeper-operator/pull/383
[#384]: https://github.com/stackabletech/zookeeper-operator/pull/384
[#406]: https://github.com/stackabletech/zookeeper-operator/pull/406
[#421]: https://github.com/stackabletech/zookeeper-operator/pull/421

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
