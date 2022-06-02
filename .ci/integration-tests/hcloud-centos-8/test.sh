#!/bin/bash
git clone -b "$GIT_BRANCH" https://github.com/stackabletech/zookeeper-operator.git
(cd zookeeper-operator/ && ./scripts/run_tests.sh --parallel 4)
exit_code=$?
./operator-logs.sh zookeeper > /target/zookeeper-operator.log
exit $exit_code
