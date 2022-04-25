git clone -b $GIT_BRANCH https://github.com/stackabletech/integration-tests.git
(cd zookeeper-operator/ && ./scripts/run_tests.sh)
exit_code=$?
./operator-logs.sh zookeeper > /target/zookeeper-operator.log
exit $exit_code
