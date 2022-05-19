#!/usr/bin/env bash
# Usage: test_heap.sh

# 0.5Gi * 1024 -> 512Mebi * 0.8 -> 409
EXPECTED_HEAP=409

# Check if ZK_SERVER_HEAP is set to the correct calculated value
if [[ $ZK_SERVER_HEAP == "$EXPECTED_HEAP" ]]
then
  echo "[SUCCESS] ZK_SERVER_HEAP set to $EXPECTED_HEAP"
else
  echo "[ERROR] ZK_SERVER_HEAP not set or set with wrong value: $ZK_SERVER_HEAP"
  exit 1
fi

echo "[SUCCESS] All heap settings tests successful!"
