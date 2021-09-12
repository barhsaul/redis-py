#!/bin/bash

# This is the entrypoint for "make test". It invokes Tox. If running
# outside the CI environment, it disables uploading the coverage report to codecov

set -eu

REDIS_MASTER="${REDIS_MASTER_HOST}":"${REDIS_MASTER_PORT}"
echo "Testing against Redis Server: ${REDIS_MASTER}"

# skip the "codecov" env if not running on Travis
if [ "${GITHUB_ACTIONS}" = true ] ; then
    echo "Skipping codecov"
    export TOX_SKIP_ENV="codecov"
fi

# use the wait-for-it util to ensure the server is running before invoking Tox
util/wait-for-it.sh ${REDIS_MASTER} -- tox -- --redis-url=redis://"${REDIS_MASTER}"/${DB}

if [[ -v CLUSTER_MASTER_HOST ]] ; then
  # Test Cluster Redis
  CLUSTER_MASTER="${CLUSTER_MASTER_HOST}":"${CLUSTER_MASTER_PORT}"
  echo "Testing against Cluster-Redis Server: ${CLUSTER_MASTER}"
  # We can skip these environments since they were already tested in the strict Redis tests
  export TOX_SKIP_ENV='.*?(flake8|covreport|codecov)'
  tox -- --redis-url=redis://"${CLUSTER_MASTER}"/${CLUSTER_DB}
fi


