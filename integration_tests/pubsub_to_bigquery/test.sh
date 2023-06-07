#!/bin/bash

export BIGQUERY_TABLE=taxi-test-$RANDOM
export SUBSCRIPTION=taxi-test-$RANDOM
export DATASET=buildflow_walkthrough_$RANDOM

ray start --head --num-cpus=2

buildflow run buildflow.samples.pubsub_walkthrough:app --apply-infrastructure &
main_pid=$!

sleep 30
query="SELECT COUNT(*) as count FROM \`$GCP_PROJECT.$DATASET.$BIGQUERY_TABLE\`"
echo "Running query: $query"
num_rows=$(bq query --location=US --nouse_legacy_sql $query | grep -Po '.* \K\d+\.*\d*')

kill $main_pid
wait $main_pid
ray stop --force

if [[ $num_rows > 0 ]];
then
  echo "Found $num_rows rows"
  echo "Test passed!"
  exit 0
else
  echo "No rows found"
  echo "Test failed!"
  exit 1
fi
