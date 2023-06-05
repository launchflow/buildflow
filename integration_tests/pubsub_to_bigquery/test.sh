#!/bin/bash

export BIGQUERY_TABLE=taxi-test-$RANDOM
export SUBSCRIPTION=taxi-test-$RANDOM

ray start --head --num-cpus=2

buildflow run buildflow.samples.pubsub_walkthrough:app --apply-infrastructure --destroy-infrastructure &
main_pid=$!

sleep 30
query="SELECT COUNT(*) as count FROM \`$GCP_PROJECT.buildflow_walkthrough.${bq_table}\`"
num_rows=$(bq query --location=US --nouse_legacy_sql $query | grep -Po '.* \K\d+\.*\d*')
if [[ $num_rows > 0 ]]; then exit_code=0; else exit_code=1; fi

kill $main_pid
wait $main_pid
ray stop --force

pkill -f java

echo "Found $num_rows rows"
if [[ $exit_code == 0 ]]
then
  echo "Test passed!"
else
  echo "Test failed!"
fi
exit $exit_code
