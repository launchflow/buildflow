#!/bin/bash

export BIGQUERY_TABLE=taxi-test-$RANDOM

cd buildflow/samples/pubsub_walkthrough
ray start --head --num-cpus=2
buildflow init --directory=. --default-cloud-provider=gcp --default-gcp-project=$GCP_PROJECT
pwd
buildflow plan main:app || {
  echo 'plan failed'
  exit 1
}
buildflow run main:app &
main_pid=$!

sleep 45
query="SELECT COUNT(*) as count FROM \`$GCP_PROJECT.buildflow_walkthrough.$BIGQUERY_TABLE\`"
echo "Running query: $query"
num_rows=$(bq query --location=US --nouse_legacy_sql $query | grep -Po '.* \K\d+\.*\d*')

kill $main_pid
wait $main_pid
buildflow destroy main:app || {
  echo 'destroy failed'
  exit 1
}
ray stop --force

if [[ $num_rows > 0 ]]; then
  echo "Found $num_rows rows"
  echo "Test passed!"
  exit 0
else
  echo "No rows found"
  echo "Test failed!"
  exit 1
fi
