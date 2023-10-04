#!/bin/bash

export BIGQUERY_TABLE=taxi-test-$RANDOM

cd buildflow/samples/pubsub_walkthrough
ray start --head --num-cpus=2
buildflow init --directory=. --project=pubsub-to-bigquery
buildflow plan || {
  echo 'plan failed'
  exit 1
}
buildflow apply || {
  echo 'apply failed'
  exit 1
}
buildflow run &
main_pid=$!

sleep 45
query="SELECT COUNT(*) as count FROM \`$GCP_PROJECT.buildflow_pubsub_to_bigquery_test.$BIGQUERY_TABLE\`"
echo "Running query: $query"
bq query --location=US --nouse_legacy_sql $query
num_rows=$(bq query --location=US --nouse_legacy_sql $query | awk 'NR==4 {print $2}')

kill -s 2 $main_pid
wait $main_pid
buildflow destroy || {
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
