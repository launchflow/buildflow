#!/bin/bash

final_output=1
gcloud beta emulators pubsub start --project=pubsub-test-project --host-port=localhost:8085 &
sleep 5
export PUBSUB_EMULATOR_HOST=localhost:8085

file_path=/tmp/$(uuidgen)/output.parquet

export OUTPUT_FILE_PATH=$file_path
buildflow run pubsub_main:app &
python pubsub_publish.py
python pubsub_validation.py --file_path=$file_path
final_output=$?

unset FLOW_FILE
unset FLOW_DEPLOYMENT_FILE
unset PUBSUB_EMULATOR_HOST

ray stop --force
pkill -f java
exit $final_output
