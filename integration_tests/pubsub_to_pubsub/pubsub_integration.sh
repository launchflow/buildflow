#!/bin/bash

final_output=1
gcloud beta emulators pubsub start --project=pubsub-test-project --host-port=localhost:8085 &
sleep 5
export PUBSUB_EMULATOR_HOST=localhost:8085

python pubsub_main.py --disable_usage_stats &
sleep 15
python pubsub_publish.py
python pubsub_validation.py
final_output=$?

unset FLOW_FILE
unset FLOW_DEPLOYMENT_FILE
unset PUBSUB_EMULATOR_HOST

ray stop --force
pkill -f java
exit $final_output
