#!/bin/bash

export INCOMING_TOPIC=incoming-$RANDOM
export OUTGOING_TOPIC=outgoing-$RANDOM
export VALIDATION_SUB=validation-$RANDOM
export MAIN_SUB=main-$RANDOM

gcloud pubsub topics create projects/$GCP_PROJECT/topics/$INCOMING_TOPIC

ray start --head --num-cpus=2

final_output=1

buildflow run pubsub_main:app --apply-infrastructure --destroy-infrastructure &
main_pid=$!

python pubsub_publish.py
python pubsub_validation.py

final_output=$?

kill $main_pid
wait $main_pid
ray stop --force

pkill -f java
gcloud pubsub topics delete projects/$GCP_PROJECT/topics/$INCOMING_TOPIC
gcloud pubsub subscriptions delete projects/$GCP_PROJECT/subscriptions/$VALIDATION_SUB

exit $final_output
