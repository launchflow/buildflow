#!/bin/bash

export INCOMING_TOPIC=incoming-$RANDOM
export OUTGOING_TOPIC=outgoing-$RANDOM
export VALIDATION_SUB=validation-$RANDOM
export MAIN_SUB=main-$RANDOM

cd release_tests/pubsub_to_pubsub
mkdir -p .buildflow/_pulumi/local
ray start --head --num-cpus=2

final_output=1
buildflow preview || {
    echo 'preview failed'
    exit 1
}
buildflow apply || {
    echo 'apply failed'
    exit 1
}
buildflow run &
main_pid=$!

python pubsub_publish.py
python pubsub_validation.py

final_output=$?

kill -s 2 $main_pid
wait $main_pid
buildflow destroy || {
    echo 'destroy failed'
    exit 1
}
ray stop --force

gcloud pubsub subscriptions delete projects/$GCP_PROJECT/subscriptions/$VALIDATION_SUB

exit $final_output
