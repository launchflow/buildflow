#!/bin/bash

export INPUT_QUEUE=input-$RANDOM
export OUTPUT_QUEUE=output-$RANDOM

cd release_tests/sqs_to_sqs
mkdir -p .buildflow/_pulumi/local
ray start --head --num-cpus=2

buildflow preview || {
    echo 'preview failed'
    exit 1
}

buildflow apply || {
    echo 'apply failed'
    exit 1
}

input_queue_url=$(aws sqs get-queue-url --region=us-east-1 --queue-name $INPUT_QUEUE --output text --query "QueueUrl")
output_queue_url=$(aws sqs get-queue-url --region=us-east-1 --queue-name $OUTPUT_QUEUE --output text --query "QueueUrl")

buildflow run &
main_pid=$!

sleep 20
aws sqs send-message --region=us-east-1 --queue-url $input_queue_url --message-body '{"val": 1}'
send_message_code=$?
sleep 60

kill -s 2 $main_pid
wait $main_pid
final_code=$?

num_input_messages=$(aws sqs get-queue-attributes --queue-url $input_queue_url --region=us-east-1 --attribute-names=ApproximateNumberOfMessages --output text --query "Attributes.ApproximateNumberOfMessages")
num_output_messages=$(aws sqs get-queue-attributes --queue-url $output_queue_url --region=us-east-1 --attribute-names=ApproximateNumberOfMessages --output text --query "Attributes.ApproximateNumberOfMessages")

buildflow destroy || {
    echo 'destroy failed'
    exit 1
}
ray stop --force

# NOTE: we do all the validation down here to ensure we cleanup the resources even if there is a failure
if [[ $num_input_messages > 0 ]]; then
    echo "Found $num_input_messages messages still on the input queue"
    echo "Test Failed!"
    exit 1
fi

if [[ $num_output_messages > 0 ]]; then
    echo "Found $num_output_messages messages still on the output queue"
    echo "Test Failed!"
    exit 1
fi

if [[ $send_message_code != 0 ]]; then
    echo "Failed to send message to input queue"
    echo "Test Failed!"
    exit 1
fi

exit $final_code
