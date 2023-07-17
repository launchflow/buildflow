#include <iostream>
#include <string>
#include <vector>

#include "google/pubsub/v1/pubsub.grpc.pb.h"
#include "google/pubsub/v1/pubsub.pb.h"
#include "grpc++/grpc++.h"

#include "buildflow/core/io/gcp/strategies/_cython/pubsub_stream.h"

buildflow::CPubSubStream::CPubSubStream(std::string sub_id) {
  subscription_id = sub_id;
  creds = grpc::GoogleDefaultCredentials();
  channel = grpc::CreateChannel("pubsub.googleapis.com", creds);
  stub = std::make_unique<google::pubsub::v1::Subscriber::Stub>(channel);
  context = std::make_unique<grpc::ClientContext>();
  pubsub_stream = stub->StreamingPull(context.get());
}

std::vector<buildflow::CPubSubData> buildflow::CPubSubStream::pull() {
  if (!is_stream_initialized) {
    google::pubsub::v1::StreamingPullRequest request;
    request.set_subscription(subscription_id);
    request.set_stream_ack_deadline_seconds(10);
    pubsub_stream->Write(request);
    is_stream_initialized = true;
  }
  google::pubsub::v1::StreamingPullResponse response;
  pubsub_stream->Read(&response);

  std::vector<CPubSubData> v;
  for (const auto &message : response.received_messages()) {
    v.push_back(CPubSubData(message.message().data(), message.ack_id()));
  }
  return v;
}

void buildflow::CPubSubStream::ack(std::vector<std::string> ack_ids) {
  google::pubsub::v1::StreamingPullRequest request;
  for (const auto &ack_id : ack_ids) {
    request.add_ack_ids(ack_id);
  }
  pubsub_stream->Write(request);
}

buildflow::CPubSubData::CPubSubData(std::string d, std::string a) {
  data = d;
  ack_id = a;
}
