#ifndef PUBSUB_STREAM_H
#define PUBSUB_STREAM_H

#include <string>
#include <utility>
#include <vector>

#include "google/pubsub/v1/pubsub.grpc.pb.h"
#include "google/pubsub/v1/pubsub.pb.h"
#include "grpc++/grpc++.h"

namespace buildflow {

class CPubSubData {
public:
  CPubSubData(std::string data, std::string ack_id);
  std::string data;
  std::string ack_id;
};

class CPubSubStream {
private:
  std::string subscription_id;
  std::shared_ptr<grpc::ChannelCredentials> creds;
  std::shared_ptr<grpc::Channel> channel;
  std::unique_ptr<google::pubsub::v1::Subscriber::Stub> stub;
  std::unique_ptr<
      grpc::ClientReaderWriter<google::pubsub::v1::StreamingPullRequest,
                               google::pubsub::v1::StreamingPullResponse>>
      pubsub_stream;
  std::unique_ptr<grpc::ClientContext> context;
  // Will be initialized upon first call to pull()
  bool is_stream_initialized = false;

public:
  CPubSubStream(std::string subscription_id);
  void run();
  std::vector<CPubSubData> pull();
  void ack(std::vector<std::string> ack_ids);
};
}; // namespace buildflow

#endif
