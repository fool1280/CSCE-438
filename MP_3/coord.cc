#include <ctime>

#include <google/protobuf/timestamp.pb.h>
#include <google/protobuf/duration.pb.h>

#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <vector>
#include <stdlib.h>
#include <unistd.h>
#include <google/protobuf/util/time_util.h>
#include <grpc++/grpc++.h>
#include <glog/logging.h>

#define log(severity, msg) \
  LOG(severity) << msg;    \
  google::FlushLogFiles(google::severity);

#include "sns.grpc.pb.h"
using csce438::ListReply;
using csce438::Message;
using csce438::Reply;
using csce438::Request;
using csce438::SNSService;

#include "snsCoordinator.grpc.pb.h"
using snsCoordinator::ClusterId;
using snsCoordinator::FollowSyncs;
using snsCoordinator::Heartbeat;
using snsCoordinator::ServerInfo;
using snsCoordinator::ServerType;
using snsCoordinator::SNSCoordinator;
using snsCoordinator::User;
using snsCoordinator::Users;

using google::protobuf::Duration;
using google::protobuf::Timestamp;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ServerReaderWriter;
using grpc::ServerWriter;
using grpc::Status;

struct Cluster
{
  int port = -1;
  bool active = false;
  google::protobuf::Timestamp timestamp;
};

std::vector<Cluster> master;
std::vector<Cluster> slave;
std::vector<Cluster> followsync;
std::map<ServerType, std::string> enumName;

void initData()
{
  for (int i = 0; i < 3; i++)
  {
    master.push_back(Cluster());
    slave.push_back(Cluster());
    followsync.push_back(Cluster());
    enumName[ServerType::MASTER] = "Master";
    enumName[ServerType::SLAVE] = "Slave";
    enumName[ServerType::SYNC] = "Sync";
  }
}
class SNSCoordinatorImpl final : public SNSCoordinator::Service
{
  Status HandleHeartBeats(ServerContext *context, ServerReaderWriter<Heartbeat, Heartbeat> *stream) override
  {
    log(INFO, "Heartbeats Request");
    Heartbeat heartbeat;
    while (stream->Read(&heartbeat))
    {
      int server_id = heartbeat.server_id();
      ServerType server_type = heartbeat.server_type();
      std::string server_ip = heartbeat.server_ip();
      std::string server_port = heartbeat.server_port();
      google::protobuf::Timestamp timestamp = heartbeat.timestamp();

      std::string debugString = "ip=" + server_ip + ",type=" + enumName[server_type] + ",port=" + server_port + ",timestamp=";
      log(INFO, debugString);
    };
    return Status::OK;
  }
};

int main(int argc, char **argv)
{
  std::string port = "9090";

  int opt = 0;
  while ((opt = getopt(argc, argv, "p:")) != -1)
  {
    switch (opt)
    {
    case 'p':
      port = optarg;
      break;
    default:
      std::cerr << "Invalid port number for coordinator\n";
    }
  }

  std::string log_file_name = std::string("coordinator-") + port;
  google::InitGoogleLogging(log_file_name.c_str());
  FLAGS_log_dir = "/Users/anhnguyen/Data/CSCE438/CSCE-438/MP_3/tmp";
  log(INFO, "Logging Initialized. Coordinator starting on port " + port);

  return 0;
}