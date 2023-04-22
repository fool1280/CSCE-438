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
  std::string ip = "";
  std::string port = "";
  bool active = false;
  std::string timestamp = "";
  std::string serverId = "";
};

std::vector<Cluster> master;
std::vector<Cluster> slave;
std::vector<Cluster> followsync;
std::map<ServerType, std::string> enumName;

void initData()
{
  for (int i = 0; i < 3; i++)
  {
    Cluster a = {"", "", false, "", ""};
    Cluster b = {"", "", false, "", ""};
    Cluster c = {"", "", false, "", ""};
    master.push_back(a);
    slave.push_back(b);
    followsync.push_back(c);
    enumName[ServerType::MASTER] = "Master";
    enumName[ServerType::SLAVE] = "Slave";
    enumName[ServerType::SYNC] = "Sync";
  }
}

void printCoordinator()
{
  for (int i = 0; i < 3; i++)
  {
    std::cout << "Index " + std::to_string(i) << std::endl;
    std::cout << "Master: serverId=" + master[i].serverId + ", ip=" + master[i].ip + +", port=" + master[i].port + ", active=" + (master[i].active ? "true, " : "false, ") + master[i].timestamp << std::endl;
    std::cout << "Slave: serverId=" + slave[i].serverId + ", ip=" + slave[i].ip + ", port=" + slave[i].port + ", active=" + (slave[i].active ? "true, " : "false, ") + slave[i].timestamp << std::endl;
    std::cout << "Follow sync: serverId=" + followsync[i].serverId + ", ip=" + followsync[i].ip + ", port=" + followsync[i].port + ", active=" + (followsync[i].active ? "true, " : "false, ") + followsync[i].timestamp << std::endl;
  }
  std::cout << std::endl;
}

int64_t difference(Cluster cluster)
{
  google::protobuf::Timestamp lastTimestamp;
  google::protobuf::util::TimeUtil::FromString(cluster.timestamp, &lastTimestamp);
  google::protobuf::Timestamp *currentTimestamp = new google::protobuf::Timestamp();
  currentTimestamp->set_seconds(time(NULL));
  currentTimestamp->set_nanos(0);
  int64_t difference = (currentTimestamp->seconds() - lastTimestamp.seconds()) * 1000 + (currentTimestamp->nanos() - lastTimestamp.nanos()) / 1000000;
  return difference;
}

void updateCoordinator(int server_id, ServerType server_type, std::string server_ip, std::string server_port, std::string timestamp)
{
  int pos = (server_id % 3);
  Cluster newCluster = {server_ip, server_port, true, timestamp, std::to_string(server_id)};
  if (enumName[server_type] == "Master")
  {
    Cluster cluster = master[pos];
    // std::cout << "Diff: " << std::to_string(difference) << std::endl;
    if (cluster.active && difference(cluster) <= 20000)
    {
      master[pos].timestamp = timestamp;
      return;
    }
    master[pos] = newCluster;
  }
  else if (enumName[server_type] == "Slave")
  {
    Cluster cluster = slave[pos];
    // std::cout << "Diff: " << std::to_string(difference) << std::endl;
    if (cluster.active && difference(cluster) <= 20000)
    {
      slave[pos].timestamp = timestamp;
      return;
    }
    slave[pos] = newCluster;
  }
  else if (enumName[server_type] == "Sync")
  {
    Cluster cluster = followsync[pos];
    // std::cout << "Diff: " << std::to_string(difference) << std::endl;
    if (cluster.active && difference(cluster) <= 20000)
    {
      followsync[pos].timestamp = timestamp;
      return;
    }
    followsync[pos] = newCluster;
  }
}

class SNSCoordinatorImpl final : public SNSCoordinator::Service
{
  Status HandleHeartBeats(ServerContext *context, ServerReaderWriter<Heartbeat, Heartbeat> *stream) override
  {
    Heartbeat heartbeat;
    while (stream->Read(&heartbeat))
    {
      int server_id = heartbeat.server_id();
      ServerType server_type = heartbeat.server_type();
      std::string server_ip = heartbeat.server_ip();
      std::string server_port = heartbeat.server_port();
      google::protobuf::Timestamp timestamp = heartbeat.timestamp();
      log(INFO, "Heartbeat received for server id=" + std::to_string(server_id) + ",ip=" + server_ip + ",type=" + enumName[server_type] + ",port=" + server_port + ",timestamp=" + google::protobuf::util::TimeUtil::ToString(timestamp));

      updateCoordinator(server_id, server_type, server_ip, server_port, google::protobuf::util::TimeUtil::ToString(timestamp));
      printCoordinator();
    };
    return Status::OK;
  }

  Status GetServer(ServerContext *context, const User *request, ServerInfo *reply) override
  {
    int user_id = request->user_id();
    int pos = (user_id % 3);
    log(INFO, "Difference from master: " + std::to_string(difference(master[pos])));

    if (difference(master[pos]) <= 20000) // master server still active
    {
      Cluster cluster = master[pos];
      reply->set_server_ip(cluster.ip);
      reply->set_port_num(cluster.port);
      reply->set_server_id(stoi(cluster.serverId));
      reply->set_server_type(ServerType::MASTER);
      log(INFO, "client id=" + std::to_string(user_id) + " connect to master server id=" + cluster.serverId);
    }
    else
    {
      std::cout << "Master is inactive" << std::endl;
      Cluster cluster = slave[pos];
      reply->set_server_ip(cluster.ip);
      reply->set_port_num(cluster.port);
      reply->set_server_id(stoi(cluster.serverId));
      reply->set_server_type(ServerType::SLAVE);
      log(INFO, "client id=" + std::to_string(user_id) + " connect to slave server id=" + cluster.serverId);
      std::cout << "Slave: " << cluster.ip << ", port: " << cluster.port << std::endl;
      log(INFO, "Slave server id=" + cluster.serverId + " becomes master");
    }
    return Status::OK;
  }

  Status GetSlave(ServerContext *context, const ClusterId *request, ServerInfo *reply) override
  {
    int cluster_id = request->cluster();
    int i = (cluster_id % 3);
    Cluster cluster = slave[i];
    reply->set_server_ip(cluster.ip);
    reply->set_port_num(cluster.port);
    reply->set_server_id(stoi(cluster.serverId));
    reply->set_server_type(ServerType::SLAVE);
    log(INFO, "Return slave server with serverId=" + slave[i].serverId + ", ip=" + slave[i].ip + ", active=" + (slave[i].active ? "true, " : "false, ") + slave[i].timestamp);
    return Status::OK;
  }
};

void RunServer(std::string port)
{
  std::string server_address = "0.0.0.0:" + port;
  SNSCoordinatorImpl service;

  ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Coordinator listening on " << server_address << std::endl;
  log(INFO, "Coordinator listening on " + server_address);

  server->Wait();
}

int main(int argc, char **argv)
{
  std::string port = "9090";
  initData();
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
  FLAGS_log_dir = "/home/csce438/CSCE-438/MP_3/tmp";
  log(INFO, "Logging Initialized. Coordinator starting on port " + port);
  RunServer(port);
  return 0;
}