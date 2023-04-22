/*
 *
 * Copyright 2015, Google Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 *     * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above
 * copyright notice, this list of conditions and the following disclaimer
 * in the documentation and/or other materials provided with the
 * distribution.
 *     * Neither the name of Google Inc. nor the names of its
 * contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

#include <ctime>

#include <google/protobuf/timestamp.pb.h>
#include <google/protobuf/duration.pb.h>

#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <chrono>
#include <stdlib.h>
#include <algorithm>
#include <unistd.h>
#include <google/protobuf/util/time_util.h>
#include <grpc++/grpc++.h>
#include <glog/logging.h>
#include <sys/stat.h>
#include <vector>
#include <map>
#include <set>

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
using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReader;
using grpc::ClientReaderWriter;
using grpc::ClientWriter;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ServerReaderWriter;
using grpc::ServerWriter;
using grpc::Status;

std::string coordIp = "0.0.0.0";
std::string coordPort = "9090";
std::string serverPort = "3010";
int serverId = 0;
std::string serverType = "master";
std::unique_ptr<SNSCoordinator::Stub> stub_;
std::unique_ptr<SNSService::Stub> slavestub_;

using std::cout, std::cin, std::endl, std::string;
using std::ofstream, std::ifstream, std::istringstream;
using std::vector, std::map, std::set, std::pair, std::find, std::getline;

set<string> all_users;
map<string, vector<string>> following_users;
map<string, ServerReaderWriter<Message, Message> *> streams;

void writefile()
{
  ofstream myfile;
  string filedir = "./" + serverType + "_" + std::to_string(serverId);
  myfile.open(filedir + "/followers.txt");
  for (auto i : following_users)
  {
    myfile << i.first;
    vector<string> following = i.second;
    for (auto j : following)
    {
      myfile << " " << j;
    }
    myfile << endl;
  }
  myfile.close();
};

void readfile()
{
  string line;
  string filedir = "./" + serverType + "_" + std::to_string(serverId);
  ifstream myfile(filedir + "/followers.txt");
  if (myfile.is_open())
  {
    set<string> new_all_users;
    map<string, vector<string>> new_following_users;
    while (getline(myfile, line))
    {
      istringstream ss(line);
      string username;
      ss >> username;
      new_following_users.insert(pair<string, vector<string>>(username, vector<string>()));

      string follower;
      while (ss >> follower)
      {
        new_following_users[username].push_back(follower);
      }
    }
    following_users = new_following_users;
    myfile.close();

    // cout <<  "After reading files: " << endl;
    for (auto i : following_users)
    {
      // cout <<  "User " << i.first << ":";
      vector<string> following = i.second;
      for (auto j : following)
      {
        // cout <<  " " << j;
      }
      // cout <<  endl;
    }
  }
  else
  {
    // cout <<  "Unable to open file" << endl;
  }
}

void replicateToSlave(std::string command, const Request request)
{
  if (serverType == "slave")
    return;
  ClientContext context;
  ClusterId clusterId;
  clusterId.set_cluster(serverId);
  ServerInfo serverInfo;

  std::string login_info = coordIp + ":" + coordPort;
  stub_ = std::unique_ptr<SNSCoordinator::Stub>(SNSCoordinator::NewStub(
      grpc::CreateChannel(
          login_info, grpc::InsecureChannelCredentials())));
  log(INFO, "Connect to coordinator " + login_info);
  Status status = stub_->GetSlave(&context, clusterId, &serverInfo);

  if (status.ok())
  {
    std::string hostname = serverInfo.server_ip();
    std::string port = serverInfo.port_num();
    log(INFO, "Received a response about slave server info with ip=" + hostname + ", port=" + port);

    std::string login_info = hostname + ":" + port;
    slavestub_ = std::unique_ptr<SNSService::Stub>(SNSService::NewStub(
        grpc::CreateChannel(
            login_info, grpc::InsecureChannelCredentials())));

    if (command == "follow")
    {
      ClientContext slaveContext;
      Reply slaveReply;
      Status slaveStatus = slavestub_->Follow(&slaveContext, request, &slaveReply);
      if (slaveStatus.ok())
      {
        log(INFO, "Replicate follow to slave server info with ip=" + hostname + ", port=" + port);
      }
    }
    if (command == "login")
    {
      ClientContext slaveContext;
      Reply slaveReply;
      Status slaveStatus = slavestub_->Login(&slaveContext, request, &slaveReply);
      if (slaveStatus.ok())
      {
        log(INFO, "Replicate login to slave server info with ip=" + hostname + ", port=" + port);
      }
    }
  }
}

class SNSServiceImpl final : public SNSService::Service
{
  Status List(ServerContext *context, const Request *request, ListReply *reply) override
  {
    // ------------------------------------------------------------
    // In this function, you are to write code that handles
    // LIST request from the user. Ensure that both the fields
    // all_users & following_users are populated
    // ------------------------------------------------------------
    readfile();
    string username = request->username();
    // cout <<  "List request for username " << username << endl;
    for (auto i : all_users)
    {
      // cout <<  i << endl;
      reply->add_all_users(i);
    }
    vector<string> following = following_users[username];
    for (auto i : following)
    {
      // cout <<  "following " << i << endl;
      reply->add_followers(i);
    }
    writefile();
    return Status::OK;
  }

  Status Follow(ServerContext *context, const Request *request, Reply *reply) override
  {
    // ------------------------------------------------------------
    // In this function, you are to write code that handles
    // request from a user to follow one of the existing
    // users
    // ------------------------------------------------------------
    readfile();
    replicateToSlave("follow", *request);
    string currentUser = request->username();
    string userToFollow = request->arguments().at(0);
    vector<string> currentFollow = following_users[currentUser];
    auto exist = all_users.find(userToFollow);
    auto hasFollow = std::find(currentFollow.begin(), currentFollow.end(), userToFollow);
    // cout <<  "User exist: " << (bool)(exist != all_users.end()) << endl;
    // cout <<  "User " << currentUser << " has follow: " << (bool)(hasFollow != currentFollow.end()) << endl;
    if (exist != all_users.end() && hasFollow == currentFollow.end())
    {
      following_users[currentUser].push_back(userToFollow);
      sort(following_users[currentUser].begin(), following_users[currentUser].end());
      writefile();
      reply->set_msg("Follow Successful");
    }
    else if (exist == all_users.end())
    {
      reply->set_msg("unkown user name");
    }
    else if (hasFollow != currentFollow.end())
    {
      reply->set_msg("you have already joined");
    }
    return Status::OK;
  }

  Status UnFollow(ServerContext *context, const Request *request, Reply *reply) override
  {
    // ------------------------------------------------------------
    // In this function, you are to write code that handles
    // request from a user to unfollow one of his/her existing
    // followers
    // ------------------------------------------------------------
    // string currentUser = request->username();
    // string userToFollow = request->arguments().at(0);
    // auto exist = all_users.find(userToFollow);
    // auto hasFollow = following_users[currentUser].find(userToFollow);
    readfile();
    string currentUser = request->username();
    string userToUnfollow = request->arguments().at(0);
    vector<string> currentFollow = following_users[currentUser];
    auto exist = all_users.find(userToUnfollow);
    auto hasFollow = std::find(currentFollow.begin(), currentFollow.end(), userToUnfollow);
    // cout <<  "User exist: " << (bool)(exist != all_users.end()) << endl;
    // cout <<  "User " << currentUser << " has follow: " << (bool)(hasFollow != currentFollow.end()) << endl;
    if (exist != all_users.end() && hasFollow != currentFollow.end())
    {
      following_users[currentUser].erase(
          std::remove(following_users[currentUser].begin(), following_users[currentUser].end(), userToUnfollow),
          following_users[currentUser].end());
      writefile();
      return Status::OK;
    }
    return Status::CANCELLED;
  }

  Status Login(ServerContext *context, const Request *request, Reply *reply) override
  {
    // ------------------------------------------------------------
    // In this function, you are to write code that handles
    // a new user and verify if the username is available
    // or already taken
    // ------------------------------------------------------------
    readfile();
    replicateToSlave("login", *request);
    string username = request->username();
    auto it = all_users.find(username);
    if (it == all_users.end())
    {
      // cout <<  "Username not exist, intialize " << username << endl;
      all_users.insert(username);

      auto it = following_users.find(username);
      if (it == following_users.end())
      {
        following_users.insert(pair<string, vector<string>>(username, vector<string>()));
        following_users[username].push_back(username);
      }
      writefile();
      return Status::OK;
    }
    // cout <<  "Username already exists " << username << endl;
    return Status::CANCELLED;
  }

  Status Timeline(ServerContext *context, ServerReaderWriter<Message, Message> *stream) override
  {
    // ------------------------------------------------------------
    // In this function, you are to write code that handles
    // receiving a message/post from a user, recording it in a file
    // and then making it available on his/her follower's streams
    // ------------------------------------------------------------
    readfile();
    auto data = context->client_metadata().find("username")->second;
    string username(data.data(), data.size());

    if (streams.find(username) == streams.end())
    {
      streams.insert(pair<string, ServerReaderWriter<Message, Message> *>(username, stream));
    }

    vector<Message> last_20_messages;
    string line;
    string filename = username + "_timeline.txt";
    ifstream myfile(filename);
    if (myfile.is_open())
    {
      int count = 0;
      string sender;
      // cout <<  "Open file " << filename << " successfully " << endl;
      while (getline(myfile, sender))
      {
        string message;
        getline(myfile, message);
        string timestamp_str;
        getline(myfile, timestamp_str);

        // cout <<  "Username: " << sender << endl;
        // cout <<  "Message: " << message << endl;
        // cout <<  "Timestamp: " << timestamp_str << endl;

        Message msg;
        msg.set_username(sender);
        msg.set_msg(message);
        google::protobuf::Timestamp time;
        google::protobuf::util::TimeUtil::FromString(timestamp_str, &time);
        google::protobuf::Timestamp *timestamp = msg.mutable_timestamp();
        timestamp->set_seconds(time.seconds());
        last_20_messages.push_back(msg);
        count += 1;
        if (count == 20)
        {
          break;
        }
      }
      myfile.close();
      for (int i = last_20_messages.size() - 1; i >= 0; i--)
      {
        stream->Write(last_20_messages[i]);
        // cout <<  "Write successfully " << last_20_messages[i].msg() << endl;
      }
    }

    Message msg;
    while (stream->Read(&msg))
    {
      // cout <<  "Received message: " << msg.msg() << endl;
      for (auto i : following_users)
      {
        string user = i.first;
        vector<string> followers = i.second;
        if (std::find(followers.begin(), followers.end(), username) != followers.end())
        {
          // cout <<  user << " follows " << username << endl;
          if (streams.find(user) != streams.end() && username != user)
          {
            // cout <<  "Write to SeverReaderWriter" << endl;
            streams[user]->Write(msg);
          }
          ofstream ofile;
          ofile.open(user + "_timeline.txt", std::ios_base::app);
          ofile << msg.username() << endl;
          ofile << msg.msg() << endl;
          ofile << google::protobuf::util::TimeUtil::ToString(msg.timestamp()) << endl;
          ofile.close();
        }
      }
    }
    return Status::OK;
  }
};

void sendHeartbeat()
{
  int i = 0;
  std::string login_info = coordIp + ":" + coordPort;
  stub_ = std::unique_ptr<SNSCoordinator::Stub>(SNSCoordinator::NewStub(
      grpc::CreateChannel(
          login_info, grpc::InsecureChannelCredentials())));
  log(INFO, "Connect to coordinator " + login_info);

  ClientContext context;

  std::unique_ptr<ClientReaderWriter<Heartbeat, Heartbeat>> stream(
      stub_->HandleHeartBeats(&context));

  while (true)
  {
    Heartbeat heartbeat;
    heartbeat.set_server_id(serverId);
    if (serverType == "master")
    {
      heartbeat.set_server_type(ServerType::MASTER);
    }
    else if (serverType == "slave")
    {
      heartbeat.set_server_type(ServerType::SLAVE);
    }
    heartbeat.set_server_ip("localhost");
    heartbeat.set_server_port(serverPort);
    google::protobuf::Timestamp *timestamp = new google::protobuf::Timestamp();
    timestamp->set_seconds(time(NULL));
    timestamp->set_nanos(0);
    heartbeat.set_allocated_timestamp(timestamp);

    stream->Write(heartbeat);

    log(INFO, "Signal " + std::to_string(i));
    std::this_thread::sleep_for(std::chrono::seconds(10));
    i += 1;
  }
}

void RunServer(std::string port)
{
  log(INFO, "coord ip: " + coordIp);
  log(INFO, "coord port: " + coordPort);
  log(INFO, "server id: " + std::to_string(serverId));
  log(INFO, "server port: " + serverPort);
  log(INFO, "type: " + serverType);

  string filedir = serverType + "_" + std::to_string(serverId);
  mkdir(filedir.c_str(), 0777);
  std::thread signalThread(sendHeartbeat);
  signalThread.detach();

  std::string server_address = "0.0.0.0:" + port;
  SNSServiceImpl service;

  ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;
  log(INFO, "Server listening on " + server_address);

  server->Wait();
}

int main(int argc, char **argv)
{

  // $./server -i <coordinatorIP> -c <coordinatorPort> -p <portNum> -d <idNum> -t <master/slave>

  int opt = 0;
  while ((opt = getopt(argc, argv, "i:c:p:d:t:")) != -1)
  {
    switch (opt)
    {
    case 'i':
      coordIp = optarg;
      break;
    case 'c':
      coordPort = optarg;
      break;
    case 'p':
      serverPort = optarg;
      break;
    case 'd':
      serverId = atoi(optarg);
      break;
    case 't':
      if (strcmp(optarg, "slave") == 0 || strcmp(optarg, "master") == 0)
      {
        serverType = optarg;
        break;
      }
    default:
      std::cerr << "Invalid Command Line Argument\n";
    }
  }

  std::string log_file_name = std::string("server-") + serverPort;
  google::InitGoogleLogging(log_file_name.c_str());
  FLAGS_log_dir = "/home/csce438/CSCE-438/MP_3/tmp";
  log(INFO, "Logging Initialized. Server starting...");
  RunServer(serverPort);

  return 0;
}
