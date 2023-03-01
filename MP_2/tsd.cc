#include <ctime>

#include <google/protobuf/timestamp.pb.h>
#include <google/protobuf/duration.pb.h>

#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <stdlib.h>
#include <unistd.h>
#include <google/protobuf/util/time_util.h>
#include <grpc++/grpc++.h>

#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/time.h>
#include <fcntl.h>
#include <errno.h>
#include <vector>
#include <map>

#include "sns.grpc.pb.h"

using csce438::Message;
using csce438::Reply;
using csce438::Request;
using csce438::SNSService;
using google::protobuf::Duration;
using google::protobuf::Timestamp;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ServerReaderWriter;
using grpc::ServerWriter;
using grpc::Status;
using std::cout, std::cin, std::endl, std::string, std::vector, std::map;

vector<string> all_users;
map<string, vector<string>> following_users;

class SNSServiceImpl final : public SNSService::Service
{
  Status List(ServerContext *context, const Request *request, Reply *reply) override
  {
    // ------------------------------------------------------------
    // In this function, you are to write code that handles
    // LIST request from the user. Ensure that both the fields
    // all_users & following_users are populated
    // ------------------------------------------------------------
    return Status::OK;
  }

  Status Follow(ServerContext *context, const Request *request, Reply *reply) override
  {
    // ------------------------------------------------------------
    // In this function, you are to write code that handles
    // request from a user to follow one of the existing
    // users
    // ------------------------------------------------------------
    return Status::OK;
  }

  Status UnFollow(ServerContext *context, const Request *request, Reply *reply) override
  {
    // ------------------------------------------------------------
    // In this function, you are to write code that handles
    // request from a user to unfollow one of his/her existing
    // followers
    // ------------------------------------------------------------
    return Status::OK;
  }

  Status Login(ServerContext *context, const Request *request, Reply *reply) override
  {
    // ------------------------------------------------------------
    // In this function, you are to write code that handles
    // a new user and verify if the username is available
    // or already taken
    // ------------------------------------------------------------
    return Status::OK;
  }

  Status Timeline(ServerContext *context, ServerReaderWriter<Message, Message> *stream) override
  {
    // ------------------------------------------------------------
    // In this function, you are to write code that handles
    // receiving a message/post from a user, recording it in a file
    // and then making it available on his/her follower's streams
    // ------------------------------------------------------------
    return Status::OK;
  }
};

void RunServer(std::string port_no)
{
  // ------------------------------------------------------------
  // In this function, you are to write code
  // which would start the server, make it listen on a particular
  // port number.
  // ------------------------------------------------------------
  const std::string db_path = port_no;
  std::string server_address("0.0.0.0:" + port_no);
  SNSServiceImpl service;

  ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;
  server->Wait();
}

int main(int argc, char **argv)
{
  std::string port = "3010";
  int opt = 0;
  while ((opt = getopt(argc, argv, "p:")) != -1)
  {
    switch (opt)
    {
    case 'p':
      port = optarg;
      break;
    default:
      std::cerr << "Invalid Command Line Argument\n";
    }
  }
  RunServer(port);
  return 0;
}
