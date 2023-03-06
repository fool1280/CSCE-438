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
#include <set>

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
using std::cout, std::cin, std::endl, std::string;
using std::ofstream;
using std::vector, std::map, std::set, std::pair, std::find;

set<string> all_users;
map<string, vector<string>> following_users;

void writefile()
{
  ofstream myfile;
  myfile.open("followers.txt");
  for (auto i : all_users)
  {
    myfile << i;
    vector<string> following = following_users[i];
    for (auto j : following)
    {
      myfile << " " << j;
    }
    myfile << endl;
  }
  myfile.close();
};

class SNSServiceImpl final : public SNSService::Service
{
  Status List(ServerContext *context, const Request *request, Reply *reply) override
  {
    // ------------------------------------------------------------
    // In this function, you are to write code that handles
    // LIST request from the user. Ensure that both the fields
    // all_users & following_users are populated
    // ------------------------------------------------------------
    string username = request->username();
    cout << "List request for username " << username << endl;
    for (auto i : all_users)
    {
      cout << i << endl;
      reply->add_all_users(i);
    }
    vector<string> following = following_users[username];
    for (auto i : following)
    {
      cout << "following " << i << endl;
      reply->add_following_users(i);
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
    string currentUser = request->username();
    string userToFollow = request->arguments().at(0);
    vector<string> currentFollow = following_users[currentUser];
    auto exist = all_users.find(userToFollow);
    auto hasFollow = std::find(currentFollow.begin(), currentFollow.end(), userToFollow);
    cout << "User exist: " << (bool)(exist != all_users.end()) << endl;
    cout << "User " << currentUser << " has follow: " << (bool)(hasFollow != currentFollow.end()) << endl;
    if (exist != all_users.end() && hasFollow == currentFollow.end())
    {
      following_users[currentUser].push_back(userToFollow);
      writefile();
      return Status::OK;
    }
    return Status::CANCELLED;
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
    string currentUser = request->username();
    string userToUnfollow = request->arguments().at(0);
    vector<string> currentFollow = following_users[currentUser];
    auto exist = all_users.find(userToUnfollow);
    auto hasFollow = std::find(currentFollow.begin(), currentFollow.end(), userToUnfollow);
    cout << "User exist: " << (bool)(exist != all_users.end()) << endl;
    cout << "User " << currentUser << " has follow: " << (bool)(hasFollow != currentFollow.end()) << endl;
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
    string username = request->username();
    auto it = all_users.find(username);
    if (it == all_users.end())
    {
      cout << "Username not exist, intialize " << username << endl;
      all_users.insert(username);
      following_users.insert(pair<string, vector<string>>(username, vector<string>()));
      following_users[username].push_back(username);
      writefile();
      return Status::OK;
    }
    cout << "Username already exists " << username << endl;
    return Status::CANCELLED;
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
