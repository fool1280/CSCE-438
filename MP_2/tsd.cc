#include <ctime>

#include <google/protobuf/timestamp.pb.h>
#include <google/protobuf/duration.pb.h>
#include <google/protobuf/util/time_util.h>

#include <fstream>
#include <iostream>
#include <sstream>
#include <memory>
#include <string>
#include <stdlib.h>
#include <unistd.h>
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
using std::ofstream, std::ifstream, std::istringstream;
using std::vector, std::map, std::set, std::pair, std::find, std::getline;

set<string> all_users;
map<string, vector<string>> following_users;
map<string, ServerReaderWriter<Message, Message> *> streams;

void writefile()
{
  ofstream myfile;
  myfile.open("followers.txt");
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
  ifstream myfile("followers.txt");
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

class SNSServiceImpl final : public SNSService::Service
{
  Status List(ServerContext *context, const Request *request, Reply *reply) override
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
    readfile();
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
  // std::// cout <<   "Server listening on " << server_address << std::endl;
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
