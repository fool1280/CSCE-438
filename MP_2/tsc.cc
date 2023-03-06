#include <iostream>
#include <sstream>
#include <string>
#include <unistd.h>
#include <grpc++/grpc++.h>
#include "client.h"

#include "sns.grpc.pb.h"

using csce438::Reply;
using csce438::Request;
using csce438::SNSService;
using grpc::ClientContext;
using grpc::Status;
using std::string, std::istringstream, std::cout;

class Client : public IClient
{
public:
    Client(const std::string &hname,
           const std::string &uname,
           const std::string &p)
        : hostname(hname), username(uname), port(p)
    {
    }

protected:
    virtual int connectTo();
    virtual IReply processCommand(std::string &input);
    virtual void processTimeline();

private:
    std::string hostname;
    std::string username;
    std::string port;
    std::unique_ptr<SNSService::Stub> stub;

    // You can have an instance of the client stub
    // as a member variable.
    // std::unique_ptr<NameOfYourStubClass::Stub> stub_;
};

int main(int argc, char **argv)
{

    std::string hostname = "localhost";
    std::string username = "default";
    std::string port = "3010";
    int opt = 0;
    while ((opt = getopt(argc, argv, "h:u:p:")) != -1)
    {
        switch (opt)
        {
        case 'h':
            hostname = optarg;
            break;
        case 'u':
            username = optarg;
            break;
        case 'p':
            port = optarg;
            break;
        default:
            std::cerr << "Invalid Command Line Argument\n";
        }
    }

    Client myc(hostname, username, port);
    // You MUST invoke "run_client" function to start business logic
    myc.run_client();

    return 0;
}

int Client::connectTo()
{
    // ------------------------------------------------------------
    // In this function, you are supposed to create a stub so that
    // you call service methods in the processCommand/porcessTimeline
    // functions. That is, the stub should be accessible when you want
    // to call any service methods in those functions.
    // I recommend you to have the stub as
    // a member variable in your own Client class.
    // Please refer to gRpc tutorial how to create a stub.
    // ------------------------------------------------------------
    auto channel = grpc::CreateChannel("localhost:" + this->port, grpc::InsecureChannelCredentials());
    this->stub = SNSService::NewStub(channel);
    ClientContext context;
    Request request = Request();
    Reply response;
    request.set_username(this->username);
    Status status = this->stub->Login(&context, request, &response);
    if (status.ok())
    {
        return 1;
    }
    else
    {
        return -1;
    }
}

IReply Client::processCommand(std::string &input)
{
    // ------------------------------------------------------------
    // GUIDE 1:
    // In this function, you are supposed to parse the given input
    // command and create your own message so that you call an
    // appropriate service method. The input command will be one
    // of the followings:
    //
    // FOLLOW <username>
    // UNFOLLOW <username>
    // LIST
    // TIMELINE
    //
    // ------------------------------------------------------------

    // ------------------------------------------------------------
    // GUIDE 2:
    // Then, you should create a variable of IReply structure
    // provided by the client.h and initialize it according to
    // the result. Finally you can finish this function by returning
    // the IReply.
    // ------------------------------------------------------------

    // ------------------------------------------------------------
    // HINT: How to set the IReply?
    // Suppose you have "Follow" service method for FOLLOW command,
    // IReply can be set as follow:
    //
    //     // some codes for creating/initializing parameters for
    //     // service method
    //     IReply ire;
    //     grpc::Status status = stub_->Follow(&context, /* some parameters */);
    //     ire.grpc_status = status;
    //     if (status.ok()) {
    //         ire.comm_status = SUCCESS;
    //     } else {
    //         ire.comm_status = FAILURE_NOT_EXISTS;
    //     }
    //
    //      return ire;
    //
    // IMPORTANT:
    // For the command "LIST", you should set both "all_users" and
    // "following_users" member variable of IReply.
    // ------------------------------------------------------------
    IReply ire;
    if (input == "LIST")
    {
        ClientContext context;
        Request request = Request();
        Reply response;
        request.set_username(this->username);
        Status status = this->stub->List(&context, request, &response);
        ire.grpc_status = status;
        if (status.ok())
        {
            ire.comm_status = SUCCESS;
            auto all_users = response.all_users();
            for (auto i : all_users)
            {
                ire.all_users.push_back(i);
            }
            auto following_users = response.following_users();
            for (auto i : following_users)
            {
                ire.following_users.push_back(i);
            }
        }
        else
        {
            ire.comm_status = FAILURE_NOT_EXISTS;
        }
    }
    else if (input == "TIMELINE")
    {
    }
    else
    {
        std::istringstream ss(input);
        string command, username;
        ss >> command;
        ss >> username;

        ClientContext context;
        Request request = Request();
        Reply response;
        Status status;
        if (command == "FOLLOW")
        {
            if (username == this->username)
            {
                ire.grpc_status = Status::OK;
                ire.comm_status = FAILURE_ALREADY_EXISTS;
                return ire;
            }
            request.set_username(this->username);
            request.add_arguments(username);
            status = this->stub->Follow(&context, request, &response);
        }
        else if (command == "UNFOLLOW")
        {
            if (username == this->username)
            {
                ire.grpc_status = Status::OK;
                ire.comm_status = FAILURE_INVALID_USERNAME;
                return ire;
            }
            request.set_username(this->username);
            request.add_arguments(username);
            status = this->stub->UnFollow(&context, request, &response);
        }
        ire.grpc_status = Status::OK;
        if (status.ok())
        {
            ire.comm_status = SUCCESS;
        }
        else
        {
            ire.comm_status = FAILURE_INVALID_USERNAME;
        }
    }
    return ire;
}

void Client::processTimeline()
{
    // ------------------------------------------------------------
    // In this function, you are supposed to get into timeline mode.
    // You may need to call a service method to communicate with
    // the server. Use getPostMessage/displayPostMessage functions
    // for both getting and displaying messages in timeline mode.
    // You should use them as you did in hw1.
    // ------------------------------------------------------------

    // ------------------------------------------------------------
    // IMPORTANT NOTICE:
    //
    // Once a user enter to timeline mode , there is no way
    // to command mode. You don't have to worry about this situation,
    // and you can terminate the client program by pressing
    // CTRL-C (SIGINT)
    // ------------------------------------------------------------
}
