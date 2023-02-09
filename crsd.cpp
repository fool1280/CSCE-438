#include <glog/logging.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/time.h>
#include <fcntl.h>
#include <errno.h>

#include <stdio.h>
#include <stdlib.h>
#include <string>
#include "interface.h"
#include <map>
#include <vector>
// TODO: Implement Chat Server.

#define PORT 8080
#define MAX_CLIENTS 30

struct room
{
    int port;
    int master_socket;
    int max_socket;
    std::vector<int> slave_socket;
};

void debug(std::map<std::string, room> &database)
{
    for (std::map<std::string, room>::iterator iter = database.begin(); iter != database.end(); ++iter)
    {
        LOG(WARNING) << "Chatroom name: " << iter->first << "; port: " << iter->second.port << "; master socket: " << iter->second.master_socket;
        if (iter->second.slave_socket.size())
        {
            LOG(WARNING) << "No socket"
                         << "\n";
        }
        for (int i = 0; i < iter->second.slave_socket.size(); i++)
        {
            LOG(WARNING) << "socket: " << i;
        }
    }
    LOG(WARNING) << "\n";
}

int new_socket(int port)
{
    int listenfd;
    struct sockaddr_in servaddr;
    int opt = 1;

    // Creating socket
    if ((listenfd = socket(AF_INET, SOCK_STREAM, 0)) < 0)
    {
        LOG(ERROR) << "Socket creation error";
        exit(EXIT_FAILURE);
    }

    if (setsockopt(listenfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)))
    {
        perror("setsockopt");
        exit(EXIT_FAILURE);
    }

    bzero(&servaddr, sizeof(servaddr));
    servaddr.sin_family = AF_INET;
    servaddr.sin_addr.s_addr = htonl(INADDR_ANY);
    servaddr.sin_port = htons(port);
    int servaddr_len = sizeof(servaddr);

    if (bind(listenfd, (sockaddr *)&servaddr, servaddr_len) < 0)
    {
        LOG(ERROR) << "Socket binding error";
    }
    if (listen(listenfd, 10) < 0)
    {
        LOG(ERROR) << "Server listening error";
    }
    return listenfd;
}

void cleanup(std::map<std::string, room> &database)
{
    for (std::map<std::string, room>::iterator iter = database.begin(); iter != database.end(); ++iter)
    {
        shutdown(iter->second.master_socket, SHUT_RDWR);
        for (int i = 0; i < iter->second.slave_socket.size(); i++)
        {
            close(i);
        }
    }
}

void process_command(int n, int connfd, char (&recvline)[MAX_DATA], std::map<std::string, room> &database, int &nextPort)
{
    if ((n = read(connfd, recvline, MAX_DATA)) > 0)
    {
        LOG(WARNING) << "Received " << recvline;
        std::string command = "";
        std::string chatroom_name = "";
        int i = 0;
        while (recvline[i] != ' ' && i < 256 & recvline[i] != '\0')
        {
            command += recvline[i];
            i += 1;
        }
        LOG(WARNING) << "Command: " << command << "; size: " << command.length();
        Reply reply;
        if (command == "CREATE")
        {
            i += 1;
            while (recvline[i] != ' ' && i < 256 & recvline[i] != '\0')
            {
                chatroom_name += recvline[i];
                i += 1;
            }
            LOG(WARNING) << "Chatroom name: " << chatroom_name << "; size: " << chatroom_name.length();
            int found = database.count(chatroom_name);

            if (chatroom_name.length() == 0)
            {
                reply.status = FAILURE_INVALID;
            }
            else if (found == 0)
            {
                int master_socket = new_socket(nextPort);
                database[chatroom_name] = (room){nextPort, master_socket, master_socket};
                reply.status = SUCCESS;
                nextPort += 1;
            }
            else if (found > 0)
            {
                reply.status = FAILURE_ALREADY_EXISTS;
            }
        }
        else if (command == "JOIN")
        {
        }
        else if (command == "DELETE")
        {
        }
        else if (command == "LIST")
        {
            std::string res = "";
            for (std::map<std::string, room>::iterator iter = database.begin(); iter != database.end(); ++iter)
            {
                std::string k = iter->first;
                if (std::next(iter, 1) != database.end())
                {
                    res = res + k + ", ";
                }
                else
                {
                    res = res + k;
                }
            }
            LOG(WARNING) << "List room: " << res;
            reply.status = SUCCESS;
            strcpy(reply.list_room, res.c_str());
            debug(database);
        }
        else
        {
            reply.status = FAILURE_INVALID;
        }
        send(connfd, &reply, sizeof(reply), 0);
    }
}

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    int listenfd, connfd, n;
    struct sockaddr_in servaddr;
    char recvline[MAX_DATA];
    int opt = 1;
    int nextPort = 8081;
    std::map<std::string, room> database;

    // initialize
    int max_sd;
    int client_socket[MAX_CLIENTS];
    for (int i = 0; i < MAX_CLIENTS; i++)
    {
        client_socket[i] = 0;
    }

    // Creating socket
    if ((listenfd = socket(AF_INET, SOCK_STREAM, 0)) < 0)
    {
        LOG(ERROR) << "Socket creation error";
        exit(EXIT_FAILURE);
    }
    // fcntl(listenfd, F_SETFL, O_NONBLOCK);

    if (setsockopt(listenfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)))
    {
        perror("setsockopt");
        exit(EXIT_FAILURE);
    }

    bzero(&servaddr, sizeof(servaddr));
    servaddr.sin_family = AF_INET;
    servaddr.sin_addr.s_addr = htonl(INADDR_ANY);
    servaddr.sin_port = htons(PORT);
    int servaddr_len = sizeof(servaddr);

    if (bind(listenfd, (sockaddr *)&servaddr, servaddr_len) < 0)
    {
        LOG(ERROR) << "Socket binding error";
    }
    if (listen(listenfd, 10) < 0)
    {
        LOG(ERROR) << "Server listening error";
    }
    LOG(INFO) << "Starting Server";
    fd_set readfds;

    while (true)
    {
        FD_ZERO(&readfds);
        FD_SET(listenfd, &readfds);
        max_sd = listenfd;
        for (int i = 0; i < MAX_CLIENTS; i++)
        {
            // socket descriptor
            int sd = client_socket[i];
            // if valid socket descriptor then add to read list
            if (sd > 0)
                FD_SET(sd, &readfds);
            // highest file descriptor number, need it for the select function
            if (sd > max_sd)
                max_sd = sd;
        }
        select(max_sd + 1, &readfds, NULL, NULL, NULL);

        if (FD_ISSET(listenfd, &readfds))
        {
            connfd = accept(listenfd, (sockaddr *)&servaddr, (socklen_t *)&servaddr_len);
            process_command(n, connfd, recvline, database, nextPort);
            // add new socket to array of sockets
            for (int i = 0; i < MAX_CLIENTS; i++)
            {
                // if position is empty
                if (client_socket[i] == 0)
                {
                    client_socket[i] = connfd;
                    LOG(WARNING) << "Add new socket: " << connfd;
                    break;
                }
            }
        }
        for (int i = 0; i < MAX_CLIENTS; i++)
        {
            int sd = client_socket[i];

            if (FD_ISSET(sd, &readfds))
            {
                // Check if it was for closing, and also read theincoming message
                if ((n = read(sd, recvline, MAX_DATA)) == 0)
                {
                    // Close the socket and mark as 0 in list for reuse
                    LOG(WARNING) << "Close socket: " << sd;
                    close(sd);
                    client_socket[i] = 0;
                }
                // Echo back the message that came in
                else
                {
                    process_command(n, sd, recvline, database, nextPort);
                }
            }
        }
    }
    LOG(WARNING) << "Shutdown server and master socket";
    shutdown(listenfd, SHUT_RDWR);
    cleanup(database);
    return 0;
}
