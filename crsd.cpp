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
#include <set>

#define PORT 8080
#define MAX_CONNECTIONS 30

struct room
{
    int port;
    int master_socket;
    int slave_socket[MAX_CONNECTIONS];
};

void debug(std::map<std::string, room> &database)
{
    for (std::map<std::string, room>::iterator iter = database.begin(); iter != database.end(); ++iter)
    {
        LOG(WARNING) << "Chatroom name: " << iter->first << "; port: " << iter->second.port << "; master socket: " << iter->second.master_socket;
        int count = 0;
        for (int i = 0; i < MAX_CONNECTIONS; i++)
        {
            int socket = iter->second.slave_socket[i];
            if (socket > 0)
            {
                count += 1;
                LOG(WARNING) << "Socket: " << socket;
            }
        }
        LOG(WARNING) << "Number of memebers: " << count;
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

void cleanup(std::map<std::string, room> &database, std::string chatroom)
{
    std::map<std::string, room>::iterator it = database.find(chatroom);
    if (it != database.end())
    {
        room curr = it->second;
        shutdown(curr.master_socket, SHUT_RDWR);
        for (int i = 0; i < MAX_CONNECTIONS; i++)
        {
            int sd = curr.slave_socket[i];
            if (sd > 0)
            {
                close(sd);
            }
        }
        database.erase(chatroom);
    }
}

void process_command(int connfd, char (&recvline)[MAX_DATA], std::map<std::string, room> &database, int &nextPort)
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
            database[chatroom_name] = (room){nextPort, master_socket};
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
            reply.status = FAILURE_NOT_EXISTS;
        }
        else if (found > 0)
        {
            std::map<std::string, room>::iterator it = database.find(chatroom_name);
            if (it != database.end())
            {
                room curr = it->second;
                reply.status = SUCCESS;
                int count = 0;
                for (int i = 0; i < MAX_CONNECTIONS; i++)
                {
                    if (curr.slave_socket[i] > 0)
                    {
                        count += 1;
                    }
                }
                reply.num_member = count;
                reply.port = curr.port;
            }
        }
    }
    else if (command == "DELETE")
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
            reply.status = FAILURE_NOT_EXISTS;
        }
        else if (found > 0)
        {
            cleanup(database, chatroom_name);
            reply.status = SUCCESS;
        }
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

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    int listenfd, connfd, n;
    struct sockaddr_in servaddr;
    char recvline[MAX_DATA];
    int opt = 1;
    int nextPort = 8081;
    std::map<std::string, room> database;

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
    servaddr.sin_port = htons(PORT);
    socklen_t servaddr_len = sizeof(servaddr);

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

    // initialize
    int max_sd;
    int client_socket[MAX_CONNECTIONS];

    for (int i = 0; i < MAX_CONNECTIONS; i++)
    {
        client_socket[i] = 0;
    }

    while (true)
    {
        FD_ZERO(&readfds);
        FD_SET(listenfd, &readfds);
        max_sd = listenfd;
        for (int i = 0; i < MAX_CONNECTIONS; i++)
        {
            if (client_socket[i] > 0)
            {
                FD_SET(client_socket[i], &readfds);
            }
            if (client_socket[i] > max_sd)
            {
                max_sd = client_socket[i];
            }
        }
        select(max_sd + 1, &readfds, NULL, NULL, NULL);

        if (FD_ISSET(listenfd, &readfds))
        {
            connfd = accept(listenfd, (sockaddr *)&servaddr, (socklen_t *)&servaddr_len);
            if (connfd < 0)
            {
                close(connfd);
                exit(EXIT_FAILURE);
            }
            for (int i = 0; i < MAX_CONNECTIONS; i++)
            {
                if (client_socket[i] == 0)
                {
                    client_socket[i] = connfd;
                    break;
                }
            }
        }
        for (int i = 0; i < MAX_CONNECTIONS; i++)
        {
            int sd = client_socket[i];

            if (FD_ISSET(sd, &readfds))
            {
                n = read(sd, &recvline, MAX_DATA);
                if (n == 0)
                {
                    LOG(WARNING) << "Close socket: " << sd;
                    client_socket[i] = 0;
                    close(sd);
                }
                else
                {
                    process_command(sd, recvline, database, nextPort);
                    client_socket[i] = 0;
                    close(sd);
                }
            }
        }
        debug(database);
    }
    LOG(WARNING) << "Shutdown server and master socket";
    shutdown(listenfd, SHUT_RDWR);
    return 0;
}
