#include <glog/logging.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/time.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string>
#include "interface.h"
// TODO: Implement Chat Server.

#define PORT 8080

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    int listenfd, connfd, n;
    struct sockaddr_in servaddr;
    char recvline[MAX_DATA];
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
        select(listenfd + 1, &readfds, NULL, NULL, NULL);
        if (FD_ISSET(listenfd, &readfds))
        {
            connfd = accept(listenfd, (sockaddr *)&servaddr, (socklen_t *)&servaddr_len);
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
                command += "\0";
                i += 1;
                while (recvline[i] != ' ' && i < 256 & recvline[i] != '\0')
                {
                    chatroom_name += recvline[i];
                    i += 1;
                }
                chatroom_name += "\0";
                LOG(WARNING) << "Command: " << command << "; size: " << command.length();
                LOG(WARNING) << "Chatroom name: " << chatroom_name << "; size: " << chatroom_name.length();
            }
            close(connfd);
        }
    }
    shutdown(listenfd, SHUT_RDWR);
    return 0;
}
