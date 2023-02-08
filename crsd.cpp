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
#include "interface.h"
// TODO: Implement Chat Server.

#define PORT 8080
int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    int listenfd, connfd, n;
    struct sockaddr_in servaddr;
    int servaddr_len = sizeof(servaddr);
    uint8_t buff[MAX_DATA], recvline[MAX_DATA];
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

    if (bind(listenfd, (sockaddr *)&servaddr, servaddr_len) < 0)
    {
        LOG(ERROR) << "Socket binding error";
    }
    if (listen(listenfd, 10) < 0)
    {
        LOG(ERROR) << "Server listening error";
    }
    LOG(INFO) << "Starting Server";
    while (true)
    {
        connfd = accept(listenfd, (sockaddr *)&servaddr, (socklen_t *)&servaddr_len);
        if ((n = recv(connfd, recvline, MAX_DATA, 0)) > 0)
        {
            LOG(INFO) << "Receive command" << recvline;
        }
        close(connfd);
    }
    shutdown(listenfd, SHUT_RDWR);
    return 0;
}
