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

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);

    LOG(INFO) << "Starting Server";
    return 0;
}
