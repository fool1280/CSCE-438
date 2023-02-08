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

int connect_to(const char *host, const int port);
struct Reply process_command(const int sockfd, char *command);
void process_chatmode(const char *host, const int port);

int main(int argc, char **argv)
{
	if (argc != 3)
	{
		LOG(ERROR) << "USAGE: Enter host address and port number";
		exit(1);
	}
	google::InitGoogleLogging(argv[0]);

	display_title();

	while (1)
	{

		int sockfd = connect_to(argv[1], atoi(argv[2]));

		char command[MAX_DATA];
		get_command(command, MAX_DATA);

		struct Reply reply = process_command(sockfd, command);
		display_reply(command, reply);

		if (reply.status == SUCCESS)
		{
			touppercase(command, strlen(command) - 1);
			if (strncmp(command, "JOIN", 4) == 0)
			{
				printf("Now you are in the chatmode\n");
				process_chatmode(argv[1], reply.port);
			}
		}

		close(sockfd);
	}

	return 0;
}

/*
 * Connect to the server using given host and port information
 *
 * @parameter host    host address given by command line argument
 * @parameter port    port given by command line argument
 *
 * @return socket fildescriptor
 */
int connect_to(const char *host, const int port)
{
	// ------------------------------------------------------------
	// In this function, we exstablish connection with the server.
	//
	// Finally, the socket fildescriptor is returned
	// so that other functions such as "process_command" can use it
	// ------------------------------------------------------------

	struct sockaddr_in server_addr;
	memset((char *)&server_addr, 0, sizeof(struct sockaddr_in));

	int sockfd;

	// Creating socket
	// AF_INET = Address Family - Internet
	// SOCK_STREAM = stream socket (other are datagram, which is packet == a chunk of data)
	// 0 select TCP protocol
	if ((sockfd = socket(AF_INET, SOCK_STREAM, 0)) < 0)
	{
		LOG(ERROR) << "ERROR: could not open socket";
		exit(EXIT_FAILURE);
	}

	// Specify internet address and port
	server_addr.sin_family = AF_INET;
	// host to network, short
	// convert big endian or little endian to the network byte order
	server_addr.sin_port = htons(port);

	// convert host address from string to decimal format and store in the server_addr struct
	if (inet_aton(host, &server_addr.sin_addr) == 0)
	{
		LOG(ERROR) << "ERROR: invalid host address";
		exit(EXIT_FAILURE);
	}

	// connect to host on the specified port using the server_addr struct
	if (connect(sockfd, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0)
	{
		LOG(ERROR) << "ERROR: could not connect to server";
		exit(EXIT_FAILURE);
	}

	return sockfd;
}

/*
 * Send an input command to the server and return the result
 *
 * @parameter sockfd   socket file descriptor to commnunicate
 *                     with the server
 * @parameter command  command will be sent to the server
 *
 * @return    Reply
 */
struct Reply process_command(const int sockfd, char *command)
{
	// ------------------------------------------------------------
	// In this function, we parse a given command and send the message
	// in order to communicate with the server. The given command
	// will be one of the followings:
	//
	// CREATE <name>
	// DELETE <name>
	// JOIN <name>
	// LIST
	//
	// -  "<name>" is a chatroom name that you want to create, delete,
	// or join.
	//
	// - CREATE/DELETE/JOIN and "<name>" are separated by one space.
	// ------------------------------------------------------------

	if (send(sockfd, command, MAX_DATA, 0) < 0)
	{
		LOG(ERROR) << "ERROR: send failed";
		exit(EXIT_FAILURE);
	}

	// ------------------------------------------------------------
	// send message to the server and receive a result.
	// ------------------------------------------------------------
	char response_string[MAX_DATA];
	if (recv(sockfd, response_string, MAX_DATA, 0) < 0)
	{
		LOG(ERROR) << "ERROR: receive failed";
		exit(EXIT_FAILURE);
	}

	// ------------------------------------------------------------
	// Then, we create a variable of Reply structure
	// provided by the interface and initialize it according to
	// the result.
	//
	// For example, if a given command is "JOIN room1"
	// and the server successfully created the chatroom,
	// the server will reply a message including information about
	// success/failure, the number of members and port number.
	// the variable will be set as following:
	//
	// Reply reply;
	// reply.status = SUCCESS;
	// reply.num_member = number;
	// reply.port = port;
	//
	// "number" and "port" variables are just an integer variable
	// and can be initialized using the message fomr the server.
	//
	// For another example, if a given command is "CREATE room1"
	// and the server failed to create the chatroom becuase it
	// already exists, the Reply varible will be set as following:
	//
	// Reply reply;
	// reply.status = FAILURE_ALREADY_EXISTS;
	//
	// For the "LIST" command,
	// You are suppose to copy the list of chatroom to the list_room
	// variable. Each room name should be seperated by comma ','.
	// For example, if given command is "LIST", the Reply variable
	// will be set as following.
	//
	// Reply reply;
	// reply.status = SUCCESS;
	// strcpy(reply.list_room, list);
	//
	// "list" is a string that contains a list of chat rooms such
	// as "r1,r2,r3,"
	// ------------------------------------------------------------

	return *(Reply *)response_string;
}

/*
 * Get into the chat mode
 *
 * @parameter host     host address
 * @parameter port     port
 */
void process_chatmode(const char *host, const int port)
{
	// ------------------------------------------------------------
	// In order to join the chatroom, connect
	// to the server using host and port.
	// ------------------------------------------------------------
	int sockfd = connect_to(host, port);
	// ------------------------------------------------------------
	// Once the client have been connected to the server, we need
	// to get a message from the user and send it to server.
	// At the same time, the client should wait for a message from
	// the server.
	// ------------------------------------------------------------
	fd_set readfds;
	char buf[MAX_DATA];

	while (true)
	{
		// Listen for new information on socket or new input from user
		FD_ZERO(&readfds);
		FD_SET(sockfd, &readfds);
		FD_SET(STDIN_FILENO, &readfds);

		select(sockfd + 1, &readfds, NULL, NULL, NULL);

		// If there is information to read from the socket
		if (FD_ISSET(sockfd, &readfds))
		{
			if (read(sockfd, &buf, MAX_DATA) <= 0)
			{
				// If the information is empty or could not be read, disconnect from the chatroom and continue
				printf("Chatroom disconnected...\n");
				LOG(INFO) << "Chatroom disconnected.";
				close(sockfd);
				break;
			}
			else
			{
				// Otherwise, display the message
				display_message(buf);
				printf("\n");
			}
		}
		else
		{
			// If there is new input from the user, collect the message and send it to the chatroom server
			get_message(buf, MAX_DATA);
			send(sockfd, buf, MAX_DATA, 0);
		}
	}

	// ------------------------------------------------------------
	// IMPORTANT NOTICE:
	//    Once a user entered to one of chatrooms, there is no way
	//    to command mode where the user  enter other commands
	//    such as CREATE,DELETE,LIST.
	//    Don't have to worry about this situation, and you can
	//    terminate the client program by pressing CTRL-C (SIGINT)
	// ------------------------------------------------------------
}
