#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>

#include <sys/stat.h>

#include <signal.h> // for SIGPIPE

#include <fcntl.h>

#include <unistd.h> // for getopt

#include <vector>
#include <string>

#include <arpa/inet.h> // for inet addr

#include "md5.h"
#include "destination.h"
#include "connection.h"
#include "distributor.h"

#include "reader.h"
#include "unicast_sender.h"
#include "file_writer.h"

#include "log.h"

#define PORT 6789

#define MAX_FNAME_LENGTH 4096

using namespace std;

void usage() {
	// FIXME: print the usage information
	fprintf(stderr, "Usage: ...\n");
	abort();
}

// Wrapper for the FileWriter::session function, passed
// to pthread_create.
void *file_writer_routine(void *args) {
	FileWriter *file_writer = (FileWriter *)args;
	file_writer->session();
	SDEBUG("File writer finished\n");
	return NULL;
}

// Wrapper for the UnicastSender::session function, passed
// to pthread_create.
void *unicast_sender_routine(void *args) {
	UnicastSender *unicast_sender = (UnicastSender *)args;
	int retval;
	if ((retval = unicast_sender->session()) != 0) {
		// FIXME: print some information here
		ERROR("Transmission failed, status: %d\n", retval);
	}

	SDEBUG("Unicast sender thread finished\n");
	return NULL;
}

#if 0
// Arguments are: file descriptor (socket), the buff pointer
// and addresses of the destinations.
void *multicast_sender(void *arg)
{
	while (1) {
		// TODO: send file
		// TODO: wait for the file integrity replies (checksum)
		// TODO: inform the socket reader about the result
	}
}
#endif

#ifndef NDEBUG
void sigpipe_handler(int signum) {
	printf("sigpipe_handler: signal %d received\n", signum);
}
#endif

int main(int argc, char **argv) {
#ifndef NDEBUG
	// Set SIGPIPE handler for debugging
	signal(SIGPIPE, sigpipe_handler);
#else
	signal(SIGPIPE, SIG_IGN);
#endif

	// Server configurations
	in_addr_t address = INADDR_ANY;
	uint16_t port = PORT;

	// Parse the command line options
	int ch;
	while ((ch = getopt(argc, argv, "a:p:h")) != -1 ) {
		switch (ch) {
			case 'a': // Address specified
				if ((address = inet_addr(optarg)) == INADDR_NONE) {
					ERROR("Invalid address: %s\n", optarg);
					exit(EXIT_FAILURE);
				}
				break;
			case 'p': // Port specified
				if ((port = atoi(optarg)) == 0) {
					ERROR("Invalid port: %s\n", optarg);
					exit(EXIT_FAILURE);
				}
				break;
			default:
				usage();
		}
	}

	// Creates the socket to listen
	int sock = socket(PF_INET, SOCK_STREAM, 0);
	if (sock < 0) {
		ERROR("Can't create socket: %s\n", strerror(errno));
		exit(EXIT_FAILURE);
	}

	int on = 1;
	if (setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on)) != 0) {
		ERROR("Can't set the SO_REUSEADDR socket option: %s\n",
			strerror(errno));
		exit(EXIT_FAILURE);
	}

	struct sockaddr_in server_addr;
	memset(&server_addr, 0, sizeof(server_addr));
	server_addr.sin_family = AF_INET;
	server_addr.sin_addr.s_addr = address;
	server_addr.sin_port = htons(port);
	if (bind(sock, (struct sockaddr *)&server_addr,
			sizeof(server_addr)) != 0) {
		perror("Can't bind the socket");
		exit(EXIT_FAILURE);
	}
	if (listen(sock, 5) != 0) {
		perror("Can't bind the socket");
		exit(EXIT_FAILURE);
	}

	// The main server routine
	while (1) {
		struct sockaddr_in client_addr;
		socklen_t client_addr_size = sizeof(client_addr);
		int client_sock = accept(sock, (struct sockaddr *)&client_addr,
			&client_addr_size);
		if (client_sock < 0) {
			ERROR("accept call failed: %s", strerror(errno));
			exit(EXIT_FAILURE);
		} else {
			// TODO: When the code will be stable enough implement the
			// forked server here.
#ifndef NDEBUG
			char inet_addr[INET_ADDRSTRLEN];
			if (inet_ntop(AF_INET, &client_addr.sin_addr.s_addr,
					(char *)&inet_addr, INET_ADDRSTRLEN) == NULL) {
				exit(EXIT_FAILURE);
			}
			DEBUG("Received connection from: %s\n", inet_addr);
#endif

			// Session initialization
			// create and initialize the socket reader
			pthread_t usender_thread;
			bool usender_started = false;
			pthread_t fwriter_thread;
			Distributor *buff = new Distributor();
			Reader *socket_reader = new Reader(buff);
			FileWriter *file_writer = new FileWriter(buff); 
			if (socket_reader->session_init(client_sock) != 0) {
				SERROR("Can't get the initial data from the server\n");
				exit(EXIT_FAILURE);
			}

			// TODO: session_init for the multicast sender

			// Run the unicast sender sender
			if (socket_reader->dst.size() > 0) {
				usender_started = true;
				UnicastSender *unicast_sender = new UnicastSender(buff);
				SDEBUG("Initialize the unicast sender thread\n");
				int retval;
				if ((retval = unicast_sender->session_init(socket_reader->dst,
						socket_reader->nsources)) == 0) {
					// Start the unicast sender
					int error;
					if ((error = pthread_create(&usender_thread, NULL,
							unicast_sender_routine, unicast_sender)) != 0) {
						ERROR("Can't create a new thread: %s\n",
							strerror(errno));
						exit(EXIT_FAILURE);
					}
				} else {
					// An error occurred during the unicast session initialization.
					// About this error will be reported later
					DEBUG("Session initialization failed: %s\n", strerror(retval));
				}
			}

			// Conform to the source that the connection established
			try {
				if (buff->reader.status >= STATUS_FIRST_FATAL_ERROR) {
					send_error(client_sock, buff->reader.status,
						buff->reader.addr, buff->reader.message_length,
						buff->reader.message);
					exit(EXIT_FAILURE);
				} else if (buff->unicast_sender.is_present &&
						buff->unicast_sender.status >= STATUS_FIRST_FATAL_ERROR) {
					// A fatal error occurred during the unicast sender initialization
					// FIXME: race condition can take place here.
					send_error(client_sock, buff->unicast_sender.status,
						buff->unicast_sender.addr, buff->unicast_sender.message_length,
						buff->unicast_sender.message);
					exit(EXIT_FAILURE);
				} else if (buff->multicast_sender.is_present &&
						buff->multicast_sender.status >= STATUS_FIRST_FATAL_ERROR) {
					// TODO: do the same thing as with other writers
				}
			} catch (ConnectionException& e) {
				ERROR("Can't send a message to the immediate source: %s\n", e.what());
				// The TCP connection is broken.  All we can do, it just silently exit.
				exit(EXIT_FAILURE);
			}

			// Start the file writer thread
			file_writer->init(socket_reader->path, socket_reader->path_type); 
			int error;
			if ((error = pthread_create(&fwriter_thread, NULL,
					file_writer_routine, (void *)file_writer)) != 0) {
				ERROR("Can't create a new thread: %s\n", strerror(errno));
				exit(EXIT_FAILURE);
			}

			// Start the main routine (read files and directories and pass them
			// to the distributor)
			if (socket_reader->session() != 0) {
				buff->send_fatal_error(client_sock);
			}

			pthread_join(fwriter_thread, NULL);
			if (usender_started) {
				pthread_join(usender_thread, NULL);
			}

			SDEBUG("Session finished, terminate the server\n");
			delete file_writer;
			delete socket_reader;
			delete buff;
			close(client_sock);
		}
	}
}
