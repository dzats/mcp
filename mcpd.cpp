#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <sys/types.h>
#include <sys/wait.h>
#include <sys/socket.h>
#include <sys/time.h> // for gettimeofday
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

#include "unicast_receiver.h"
#include "unicast_sender.h"
#include "file_writer.h"

#include "log.h"

#define MAX_FNAME_LENGTH 4096

using namespace std;

void usage_and_exit(char *name) {
	printf("Usage:\n%s [options]\n", name);
	printf("Options:\n"
	"\t-a address\n"
	"\t\tSpecify an IP address the server will use. By default, \n"
	"\t\tserver will accept connections to all the local addresses.\n\n"
	"\t-p port\n"
	"\t\tSpecify a TCP port for the server to use instead of\n"
	"\t\tthe default port (6879).\n\n" 
	"\t-d\tRun server in the debug mode (don't go to the background,\n"
	"\t\tlog messages to the standard out/error, and don't fork on\n"
	"\t\tincoming connection.\n\n");
	exit(EXIT_FAILURE);
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

#ifndef NDEBUG
void sigpipe_handler(int signum) {
	// It is wrong to call printf here
	DEBUG("sigpipe_handler: signal %d received\n", signum);
}
#endif

// The SIGCHLD handler, simply removes zombies
void sigchld_handler(int signum) {
	// Remove zombies
	register int pid;
	while ((pid = waitpid(-1, NULL, WNOHANG)) > 0) {
		// It is wrong to call printf here
		DEBUG("Child %d picked up\n", pid);
	};
}

int main(int argc, char **argv) {
	// Server configurations
	in_addr_t address = htonl(INADDR_ANY);
	uint16_t port = UNICAST_PORT;
	bool debug_mode = false;

	// Set speed for the pseudo-random numbers
	struct timeval tv;
	gettimeofday(&tv, NULL);
	srand((tv.tv_sec << 8) + getpid() % (1 << 8));

	// Parse the command line options
	int ch;
	while ((ch = getopt(argc, argv, "a:p:dh")) != -1 ) {
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
			case 'd': // Debug mode: don't go to the background, perform a
				// simple server rather that forked
				debug_mode = true;
				break;
			default:
				usage_and_exit(argv[0]);
		}
	}

#ifndef NDEBUG
	// Set SIGPIPE handler for debugging
	struct sigaction sigpipe_action;
	memset(&sigpipe_action, 0, sizeof(sigpipe_action));
	sigpipe_action.sa_handler = sigpipe_handler;
	sigpipe_action.sa_flags = SA_RESTART;
	if (sigaction(SIGPIPE, &sigpipe_action, NULL)) {
		ERROR("sigaction: %s\n", strerror(errno));
		exit(EXIT_FAILURE);
	}
#else
	signal(SIGPIPE, SIG_IGN);
#endif

	struct sigaction sigchld_action;
	memset(&sigchld_action, 0, sizeof(sigchld_action));
	sigchld_action.sa_handler = sigchld_handler;
	sigchld_action.sa_flags = SA_RESTART;
	if (sigaction(SIGCHLD, &sigchld_action, NULL)) {
		ERROR("sigaction: %s\n", strerror(errno));
		exit(EXIT_FAILURE);
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

	// Become a daemon process and proceeed to the home directory
	if (!debug_mode) {
		daemon(1, 1);
		char *homedir;
		if ((homedir = getenv("HOME")) == NULL ||
			chdir(homedir) != 0) {
			ERROR("Can't proceed to the home directory: %s\n", strerror(errno));
			// continue execution
		}
	}

	// The main server routine
	while (1) {
		struct sockaddr_in client_addr;
		socklen_t client_addr_size = sizeof(client_addr);
		int client_sock = accept(sock, (struct sockaddr *)&client_addr,
			&client_addr_size);
		if (client_sock < 0) {
			ERROR("accept call failed: %s\n", strerror(errno));
			exit(EXIT_FAILURE);
		} else {
			if (!debug_mode) {
				// Implement the forked server
				pid_t pid = fork();
				if (pid > 0) {
					// Parent process
					close(client_sock);
					// Return to the accept call
					continue;
				} else if (pid == 0) {
					close(sock);
					// execute the main routine
				} else {
					ERROR("fork: %s\n", strerror(errno));
					exit(EXIT_FAILURE);
				}
			}
#ifndef NDEBUG
			char iaddr[INET_ADDRSTRLEN];
			DEBUG("Received connection from: %s\n",
				inet_ntop(AF_INET, &client_addr.sin_addr.s_addr, iaddr, sizeof(iaddr)));
#endif

			// Session initialization
			// create and initialize the socket reader
			pthread_t usender_thread;
			bool usender_started = false;
			pthread_t fwriter_thread;
			UnicastReceiver *unicast_receiver = new UnicastReceiver();
			FileWriter *file_writer = new FileWriter(unicast_receiver); 
			if (unicast_receiver->session_init(client_sock) != 0) {
				SERROR("Can't get the initial data from the server\n");
				exit(EXIT_FAILURE);
			}

			// TODO: session_init for the multicast sender

			// Run the unicast sender sender
			if (unicast_receiver->dst.size() > 0) {
				usender_started = true;
				UnicastSender *unicast_sender = new UnicastSender(unicast_receiver,
					port);
				SDEBUG("Initialize the unicast sender thread\n");
				int retval;
				if ((retval = unicast_sender->session_init(unicast_receiver->dst,
						unicast_receiver->nsources)) == 0) {
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
				if (unicast_receiver->reader.status >= STATUS_FIRST_FATAL_ERROR) {
					send_error(client_sock, unicast_receiver->reader.status,
						unicast_receiver->reader.addr,
						unicast_receiver->reader.message_length,
						unicast_receiver->reader.message);
					exit(EXIT_FAILURE);
				} else if (unicast_receiver->unicast_sender.is_present &&
						unicast_receiver->unicast_sender.status >=
						STATUS_FIRST_FATAL_ERROR) {
					// A fatal error occurred during the unicast sender initialization
					// FIXME: race condition can take place here.
					send_error(client_sock, unicast_receiver->unicast_sender.status,
						unicast_receiver->unicast_sender.addr,
						unicast_receiver->unicast_sender.message_length,
						unicast_receiver->unicast_sender.message);
					exit(EXIT_FAILURE);
				} else if (unicast_receiver->multicast_sender.is_present &&
						unicast_receiver->multicast_sender.status >=
						STATUS_FIRST_FATAL_ERROR) {
					// TODO: do the same thing as with other writers
				}
			} catch (ConnectionException& e) {
				ERROR("Can't send a message to the immediate source: %s\n", e.what());
				// The TCP connection is broken.  All we can do, it just silently exit.
				exit(EXIT_FAILURE);
			}

			// Start the file writer thread
			file_writer->init(unicast_receiver->path, unicast_receiver->path_type); 
			int error;
			if ((error = pthread_create(&fwriter_thread, NULL,
					file_writer_routine, (void *)file_writer)) != 0) {
				ERROR("Can't create a new thread: %s\n", strerror(errno));
				exit(EXIT_FAILURE);
			}

			// Start the main routine (read files and directories and pass them
			// to the distributor)
			if (unicast_receiver->session() != 0) {
				unicast_receiver->send_fatal_error(client_sock);
			}

			pthread_join(fwriter_thread, NULL);
			if (usender_started) {
				pthread_join(usender_thread, NULL);
			}

			SDEBUG("Session finished, terminate the server\n");
			delete file_writer;
			delete unicast_receiver;
			close(client_sock);
			if (!debug_mode) {
				exit(EXIT_SUCCESS);
			}
		}
	}
}
