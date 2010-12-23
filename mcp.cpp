#include <stdio.h>
#include <stdlib.h>
#include <string.h> // for strerror
#include <errno.h>
#include <fcntl.h>
#include <unistd.h> // for getopt
#include <netdb.h> // for gethostbyname
#include <sys/types.h>
#include <sys/stat.h> // for stat
#include <sys/socket.h> // for socket
#include <sys/time.h> // for gettimeofday
#include <signal.h> // for SIGPIPE
#include <netinet/in.h>
#include <dirent.h> // for opendir
#include <pthread.h>
#include <vector>
#include <string>
#include <algorithm>
#include <sys/param.h> // for MAXPATHLEN

// FIXME: be carefull with the use of volatiles

using namespace std;

#include "md5.h"
#include "destination.h"
#include "connection.h"
#include "distributor.h"
#include "file_reader.h"
#include "unicast_sender.h"
#include "multicast_sender.h"
#include "log.h"

void usage_and_exit(char *name) {
	printf("Usage:\n");
	printf("%s [options] file server1[:location1] [...]\n", name);
	printf("%s [options] file1 [...] \\; server1[:location1] [...]\n", name);
	printf("Options:\n"
	"\t-p port\n"
	"\t\tSpecify a TCP port for the unicast connections.\n"
	"\t\tThe default port is 6879.\n\n" 
	"\t-o\tPreserve the specified servers order during pipelined transfert.\n\n");
	exit(EXIT_FAILURE);
}

// Wrapper for the UnicastSender::session function, passed
// to pthread_create.
void *unicast_sender_routine(void *args) {
	SDEBUG("Start the unicast sender\n");
	UnicastSender *us = (UnicastSender *)args;
	if (us->session() != 0) {
		// Signal about an error
		return (void *)-1;
	}
	return NULL;
}

// Wrapper for the MulticastSender::session function, passed
// to pthread_create.
void *multicast_sender_routine(void *args) {
	SDEBUG("Start the multicast sender\n");
	MulticastSender *ms = (MulticastSender *)args;
	if (ms->session() != 0) {
		// Signal about an error
		return (void *)-1;
	}
	return NULL;
}

#ifndef NDEBUG
void sigpipe_handler(int signum) {
	DEBUG("sigpipe_handler: signal %d received\n", signum);
}
#endif

/*
	The prepare_to_basename function return a sting corresponds to path without
	trailing slashes, dottes and double dottes. The NULL value is returnd
	if the path is incorrect. The functions allocates new memory for
	the result (if not NULL), this memory should be further deallocated
	using free(3).
*/
static char *prepare_to_basename(char *path) {
	assert(path != NULL);

	int dot_count = 0; // Number of dottes encountered
	int remove_count = 0; // Number of elements to remove
	bool path_is_absolute = path[0] == '/'; // if the path is absolute
	char work_directory[MAXPATHLEN];
	char *endp = path + strlen(path) - 1;
	while (1) {
		if (endp < path) {
			// The path string is parsed
			if (!path_is_absolute) {
				path = getcwd(work_directory, sizeof(work_directory));
				if (path == 0) {
					// getcwd error
					return NULL;
				}
				path_is_absolute = 1;
				endp = path + strlen(path) - 1;
				if (dot_count == 2) {
					++remove_count;
				}
				dot_count = 0;
			} else {
				// There is no more path left
				if (path[0] == '/' && remove_count == 0) {
					return strdup("/");
				} else {
					return NULL;
				}
			}
		}
		if (*endp == '/') {
			if (dot_count == 1) {
				// /. encountered, simply remove this
				dot_count = 0;
			} else if (dot_count == 2) {
				// /.. encountered
				++remove_count;
				dot_count = 0;
			} else if (dot_count > 2) {
				// FIXME: The is wrong
				// Incorrect name (three dots (or more) are specified as a name)
				return NULL;
			}
		} else if (*endp == '.') {
			++dot_count;
		} else {
			// Normal symbol encountered
			if (remove_count > 0) {
				dot_count = 0;
				do {
					--endp;
				} while (*endp != '/' && endp >= path);
				++endp;
				if (endp < path) {
					// The path string is parsed
					if (!path_is_absolute) {
						path = getcwd(work_directory, sizeof(work_directory));
						if (path == 0) {
							// getcwd error
							return NULL;
						}
						path_is_absolute = 1;
						endp = path + strlen(path) - 1;
					} else {
						// There is no more path left
						// FIXME: this probably could't ever happen
						return NULL;
					}
				}
				--remove_count;
			} else {
				// The basename is detected
				if (endp != path + strlen(path) - 1 || path == work_directory) {
					char *result = (char *)malloc(endp - path + dot_count + 2);
					memcpy(result, path, endp - path + dot_count + 1);
					result[endp - path + dot_count + 1] = '\0';
					return result;
				} else {
					return strdup(path);
				}
			}
		}
		--endp;
	}
}

int main(int argc, char **argv) {
	// Options
	char *prog_name = argv[0];
	bool preserve_order = false; // Don't change order of the destinations
	// TODO: realize behavior defined by the following flag
	bool overwrite_files = true; // Overwrite read-only files
	uint16_t unicast_port = UNICAST_PORT; // TCP port used for unicast connections

	// Set speed for the pseudo-random numbers
	struct timeval tv;
	gettimeofday(&tv, NULL);
	srand((tv.tv_sec << 8) + getpid() % (1 << 8));

	// Parse the command options
	int ch;
	while ((ch = getopt(argc, argv, "p:oh")) != -1) {
		switch (ch) {
			case 'p': // Port specified
				if ((unicast_port = atoi(optarg)) == 0) {
					ERROR("Invalid port: %s\n", optarg);
					exit(EXIT_FAILURE);
				}
				break;
			case 'o': // Preserve order during pipe transfert (don't
				// sort destinations)
				preserve_order = true;
				break;
			case 'n': // Don't overwrite files, if they are already exist
				// TODO: implement this functionality
				overwrite_files = false;
				break;
			default:
				usage_and_exit(prog_name);
		}
	}
	argc -= optind;
	argv += optind;

	if (argc < 2) {
		usage_and_exit(prog_name);
	}

	// Look for the delimiter symbol
	int delimiter; // Delimiter of the source file names with the servers list
	for (delimiter = 0; delimiter < argc; ++delimiter) {
		if (argv[delimiter][0] == ';' && argv[delimiter][1] == '\0') {
			break;
		}
	}
	int nsources = delimiter == argc ? 1 : delimiter;

	if (nsources < 1) {
		fprintf(stderr, "You should specify at least one source file "
			"or directory.\n");
		usage_and_exit(prog_name);
	}

	// Get the sources' names from the arguments
	char** filenames = (char **)malloc(sizeof(char **) * (nsources + 1)); // Null-terminating array of sources
	for (int i = 0; i < nsources; ++i) {
		filenames[i] = prepare_to_basename(argv[i]);
		if (filenames[i] == NULL) {
			ERROR("Incorrect path: %s\n", argv[i]);
			exit(EXIT_FAILURE);
		}
		// Check for the sources' accessability
		if (access(filenames[i], R_OK) != 0) {
			ERROR("%s: %s\n", filenames[i], strerror(errno));
			exit(EXIT_FAILURE);
		}
	}
	filenames[nsources] = NULL;
#ifndef NDEBUG
	printf("Sources:\n");
	for (int i = 0; filenames[i] != NULL; ++i) {
		printf("%s\n", filenames[i]);
	}
#endif

	// Parse the server names, and get the servers' addresses
	vector<Destination> dst;
	int first_destination = delimiter == argc ? 1 : delimiter + 1;
	struct hostent *hptr;
	for (int i = first_destination; i < argc; ++i) {
		char *hostname;
		// Delimit the server name from the destination path
		char *colon = strchr(argv[i], ':');
		if (colon != NULL) {
			hostname = (char *)malloc(colon - argv[i] + 1);
			memcpy(hostname, argv[i], colon - argv[i]);
			hostname[colon - argv[i]] = 0;
		} else {
			hostname = argv[i];
		}
		// FIXME: For portability reasons we should check whether the 
		// hostname in the dotted dicimal format here.
		if ((hptr = gethostbyname(hostname)) == NULL) {
			ERROR("Can't get address for the host %s: %s\n", argv[i],
				hstrerror(h_errno));
			exit(EXIT_FAILURE);
		}

		uint32_t addr = ntohl(*(uint32_t *)*hptr->h_addr_list);
		dst.push_back(Destination(addr,
			(colon == NULL || colon[1] == '\0') ? NULL: strdup(colon + 1)));

		if (colon != NULL) {
			free(hostname);
		}
	}

	if (dst.size() == 0) {
		fprintf(stderr, "You should specify at least one destination.\n");
		usage_and_exit(prog_name);
	}
	
	// Sort the destinations in attempt to achive a better order for
	// the piped transfert
	if (!preserve_order) {
		sort(dst.begin(), dst.end());
	}

#ifndef NDEBUG
	printf("Destinations:\n");
	for (vector<Destination>::const_iterator i = dst.begin();
			i != dst.end(); ++i) {
		printf("%d.", (*i).addr >> 24);
		printf("%d.", (*i).addr >> 16 & 0xFF);
		printf("%d.", (*i).addr >> 8 & 0xFF);
		printf("%d: ", (*i).addr & 0xFF);
		if (i->filename != NULL) {
			printf("%s\n", &*(i->filename));
		} else {
			printf("\n");
		}
	}
#endif

#ifndef NDEBUG
	// Set SIGPIPE handler for debugging
	signal(SIGPIPE, sigpipe_handler);
#else
	signal(SIGPIPE, SIG_IGN);
#endif

	// The main objects
	FileReader *source_reader = new FileReader();
	UnicastSender *unicast_sender = NULL;
	MulticastSender *multicast_sender = new MulticastSender(source_reader,
		unicast_port, nsources);
	pthread_t unicast_sender_thread;
	pthread_t multicast_sender_thread;
	bool is_unicast_sender_started = false;
	bool is_multicast_sender_started = false;

	// Establish the multicast session
	vector<Destination> *remaining_dst = multicast_sender->session_init(dst,
		nsources);
	if (remaining_dst == NULL) {
		source_reader->display_error();
		exit(EXIT_FAILURE);
	}
	if (preserve_order) {
		// FIXME: Do something here to implement the preserve order policy
	}
#ifndef NDEBUG
	SDEBUG("Destinations unreachable through multicast:\n");
	for (vector<Destination>::const_iterator i = remaining_dst->begin();
			i != remaining_dst->end(); ++i) {
		printf("%d.", (*i).addr >> 24);
		printf("%d.", (*i).addr >> 16 & 0xFF);
		printf("%d.", (*i).addr >> 8 & 0xFF);
		printf("%d: ", (*i).addr & 0xFF);
		if (i->filename != NULL) {
			printf("%s\n", &*(i->filename));
		} else {
			printf("\n");
		}
	}
#endif
	// FIXME: some other evaluation should done be here
	if (remaining_dst->size() < dst.size()) {
		// Start the multicast sender
		int error;
		if ((error = pthread_create(&multicast_sender_thread, NULL,
				multicast_sender_routine, (void *)multicast_sender)) != 0) {
			ERROR("Can't create a new thread: %s\n", strerror(errno));
			exit(EXIT_FAILURE);
		}
		is_multicast_sender_started = true;
	} else {
		// Delete the multicast sender
		delete multicast_sender;
		multicast_sender = NULL;
	}

	if (remaining_dst->size() > 0) {
		unicast_sender = new UnicastSender(source_reader, unicast_port);
		// Establish the unicast session
		if (unicast_sender->session_init(*remaining_dst, nsources) != 0) {
			source_reader->display_error();
			return EXIT_FAILURE;
		}

		// Start the unicast_sender thread
		int error;
		if ((error = pthread_create(&unicast_sender_thread, NULL,
				unicast_sender_routine, (void *)unicast_sender)) != 0) {
			ERROR("Can't create a new thread: %s\n", strerror(errno));
			exit(EXIT_FAILURE);
		}
		is_unicast_sender_started = true;
	}

	delete remaining_dst;

	bool display_error = false;
	if (source_reader->read_sources(filenames) != 0) {
		display_error = true;
	}

	// Wait for the reader and multicast_sender
	void *result;
	if (is_unicast_sender_started) {
		pthread_join(unicast_sender_thread, &result);
		if (result != NULL) {
			display_error = true;
		}
	}
	if (is_multicast_sender_started) {
		pthread_join(multicast_sender_thread, &result);
		if (result != NULL) {
			display_error = true;
		}
	}
	if (display_error) {
		source_reader->display_error();
	}

	SDEBUG("Finished, clean up the data\n");
	if (multicast_sender != NULL) { delete multicast_sender; }
	if (unicast_sender != NULL) { delete unicast_sender; }
	delete source_reader;
	for (int i = 0; i < nsources; ++i) {
		free(filenames[i]);
	}
	free(filenames);
	return EXIT_SUCCESS;
}
