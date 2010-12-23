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
#include <signal.h> // for SIGPIPE
#include <netinet/in.h>
#include <arpa/inet.h> // for inet_ntoa
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
#include "reader.h"
#include "unicast_sender.h"
#include "log.h"

void usage_and_exit() {
	// TODO: print the usage instructions
	fprintf(stderr, "Usage: ...\n");
	exit(EXIT_FAILURE);
}

// Wrapper for the Reader::read_sources function, passed
// to pthread_create.
void *source_reader_routine(void *args) {
	Reader *s = ((Reader::ThreadArgs *)args)->source_reader;
	char **filenames = ((Reader::ThreadArgs *)args)->filenames;
	int error;
	if ((error = s->read_sources(filenames)) != 0) {
		ERROR("file_reader session error: %d\n", error);
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
	bool preserve_order = false; // Don't change order of the destinations
	// TODO: realize behavior defined by the following flag
	bool overwrite_files = true; // Overwrite read-only files

	// Parse the command options
	int ch;
	while ((ch = getopt(argc, argv, "oh")) != -1) {
		switch (ch) {
			case 'o': // Preserve order during pipe transfert (don't
				// sort destinations)
				preserve_order = true;
				break;
			case 'n': // Don't overwrite files, if they are already exist
				overwrite_files = false;
				break;
			default:
				usage_and_exit();
		}
	}
	argc -= optind;
	argv += optind;

	if (argc < 2) {
		usage_and_exit();
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
		usage_and_exit();
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
			ERROR("%s: %s", filenames[i], strerror(errno));
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
			fprintf(stderr, "Can't get address for the host %s: %s\n", argv[i],
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
		usage_and_exit();
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
	Distributor *buff = new Distributor();
	Reader *source_reader = new Reader(buff);
	UnicastSender *unicast_sender = new UnicastSender(buff);

	Reader::ThreadArgs freader_args = {source_reader, filenames};

	// Start the filereader thread
	pthread_t source_reader_thread;
	int error;
	if ((error = pthread_create(&source_reader_thread, NULL,
			source_reader_routine, (void *)&freader_args)) != 0) {
		ERROR("Can't create a new thread: %s\n", strerror(errno));
		exit(EXIT_FAILURE);
	}

	int retval;
	if ((retval = unicast_sender->session_init(dst, nsources)) != 0) {
		// TODO: report about an error somehow
		fprintf(stderr, "Session initialization failed: %s\n", strerror(retval));
		return EXIT_FAILURE;
	}
	if ((retval = unicast_sender->session()) != 0) {
		fprintf(stderr, "Transmission failed: %s\n", strerror(retval));
		return EXIT_FAILURE;
	}

	// Wait for readers
	pthread_join(source_reader_thread, NULL);

	// not necessary
	SDEBUG("TaskHeader finished, clean up the data\n");

	delete unicast_sender;
	delete source_reader;
	delete buff;
	for (int i = 0; i < nsources; ++i) {
		free(*filenames);
	}
	free(filenames);
	return EXIT_SUCCESS;
}
