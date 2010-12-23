#include <stdio.h>
#include <stdlib.h>
#include <string.h> // for strerror
#include <errno.h>
#include <fcntl.h>
#include <unistd.h> // for getopt
#include <netdb.h> // for gethostbyname
#include <sys/types.h>
#include <sys/stat.h> // for stat
#include <sys/time.h> // for gettimeofday
#include <signal.h> // for SIGPIPE

#include <sys/socket.h> // for socket
#include <netinet/in.h>

#include <dirent.h> // for opendir
#include <pthread.h>
#include <vector>
#include <string>
#include <algorithm>
#include <sys/param.h> // for MAXPATHLEN

#include "md5.h"
#include "destination.h"
#include "connection.h"
#include "distributor.h"
#include "file_reader.h"
#include "unicast_sender.h"
#include "multicast_sender.h"
#include "log.h"

#define MAX_RETRANSMISSIONS 3

using namespace std;

// Print usage information and exit
void usage_and_exit(char *name)
{
  printf("Usage:\n");
  printf("%s [options] file server1[:location1] [...]\n", name);
  printf("%s [options] file1 [...] \\; server1[:location1] [...]\n", name);
  printf("Options:\n"
  "\t-p port\n"
  "\t\tSpecify port the server will use for both unicast and multicast \n"
  "\t\tconnections instead of the default ports ("
  NUMBER_TO_STRING(UNICAST_PORT) ", " NUMBER_TO_STRING(MULTICAST_PORT) ").\n\n" 
  "\t-P port\n"
  "\t\tSpecify port the server will use for multicast connections\n"
  "\t\tinstead of the default port (" NUMBER_TO_STRING(MULTICAST_PORT) ").\n\n" 
  "\t-U\tUnicast only (for all hops)\n"
  "\t-u\tUnicast only (for the first hop)\n"
  "\t-m\tMulticast only\n"
  "\t-c\tVerify the file checksums twise. The second verification is\n"
  "\t\tperformed on data written to the disk.\n"
  "\t-o\tPreserve the specified order of targets during the pipelined\n"
  "\t\ttransfert.\n");
  exit(EXIT_FAILURE);
}

// Wrapper for the UnicastSender::session function, passed
// to pthread_create.
void *unicast_sender_routine(void *args)
{
  SDEBUG("Start the unicast sender\n");
  UnicastSender *us = (UnicastSender *)args;
  if (us->session() != 0) {
    // Report about an error
    return (void *)-1;
  }
  return NULL;
}

// Wrapper for the MulticastSender::session function, passed
// to pthread_create.
void *multicast_sender_routine(void *args)
{
  SDEBUG("Start the multicast sender\n");
  MulticastSender *ms = (MulticastSender *)args;
  if (ms->session() != 0) {
    // Signal about an error
    SDEBUG("Multicast sender finished with an error\n");
    return (void *)-1;
  }
  return NULL;
}

#ifndef NDEBUG
void sigpipe_handler(int signum)
{
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
static char *prepare_to_basename(char *path)
{
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

int main(int argc, char **argv)
{
  char *prog_name = argv[0];
  bool is_unrecoverable_error_occurred = false; // whether some unrecoverable
    // errors occurred
  
  // Configurable variables
  bool is_order_preserved = false; // Don't change order of the destinations
  // TODO: implement behavior defined by the following flag
  //bool overwrite_files = true; // Overwrite read-only files
  bool is_multicast_only = false; // Use only the link-local multicast traffic
  bool is_first_hop_unicast_only = false; // Use only the unicast traffic
    // for the first hop
  bool verify_checksums_twise = false; // Verify the file checksums twise
  uint16_t unicast_port = UNICAST_PORT; // TCP port used for unicast connections
  uint16_t multicast_port = MULTICAST_PORT; // UDP port used
    // for multicast connections
  uint32_t flags = 0;
#ifdef USE_MULTICAST // Multicast is temporary switched off
  bool is_unicast_only = false; // If it's true, use only the unicast traffic
#else
  bool is_unicast_only = true; // If it's true, use only the unicast traffic
  flags |= UNICAST_ONLY_FLAG;
#endif

  // Set speed for the pseudo-random numbers
  struct timeval tv;
  gettimeofday(&tv, NULL);
  srand((tv.tv_sec << 8) + getpid() % (1 << 8));

  // Parse the command options
  int ch;
  while ((ch = getopt(argc, argv, "p:P:omUuch")) != -1) {
    switch (ch) {
      case 'p': // Port specified
        if ((unicast_port = atoi(optarg)) == 0) {
          ERROR("Invalid port: %s\n", optarg);
          exit(EXIT_FAILURE);
        }
        if (multicast_port == MULTICAST_PORT) {
          multicast_port = unicast_port;
        }
        break;
      case 'P': // Multicast port
        if ((multicast_port = atoi(optarg)) == 0) {
          ERROR("Invalid port: %s\n", optarg);
          exit(EXIT_FAILURE);
        }
        break;
      case 'o': // Preserve order during pipe transfert (don't
        // sort destinations)
        is_order_preserved = true;
        flags |= PRESERVE_ORDER_FLAG;
        break;
      case 'm': // Use only the link-local multicast traffic
        is_multicast_only = true;
        is_unicast_only = false;
        flags &= ~UNICAST_ONLY_FLAG;
        break;
      case 'U': // Use only the unicast traffic
        is_unicast_only = true;
        is_multicast_only = false;
        flags |= UNICAST_ONLY_FLAG;
        break;
      case 'u': // Use only the unicast traffic for the first hop
        is_first_hop_unicast_only = true;
        break;
      case 'c': // Verify the file checksums twise
        verify_checksums_twise = true;
        flags |= VERIFY_CHECKSUMS_TWISE_FLAG;
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
  uint32_t n_sources = delimiter == argc ? 1 : delimiter;

  if (n_sources < 1) {
    fprintf(stderr, "You should specify at least one source file "
      "or directory.\n");
    usage_and_exit(prog_name);
  }

  // Get the sources' names from the arguments
  char** filenames; // Null-terminating array of sources
  filenames = (char **)malloc(sizeof(char **) * (n_sources + 1));
  int *filename_offsets = (int *)malloc(sizeof(int) * n_sources);
  for (unsigned i = 0; i < n_sources; ++i) {
    filenames[i] = prepare_to_basename(argv[i]);
    filename_offsets[i] = 0;
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
  filenames[n_sources] = NULL;
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
  if (!is_order_preserved) {
    sort(dst.begin(), dst.end());
  }

#ifndef NDEBUG
  // Set SIGPIPE handler for debugging
  signal(SIGPIPE, sigpipe_handler);
#else
  signal(SIGPIPE, SIG_IGN);
#endif

  // Start of the global part, which is 
  bool is_all_done = false;
  unsigned n_retransmissions = 0;
  do {
    // The main objects
    FileReader *source_reader = new FileReader();
    UnicastSender *unicast_sender = NULL;
    MulticastSender *multicast_sender = NULL;
    pthread_t unicast_sender_thread;
    pthread_t multicast_sender_thread;
    bool is_unicast_sender_started = false;
    bool is_multicast_sender_started = false;

#ifndef NDEBUG
    SDEBUG("Destinations:\n");
    for (vector<Destination>::const_iterator i = dst.begin();
        i != dst.end(); ++i) {
      char saddr[INET_ADDRSTRLEN];
      uint32_t iaddr = htonl((*i).addr);
      if (i->filename != NULL) {
        DEBUG("%s: %s\n", inet_ntop(AF_INET, &iaddr, saddr, sizeof(saddr)),
          &*(i->filename));
      } else {
        DEBUG("%s:\n", inet_ntop(AF_INET, &iaddr, saddr, sizeof(saddr)));
      }
    }
#endif
  
    // Initialize the multicast sender
    const vector<Destination> *remaining_dst = NULL;
    if (!is_unicast_only && !is_first_hop_unicast_only) {
      multicast_sender = MulticastSender::create_and_initialize(dst,
        &remaining_dst, n_sources, is_multicast_only,
        source_reader, MulticastSender::client_mode, multicast_port,
        n_retransmissions);
      if (remaining_dst == NULL) {
        // Some error occurred during the multicast sender initialization
        source_reader->display_errors();
        exit(EXIT_FAILURE);
      }
      if (is_multicast_only && remaining_dst->size() != 0) {
        char *hosts = (char *)strdup("");
        for (vector<Destination>::const_iterator i = remaining_dst->begin();
            i != remaining_dst->end();) {
          uint32_t address = htonl(i->addr);
          ++i;
          char addr[INET_ADDRSTRLEN + 2];
          inet_ntop(AF_INET, &address, addr, sizeof(addr));
          if (i != remaining_dst->end()) {
            strcat(addr, ", ");
          }
          hosts = (char *)realloc(hosts, strlen(hosts) + strlen(addr) + 1);
          strcat(hosts, addr);
        }
        ERROR("Can't establish multicast connection with the hosts (%zu): %s\n",
          remaining_dst->size(), hosts);
        free(hosts);
        exit(EXIT_FAILURE);
      }
    } else {
      remaining_dst = &dst;
    }

    if (remaining_dst->size() < dst.size()) {
      // Start the multicast sender
      int error;
      error = pthread_create(&multicast_sender_thread, NULL,
        multicast_sender_routine, (void *)multicast_sender);
      if (error != 0) {
        ERROR("Can't create a new thread: %s\n", strerror(errno));
        exit(EXIT_FAILURE);
      }
      is_multicast_sender_started = true;
    }
  
    if (remaining_dst->size() > 0) {
      unicast_sender = new UnicastSender(source_reader,
        UnicastSender::client_mode, unicast_port, flags);
      // Establish the unicast session
      if (unicast_sender->session_init(*remaining_dst, n_sources) != 0) {
        source_reader->display_errors();
        return EXIT_FAILURE;
      }
  
      // Start the unicast_sender thread
      int error;
      error = pthread_create(&unicast_sender_thread, NULL,
        unicast_sender_routine, (void *)unicast_sender);
      if (error != 0) {
        ERROR("Can't create a new thread: %s\n", strerror(errno));
        exit(EXIT_FAILURE);
      }
      is_unicast_sender_started = true;
    }
  
    bool is_unrecoverable_error_occurred = false;
    if (source_reader->read_sources(filenames, filename_offsets) != 0) {
      is_unrecoverable_error_occurred = true;
    }
  
    // Wait for the reader and multicast_sender
    void *result;
    if (is_unicast_sender_started) {
      pthread_join(unicast_sender_thread, &result);
      if (result != NULL) {
        is_unrecoverable_error_occurred = true;
      }
    }
    if (is_multicast_sender_started) {
      pthread_join(multicast_sender_thread, &result);
      if (result != NULL) {
        is_unrecoverable_error_occurred = true;
      }
    }

    source_reader->display_errors();
    if (source_reader->is_unrecoverable_error_occurred()) {
      is_unrecoverable_error_occurred = true;
    }
    if (is_unrecoverable_error_occurred) {
      SDEBUG("Unrecoverable error occurred\n");
      return EXIT_FAILURE;
    }

    SDEBUG("Finished, clean up the data\n");
    if (multicast_sender != NULL) { delete multicast_sender; }
    if (unicast_sender != NULL) { delete unicast_sender; }
    for (unsigned i = 0; i < n_sources; ++i) {
      free(filenames[i]);
    }
    if (remaining_dst != NULL && remaining_dst != &dst) {
      delete remaining_dst;
    }
    free(filenames);
    free(filename_offsets);
  
    // Check if some file retransmissions required
    filenames = source_reader->errors.get_retransmissions(&dst,
      &filename_offsets);
    if (dst.size() == 0) {
      // Finish work, no retransmissions required
      is_all_done = true;
      SDEBUG("No retransmissions required\n");
    } else {
      // Retransmission required for some files
      n_sources = 2; // to use existing directory in the case when n_sources has
        // been equal to 1 initially
      ++n_retransmissions;
      if (n_retransmissions > MAX_RETRANSMISSIONS) {
        char **f = filenames;
        string files;
        string destinations;
        vector<Destination>::const_iterator i = dst.begin();
        while (1) {
          char saddr[INET_ADDRSTRLEN];
          uint32_t baddr = htonl((*i).addr);
          destinations += inet_ntop(AF_INET, &baddr, saddr, sizeof(saddr));
          ++i;
          if (i != dst.end()) {
            destinations += ", ";
          } else {
            break;
          }
        }
        while (1) {
          files += *f;
          ++f;
          if (*f != NULL) {
            files += ", ";
          } else {
            break;
          }
        }
        ERROR("Too many retransmissions for files %s to hosts %s\n",
          files.c_str(), destinations.c_str());
        return EXIT_FAILURE;
      }
    }
  
    delete source_reader;
  } while (!is_all_done);

  if (!is_unrecoverable_error_occurred) {
    return EXIT_SUCCESS;
  } else {
    return EXIT_FAILURE;
  }
}
