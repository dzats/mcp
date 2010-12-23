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
#include <net/if.h> // for SIOCGIFADDR
#include <sys/ioctl.h>

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

#define MAX_RETRANSMISSIONS 5
#define RECONNECTION_TIMEOUT 10 // seconds. The actual timeout is randomly
  // varied in the range from the specified value to the specified value +
  // RECONNECTION_TIMEOUT_SPREAD.
#define RECONNECTION_TIMEOUT_SPREAD 2 // seconds. The actual timeout is randomly
  // varied in the range from the specified value to the doubled specified
  // value.

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
  "\t-U\tUnicast only (for all hops).\n\n"
  "\t-u\tUnicast only (for the first hop).\n\n"
  "\t-m\tMulticast only.\n\n"
  "\t-g interface\n"
  "\t\tTry to establish multicast connection with all the destinations,\n"
  "\t\tnot only the link-local ones. Use the specified interface for\n"
  "\t\tfor multicast traffic.\n\n"
  "\t-c\tVerify the file checksums twise. The second verification is\n"
  "\t\tperformed on data written to the disk.\n\n"
  "\t-b value\n"
  "\t\tSet per-sender bandwidth limit (suffix 'm' - megabits/second,\n"
  "\t\tsuffix 'M' - megabytes/second, without suffix - bytes/second).\n\n"
  "\t-f rate\n"
  "\t\tSet fixed-rate mode for the multicast transfert.  Rate is\n"
  "\t\tspecified in bytes/second (suffix 'm' - megabits/second,\n"
  "\t\tsuffix 'M' - megabytes/second).\n\n"
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
  uint8_t status = us->session();
  if (status != STATUS_OK) {
    // Report about an error
    DEBUG("Unicast sender finished with error: %u\n", status);
    return (void *)status;
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
  bool is_multicast_only = false; // Use only the multicast traffic
  bool is_first_hop_unicast_only = false; // Use only the unicast traffic
    // for the first hop
  bool verify_checksums_twise = false; // Verify the file checksums twise
  uint16_t unicast_port = UNICAST_PORT; // TCP port used for unicast connections
  uint16_t multicast_port = MULTICAST_PORT; // UDP port used
    // for multicast connections
  unsigned bandwidth = 0; // Maximum data bandwidth per one sender
    // (in bytes per 1.048576 seconds)
  uint32_t flags = 0;
#ifdef USE_MULTICAST // Multicast is temporary switched off
  bool is_unicast_only = false; // If it's true, use only the unicast traffic
#else
  bool is_unicast_only = true; // If it's true, use only the unicast traffic
  flags |= UNICAST_ONLY_FLAG;
#endif
  bool use_global_multicast = false; // Don't limit multicast by the local link
  uint32_t multicast_interface = INADDR_NONE; // Interface that will be used
    // for global multicast traffic
  bool use_fixed_rate_multicast = false; // Don't use any congestion control
    // technique, instead send multicast traffic with constant rate

#ifdef NDEBUG
  openlog(argv[0], LOG_PERROR | LOG_PID, LOG_DAEMON);
#endif

  // Set speed for the pseudo-random numbers
  struct timeval tv;
  gettimeofday(&tv, NULL);
  srand((tv.tv_sec << 8) + getpid() % (1 << 8));

  // Parse the command options
  int ch;
  while ((ch = getopt(argc, argv, "p:P:omg:Uub:f:ch")) != -1) {
    switch (ch) {
      case 'p': // Port specified
        if ((unicast_port = atoi(optarg)) == 0) {
          ERROR("Invalid port: %s\n", optarg);
          return EXIT_FAILURE;
        }
        if (multicast_port == MULTICAST_PORT) {
          multicast_port = unicast_port;
        }
        break;
      case 'P': // Multicast port
        if ((multicast_port = atoi(optarg)) == 0) {
          ERROR("Invalid port: %s\n", optarg);
          return EXIT_FAILURE;
        }
        break;
      case 'o': // Preserve order during pipe transfert (don't
        // sort destinations)
        is_order_preserved = true;
        flags |= PRESERVE_ORDER_FLAG;
        break;
      case 'm': // Use only the multicast traffic
        is_multicast_only = true;
        is_unicast_only = false;
        flags &= ~UNICAST_ONLY_FLAG;
        break;
      case 'g': // Don't limit multicast by the local link
        use_global_multicast = true;
        // Get address of the specified interface
        {
          int sock = socket(AF_INET, SOCK_STREAM, 0);
          if (sock < 0) {
            ERROR("Can't create a TCP socket: %s\n", strerror(errno));
            return EXIT_FAILURE;
          }
          struct ifreq ifr;
          strncpy(ifr.ifr_name, optarg, IFNAMSIZ);
          ifr.ifr_addr.sa_family = AF_INET;
          if (ioctl(sock, SIOCGIFADDR, &ifr) < 0) {
            ERROR("Can't get ip address of the interface %s: %s\n",
              optarg, strerror(errno));
            return EXIT_FAILURE;
          }
          multicast_interface =
            ((struct sockaddr_in *)&ifr.ifr_addr)->sin_addr.s_addr;
#ifndef NDEBUG
          char mi_addr[INET_ADDRSTRLEN];
          DEBUG("Multicast interface: %s\n",
            inet_ntop(AF_INET, &multicast_interface, mi_addr, sizeof(mi_addr)));
#endif
          multicast_interface = ntohl(multicast_interface);
          close(sock);
        }
        break;
      case 'U': // Use only the unicast traffic
        is_unicast_only = true;
        is_multicast_only = false;
        flags |= UNICAST_ONLY_FLAG;
        break;
      case 'u': // Use only the unicast traffic for the first hop
        is_first_hop_unicast_only = true;
        break;
      case 'b': // Set per-sender bandwidth
      case 'f': // Set fixed rate for multicast traffic
        for (unsigned i = 0; i < strlen(optarg) - 1; ++i) {
          if (optarg[i] < '0' || optarg[i] > '9') {
            ERROR("Incorrect bandwidth: %s\n", optarg);
            return EXIT_FAILURE;
          }
        }

        bandwidth = atoi(optarg);
        if (optarg[strlen(optarg) - 1] < '0' ||
            optarg[strlen(optarg) - 1] > '9') {
          if (optarg[strlen(optarg) - 1] == 'm') {
            bandwidth *= 131072;
          } else if (optarg[strlen(optarg) - 1] == 'M') {
            bandwidth *= 1048576;
          } else {
            ERROR("Unknown bandwidth suffix: %c\n", optarg[strlen(optarg) - 1]);
            return EXIT_FAILURE;
          }
        }
  
        if (bandwidth != 0) {
          // Change the bandwidth's unit of measurement
          uint64_t b = bandwidth;
          b = (b << 20) / 1000000;
          bandwidth = b;
        } else {
          ERROR("Incorrect bandwidth: %s\n", optarg);
          return EXIT_FAILURE;
        }
        if (ch == 'f') { use_fixed_rate_multicast = true; }
        DEBUG("Bandwidth (in bytes per 1.048576 seconds): %u\n", bandwidth);
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
    filename_offsets[i] = -1;
    if (filenames[i] == NULL) {
      ERROR("Incorrect path: %s\n", argv[i]);
      exit(EXIT_FAILURE);
    } else if (filenames[i][0] == '/' && filenames[i][1] == '\0') {
      SERROR("You can't copy the / directory\n");
      exit(EXIT_FAILURE);
    }
    // Check for the sources' accessability
    if (access(filenames[i], R_OK) != 0) {
      ERROR("%s: %s\n", filenames[i], strerror(errno));
      exit(EXIT_FAILURE);
    }
  }
  filenames[n_sources] = NULL;

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
  uint32_t is_source_a_single_directory = false;
  char *root_source_directory = NULL;
  if (n_sources == 1) {
    struct stat tstat;
    if (stat(filenames[0], &tstat) != 0) {
      ERROR("Can't get attributes for file/directory %s: %s\n", filenames[0],
        strerror(errno));
      exit(EXIT_FAILURE);
    }
    if (S_ISDIR(tstat.st_mode)) {
      is_source_a_single_directory = true;
      root_source_directory = strdup(filenames[0]);
    }
  }
  do {
    // The main objects
    FileReader *source_reader;
    UnicastSender *unicast_sender = NULL;
    MulticastSender *multicast_sender = NULL;
    pthread_t unicast_sender_thread;
    pthread_t multicast_sender_thread;
    bool is_unicast_sender_started = false;
    bool is_multicast_sender_started = false;

    const vector<Destination> *remaining_dst = NULL;

#ifndef NDEBUG
    printf("Sources:\n");
    for (int i = 0; filenames[i] != NULL; ++i) {
      printf("%s\n", filenames[i]);
    }
#endif

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
    try_to_connect_again:
    source_reader = new FileReader();
  
    // Initialize the multicast sender
    if (!is_unicast_only && !is_first_hop_unicast_only) {
      multicast_sender = MulticastSender::create_and_initialize(dst,
        &remaining_dst, n_sources,
        is_multicast_only, use_global_multicast, multicast_interface,
        source_reader, MulticastSender::client_mode,
        multicast_port, bandwidth, use_fixed_rate_multicast, n_retransmissions);
      if (remaining_dst == NULL) {
        // Some error occurred during the multicast sender initialization
        SDEBUG("Initialization of the multicast sender failed.\n");
        if (source_reader->is_unrecoverable_error_occurred()) {
          source_reader->display_errors();
          delete source_reader;
          exit(EXIT_FAILURE);
        } else {
          //source_reader->delete_recoverable_errors();
          delete source_reader;
          if (source_reader->is_server_busy()) {
            DEBUG("Will try to connect again after %u microseconds\n", 
              1000000 * RECONNECTION_TIMEOUT +
              unsigned((RECONNECTION_TIMEOUT_SPREAD * 1000000) *
              ((float)rand()/RAND_MAX)));
            usleep(1000000 * RECONNECTION_TIMEOUT +
              unsigned((RECONNECTION_TIMEOUT_SPREAD * 1000000) *
              ((float)rand()/RAND_MAX)));
          }
          // FIXME: add limitation to the muximum time a client can wait
          // till some server is busy.
          goto try_to_connect_again;
        }
      }
      if (is_multicast_only && remaining_dst->size() != 0) {
        char *hosts = (char *)strdup("");
        for (vector<Destination>::const_iterator i = remaining_dst->begin();
            i != remaining_dst->end();) {
          uint32_t address = htonl(i->addr);
          ++i;
          char addr[INET_ADDRSTRLEN + 2];
          inet_ntop(AF_INET, &address, addr, sizeof(addr));
          if (i != remaining_dst->end()) { strcat(addr, ", "); }
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
  
    if (remaining_dst->size() > 0) {
      unicast_sender = new UnicastSender(source_reader,
        UnicastSender::client_mode, unicast_port, flags, bandwidth);
      // Establish the unicast session
      uint8_t session_init_result =
        unicast_sender->session_init(*remaining_dst, n_sources);
      if (session_init_result != STATUS_OK) {
        if (session_init_result == STATUS_SERVER_IS_BUSY ||
            session_init_result == STATUS_PORT_IN_USE) {
          // It is recoverable error, just try to establish conection again
          delete unicast_sender;
          unicast_sender = NULL;
          if (multicast_sender != NULL) {
            multicast_sender->abnormal_termination();
            delete multicast_sender;
            multicast_sender = NULL;
          }
          if (remaining_dst != NULL && remaining_dst != &dst) {
            delete remaining_dst;
          }
          remaining_dst = NULL;
          //source_reader->delete_server_is_busy_errors();
          delete source_reader;
          if (session_init_result == STATUS_SERVER_IS_BUSY) {
            // Wait for some time interval
            DEBUG("Will try to connect again after %u microseconds\n", 
              1000000 * RECONNECTION_TIMEOUT +
              unsigned((RECONNECTION_TIMEOUT_SPREAD * 1000000) *
              ((float)rand()/RAND_MAX)));
            usleep(1000000 * RECONNECTION_TIMEOUT +
              unsigned((RECONNECTION_TIMEOUT_SPREAD * 1000000) *
              ((float)rand()/RAND_MAX)));
          }
          // FIXME: add limitation for the muximum time the client can wait
          goto try_to_connect_again;
        } else {
          source_reader->display_errors();
          SDEBUG("UnicastSender::session_init method failed\n");
          return EXIT_FAILURE;
        }
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
  
    bool is_unrecoverable_error_occurred = false;
    bool is_recoverable_error_occurred = false;
    uint8_t source_reader_result = 
      source_reader->read_sources(filenames, filename_offsets);
    if (source_reader_result != STATUS_OK &&
        source_reader_result != STATUS_INCORRECT_CHECKSUM &&
        source_reader_result != STATUS_PORT_IN_USE &&
        source_reader_result != STATUS_SERVER_IS_BUSY) {
      SDEBUG("The SourceReader finished with unrecoverable error\n");
      is_unrecoverable_error_occurred = true;
    }
  
    // Wait for the reader and multicast_sender
    void *result;
    if (is_unicast_sender_started) {
      pthread_join(unicast_sender_thread, &result);
      uint8_t status = (uint8_t)(unsigned long)result;
      if (status != STATUS_OK) {
        assert(status != STATUS_INCORRECT_CHECKSUM);
        if (status == STATUS_SERVER_IS_BUSY) {
          SDEBUG("The UnicastSender finished with the SERVER_IS_BUSY error\n");
          is_recoverable_error_occurred = true;
        } else if (status == STATUS_PORT_IN_USE) {
          SDEBUG("The UnicastSender finished with the STATUS_PORT_IN_USE "
            "error\n");
          is_recoverable_error_occurred = true;
        } else {
          SDEBUG("The UnicastSender finished with an unrecoverable error\n");
          is_unrecoverable_error_occurred = true;
        }
      }
    }
    if (is_multicast_sender_started) {
      pthread_join(multicast_sender_thread, &result);
      if (result != NULL) {
        SDEBUG("The MulticastSender finished with fatal error\n");
        is_unrecoverable_error_occurred = true;
      }
    }

    bool is_server_busy = source_reader->is_server_busy();
    source_reader->delete_recoverable_errors();
    source_reader->display_errors();
    if (source_reader->is_unrecoverable_error_occurred()) {
      is_unrecoverable_error_occurred = true;
    }
    if (is_unrecoverable_error_occurred) {
      SDEBUG("Unrecoverable error occurred\n");
      return EXIT_FAILURE;
    }

    SDEBUG("Finished, clean up the data\n");
    if (multicast_sender != NULL) {
      delete multicast_sender;
      multicast_sender = NULL;
    }
    if (unicast_sender != NULL) {
      delete unicast_sender;
      unicast_sender = NULL;
    }
    if (remaining_dst != NULL && remaining_dst != &dst) {
      delete remaining_dst;
    }

    // Restart the client if some of the servers have been busy
    if (is_recoverable_error_occurred) {
      if (is_server_busy) {
        DEBUG("Will try to connect again after %u microseconds\n", 
          1000000 * RECONNECTION_TIMEOUT +
          unsigned((RECONNECTION_TIMEOUT_SPREAD * 1000000) *
          ((float)rand()/RAND_MAX)));
        usleep(1000000 * RECONNECTION_TIMEOUT +
          unsigned((RECONNECTION_TIMEOUT_SPREAD * 1000000) *
          ((float)rand()/RAND_MAX)));
      }
      // FIXME: add limitation to the muximum time the client can wait
      delete source_reader;
      goto try_to_connect_again;
    }

    char **p = filenames;
    while (*p != NULL) {
      free(*p);
      ++p;
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
      if (is_source_a_single_directory) {
        // This is the special case which requires a workaround to
        // allow file retransmissions
        n_sources = UINT32_MAX;
        // Detect the number of subdirectories in the root directory
        // which have the same name as the root directory
        DIR *dirp;
        struct dirent *dp;
        if ((dirp = opendir(root_source_directory)) == NULL) {
          ERROR("Can't read directory %s: %s\n", root_source_directory,
            strerror(errno));
          exit(EXIT_FAILURE);
        }
        char *current_directory = strdup(root_source_directory);
        char *seek_name = strrchr(root_source_directory, '/');
        if (seek_name == NULL) {
          seek_name = root_source_directory;
        } else {
          ++seek_name;
        }
        while ((dp = readdir(dirp)) != NULL) {
          if (!strcmp(dp->d_name, ".") || !strcmp(dp->d_name, "..")) {
            continue;
          }
          if (strlen(current_directory) + 1 + strlen(dp->d_name) > MAXPATHLEN) {
            ERROR("%s/%s: name is too long\n", current_directory, dp->d_name);
            continue;
          }
          if (!strcmp(dp->d_name, seek_name)) {
            // We found directory with the same name
            closedir(dirp);
            char *new_directory = (char *)malloc(strlen(current_directory) + 1
              + strlen(dp->d_name) + 1);
            sprintf(new_directory, "%s/%s", current_directory, dp->d_name);
            free(current_directory);
            current_directory = new_directory;
            if ((dirp = opendir(current_directory)) == NULL) {
              break;
            } else {
              --n_sources;
            }
          }
        }
        free(current_directory);
        closedir(dirp);
      }
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
