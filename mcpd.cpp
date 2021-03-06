#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <unistd.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <sys/mman.h> // for mmap
#include <sys/socket.h>
#include <sys/time.h> // for gettimeofday
#include <fcntl.h>
#include <poll.h>
#include <pwd.h>

#include <netinet/in.h>
#include <arpa/inet.h> // for inet_addr

#include <vector>
#include <set>
#include <string>

#include "md5.h"
#include "destination.h"
#include "connection.h"
#include "distributor.h"

#include "log.h"

#include "multicast_receiver.h"
#include "unicast_receiver.h"
#include "multicast_sender.h"
#include "unicast_sender.h"
#include "file_writer.h"

using namespace std;

// Structure storing information about an established multicast connection
struct MulticastConnection {
  uint32_t source; // Source of the multicast connection
  uint32_t session_id; // Session id of the multicast connection
  pid_t pid; // PID of the process handling the 
  bool is_in_time_wait; // Whether this connection is in the
    // time wait state
  MulticastConnection(uint32_t src, uint32_t sid, pid_t p) : source(src),
    session_id(sid), pid(p), is_in_time_wait(false) {}

  bool operator==(const MulticastConnection& arg) const
  {
    return source == arg.source && session_id == arg.session_id;
  }
  bool operator<(const MulticastConnection& arg) const
  {
    return source < arg.source ||
      (source == arg.source && session_id < arg.session_id);
  }
};

// Function object to match Multicst Connection with particular PID
struct MulticastConnectionPidCompare
{
  pid_t pid;
public:
  MulticastConnectionPidCompare(pid_t p) : pid(p) {}
  bool operator()(const MulticastConnection& arg)
  {
    return pid == arg.pid;
  }
};

// Global variables
uid_t uid; // User id of the daemon
uid_t gid; // Group id of the daemon
char *homedir; // Home directory of the daemon

set<MulticastConnection> multicast_sessions; // Established multicast
  // sessions

sig_atomic_t max_n_connections; // Max number of the simultaneous
  // connections allowed to this server
volatile sig_atomic_t n_connections; // Number of currently established
  // connections

void usage_and_exit(char *name)
{
  printf("Usage:\n%s [options]\n", name);
  printf("Options:\n"
  "\t-a address\n"
  "\t\tSpecify an IP address the server will use. By default, \n"
  "\t\tserver will accept connections to all the local addresses.\n\n"
  "\t-p port\n"
  "\t\tSpecify port the server will use for both unicast and multicast \n"
  "\t\tconnections instead of the default ports ("
  NUMBER_TO_STRING(UNICAST_PORT) ", " NUMBER_TO_STRING(MULTICAST_PORT) ").\n\n" 
  "\t-P port\n"
  "\t\tSpecify port the server will use for multicast connections\n"
  "\t\tinstead of the default port (" NUMBER_TO_STRING(MULTICAST_PORT) ").\n\n" 
  "\t-c number\n"
  "\t\tSet limit to the maximum number of simultaneous connections.\n\n"
  "\t-b value\n"
  "\t\tSet limit for the total incoming bandwidth (suffix 'm' - \n"
  "\t\tmegabits/second, suffix 'M' - megabytes/second, \n"
  "\t\twithout suffix - bytes/second).\n\n"
  "\t-u UID\tChange UID, GID and home directory for the process.\n\n"
  "\t-U\tUnicast only (don't accept multicast connections).\n\n"
  "\t-m\tMulticast only (don't accept unicast connections).\n\n"
  "\t-d\tRun server in the debug mode (don't go to the background,\n"
  "\t\tlog messages to the standard out/error, and don't fork on\n"
  "\t\tincoming connection).\n");
  exit(EXIT_FAILURE);
}

// Wrapper for the FileWriter::session function, passed
// to pthread_create.
void *file_writer_routine(void *args)
{
  FileWriter *file_writer = (FileWriter *)args;
  file_writer->session();
  SDEBUG("File writer finished\n");
  return NULL;
}

// Wrapper for the UnicastSender::session function, passed
// to pthread_create.
void *unicast_sender_routine(void *args)
{
  UnicastSender *unicast_sender = (UnicastSender *)args;
  int retval;
  if ((retval = unicast_sender->session()) != 0) {
    DEBUG("Transmission failed, status: %d\n", retval);
  }

  SDEBUG("Unicast sender thread finished\n");
  return NULL;
}

// Wrapper for the MulticastSender::session function, passed
// to pthread_create.
void *multicast_sender_routine(void *args)
{
  SDEBUG("Start the multicast sender\n");
  MulticastSender *ms = (MulticastSender *)args;
  SDEBUG("Multicast sender finished\n");
  if (ms->session() != 0) {
    // Signal about an error
    return (void *)-1;
  }
  return NULL;
}

#ifndef NDEBUG
void sigpipe_handler(int signum)
{
  // It is wrong to call printf here
  DEBUG("sigpipe_handler: signal %d received\n", signum);
}
#endif

// The SIGCHLD handler, simply removes zombies
void sigchld_handler(int signum)
{
  // Remove zombies
  register int pid;
  while ((pid = waitpid(-1, NULL, WNOHANG)) > 0) {
    // Delete the closed multicast session
    set<MulticastConnection>::iterator i = find_if(multicast_sessions.begin(),
      multicast_sessions.end(), MulticastConnectionPidCompare(pid));
    if (i != multicast_sessions.end()) {
      if (i->is_in_time_wait) {
        ++n_connections;
      }
      multicast_sessions.erase(i);
    }
    --n_connections;

    // It is wrong to call printf here
    DEBUG("Child %d picked up\n", pid);
    DEBUG("Number of active connections: %lu.\n",
        (unsigned long)n_connections);
  };
}

// The SIGUSR1 handler, catches signals that have been sent by the 
// multicast receivers that moved to the time wait state
void sigusr1_sigaction(int signum, siginfo_t *info, void *uap)
{
  set<MulticastConnection>::iterator i = find_if(multicast_sessions.begin(),
    multicast_sessions.end(), MulticastConnectionPidCompare(info->si_pid));
  if (i != multicast_sessions.end()) {
    MulticastConnection mc = *i;
    multicast_sessions.erase(i++);
    mc.is_in_time_wait = true;
    multicast_sessions.insert(i, mc);
    DEBUG("Process %u entered to the time wait state\n", info->si_pid);
    --n_connections;
  }

  // It is wrong to call printf here
  DEBUG("Signal %u from %u received.\n", signum, info->si_pid);
  DEBUG("Number of active connections: %lu.\n", (unsigned long)n_connections);
}

// Send a UDP datagram
static inline int send_datagram(int sock, uint8_t *message, size_t size,
    const struct sockaddr_in *addr) {
  register int sendto_result;
  do {
    sendto_result = sendto(sock, message, size, 0, (struct sockaddr *)addr,
      sizeof(*addr));
  } while (sendto_result < 0 && errno == ENOBUFS);
  return sendto_result;
}

// Sends the multicast_init_reply message to 'destination_addr'
static int send_multicast_init_reply(int sock, uint32_t session_id,
    uint32_t number, const vector<uint32_t>& addresses,
    const struct sockaddr_in *destination_addr, unsigned delay)
{
  // Compose the reply message
  uint8_t reply_message[sizeof(MulticastMessageHeader) +
    addresses.size() * sizeof(uint32_t)];
  MulticastMessageHeader *reply_header =
    new(reply_message) MulticastMessageHeader(MULTICAST_INIT_REPLY, session_id);
  reply_header->set_number(number);
  uint32_t *p = (uint32_t *)(reply_header + 1);
  for (unsigned i = 0; i < addresses.size(); ++i) {
    p[i] = htonl(addresses[i]);
  }

  // Delay the response for some time
  // FIXME: Such delay will blocks the server for some time.
  // FIXME: Precision of usleep can be not enough. (it depends
  // on the tick length)
  if (delay > 0) {
    usleep(delay);
  }

  // Send the reply
  SDEBUG("Send the reply\n");
  if (send_datagram(sock, reply_message, sizeof(reply_message),
      destination_addr) < 0) {
    ERROR("Can't send the session initialization reply: %s\n",
      strerror(errno));
    return -1;
  }
  return 0;
}

// Sends the multicast STATUS_SERVER_IS_BUSY error message
static int send_multicast_error(uint8_t status, int sock,
    uint32_t session_id, uint32_t number,
    const struct sockaddr_in *destination_addr, uint32_t local_address)
{
  // Compose the reply message
  uint8_t error_message[sizeof(MulticastMessageHeader) + sizeof(ReplyHeader)];
  MulticastMessageHeader *mmh = new(error_message)
    MulticastMessageHeader(MULTICAST_ERROR_MESSAGE, session_id);
  mmh->set_number(number);

  new(mmh + 1) ReplyHeader(status, local_address, 0);

  // Send the error
  if (send_datagram(sock, error_message, sizeof(error_message),
      destination_addr) < 0) {
    // Some internal error occurred
    ERROR("Can't send an error: %s\n", strerror(errno));
    return -1;
  }
  return 0;
}

// Change UID, GID and home directory of the daemon
void change_user(const char *name)
{
  if (*name == '\0') {
    SERROR("Username passed to the -u option is empty\n");
    exit(EXIT_FAILURE);
  }
  free(homedir);
  uid = atoi(name);
  char *endptr;
  unsigned long n = strtoul(name, &endptr, 10);
  struct passwd* pw;
  errno = 0; // According to POSIX
  if (*endptr != '\0') {
    // The parameter specified as a string
    // Search for UID, GID and home directory in the passwd file
    pw = getpwnam(name);
    if (pw == NULL) {
      if (errno != 0) {
        ERROR("Can't get passwd entry for %s: %s\n", name,
          strerror(errno));
      } else {
        ERROR("No passwd entry for %s\n", name);
      }
      exit(EXIT_FAILURE);
    }
  } else {
    // The parameter specified as a number
    // Search for GID and home directory in the passwd file
    pw = getpwuid(n);
    if (pw == NULL) {
      if (errno != 0) {
        ERROR("Can't get passwd entry for uid %lu: %s\n", n, strerror(errno));
      } else {
        ERROR("No passwd entry for uid %lu\n", n);
      }
      exit(EXIT_FAILURE);
    }
  }

  uid = pw->pw_uid;
  gid = pw->pw_gid;
  homedir = strdup(pw->pw_dir);
  DEBUG("New UID: %u, new GID: %u, new home directory: %s\n", uid,
    gid, homedir);
}

int main(int argc, char **argv)
{
  // Server configurations
  in_addr_t address = htonl(INADDR_ANY);
  uint16_t unicast_port = UNICAST_PORT;
  uint16_t multicast_port = MULTICAST_PORT;
  bool debug_mode = false;
  bool unicast_only = false;
  bool multicast_only = false;
#ifdef USE_MULTICAST
  unsigned multicast_sender_number = 0;
#endif
  unsigned bandwidth = 0; // bandwidth (bytes per 1.048576 seconds)
  uid = getuid();
  gid = getgid();
  homedir = getenv("HOME");
  if (homedir == NULL) {
    ERROR("Can't get the home directory: %s\n", strerror(errno));
    homedir = (char *)"/";
  }
  homedir = strdup(homedir);

#ifdef NDEBUG
  openlog(argv[0], LOG_PID, LOG_DAEMON);
#endif

  // Set speed for the pseudo-random numbers
  struct timeval tv;
  gettimeofday(&tv, NULL);
  srand((tv.tv_sec << 8) + getpid() % (1 << 8));

  // Parse the command line options
  int ch;
  while ((ch = getopt(argc, argv, "a:p:P:b:c:u:Umdh")) != -1 ) {
    switch (ch) {
      case 'a': // Address specified
        if ((address = inet_addr(optarg)) == INADDR_NONE) {
          ERROR("Invalid address: %s\n", optarg);
          exit(EXIT_FAILURE);
        }
        break;
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
        multicast_port = atoi(optarg);
        if (multicast_port == 0) {
          ERROR("Invalid port: %s\n", optarg);
          exit(EXIT_FAILURE);
        }
        break;
      case 'b': // Limit the receiver's bandwidth
        bandwidth = atoi(optarg);
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
        DEBUG("Bandwidth (in bytes per 1.048576 seconds): %u\n", bandwidth);
        break;
      case 'c': // Limit the maximum number of simultaneous
          // connections allowed
        max_n_connections = atoi(optarg);
        if (max_n_connections == 0) {
          ERROR("Invalid value of the argument '-c': %s\n", optarg);
          exit(EXIT_FAILURE);
        }
        break;
      case 'u': // Change UID, GID and home directory for the daemon
        change_user(optarg);
        break;
      case 'U': // Don't accept the multicast connections
        unicast_only = true;
        break;
      case 'm': // Don't accept the unicast connections
        multicast_only = true;
        break;
      case 'd': // Debug mode: don't go to the background, perform a
        // simple server rather that forked
        debug_mode = true;
        break;
      default:
        usage_and_exit(argv[0]);
    }
  }
        
  if (gid != getgid() && setresgid(gid, gid, gid) != 0) {
    ERROR("Can't change GID to %u: %s\n", gid, strerror(errno));
    exit(EXIT_FAILURE);
  }
  if (uid != getuid() && setresuid(uid, uid, uid) != 0) {
    ERROR("Can't change UID to %u: %s\n", uid, strerror(errno));
    exit(EXIT_FAILURE);
  }
  if (!debug_mode && chdir(homedir) != 0) {
    ERROR("Can't proceed to the home directory %s: %s\n", homedir,
      strerror(errno));
  }

  if (multicast_only && unicast_only) {
    SERROR("The multicast only and the unicast only modes can't be choosen "
      "simultaneously\n");
    exit(EXIT_FAILURE);
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
  sigchld_action.sa_handler = sigchld_handler;
  sigchld_action.sa_flags = SA_RESTART;
  sigemptyset(&sigchld_action.sa_mask);
  sigaddset(&sigchld_action.sa_mask, SIGUSR1);
  if (sigaction(SIGCHLD, &sigchld_action, NULL)) {
    ERROR("sigaction: %s\n", strerror(errno));
    exit(EXIT_FAILURE);
  }

  struct sigaction sigusr1_action;
  sigusr1_action.sa_sigaction = sigusr1_sigaction;
  sigusr1_action.sa_flags = SA_RESTART | SA_SIGINFO;
  sigemptyset(&sigusr1_action.sa_mask);
  sigaddset(&sigusr1_action.sa_mask, SIGCHLD);
  if (sigaction(SIGUSR1, &sigusr1_action, NULL)) {
    ERROR("sigaction: %s\n", strerror(errno));
    exit(EXIT_FAILURE);
  }

  sigset_t multicast_sessions_changers;
  sigemptyset(&multicast_sessions_changers);
  sigaddset(&multicast_sessions_changers, SIGCHLD);
  sigaddset(&multicast_sessions_changers, SIGUSR1);

  int unicast_sock = -1;
  unsigned n_multicast_sockets = 0;
  int *multicast_sockets = NULL;
  struct pollfd *pfds;
  nfds_t n_pfds;
  vector<uint32_t> addresses; // Local IP addresses (non-loopback)

#ifdef USE_MULTICAST
  if (!unicast_only) {
    // Some initialization routine to accept multicast connections

    // Get the local ip addresses
    int sock;
    sock = socket(PF_INET, SOCK_DGRAM, 0);
    if (sock == -1) {
      ERROR("Can't create a UDP socket: %s\n", strerror(errno));
      exit(EXIT_FAILURE);
    }
    if (get_local_addresses(sock, &addresses, NULL) != 0) {
      exit(EXIT_FAILURE);
    }
    close(sock);

    n_multicast_sockets = addresses.size();
    multicast_sockets = new int[n_multicast_sockets];
    n_pfds = n_multicast_sockets + (multicast_only ? 0 : 1);
    pfds = new pollfd[n_pfds];
  
    // Create and prepare the UDP sockets
    memset(pfds, 0, sizeof(pollfd) * (n_pfds));
    for (unsigned i = 0; i < n_multicast_sockets; ++i) {
      multicast_sockets[i] = socket(PF_INET, SOCK_DGRAM, 0);
      if (multicast_sockets[i] == -1) {
        ERROR("Can't create a UDP socket: %s\n", strerror(errno));
        exit(EXIT_FAILURE);
      }
  
      const int on = 1;
#ifdef linux
      if (setsockopt(multicast_sockets[i], SOL_SOCKET, SO_REUSEADDR, &on,
          sizeof(on)) != 0) {
        ERROR("Can't set the SO_REUSEADDR option to the socket: %s\n",
          strerror(errno));
        exit(EXIT_FAILURE);
      }
#else
      if (setsockopt(multicast_sockets[i], SOL_SOCKET, SO_REUSEPORT, &on,
          sizeof(on)) != 0) {
        ERROR("Can't set the SO_REUSEPORT option to the socket: %s\n",
          strerror(errno));
        exit(EXIT_FAILURE);
      }
#endif

      // Bind UDP socket
      struct sockaddr_in servaddr;
      memset(&servaddr, 0, sizeof(servaddr));
      servaddr.sin_family = AF_INET;
      servaddr.sin_addr.s_addr = htonl(INADDR_ANY);
      servaddr.sin_port = htons(multicast_port);
    
      if (bind(multicast_sockets[i], (struct sockaddr *) &servaddr,
          sizeof(servaddr)) != 0) {
        ERROR("Can't bind a UDP socket: %s\n", strerror(errno));
        exit(EXIT_FAILURE);
      }
  
      // Join socket to the multicast group
      struct ip_mreq mreq;
      struct in_addr maddr;
      maddr.s_addr = inet_addr(DEFAULT_MULTICAST_ADDR);
      memcpy(&mreq.imr_multiaddr, &maddr, sizeof(maddr));
      mreq.imr_interface.s_addr = htonl(addresses[i]);
  
      if (setsockopt(multicast_sockets[i], IPPROTO_IP, IP_ADD_MEMBERSHIP, &mreq,
          sizeof(mreq)) != 0) {
        ERROR("Can't join the multicast group " DEFAULT_MULTICAST_ADDR ": %s\n",
          strerror(errno));
        exit(EXIT_FAILURE);
      }
  
      pfds[i].fd = multicast_sockets[i];
      pfds[i].events = POLLIN;
    }
  } else {
#endif
    n_pfds = 1;
    pfds = new pollfd;
    memset(pfds, 0, sizeof(pollfd));
#ifdef USE_MULTICAST
  }
#endif

  // Some initialization routine for the unicast connection
  if (!multicast_only) {
    // Creates the socket to listen
    unicast_sock = socket(PF_INET, SOCK_STREAM, 0);
    if (unicast_sock < 0) {
      ERROR("Can't create socket: %s\n", strerror(errno));
      exit(EXIT_FAILURE);
    }

    pfds[n_multicast_sockets].fd = unicast_sock;
    pfds[n_multicast_sockets].events = POLLIN | POLLPRI;
  
    int on = 1;
    if (setsockopt(unicast_sock, SOL_SOCKET, SO_REUSEADDR, &on,
        sizeof(on)) != 0) {
      ERROR("Can't set the SO_REUSEADDR socket option: %s\n",
        strerror(errno));
      exit(EXIT_FAILURE);
    }
  
    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = address;
    server_addr.sin_port = htons(unicast_port);
    if (bind(unicast_sock, (struct sockaddr *)&server_addr,
        sizeof(server_addr)) != 0) {
      perror("Can't bind the socket");
      exit(EXIT_FAILURE);
    }
    if (listen(unicast_sock, 5) != 0) {
      perror("Can't bind the socket");
      exit(EXIT_FAILURE);
    }
  }
#ifndef USE_MULTICAST
  else {
    SERROR("The mcpd daemon was built without the multicast support. "
      "You can't use the -m option.\n");
    exit(EXIT_FAILURE);
  }
#endif

  // Become a daemon process and proceeed to the home directory
  if (!debug_mode) {
    int daemon_result;
#ifdef NDEBUG
    daemon_result = daemon(1, 0);
#else
    daemon_result = daemon(1, 1);
#endif
    if (daemon_result != 0) {
      ERROR("daemon(3) error: %s\n", strerror(errno));
      exit(EXIT_FAILURE);
    }
  }

  // Start the multicast receiver
  char *udp_buffer = NULL;
  struct sockaddr_in multicast_sender_addr;
  socklen_t multicast_sender_addr_size = sizeof(multicast_sender_addr);
  struct sockaddr_in unicast_sender_addr;
  socklen_t unicast_sender_addr_size = sizeof(unicast_sender_addr);
  if (!unicast_only) {
    udp_buffer = (char *)malloc(UDP_MAX_LENGTH);
  }

  // Initialize the shared memory to implement the incoming bandwidth
  // limitation
  void *shmem = mmap(NULL, sizeof(unsigned), PROT_READ | PROT_WRITE,
    MAP_ANON | MAP_SHARED, -1, 0);
  if (shmem == MAP_FAILED) {
    ERROR("Can't map a shared memory: %s\n", strerror(errno));
    exit(EXIT_FAILURE);
  }

  // The main routine of the server
  // Wait for incoming connections in the infinite loop
  while (1) {
    while (poll(pfds, n_pfds, -1) <= 0) {
      if (errno != EINTR) {
        ERROR("poll error: %s\n", strerror(errno));
        exit(EXIT_FAILURE);
      }
    }
    // Get the number of the socket ready for read
    unsigned sock_num;
    for (sock_num = 0; sock_num < n_multicast_sockets; ++sock_num) {
      if ((pfds[sock_num].revents & POLLIN) != 0) {
        break;
      }
    }
    if (sock_num != n_multicast_sockets) {
      // A multicast connection available
      unsigned reply_delay = 0; // Time (in milliseconds) the multicast reply
        // will be delayed
      int len = recvfrom(multicast_sockets[sock_num], udp_buffer,
        UDP_MAX_LENGTH, 0, (struct sockaddr *)&multicast_sender_addr,
        &multicast_sender_addr_size);
      if (len > 0) {
        MulticastMessageHeader *mmh = (MulticastMessageHeader *)udp_buffer;
        if (mmh->get_message_type() != MULTICAST_INIT_REQUEST) {
          SDEBUG("Unexpected datargam received\n");
          continue;
        }

        MulticastInitData *mid = (MulticastInitData *)(mmh + 1);
        if (!mid->check_version()) {
          ERROR("Incorrect protocol version %u in the Multicast Init Message, "
            "current version is %u\n", (unsigned)mid->get_version(),
            UNICAST_VERSION_AND_UNUSED >> 24);
          continue;
        }
        uint32_t ephemeral_addr = mid->get_ephemeral_address();
#ifndef NDEBUG
        char ephemeral_address[INET_ADDRSTRLEN];
        uint32_t e_addr = htonl(ephemeral_addr);
        DEBUG("Ephemeral address: %s\n",
          inet_ntop(AF_INET, &e_addr, ephemeral_address,
          sizeof(ephemeral_address)));
#endif

        uint16_t ephemeral_port = ntohs(multicast_sender_addr.sin_port);
        struct sockaddr_in source_addr = multicast_sender_addr;
        source_addr.sin_port = htons(ephemeral_port);
#ifndef NDEBUG
        char cl_addr[INET_ADDRSTRLEN];
        DEBUG("Received %zu (%u) destinations from %s, port: %u\n",
          (len - sizeof(MulticastMessageHeader) - sizeof(MulticastInitData)) /
          sizeof(MulticastHostRecord), len,
          inet_ntop(AF_INET, &multicast_sender_addr.sin_addr, cl_addr,
          sizeof(cl_addr)), ephemeral_port);
#endif
        // Look for the matching addresses
        MulticastHostRecord *hr = (MulticastHostRecord *)(mid + 1);
        vector<uint32_t> matches;
        uint32_t local_address = 0;
        for(unsigned i = 0; i < addresses.size(); ++i) {
          for (unsigned j = 0; j < (len - sizeof(MulticastMessageHeader) -
              sizeof(MulticastInitData)) / sizeof(MulticastHostRecord); ++j) {
            if (hr[j].get_addr() == addresses[i]) {
              matches.push_back(hr[j].get_addr());
              if (local_address == 0) {
                local_address = hr[j].get_addr();
              }
              SDEBUG("Match!\n");
              // FIXME: use another constants here, this is just to
              // highlight the idea of delaying
              unsigned last_dst = (len - sizeof(MulticastMessageHeader)) /
                sizeof(MulticastHostRecord) - 1;
              reply_delay  = (100 * (last_dst - j)) / (last_dst + 1);
              break;
            }
          }
        }
  
        if (matches.size() > 0) {
          // At least one of the addresses found, establish connection.
          sigprocmask(SIG_BLOCK, &multicast_sessions_changers, NULL);
          set<MulticastConnection>::iterator established_session =
            multicast_sessions.find(MulticastConnection(
            multicast_sender_addr.sin_addr.s_addr, mmh->get_session_id(), 0));
          
          if (established_session == multicast_sessions.end() ||
              established_session->is_in_time_wait) {
            sigprocmask(SIG_UNBLOCK, &multicast_sessions_changers, NULL);
            // Connection has not been already established for this session
            if (max_n_connections > 0 &&
                n_connections >= max_n_connections) {
              // Server is busy
              send_multicast_error(STATUS_SERVER_IS_BUSY,
                multicast_sockets[sock_num], mmh->get_session_id(),
                mmh->get_number(), &source_addr, local_address);
              continue;
            }
            // Try availability of the ephemeral port
            int ephemeral_sock;
            ephemeral_sock = socket(PF_INET, SOCK_DGRAM, 0);
            if (ephemeral_sock == -1) {
              ERROR("Can't create a UDP socket: %s\n", strerror(errno));
              exit(EXIT_FAILURE);
            }
            DEBUG("Ephemeral socket %d created\n", ephemeral_sock);
            struct sockaddr_in l_addr;
            memset(&l_addr, 0, sizeof(l_addr));
            // TODO: Cleverly choose the address that will be used
            // in the conneciton
            l_addr.sin_family = AF_INET;
            l_addr.sin_addr.s_addr = htonl(INADDR_ANY);
            l_addr.sin_port = htons(ephemeral_port);

            if (bind(ephemeral_sock, (struct sockaddr *)&l_addr,
                sizeof(l_addr)) != 0) {
              if (errno == EADDRINUSE) {
                // TODO: send apropriate error message to the source
                DEBUG("Port %u is already in use\n", ephemeral_port);
                close(ephemeral_sock);
                send_multicast_error(STATUS_PORT_IN_USE,
                  multicast_sockets[sock_num], mmh->get_session_id(),
                  mmh->get_number(), &source_addr, local_address);
                continue;
              } else {
                ERROR("Can't bind a UDP socket: %s\n", strerror(errno));
                close(ephemeral_sock);
                exit(EXIT_FAILURE);
              }
            }
            if (send_multicast_init_reply(multicast_sockets[sock_num],
                mmh->get_session_id(), mmh->get_number(), matches,
                &source_addr, reply_delay) == 0) {
              // Create a new process, that will handle this connection
              pid_t pid = fork();
              if (pid == 0) {
                for (unsigned i = 0; i < n_multicast_sockets; ++i) {
                  close(multicast_sockets[i]);
                }
                close(unicast_sock);
                // Create a MulticastReceiver object
                MulticastReceiver *multicast_receiver = new MulticastReceiver(
                  ephemeral_sock, shmem, ephemeral_addr, source_addr,
                  mmh->get_session_id(),
                  local_address, addresses[sock_num], bandwidth);
                int status = multicast_receiver->session();
                // finish the work
                delete multicast_receiver;
                DEBUG("Process %u finished\n", getpid());
                exit(status);
              } else if (pid > 0) {
                // Session established, continue work
                ++n_connections;
                close(ephemeral_sock);
                sigprocmask(SIG_BLOCK, &multicast_sessions_changers, NULL);
                multicast_sessions.insert(MulticastConnection(
                  multicast_sender_addr.sin_addr.s_addr,
                  mmh->get_session_id(), pid));
                sigprocmask(SIG_UNBLOCK, &multicast_sessions_changers, NULL);
              } else {
                ERROR("fork returned the error: %s\n", strerror(errno));
                exit(EXIT_FAILURE);
              }
            } else {
              close(ephemeral_sock);
            }
          } else {
            DEBUG("Session already established (number of sessions: %zu).\n",
              multicast_sessions.size());
            sigprocmask(SIG_UNBLOCK, &multicast_sessions_changers, NULL);
            send_multicast_init_reply(multicast_sockets[sock_num],
              mmh->get_session_id(), mmh->get_number(), matches,
              &source_addr, reply_delay);
          }
        }
      } else if(len != 0) {
        ERROR("recvfrom error: %s\n", strerror(errno));
        exit(EXIT_FAILURE);
      }
    } else {
      // A unicast connection available
      int client_sock = accept(unicast_sock,
        (struct sockaddr *)&unicast_sender_addr, &unicast_sender_addr_size);
      if (client_sock < 0) {
        ERROR("accept call failed: %s\n", strerror(errno));
        exit(EXIT_FAILURE);
      } else {
        if (max_n_connections > 0 && n_connections >= max_n_connections) {
          // Limit to the number of the allowed connections reached.
          try {
            send_server_is_busy(client_sock,
              ntohl(unicast_sender_addr.sin_addr.s_addr));
          } catch(ConnectionException& e) {
            ERROR("Can't send a message to the immediate source: %s\n",
              e.what());
            // The TCP connection is broken. We can't deliver the error to
            // the source.
          }
          close(client_sock);
          continue;
        }
        if (!debug_mode) {
          // Implement the forked server
          pid_t pid = fork();
          if (pid > 0) {
            ++n_connections;
            // Parent process
            close(client_sock);
            // Return to the accept call
            continue;
          } else if (pid == 0) {
            for (unsigned i = 0; i < n_multicast_sockets; ++i) {
              close(multicast_sockets[i]);
            }
            close(unicast_sock);

            // Set the random number generator's seed
            struct timeval tv;
            gettimeofday(&tv, NULL);
            srand((tv.tv_sec << 8) + getpid() % (1 << 8));

            // execute the main routine
          } else {
            ERROR("fork: %s\n", strerror(errno));
            exit(EXIT_FAILURE);
          }
        }
#ifndef NDEBUG
        char iaddr[INET_ADDRSTRLEN];
        DEBUG("Received connection from: %s\n",
          inet_ntop(AF_INET, &unicast_sender_addr.sin_addr.s_addr, iaddr,
          sizeof(iaddr)));
#endif
        // Session initialization
        // create and initialize the unicast receiver
        UnicastReceiver *unicast_receiver = new UnicastReceiver(shmem,
          bandwidth);
        UnicastSender *unicast_sender = NULL;
        MulticastSender *multicast_sender = NULL;
        pthread_t file_writer_thread;
        pthread_t unicast_sender_thread;
        pthread_t multicast_sender_thread;
        bool is_unicast_sender_started = false;
        bool is_multicast_sender_started = false;

        if (unicast_receiver->session_init(client_sock) != 0) {
          SERROR("Can't get the initial data from the server\n");
          unicast_receiver->send_errors(client_sock);
          delete unicast_receiver;
          if (!debug_mode) {
            return EXIT_FAILURE;
          } else {
            continue;
          }
        }

        const vector<Destination> *remaining_dst;
        try {
#ifdef USE_MULTICAST
          if ((unicast_receiver->flags & UNICAST_ONLY_FLAG) == 0) {
            multicast_sender = MulticastSender::create_and_initialize(
              unicast_receiver->destinations, &remaining_dst,
              unicast_receiver->n_sources,
              false, // is_multicast_only
              false, // use_global_multicast
              INADDR_NONE, // interface for global multicast traffic
              unicast_receiver,
              MulticastSender::server_mode, multicast_port,
              0 /* bandwidth */,
              false, // use_fixed_rate_multicast
              multicast_sender_number);
            ++multicast_sender_number;
            if (remaining_dst == NULL) {
              // Some error occurred during the multicast sender initialization
              unicast_receiver->send_errors(client_sock);
              delete unicast_receiver;
              if (!debug_mode) {
                return EXIT_FAILURE;
              } else {
                continue;
              }
            }
          } else {
            remaining_dst = &unicast_receiver->destinations;
          }

          // FIXME: some other evaluation should done be here
          if (remaining_dst->size() < unicast_receiver->destinations.size()) {
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
#else
          remaining_dst = &unicast_receiver->destinations;
#endif
  
          // Run the unicast sender sender
          if (remaining_dst->size() > 0) {
            is_unicast_sender_started = true;
            unicast_sender = new UnicastSender(unicast_receiver,
              UnicastSender::server_mode, unicast_port,
              unicast_receiver->flags, 0 /* bandwidth */);
            SDEBUG("Initialize the unicast sender thread\n");
            int retval;
            retval = unicast_sender->session_init(*remaining_dst,
              unicast_receiver->n_sources);
            if (retval == STATUS_OK) {
              // Start the unicast sender
              int error;
              error = pthread_create(&unicast_sender_thread, NULL,
                unicast_sender_routine, unicast_sender);
              if (error != 0) {
                ERROR("Can't create a new thread: %s\n",
                  strerror(errno));
                exit(EXIT_FAILURE);
              }
            } else {
              // An error occurred during the unicast session initialization.
              // About this error will be reported later
              SDEBUG("Session initialization failed\n");
              unicast_receiver->send_errors(client_sock);
              delete multicast_sender;
              delete unicast_sender;
              delete unicast_receiver;
              if (!debug_mode) {
                return EXIT_FAILURE;
              } else {
                continue;
              }
            }
          }
        } catch (ConnectionException& e) {
          ERROR("Can't send a message to the immediate source: %s\n", e.what());
          // The TCP connection is broken.  All we can do, just silently exit.
          delete multicast_sender;
          delete unicast_sender;
          delete unicast_receiver;

          if (!debug_mode) {
            return EXIT_FAILURE;
          } else {
            continue;
          }
        }

        FileWriter *file_writer = new FileWriter(unicast_receiver,
          unicast_receiver->flags); 
  
        // Start the file writer thread
        file_writer->init(unicast_receiver->path, unicast_receiver->path_type,
          unicast_receiver->n_sources); 
        int error;
        error = pthread_create(&file_writer_thread, NULL,
          file_writer_routine, (void *)file_writer);
        if (error != 0) {
          ERROR("Can't create a new thread: %s\n", strerror(errno));
          return EXIT_FAILURE;
        }
  
        // Start the main routine (read files and directories and pass them
        // to the distributor)
        int unicast_receiver_session_result = unicast_receiver->session();
  
        pthread_join(file_writer_thread, NULL);
        if (is_unicast_sender_started) {
          pthread_join(unicast_sender_thread, NULL);
        }
        if (is_multicast_sender_started) {
          pthread_join(multicast_sender_thread, NULL);
        }
  
        SDEBUG("Session finished, terminate the server\n");
        if (remaining_dst != &unicast_receiver->destinations) {
          delete remaining_dst;
        }
        delete file_writer;
        delete unicast_receiver;
        delete unicast_sender;
        delete multicast_sender;
        if (!debug_mode) {
          return unicast_receiver_session_result;
        } else {
          continue;
        }
      }
    }
  }
}
