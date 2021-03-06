#ifndef MULTICAST_SENDER_H_HEADER
#define MULTICAST_SENDER_H_HEADER 1
#include <netinet/in.h>
#include <arpa/inet.h>

#include <sys/time.h> // for struct timeval

#include <vector>
#include <set>

#include "destination.h"
#include "writer.h"
#include "connection.h"

#include "multicast_send_queue.h"

// Objects that sends files to the multicast destinations
class MulticastSender : private Writer
{
public:
  enum Mode {server_mode, client_mode};
private:
  struct ErrorMessage
  {
    uint32_t number;
    uint32_t from;
    struct timeval timestamp;
  
    ErrorMessage(uint32_t num, uint32_t f) : number(num), from(f) {}
    ErrorMessage(uint32_t num, uint32_t f, const timeval& ts) : number(num),
        from(f), timestamp(ts) {}

    bool operator<(const ErrorMessage& second) const
    {
      return number < second.number ||
        (number == second.number && from < second.from);
    }
    bool operator==(const ErrorMessage& second) const
    {
      return number == second.number &&
        from == second.from;
    }
  };

  static const unsigned MAX_INITIALIZATION_RETRIES = 9;
  static const unsigned MAX_UNREPLID_INIT_RETRIES = 3;
  static const unsigned MAX_ABNORMAL_TERMINATION_RETRIES = 3;
  static const unsigned INIT_RETRANSMISSION_RATE = 200000; // rate for the
    // session initialization message (in microseconds)
  static const unsigned DEFAULT_TERMINATE_RETRANSMISSION_RATE = 200000000;
    // retransmission rate of the termination request message
    // (in nanoseconds)
  static const unsigned DEFAULT_ROUND_TRIP_TIME = 40000; // (in milliseconds)
  static const unsigned MAX_ERROR_QUEUE_SIZE_MULTIPLICATOR = 4; // Max size
    // of the error queue / number of targets
  static const unsigned MAX_NUMBER_OF_TERMINATION_RETRANS = 32; // Max number
    // of the multicast session termination retry messages to be send
  static const unsigned MAX_RETRANSMISSION_TIMEOUT = 4; // (in seconds)
    // Maximum timeout between two successive retransmissions of the
    // MULTICAST_TERMINATION_REQUEST message
  static const unsigned MAX_PORT_CHOOSING_TRIES = 100;
  static const unsigned EPHEMERAL_PORT_CHOOSING_STEP = 111;
  static const unsigned MAX_REMOTE_PORT_TRIES = 11; // Maximum amount of
    // attempts to initialize a multicast session when some of the destinations
    // returns STATUS_PORT_IN_USE.

  Mode mode; // Whether the UnicastSender object is used by the client (mcp)
    // tool or by the server mcpd tool

  int sock; // Socket used for multicast connection
  uint32_t local_address; // Local IP address of the interface used for
    // multicast connection
  struct sockaddr_in target_address; // address used for multicast connection
  unsigned bandwidth; // Bandwidth limit for this particular sender
  bool use_fixed_rate_multicast; // Don't use any congestion control
    // technique, instead send multicast traffic with constant rate

  int64_t allowed_to_send; // auxilary variable used to
    // implement the bandwidth limitation
  struct timeval bandwidth_estimation_timestamp; // auxilary variable used to
    // implement the bandwidth limitation

  uint32_t address; // Multicast address that will be used in the connection
  uint16_t port; // port that will be used for multicast connections

  uint32_t session_id; // Multicast session ID

  MulticastSendQueue *send_queue; // queue of the send messages, used to
    // provide reliability
  unsigned next_message; // number of the next message
  unsigned next_responder; // ordinal number of the next responder
#ifndef NDEBUG
  unsigned total_bytes_sent; // Total number of bytes that have been send
#endif

  // Received errors and file retransmission requests (used for protection
  // against replies of such messages).
  std::set<ErrorMessage> received_errors;

  uint32_t n_sources; // number of the specified sources, used to detect
    // the target path
  std::vector<Destination> targets;

  MulticastSender(Reader* b, Mode m, uint16_t p, uint32_t n_sources,
      unsigned n_retrans, unsigned bw) :
      Writer(b, (Reader::Client *)&b->multicast_sender),
      mode(m),
      sock(-1),
      bandwidth(bw),
      port(p),
      send_queue(NULL),
      next_message(0),
      next_responder(0),
      n_sources(n_sources)
  {
    address = inet_addr(DEFAULT_MULTICAST_ADDR);
    session_id = getpid() + ((n_retrans & 0xFF) << 24);
#ifndef NDEBUG
    total_bytes_sent = 0; // Total number of bytes that have been send
#endif
  }

  /*
    This initialization routine tries to establish a multicast
    session with the destinations specified in dst.
    The return value is 0 on success, -1 in the case of local error and
    error status received in the reply if some remote error occurred.
  */
  int session_init(
    uint32_t local_addr,
    const std::vector<Destination>& dst,
    bool use_global_multicast,
    bool use_fixed_rate_multicast);
public:

  ~MulticastSender()
  {
    if (sock != -1) { close(sock); }
    delete send_queue;
  }

  /*
    This routine analyses targets, then creates and initializes
    multicast sender for link-local targets, if it is reasonable
    in the particular case.  If some error occurred remaining_dst
    is set to NULL.
  */
  static MulticastSender *create_and_initialize(
    const std::vector<Destination>& all_destinations,
    const std::vector<Destination> **remaining_dst,
    uint32_t n_sources,
    bool is_multicast_only,
    bool use_global_multicast,
    uint32_t multicast_interface,
    Reader *reader,
    Mode mode,
    uint16_t multicast_port,
    unsigned bandwidth,
    bool use_fixed_rate_multicast,
    unsigned n_retransmissions);

  /*
    This is the main routine of the multicast sender. It sends
    files and directories to destinations.
  */
  int session();

  // Abnormal multicast connection termination
  void abnormal_termination();

private:
  // A helper function that chooses a UDP port and binds socket to it
  uint16_t choose_ephemeral_port(bool use_global_multicast);

  // Helper fuction that sends 'message' to the udp socket.
  void udp_send(const void *message, int size, int flags);

  // Helper fuction which reliably sends message to the multicast connection.
  void mcast_send(const void *message, int size);

  // A helper function which sends file to the multicast destinations
  void send_file();

  // Routine that controls the multicast packets delivery,
  // should be started in a separate thread
  void multicast_delivery_control();
  // A wrapper function for multicast_delivery_control
  static void* multicast_delivery_control_wrapper(void *arg);

  // Register an error and finish the current task
  void register_error(uint8_t status, const char *fmt, const char *error);
};
#endif
