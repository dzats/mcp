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
class MulticastSender : public Writer
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
        number == second.number && from < second.from;
    }
    bool operator==(const ErrorMessage& second) const
    {
      return number == second.number &&
        from == second.from;
    }
  };

  static const unsigned MAX_INITIALIZATION_RETRIES = 9;
  static const unsigned INIT_RETRANSMISSION_RATE = 20000; // retransmission
    // rate of the session initialization message (in microseconds)
  static const unsigned DEFAULT_TERMINATE_RETRANSMISSION_RATE = 200000000;
    // retransmission rate of the termination request message
    // (in nanoseconds)
  static const unsigned MAX_ERROR_QUEUE_SIZE_MULTIPLICATOR = 4; // Max size
    // of the error queue / number of targets

  static const unsigned MAX_NUMBER_OF_TERMINATION_RETRANS = 16; // Max number
    // of the multicast session termination retry messages to be send

  static const unsigned MAX_PORT_CHOOSING_TRIES = 10;

  Mode mode; // Whether the UnicastSender object is used by the client (mcp)
    // tool or by the server mcpd tool

  int sock; // Socket used for multicast connection
  struct sockaddr_in target_address; // address used for multicast connection

  uint32_t address; // Multicast address that will be used in connection
  uint16_t port; // port that will be used for multicast connections

  uint32_t session_id; // Multicast session ID

  MulticastSendQueue *send_queue; // queue of the send messages, used to
    // provide reliability
  unsigned next_message; // number of the next message
  unsigned next_responder; // ordinal number of the next responder

  // Received errors and file retransmission requests (used for protection
  // against replies of such messages).
  std::set<ErrorMessage> received_errors;

  uint32_t nsources; // number of the specified sources, used to detect
    // the target path
  std::vector<Destination> targets;
public:

  MulticastSender(Reader* b, Mode m, uint16_t p, uint32_t n_sources,
      unsigned n_retrans) : Writer(b, (Reader::Client *)&b->multicast_sender),
      mode(m), sock(-1), port(p), send_queue(NULL), next_message(0),
      next_responder(0), nsources(n_sources)
  {
    address = inet_addr(DEFAULT_MULTICAST_ADDR);
    session_id = getpid() + ((n_retrans & 0xFF) << 24);
  }
  ~MulticastSender()
  {
    if (sock != -1) { close(sock); }
    if (send_queue != NULL) { delete send_queue; }
  }
  /*
    This is the initialization routine tries to establish the multicast
    session with the destinations specified in dst. The return value
    is a vector of destinations the connection has not been established
    with.
  */
  std::vector<Destination>* session_init(const std::vector<Destination>& dst,
      int nsources);

  /*
    This is the main routine of the multicast sender. It sends
    files and directories to destinations.
  */
  int session();

private:
  // A helper function that chooses a UDP port and binds socket to it
  uint16_t choose_ephemeral_port();
  // A helper fuction which reliably sends message to the multicast connection
  void mcast_send(const void *message, int size);
  // A helper function which sends file to the multicast destinations
  void send_file();

  // Routine that controls the multicast packets delivery,
  // should be started in a separate thread
  void multicast_delivery_control();
  // A wrapper function for multicast_delivery_control
  static void* multicast_delivery_thread(void *arg);

  // helper fuction that sends 'message' to the udp socket
  void udp_send(const void *message, int size, int flags);

  // Register an error and finish the current task
  void register_error(uint8_t status, const char *fmt, const char *error);
};
#endif
