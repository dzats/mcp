#ifndef MULTICAST_RECEIVER_H_HEADER
#define MULTICAST_RECEIVER_H_HEADER 1

#include <netinet/in.h>
#include <arpa/inet.h>

#include <sys/time.h>

#include <list>

#include "path.h"
#include "md5.h"
#include "multicast_recv_queue.h"
#include "multicast_error_queue.h"

// The multicast receiver object (one per multicast connection)
class MulticastReceiver
{
  static const unsigned MIN_ERROR_RETRANS_TIMEOUT = 20; // milliseconds
  static const unsigned MAX_ERROR_RETRANS_TIMEOUT = 1000; // milliseconds
  static const unsigned MAX_ERROR_RETRANS_LINEAR_ADD = 8; // milliseconds
#ifdef NDEBUG
  static const unsigned MAX_IDLE_TIME = 30000; // milliseconds
#else
  static const unsigned MAX_IDLE_TIME = 8000; // milliseconds
#endif

  int sock; // Socket used for connection
  volatile unsigned *bytes_received; // number of bytes received (from all
    // the clients)
  unsigned bandwidth; // total incoming bandwidth (for all the clients)

  const struct sockaddr_in& source_addr; // Address of the multicast sender
  uint32_t session_id; // id of the multicast sender
  uint32_t local_address; // local IP address which the session establish with
  uint32_t interface_address; // Address of the interface used for multicast
    // connection

  char *path; // local destination path
  PathType path_type;
  uint32_t n_sources; // number of sources

  in_addr_t own_addr;
  std::set<uint32_t> missed_packets;

  MulticastRecvQueue message_queue;
  MulticastErrorQueue error_queue;
  
  unsigned next_message_expected;

  int fd; // descriptor of the currently opened file
  FileInfoHeader file_info_header; // information for the current file/directory
  char *filename; // name of the current file/directory
  MD5sum checksum; // checksum for files

  // Register an error to be delivered
  void register_error(uint8_t status, const char *fmt, ...);

  // Get next timeout value for an error message retransmission
  unsigned get_error_retrans_timeout(unsigned prev_timeout);

  // Sends a UDP datagram to the multicast sender
  // Returns zero on success and error code otherwise
  int send_datagram(void *data, size_t size);

  // Send a reply for the packet 'number' to the source
  // Returns zero on success and error code otherwise.
  int send_reply(uint32_t number);

  // Sends error message with information about packets
  // that have not been received.
  // Returns zero on success and error code otherwise.
  int send_missed_packets(uint32_t message_number);

  // Send appropriate reply to the MULTICAST_TERMINATION_REQUEST or
  // MULTICAST_ABNORMAL_TERMINATION_REQUEST messages
  void reply_to_a_termination_request(const MulticastMessageHeader *mmh,
    int len, uint8_t * const buffer);

  // This method implements something like the TCP's TIME_WAIT state. It waits
  // for possible retransmissions of the MULTICAST_TERMINATION_REQUEST message.
  void time_wait(uint8_t * const buffer);

  // Finish the multicast receiver process if there were no messages from
  // the multicast sender during MAX_IDLE_TIME
  void exit_on_timeout_from_read_messages();

  // This routine reads messages from the connection and put it into the queue
  void read_messages();

  // Wrapper function to run the read_messages method in a separate thread
  static void* read_messages_wrapper(void *arg);
public:
  MulticastReceiver(int s,
      void *shared_mem,
      const struct sockaddr_in& saddr,
      uint32_t sid,
      uint32_t local_addr,
      uint32_t interface_addr,
      unsigned bw) :
      sock(s), bandwidth(bw), source_addr(saddr), session_id(sid),
      local_address(local_addr), interface_address(interface_addr),
      path(NULL), n_sources(0), next_message_expected(0), fd(-1),
      filename(NULL) {
    bytes_received = (volatile unsigned *)shared_mem;
  }
  ~MulticastReceiver()
  {
    if (path != NULL) { free(path); }
    if (fd != -1) { close(fd); }
    if (filename != NULL) { free(filename); }
    close(sock);
  }

  // The main routine which handles the multicast connection
  int session();
};
#endif
