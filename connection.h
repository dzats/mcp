#ifndef CONNECTION_H_HEADER
#define CONNECTION_H_HEADER 1
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#ifdef linux
#include <endian.h>
#include <byteswap.h>
#else
#include <sys/endian.h> // for be64toh
#endif

#include <inttypes.h> // for uint64_t
#include <arpa/inet.h> // for htonl/ntohl

#include <vector>
#include <exception>

#include "destination.h"
#include "log.h"

/*
  Macro definitions
*/
#ifndef INT32_MAX // should be included through stdint.h somehow else
#define INT32_MAX 0x7fffffff
#endif

#ifndef UINT32_MAX // should be included through stdint.h somehow else
#define UINT32_MAX 0xffffffff
#endif

#define PROTOCOL_VERSION 0x02 // Version of the binary protocol. Not
  // completely implemented yet.

// Some configurational constants
#define UNICAST_PORT 6879 // default TCP port used for the unicast connections
#define MULTICAST_PORT 6879 // default UDP port used for the multicast
  // connections
#define MAX_ERROR_LENGTH 1200 // max length of the error messages

#define DEFAULT_MULTICAST_ADDR "239.250.11.7"
#define MULTICAST_EPHEMERAL_ADDR_RANGE "239.251.0.0"
#define MULTICAST_EPHEMERAL_ADDR_MASK "255.255.0.0"

#define UDP_MAX_LENGTH 65536 // Max length for a UDP datagram
#define MAX_UDP_PACKET_SIZE 1472 // Max length for an unfragmented UDP datagram
#define UDP_PACKET_DATA_OVERHEAD 28 // IP header + UDP header

// Statuses of the reply messages used in the unicast transmission
#define STATUS_OK 0 // the last operation succeeded (not an error)
#define STATUS_INCORRECT_CHECKSUM 1 // the last operation should be
  //retransmitted
#define STATUS_NOT_FATAL_DISK_ERROR 2
// Fatal errors (numbers are greater or equal to STATUS_FIRST_FATAL_ERROR)
#define STATUS_FIRST_FATAL_ERROR 128 // fatal error with the minimum number
#define STATUS_UNICAST_INIT_ERROR STATUS_FIRST_FATAL_ERROR
#define STATUS_MULTICAST_INIT_ERROR 129
#define STATUS_UNICAST_CONNECTION_ERROR 130
#define STATUS_MULTICAST_CONNECTION_ERROR 131
#define STATUS_TOO_MANY_RETRANSMISSIONS 132
#define STATUS_FATAL_DISK_ERROR 133
#define STATUS_SERVER_IS_BUSY 150
#define STATUS_PORT_IN_USE 151 // UDP port requested by the multicast sender is
  // already in use
#define STATUS_UNKNOWN_ERROR 171

// Types of the multicast messages
#define MULTICAST_INIT_REQUEST 0xfb47a6c1
#define MULTICAST_INIT_REPLY 0xfb47a6c1
#define MULTICAST_TARGET_PATHS 0x6fd78a01
#define MULTICAST_FILE_RECORD 0x6fd78a02
#define MULTICAST_FILE_DATA 0x6fd78a03
#define MULTICAST_FILE_TRAILING 0x6fd78a04
#define MULTICAST_RECEPTION_CONFORMATION 0xa3d58ad0
#define MULTICAST_MESSAGE_RETRANS_REQUEST 0xa3d58ad1
#define MULTICAST_FILE_RETRANS_REQUEST 0x1a4f7c30
#define MULTICAST_ERROR_MESSAGE 0x4c713fa0
#define MULTICAST_TERMINATION_REQUEST 0xe679c240
#define MULTICAST_ABNORMAL_TERMINATION_REQUEST 0xe679c24F

#define MULTICAST_FINAL_TIMEOUT 4000 // (in milliseconds) Since only the
  // link-local traffic is used, this value seemed to be enough. This
  // timeout should not be set to a large value, because the ephemeral
  // UDP port is not released until the timeout expires.

// OOB data that is used to handle the files shrinked during the transfer
#define OOB_FILE_SIZE_CHANGED 1
#define OOB_FILE_TRANSMISSION_DONE 2

// Flags which determine the global behaviour
#define PRESERVE_ORDER_FLAG 1
#define UNICAST_ONLY_FLAG 2
#define VERIFY_CHECKSUMS_TWISE_FLAG 4

/*
  Data structures
*/
// Unicast session initialization header
struct UnicastSessionHeader
{
private:
  uint32_t flags; // configuration flags (currrently unused)
  uint32_t n_sources; // temporary unused field
  uint32_t path_length; // length of the target path
public:
  UnicastSessionHeader() {}
  UnicastSessionHeader(uint32_t flags, uint32_t n_sources,
      uint32_t path_length)
  {
    this->flags = htonl(flags);
    this->n_sources = htonl(n_sources);
    this->path_length = htonl(path_length);
  }

  uint32_t get_flags()
  {
    return ntohl(flags);
  }
  uint32_t get_nsources()
  {
    return ntohl(n_sources);
  }
  uint32_t get_path_length()
  {
    return ntohl(path_length);
  }
} __attribute__((packed));

// Record for one destination in the unicast session initialization message
struct DestinationHeader
{
private:
  uint32_t addr;
  uint32_t path_length;
public:
  DestinationHeader(uint32_t address, uint32_t path_len)
  {
    addr = htonl(address);
    path_length = htonl(path_len);
  }
  
  uint32_t get_addr()
  {
    return ntohl(addr);
  }
  uint32_t get_path_length()
  {
    return ntohl(path_length);
  }
} __attribute__((packed));

// Header of reply messages used in the unicast connection
struct ReplyHeader
{
private:
  uint8_t status; // if status
  uint32_t address;
  uint32_t msg_length;
public:
  ReplyHeader() {}
  ReplyHeader (uint8_t stat, uint32_t addr, uint32_t msg_len)
  {
    status = stat;
    address = htonl(addr);
    msg_length = htonl(msg_len);
  };

  uint8_t get_status()
  {
    return status;
  }
  uint32_t get_address()
  {
    return ntohl(address);
  }
  uint32_t get_msg_length()
  {
    return ntohl(msg_length);
  }

  // Receives reply from 'sock'. Returns 0 if some reply has been received
  // and -1 otherwise
  int recv_reply(int sock, char **message, int flags);
} __attribute__((packed));

enum ResourceType {resource_is_a_file = 0, resource_is_a_directory = 1};

// structure that contains information about a file or a directory
struct FileInfoHeader
{
private:
  uint8_t unused; // temporary unused field
  uint8_t type;
  uint16_t mode;
  uint16_t name_length; // length of the file/directory name
  uint16_t name_offset; // offset of the common (not source specifid) part
    // in the file/directory name
  uint32_t file_size[2]; // size of file
public:
  FileInfoHeader() : unused(0), name_length(0) {}
  FileInfoHeader(uint8_t t, uint16_t mode, uint16_t name_length,
      uint16_t name_offset, uint64_t file_size) : unused(0), type(t)
  {
    this->mode = htons(mode);
    this->name_length = htons(name_length);
    this->name_offset = htons(name_offset);
    this->file_size[0] = htonl(file_size & UINT32_MAX); 
    this->file_size[1] = htonl(file_size >> 32); 
  }

  uint8_t get_type() const
  {
    return type;
  }
  uint16_t get_mode() const
  {
    return ntohs(mode);
  }
  uint16_t get_name_length() const
  {
    return ntohs(name_length);
  }
  uint16_t get_name_offset() const
  {
    return ntohs(name_offset);
  }
  uint64_t get_file_size() const
  {
    return ((uint64_t)ntohl(file_size[1]) << 32) + ntohl(file_size[0]);
  }
  bool is_trailing_record() const
  {
    return name_length == 0;
  }
} __attribute__((packed));

// A host record in the multicast sission init message
struct MulticastHostRecord
{
private:
  uint32_t addr; // address of the host
public:
  MulticastHostRecord(uint32_t a)
  {
    addr = htonl(a);
  }

  uint32_t get_addr() const
  {
    return ntohl(addr);
  }
  void set_addr(uint32_t a)
  {
    addr = htonl(a);
  }

  bool operator==(const MulticastHostRecord& r) const
  {
    return addr == r.addr;
  }
  bool operator<(const MulticastHostRecord& r) const
  {
    // FIXME: ntohl may be required here
    return addr < r.addr;
  }
} __attribute__((packed));

struct MulticastMessageHeader
{
private:
  uint32_t message_type; // type of the multicast message
  uint32_t session_id; // some number to identify the session
  uint32_t number; // ordinal number of the multicast message in the
    // session, used for reliability implementation
  uint32_t responder; // id of the host, that must respond to this message
    // 0xffffffff means nobody should respond to this message
public:
  MulticastMessageHeader(uint32_t m, uint32_t s)
  {
    message_type = htonl(m);
    session_id = htonl(s);
  }
  uint32_t get_message_type() const { return ntohl(message_type); }
  uint32_t get_session_id() const { return ntohl(session_id); }
  uint32_t get_number() const { return ntohl(number); }
  uint32_t get_responder() const { return ntohl(responder); }

  void set_number(uint32_t n) { number = htonl(n); }
  void set_responder(uint32_t id) { responder = htonl(id); }
} __attribute__((packed));

struct MulticastInitData
{
private:
  uint32_t version_and_unused; // 8-bit protocol version + 24-bit unused
  uint32_t ephemeral_address; // ephemeral multicast address for this
    // particular session
public:
  MulticastInitData(uint8_t version, uint32_t address)
  {
    version_and_unused = htonl(version);
    ephemeral_address = htonl(address);
  }
  uint8_t get_version() const { return (uint8_t)ntohl(version_and_unused); }
  uint32_t get_ephemeral_address() const { return ntohl(ephemeral_address); }
} __attribute__((packed));

/*
  Helper functions to work with the monotonically increased unsigned numbers
*/
static inline bool cyclic_less(uint32_t a, uint32_t b)
{
  register uint32_t d = b - a;
  return d != 0 && d < INT32_MAX ? true : false;
}

static inline bool cyclic_greater(uint32_t a, uint32_t b)
{
  register uint32_t d = a - b;
  return d != 0 && d < INT32_MAX ? true : false;
}

static inline bool cyclic_greater_or_equal(uint32_t a, uint32_t b)
{
  return a - b < INT32_MAX;
}

static inline bool cyclic_less_or_equal(uint32_t a, uint32_t b)
{
  return b - a < INT32_MAX;
}

static inline uint64_t hton64(uint64_t arg)
{
#ifdef linux
#if __BYTE_ORDER == __BIG_ENDIAN
  return arg;
#elif __BYTE_ORDER == __LITTLE_ENDIAN
  return bswap_64(arg);
#else
#error Unknown byte order
#endif
#else
  return htobe64(arg);
#endif
}

static inline uint64_t ntoh64(uint64_t arg)
{
#ifdef linux
#if __BYTE_ORDER == __BIG_ENDIAN
  return arg;
#elif __BYTE_ORDER == __LITTLE_ENDIAN
  return bswap_64(arg);
#else
#error Unknown byte order
#endif
#else
  return be64toh(arg);
#endif
}

// Internal function to inmplement a more precise usleep on FreeBSD
void internal_usleep(unsigned udelay);

/*
  Helper functions for unicast connections
*/

// The exception indicating an error in work with TCP connections
class ConnectionException : public std::exception
{
public:
  enum Errors { unexpected_end_of_input = -1, corrupted_data_received = -2 };
  int error;
  ConnectionException(int e) : error(e) {}
  const char* what() const throw()
  {
    switch (error) {
      case corrupted_data_received:
        return "Corrupted data received";
      case unexpected_end_of_input:
        return "Unexpected end of input";
      default:
        return strerror(error);
    }
  }
};

// Receive 'size' bytes from 'sock' and places them to 'data'
void recvn(int sock, void *data, size_t size);

// Send 'size' bytes from 'data' to 'sock'
void sendn(int sock, const void *data, size_t size, int flags);

void send_normal_conformation(int sock, uint32_t addr);
void send_incorrect_checksum(int sock, uint32_t addr);
void send_server_is_busy(int sock, uint32_t addr);

// Returns internet addresses, which the host has
// The returned value should be futher explicitly deleted.
int get_local_addresses(int sock, std::vector<uint32_t> *addresses,
  std::vector<uint32_t> *masks);

#endif
