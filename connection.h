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

// Some configurational constants
#define UNICAST_PORT 6879 // default UDP port used for the connection
#define MAX_ERROR_LENGTH 256 // max length of the error messages
#define DEFAULT_MULTICAST_ADDR "224.0.0.78"
#define MULTICAST_UNEXISTING_RESPONDER 0xFFFFFFFF
#define UDP_MAX_LENGTH 65536 // Max length for a UDP datagram
#define MAX_UDP_PACKET_SIZE 1470 // Max length for an unfragmented UDP datagram

// Statuses of the reply messages used in the unicast transmission
#define STATUS_OK 0 // the last operation succeeded (not an error)
#define STATUS_INCORRECT_CHECKSUM 1 // the last operation should be
	//retransmitted
// Fatal errors (numbers are greater or equal to STATUS_FIRST_FATAL_ERROR)
#define STATUS_FIRST_FATAL_ERROR 128 // fatal error with the minimum number
#define STATUS_UNICAST_INIT_ERROR STATUS_FIRST_FATAL_ERROR
#define STATUS_UNICAST_CONNECTION_ERROR 130
#define STATUS_TOO_MANY_RETRANSMISSIONS 132
#define STATUS_DISK_ERROR 133
#define STATUS_UNKNOWN_ERROR 134

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
#define MULTICAST_FILE_RETRANS_REPLY 0x1a4f7c31
#define MULTICAST_TERMINATION_REQUEST 0xe679c240

/*
	Data structures
*/
// Unicast session initialization header
struct UnicastSessionHeader {
private:
	uint32_t flags; // configuration flags (currrently unused)
	uint32_t nsources; // temporary unused field
	uint32_t path_length; // length of the target path
public:
	UnicastSessionHeader() {}
	UnicastSessionHeader(uint32_t flags, uint32_t nsources,
			uint32_t path_length) {
		this->flags = htonl(flags);
		this->nsources = htonl(nsources);
		this->path_length = htonl(path_length);
	}

	uint32_t get_flags() {
		return ntohl(flags);
	}
	uint32_t get_nsources() {
		return ntohl(nsources);
	}
	uint32_t get_path_length() {
		return ntohl(path_length);
	}
} __attribute__((packed));

// Record for one destination in the unicast session initialization message
struct DestinationHeader {
private:
	uint32_t addr;
	uint32_t path_length;
public:
	DestinationHeader(uint32_t address, uint32_t path_len) {
		addr = htonl(address);
		path_length = htonl(path_len);
	}
	
	uint32_t get_addr() {
		return ntohl(addr);
	}
	uint32_t get_path_length() {
		return ntohl(path_length);
	}
} __attribute__((packed));

// Header of reply messages used in the unicast connection
struct ReplyHeader {
private:
	uint8_t status; // if status
	uint32_t address;
	uint32_t msg_length;
public:
	ReplyHeader() {}
	ReplyHeader (uint8_t stat, uint32_t addr, uint32_t msg_len) {
		status = stat;
		address = htonl(addr);
		msg_length = htonl(msg_len);
	};

	uint8_t get_status() {
		return status;
	}
	uint32_t get_address() {
		return ntohl(address);
	}
	uint32_t get_msg_length() {
		return ntohl(msg_length);
	}

	// Receives reply from 'sock'
	void recv_reply(int sock, char **message);
} __attribute__((packed));

enum ResourceType {resource_is_a_file = 0, resource_is_a_directory = 1};

// structure that contains information about a file or a directory
struct FileInfoHeader {
private:
	uint8_t unused; // temporary unused field
	uint8_t type;
	uint16_t mode;
	uint32_t name_length;
public:
	FileInfoHeader() : unused(0), type(0), mode(0), name_length(0) {}
	FileInfoHeader(uint8_t t, uint16_t mode, uint32_t name_length) : unused(0),
			type(t) {
		this->mode = htons(mode);
		this->name_length = htonl(name_length);
	}

	uint8_t get_type() {
		return type;
	}
	uint16_t get_mode() {
		return ntohs(mode);
	}
	uint16_t get_name_length() {
		return ntohl(name_length);
	}

	bool is_trailing_record() {
		// FIXME: rewrite for extensibility
		return unused == 0 && type == 0 &&
			mode == 0 && name_length == 0;
	}
	void print() {
		printf("|%u|%u| %o |  %u  |\n", unused, type, ntohs(mode),
			ntohl(name_length));
	}
} __attribute__((packed));

// A host record in the multicast sission init message
struct MulticastHostRecord {
private:
	uint32_t addr; // address of the host
public:
	MulticastHostRecord(uint32_t a) {
		addr = htonl(a);
	}

	uint32_t get_addr() const {
		return ntohl(addr);
	}
	void set_addr(uint32_t a) {
		addr = htonl(a);
	}

	bool operator==(const MulticastHostRecord& r) const {
		return addr == r.addr;
	}
	bool operator<(const MulticastHostRecord& r) const {
		// FIXME: ntohl may be required here
		return addr < r.addr;
	}
} __attribute__((packed));

struct MulticastMessageHeader {
private:
	uint32_t message_type; // type of the multicast message
	uint32_t session_id; // some number to identify the session
	uint32_t number; // ordinal number of the multicast message in the
		// session, used for reliability implementation
	uint32_t responder; // id of the host, that must respond to this message
		// 0xffffffff means nobody should respond to this message
public:
	MulticastMessageHeader(uint32_t m, uint32_t s) {
		message_type = htonl(m);
		session_id = htonl(s);
	}
	uint32_t get_message_type() {
		return ntohl(message_type);
	}
	uint32_t get_session_id() {
		return ntohl(session_id);
	}

	void set_number(uint32_t n) {
		number = htonl(n);
	}
	uint32_t get_number() {
		return ntohl(number);
	}

	void set_responder(uint32_t id) {
		responder = htonl(id);
	}
	uint32_t get_responder() {
		return ntohl(responder);
	}
} __attribute__((packed));

/*
	Helper functions to work with the monotonically increased unsigned numbers
*/
static inline bool cyclic_less(uint32_t a, uint32_t b) {
	register uint32_t d = b - a;
	return d != 0 && d < INT32_MAX ? true : false;
}

static inline bool cyclic_greater(uint32_t a, uint32_t b) {
	register uint32_t d = a - b;
	return d != 0 && d < INT32_MAX ? true : false;
}

static inline bool cyclic_greater_or_equal(uint32_t a, uint32_t b) {
	return a - b < INT32_MAX;
}

static inline bool cyclic_less_or_equal(uint32_t a, uint32_t b) {
	return b - a < INT32_MAX;
}

/*
	Helper functions to work the unicast connection
*/

// The exception indicating an error in work with TCP connections
class ConnectionException : public std::exception {
public:
	enum Errors { unexpected_end_of_input = 0, corrupted_data_received = -1 };
	int error;
	ConnectionException(int e) : error(e) {}
	const char* what() const throw() {
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
void recvn(int sock, void *data, size_t size, int flags);

// Send 'size' bytes from 'data' to 'sock'
void sendn(int sock, const void *data, size_t size, int flags);

static inline void send_normal_conformation(int sock) {
	uint8_t status = 0;
	sendn(sock, &status, sizeof(status), 0);
}

static inline void send_retransmission_request(int sock) {
	uint8_t status = 1;
	sendn(sock, &status, sizeof(status), 0);
}

// Sends error to the 'sock'
void send_error(int sock, uint32_t status, uint32_t address,
		uint32_t msg_length, char *msg);

// Returns internet addresses, which the host has
// The returned value should be futher explicitly deleted.
std::vector<uint32_t>* get_interfaces(int sock);
#endif
