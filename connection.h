#ifndef CONNECTION_H_HEADER
#define CONNECTION_H_HEADER 1
#include <unistd.h>
#include <errno.h>
#include <string.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>

#include <inttypes.h> // for uint64_t
#include <arpa/inet.h> // for htonl/ntohl

#include "destination.h"
#include "log.h"

#define INLINE inline

#define UNICAST_PORT 6879

// The exception indicating an error in work with TCP connections
class ConnectionException : public std::exception {
public:
	enum Errors { corrupted_data_received = -1 };
	int error;
	ConnectionException(int e) : error(e) {}
	const char* what() const throw() {
		switch (error) {
			case corrupted_data_received:
				return "Corrupted data received";
			case 0:
				return "Unexpected end of input";
			default:
				return strerror(error);
		}
	}
};

// Receive 'size' bytes from 'sock' and places them to 'data'
static INLINE void recvn(int sock, void *data, size_t size, int flags) {
	do {
		register int bytes_recvd = recv(sock, data, size, flags);
		if (bytes_recvd <= 0) {
			throw ConnectionException(errno);
		} else {
			size -= bytes_recvd;
			data = (uint8_t *)data + bytes_recvd;
		}
	} while(size > 0);
}

// Send 'size' bytes from 'data' to 'sock'
static INLINE void sendn(int sock, const void *data, size_t size, int flags) {
	do {
		register int bytes_sent = send(sock, data, size, flags);
		if (bytes_sent < 0) {
			throw ConnectionException(errno);
		} else {
			size -= bytes_sent;
			data = (uint8_t *)data + bytes_sent;
		}
	} while(size > 0);
}

#define MAX_ERROR_LENGTH 256 // max length of the error messages

// Statuses that are used in connection
#define STATUS_OK 0 // the last operation succeeded (not an error)
#define STATUS_INCORRECT_CHECKSUM 1 // some non-fatal error occurred, 
// the last operation should be retransmitted

// Fatal errors (numbers are greater or equal to STATUS_FIRST_FATAL_ERROR),
// terminate the session
#define STATUS_FIRST_FATAL_ERROR 128 // fatal error with the minimum number
#define STATUS_UNICAST_INIT_ERROR STATUS_FIRST_FATAL_ERROR
#define STATUS_MULTICAST_INIT_ERROR 129
#define STATUS_UNICAST_CONNECTION_ERROR 130
#define STATUS_MULTICAST_CONNECTION_ERROR 131
#define STATUS_TOO_MANY_RETRANSMISSIONS 132
#define STATUS_DISK_ERROR 133
#define STATUS_UNKNOWN_ERROR 134

// Unicast session initialization header
struct UnicastSessionHeader {
	uint32_t flags; // configuration flags (currrently unused)
	uint32_t nsources; // temporary unused field
	uint32_t path_length; // length of the target path
	// Convert from the network to the host specific representation
	void ntoh() {
		flags = ntohs(flags);
		nsources = ntohs(nsources);
		path_length = ntohl(path_length);
	}
	// Convert from the host to the network specific representation
	void hton() {
		flags = htons(flags);
		nsources = htons(nsources);
		path_length = htonl(path_length);
	}
} __attribute__((packed));

// record for one destination in the unicast session initialization message
struct DestinationHeader {
	uint32_t addr;
	uint32_t path_length;
	// Convert from the network to the host specific representation
	void ntoh() {
		addr = ntohl(addr);
		path_length = ntohl(path_length);
	}
	// Convert from the host to the network specific representation
	void hton() {
		addr = htonl(addr);
		path_length = htonl(path_length);
	}
} __attribute__((packed));

// Header of the 
struct ReplyHeader {
	uint8_t status; // if status
	uint32_t address;
	uint32_t msg_length;
	ReplyHeader() {}
	ReplyHeader (uint32_t stat, uint32_t addr, uint32_t msg_len) {
		status = stat;
		address = htonl(addr);
		msg_length = htonl(msg_len);
	};
} __attribute__((packed));

enum ResourceType {resource_is_a_file = 0, resource_is_a_directory = 1};

// structure that contains information about a file or a directory
struct FileInfoHeader {
	uint8_t unused; // temporary unused field
	uint8_t type;
	uint16_t mode;
	uint32_t name_length;
	// Convert from the network to the host specific representation
	void ntoh() {
		mode = ntohs(mode);
		name_length = ntohl(name_length);
	}
	// Convert from the host to the network specific representation
	void hton() {
		mode = htons(mode);
		name_length = htonl(name_length);
	}
	bool is_trailing_record() {
		// FIXME: rewrite for extensibility
		return unused == 0 && type == 0 &&
			mode == 0 && name_length == 0;
	}
	void print() {
		printf("|%u|%u| %o |  %u  |\n", unused, type, mode, name_length);
	}
} __attribute__((packed));

static INLINE void send_normal_conformation(int sock) {
	uint8_t status = 0;
	sendn(sock, &status, sizeof(status), 0);
}

static INLINE void send_retransmission_request(int sock) {
	uint8_t status = 1;
	sendn(sock, &status, sizeof(status), 0);
}

// Receives reply from 'sock'
ReplyHeader recv_reply(int sock, char **message);

// Sends error to the 'sock'
void send_error(int sock, uint32_t status, uint32_t address,
		uint32_t msg_length, char *msg);
#endif
