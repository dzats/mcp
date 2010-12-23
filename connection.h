#ifndef CONNECTION_H_HEADER
#define CONNECTION_H_HEADER 1
#include <unistd.h>
#include <errno.h>

#include <string.h>

#include <sys/types.h>
#include <sys/socket.h>

#include <inttypes.h> // for uint64_t
#include <arpa/inet.h> // for htonl/ntohl

#include <vector>

#include "destination.h"

// Reply statuses, TODO: need to work on this topic for more detailed report
// about errors
#define STATUS_OK 0
#define STATUS_INCORRECT_CHECKSUM 1
#define STATUS_UNRECOVERABLE_ERROR 2

#define INLINE inline

// The exception indicating an error in work with TCP connections
// FIXME: a more detailed information about the error possibly required
class ConnectionException : public std::exception {
public:
	int error;
	ConnectionException(int e) : error(e) {}
	const char* what() const throw() {
		if (error == 0) {
			return "Unexpected end of input";
		} else {
			return strerror(error);
		}
	}
};

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

static INLINE void send_normal_conformation(int sock) {
	uint8_t status = 0;
	sendn(sock, &status, sizeof(status), 0);
}

static INLINE void send_retransmission_request(int sock) {
	uint8_t status = 1;
	sendn(sock, &status, sizeof(status), 0);
}
#endif
