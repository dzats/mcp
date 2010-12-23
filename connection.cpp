#include <unistd.h>
#include <errno.h>
#include <string.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>

#include <inttypes.h> // for uint64_t
#include <arpa/inet.h> // for htonl/ntohl

#include "connection.h"

// Receives reply from 'sock'
ReplyHeader recv_reply(int sock, char **message) {
	ReplyHeader h;
	recvn(sock, &h.status, sizeof(h.status), 0);
	if (h.status != STATUS_OK && h.status != STATUS_INCORRECT_CHECKSUM) {
		recvn(sock, &h.address, sizeof(h) - sizeof(h.status), 0);
		h.address = ntohl(h.address);
		h.msg_length = ntohl(h.msg_length);

		if (h.msg_length > MAX_ERROR_LENGTH) {
			throw ConnectionException(ConnectionException::corrupted_data_received);
		}
		*message = (char *)malloc(h.msg_length + 1);
		(*message)[h.msg_length] = '\0';
		recvn(sock, *message, h.msg_length, 0);
		DEBUG("Received error message: %u, %x, %u, %s\n", h.status, h.address,
			h.msg_length, *message);
	}
	return h;
}

// Sends error to the 'sock'
void send_error(int sock, uint32_t status, uint32_t address,
		uint32_t msg_length, char *msg) {
	if (address == INADDR_NONE) {
		// Get address from the current connection
		struct sockaddr_in addr;
		socklen_t addr_len = sizeof(addr);
		
		if(getsockname(sock, (struct sockaddr *)&addr, &addr_len) != 0) {
			ERROR("Can't get address from socket: %s", strerror(errno));
		} else {
			address = addr.sin_addr.s_addr;
		}
	}

	ReplyHeader h(status, address, msg_length);
	sendn(sock, &h, sizeof(h), 0);
	sendn(sock, msg, msg_length, 0);
}
