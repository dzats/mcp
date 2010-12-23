#include <sys/socket.h>
#include <sys/time.h>
#include <net/if.h>
#include <sys/ioctl.h>

#include "connection.h"

using namespace std;

// Receive 'size' bytes from 'sock' and places them to 'data'
void recvn(int sock, void *data, size_t size, int flags) {
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
void sendn(int sock, const void *data, size_t size, int flags) {
	do {
		register int bytes_sent = send(sock, data, size, flags);
		if (bytes_sent < 0) {
			if (errno == ENOBUFS) {
				SDEBUG("ENOBUFS error occurred\n");
				usleep(200000);
				continue;
			}

			throw ConnectionException(errno);
		} else {
			size -= bytes_sent;
			data = (uint8_t *)data + bytes_sent;
		}
	} while(size > 0);
}

// Receives reply from 'sock'
void ReplyHeader::recv_reply(int sock, char **message) {
	recvn(sock, &status, sizeof(status), 0);
	if (get_status() != STATUS_OK &&
			get_status() != STATUS_INCORRECT_CHECKSUM) {
		recvn(sock, &address, sizeof(ReplyHeader) - sizeof(status), 0);

		if (get_msg_length() > MAX_ERROR_LENGTH) {
			throw ConnectionException(ConnectionException::corrupted_data_received);
		}
		*message = (char *)malloc(get_msg_length() + 1);
		(*message)[get_msg_length()] = '\0';
		recvn(sock, *message, get_msg_length(), 0);
		DEBUG("Received error message: %u, %x, %u, %s\n", get_status(),
			get_address(), get_msg_length(), *message);
	}
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

// Returns internet addresses, which the host has
// The returned value should be futher explicitly deleted.
vector<uint32_t>* get_interfaces(int sock) {
	struct ifconf ifc;
	vector<uint32_t> *addresses = new vector<uint32_t>;

	// Get the available interfaces
	int lastlen = 0;
	ifc.ifc_len = sizeof(struct ifreq) * 28;
	ifc.ifc_req = (struct ifreq *)malloc(ifc.ifc_len);
	while(1) {
		if (ioctl(sock, SIOCGIFCONF, &ifc) < 0) {
			ERROR("ioctl call returned the error: %s", strerror(errno));
			exit(EXIT_FAILURE);
		}
		if (ifc.ifc_len != lastlen || lastlen == 0) {
			lastlen = ifc.ifc_len;
			ifc.ifc_len += sizeof(struct ifreq) * 12;
			ifc.ifc_req = (struct ifreq *)realloc(ifc.ifc_req, ifc.ifc_len);
		} else {
			break;
		}
	}

	DEBUG("Number of interfaces: %d(%zu)\n", ifc.ifc_len, sizeof(struct ifreq));
	for (uint8_t *ptr = (uint8_t *)ifc.ifc_req;
			ptr < (uint8_t *)ifc.ifc_req + ifc.ifc_len;) {
		struct ifreq *ifr = (struct ifreq *)ptr;

#ifdef _SIZEOF_ADDR_IFREQ
		/*
			ptr += sizeof(ifr->ifr_name) +
				max(sizeof(struct sockaddr), (unsigned)ifr->ifr_addr.sa_len);
		*/
		ptr += _SIZEOF_ADDR_IFREQ((*ifr));
#else
		// For linux compartability
		ptr = (uint8_t *)(ifr + 1);
#endif

		if (ifr->ifr_addr.sa_family == AF_INET) {
			uint32_t addr = ((struct sockaddr_in *)&ifr->ifr_addr)->sin_addr.s_addr;

			// Get flags for the interface
			if (ioctl(sock, SIOCGIFFLAGS, ifr) < 0) {
				ERROR("ioctl call returned the error: %s", strerror(errno));
				exit(EXIT_FAILURE);
			}
			if ((ifr->ifr_flags & IFF_UP) == 0 ||
					(ifr->ifr_flags & IFF_LOOPBACK) != 0) {
				continue;
			}

#ifndef NDEBUG
			char iaddr[INET_ADDRSTRLEN];
			DEBUG("Interface %s, address %s\n", ifr->ifr_name, 
				inet_ntop(AF_INET, &addr, iaddr, sizeof(iaddr)));
#endif
			addresses->push_back(ntohl(addr));
		}
	}
	free(ifc.ifc_req);
	return addresses;
}
