#ifndef MULTICAST_RECEIVER_H_HEADER
#define MULTICAST_RECEIVER_H_HEADER 1

#include <netinet/in.h>
#include <arpa/inet.h>

#include <sys/time.h>

#include <list>

#include "path.h"
#include "md5.h"
#include "multicast_recv_queue.h"

// The multicast receiver object (one per multicast connection)
class MulticastReceiver {
	struct PacketToSend {
		unsigned number;
		struct timeval timestamp; // timestamp when the last message of this
			// type has been send
		PacketToSend(unsigned p) : number(p) {
			timestamp.tv_sec = 0;
			timestamp.tv_usec = 0;
			retrans_timeout = 0;
		}
		unsigned retrans_timeout; // retransmission timeout for this packet
		bool operator==(const PacketToSend& t) {
			return t.number == number;
		}
	};

	static const unsigned MIN_RETRANS_TIMEOUT = 2; // milliseconds
	static const unsigned MAX_RETRANS_TIMEOUT = 1000; // milliseconds
	static const unsigned MAX_RETRANS_LINEAR_ADD = 8; // milliseconds

	int sock; // Socket used for connection
	const struct sockaddr_in& source_addr; // Address of the multicast sender
	uint32_t session_id; // id of the multicast sender
	uint32_t local_address; // local IP address which the session establish with
	uint32_t interface_address; // Address of the interface used for multicast
		// connection

	char *path; // local destination path
	PathType path_type;
	uint32_t nsources; // number of sources

	in_addr_t own_addr;
	std::list<PacketToSend> missed_packets;
	std::list<uint32_t> pending_replies;

	MulticastRecvQueue message_queue;
	
	unsigned next_message_expected;

	int fd; // descriptor of the currently opened file
	char *filename;
	MD5sum checksum; // checksum for files

	// Sends a UDP datagram to the multicast sender
	void send_datagram(void *data, size_t size);

	// Send a reply for the packet 'number' to the source
	void send_reply(uint32_t number);

public:
	MulticastReceiver(int s, const struct sockaddr_in& saddr,
			uint32_t sid, uint32_t local_addr, uint32_t interface_addr) :
			sock(s), source_addr(saddr), session_id(sid),
			local_address(local_addr), interface_address(interface_addr),
			path(NULL), nsources(0), next_message_expected(0), fd(-1),
			filename(NULL) {}
	~MulticastReceiver() {
		if (path != NULL) {
			free(path);
		}
		if (fd != -1) {
			close(fd);
		}
		if (filename != NULL) {
			free(filename);
		}
		close(sock);
	}

	// The main routine which handles the multicast connection
	void session();

	// This routine reads data from the connection and put it into the queue
	void read_data();

	// Wrapper function to run the read_data method in a separate thread
	static void* read_data_wrapper(void *arg);
};
#endif
