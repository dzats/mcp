#ifndef MULTICAST_SENDER_H_HEADER
#define MULTICAST_SENDER_H_HEADER 1
#include <string>
#include <vector>

#include <netinet/in.h>
#include <arpa/inet.h>

#include "destination.h"
#include "distributor.h"
#include "connection.h"

#include "multicast_send_queue.h"

// Objects that sends files to the multicast destinations
class MulticastSender : public Distributor::Writer {
	static const unsigned MAX_INITIALIZATION_RETRIES = 9;
	static const unsigned INIT_RETRANSMISSION_RATE = 20000; // retransmission
		// rate of the session initialization message (in microseconds)
	static const unsigned TERMINATE_RETRANSMISSION_RATE = 20000000;
		// retransmission rate of the session initialization message
		// (in microseconds)

	static const unsigned MAX_PORT_CHOOSING_TRIES = 10;

	int sock; // Socket used for multicast connection
	struct sockaddr_in target_address; // address used for multicast connection

	uint32_t address; // Multicast address that will be used in connection
	uint16_t port; // port that will be used for multicast connections

	uint32_t session_id; // Multicast session ID

	MulticastSendQueue *send_queue; // queue of the send messages, used to
		// provide reliability
	unsigned next_message; // number of the next message
	unsigned next_responder; // ordinal number of the next responder

	uint32_t nsources; // number of the specified sources, used to detect
		// the target path
	std::vector<Destination> targets;
public:

	MulticastSender(Distributor* b, uint16_t p, uint32_t n_sources) :
			Distributor::Writer(b, (Distributor::Client *)&b->multicast_sender),
			sock(-1), port(p), send_queue(NULL), next_message(0), next_responder(0),
			nsources(n_sources) {
		address = inet_addr(DEFAULT_MULTICAST_ADDR);
	}
	~MulticastSender() {
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
	void mcast_send(const void *message, int size, int flags);
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
