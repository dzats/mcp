#ifndef UNICAST_SENDER_H_HEADER
#define UNICAST_SENDER_H_HEADER 1

#include "destination.h"
#include "distributor.h"
#include "connection.h"

#include <string>
#include <vector>

#include <netinet/in.h> // for in_addr_t

// Structure for send file to a destination.
// One socket writer per session
class UnicastSender : public Distributor::Writer {
	// configuration constants
	static const int DEFAULT_PORT = 6789;
	static const int MAX_INITIALIZATION_RETRIES = 3;
	int sock;

	// Tries to establish TCP connection with the host 'addr'
	void connect_to(in_addr_t addr) throw (ConnectionException);
	// Send the Unicast Session Initialization record
	void send_initial_record(int nsources, char *path,
		MD5sum *checksum) throw (ConnectionException);
	// Send the destinations from *i to *_end
	void send_destinations(std::vector<Destination>::const_iterator i,
		const std::vector<Destination>::const_iterator& _end,
		MD5sum *checksum) throw (ConnectionException);
	// Send the trailing record and checksum for all the previously sent data
	void send_destination_trailing(MD5sum *checksum) throw (ConnectionException);
	// Chooses the next destination. Returns an index in the 'dst' array.
	int choose_destination(const std::vector<Destination>& dst);

	// register an connection error and finish the current task
	void register_error(uint8_t status, const char *fmt, const char *error);
public:
	uint32_t target_address; // address of next immediate destination
	std::string last_error_message; // Message corresponding to the last error

	UnicastSender(Distributor* b) : Distributor::Writer(b,
			(Distributor::Client *)&b->unicast_sender), sock(-1) {}
	~UnicastSender() {
		if (sock != -1) { close(sock); }
	}
	/*
		This is the initialization routine that establish the unicast
		session with one of the destinations.
	*/
	int session_init(const std::vector<Destination>& dst, int nsources);

	/*
		This is the main routine of the unicast sender. This routine sends
		the files and directories to the next destination.
	*/
	int session();
};
#endif
