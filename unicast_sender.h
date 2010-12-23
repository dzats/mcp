#ifndef UNICAST_SENDER_H_HEADER
#define UNICAST_SENDER_H_HEADER 1

#include "destination.h"
#include "distributor.h"
#include "connection.h"

#include <netinet/in.h> // for in_addr_t

// Structure for send file to a destination.
// One socket writer per session
class UnicastSender : public Distributor::Writer {
	// configuration constants
	static const int DEFAULT_PORT = 6789;
	static const int MAX_INITIALIZATION_RETRIES = 3;

	int sock;

	void connect_to(in_addr_t addr); // This function tries to connect
		// to the host 'addr'
	void send_initial_record(int nsources, char *path,
		MD5sum *checksum); // Send the initial connection entry
	void send_destinations(std::vector<Destination>::const_iterator i,
		const std::vector<Destination>::const_iterator& _end, MD5sum *checksum);
	void send_destination_trailing(MD5sum *checksum);
	int choose_destination(const std::vector<Destination>& dst); // Choose
		// the next destination. Return an index in the 'dst' array.
public:
	// Structure that is passed to the SocketReader thread routine
	UnicastSender(Distributor* b) : Distributor::Writer(b,
			(Distributor::Client *)&b->unicast_sender), sock(-1) {}
	~UnicastSender() {
		if (sock != -1) { close(sock); }
	}
	/*
		This is the initialization routine that establish connection with
		the destinations.
	*/
	int session_init(const std::vector<Destination>& dst, int nsources);

	/*
		This is the main routine of the unicast sender. This routing sends
		all the file and directories along with the other destinations
		and necessary information.
	*/
	int session();
};
#endif
