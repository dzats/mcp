#include <stdio.h>
#include <stdlib.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>

#include <exception>

#include "unicast_sender.h"
#include "log.h"

// Tries to establish TCP connection with the host 'addr'
void UnicastSender::connect_to(in_addr_t addr) throw (ConnectionException) {
	if ((sock = socket(PF_INET, SOCK_STREAM, 0)) == -1) {
		//ERROR("Can't create socket: %d\n", sock);
		throw ConnectionException(errno);
	}

	struct sockaddr_in saddr;
	saddr.sin_family = AF_INET;
	saddr.sin_port = htons(port);
	saddr.sin_addr.s_addr = htonl(addr);

	if (connect(sock, (struct sockaddr *)&saddr, sizeof(saddr)) != 0) {
		close(sock);
		sock = -1;
		throw ConnectionException(errno);
	}

	int buffsize = 36600;
	/*
		This is work around some FreeBSD kernel bug in the sockatmark function.
		FIXME: don't shure that this is correct solution.
	*/
	if (setsockopt(sock, SOL_SOCKET, SO_SNDBUF, &buffsize,
			sizeof(buffsize)) != 0) {
		DEBUG("setsockopt error: %s\n", strerror(errno));
		perror("setsockopt error");
		throw ConnectionException(errno);
	}
}

// Send the Unicast Session Initialization record
void UnicastSender::send_initial_record(int nsources, char *path,
		MD5sum *checksum) throw (ConnectionException) {
	uint32_t path_len = path == NULL ? 0 : strlen(path);
	UnicastSessionHeader ush = {0 /* flags */, nsources, path_len};
	ush.hton();
	sendn(sock, &ush, sizeof(ush), 0);
	checksum->update(&ush, sizeof(ush));
	if (path_len > 0) {
		sendn(sock, path, path_len, 0);
		checksum->update(path, path_len);
	}
}

// Send the destinations from *i to *_end
void UnicastSender::send_destinations(
		std::vector<Destination>::const_iterator i,
		const std::vector<Destination>::const_iterator& _end,
		MD5sum *checksum) throw (ConnectionException) {
	for(; i != _end; ++i) {
		int path_len = &*i->filename == NULL ? 0 : strlen(i->filename.get());
		DestinationHeader dh = {i->addr, path_len};
		dh.hton();
		sendn(sock, &dh, sizeof(dh), 0);
		checksum->update(&dh, sizeof(dh));
		if (path_len > 0) {
			sendn(sock, i->filename.get(), path_len, 0);
			checksum->update(i->filename.get(), path_len);
		}
	}
}

// Send the trailing record and checksum for all the previously sent data
void UnicastSender::send_destination_trailing(
		MD5sum *checksum) throw (ConnectionException) {
	// Trailing record
	DestinationHeader dh = {0, 0};
	sendn(sock, &dh, sizeof(dh), 0);
	checksum->update((unsigned char *)&dh, sizeof(dh));
}

// Chooses the next destination 
int UnicastSender::choose_destination(const std::vector<Destination>& dst) {
	// TODO: it could be useful to parse the local addresses and look for
	// the nearest one destination by the topoligy in the list 
	return 0;
}

// Register an error and finish the current task
void UnicastSender::register_error(uint8_t status, const char *fmt,
		const char *error) {
	struct in_addr addr;
	addr.s_addr = ntohl(target_address);
	char *hostname = inet_ntoa(addr);
	// FIXME: use of the inet_ntoa_r and strerror_r instead of the
	// corresponding functions without the _r suffix.
#if 0
	char hostname[INET_ADDRSTRLEN];
	inet_ntoa_r(addr, hostname, INET_ADDRSTRLEN);
#endif
	char *error_message = (char *)malloc(strlen(fmt) + strlen(hostname) +
		strlen(error) + 1); // a bit more that required
	sprintf((char *)error_message, fmt, hostname, error);
	DEBUG("register error: %s\n", error_message);
	buff->unicast_sender.set_error(INADDR_NONE, error_message,
		strlen(error_message));
	buff->unicast_sender.status = status;
	// Finish the unicast sender
	submit_task();
}

/*
	This is the initialization routine which establish the unicast
	session with one of the destinations.
*/
int UnicastSender::session_init(const std::vector<Destination>& dst,
		int nsources) {
	SDEBUG("UnicastSender::session_init called\n");
	// Write the initial data of the session
	unsigned destination_index = choose_destination(dst);
	target_address = dst[destination_index].addr;
	try {
		// Establish connection with the nearest neighbor
		int retries = MAX_INITIALIZATION_RETRIES;
		bool do_retry;
		connect_to(target_address);
		do {
			MD5sum checksum;
			do_retry = false;
			send_initial_record(nsources, dst[destination_index].filename.get(),
				&checksum);
			send_destinations(dst.begin(), dst.begin() + destination_index,
				&checksum);
			if (dst.size() > destination_index + 1) {
				send_destinations(dst.begin() + destination_index + 1, dst.end(),
					&checksum);
			}
			
			send_destination_trailing(&checksum);
			// Send the checksum calculated on the initial record
			checksum.final();
			sendn(sock, checksum.signature, sizeof(checksum.signature), 0);
			SDEBUG("Destinations sent\n");
			// Get the reply
			char *reply_message;
			ReplyHeader h = recv_reply(sock, &reply_message);
			DEBUG("Reply received, status: %d\n", h.status);
			if (h.status == STATUS_OK) {
				// All ok
			} else if (h.status == STATUS_INCORRECT_CHECKSUM) {
				if (--retries > 0) {
					do_retry = true;
					// Retransmit the session initialization message
					ERROR("Incorrect checksum, retransmit the session initialization "
						"message in the %d time\n", MAX_INITIALIZATION_RETRIES -
						retries);
				} else {
					// Fatal error: Too many retransmissions
					register_error(STATUS_UNICAST_INIT_ERROR,
						"Can't establish unicast connection with the host %s: %s",
						"Too many retransmissions");
					close(sock);
					sock = -1;
					return -1;
				}
			} else {
				// Fatal error occurred during the session establishing
				DEBUG("Fatal error received: %s\n", reply_message);
				// Register the error
				buff->unicast_sender.status = h.status;
				buff->unicast_sender.set_error(h.address, reply_message,
					h.msg_length);
				submit_task();
				close(sock);
				sock = -1;
				return -1;
			}
		} while (do_retry);
		SDEBUG("Connection established\n");
		return 0;
	} catch (ConnectionException& e) {
		// Transmission error during session initialization
		try {
			// Try to get an error from the immediate destination
			DEBUG("Connection exception %s, Try to get an error\n", e.what());
			char *reply_message;
			ReplyHeader h = recv_reply(sock, &reply_message);
			if (h.status >= STATUS_FIRST_FATAL_ERROR) {
				buff->unicast_sender.set_error(h.address, reply_message,
					strlen(reply_message));
				buff->unicast_sender.status = h.status;
				submit_task();
				close(sock);
				sock = -1;
				return -1;
			}
		} catch(std::exception& e) {
			// Can't receive an error, generate it
		}
		register_error(STATUS_UNICAST_INIT_ERROR,
			"Can't establish unicast connection with the host %s: %s", e.what());
		close(sock);
		sock = -1;
		return -1;
	}
}

/*
	This is the main routine of the unicast sender. This routine sends
	the files and directories to the next destination.
*/
int UnicastSender::session() {
	try {
		bool is_trailing = false; // Whether the current task is the trailing one
		do {
			// Get the operation from the queue
			Distributor::TaskHeader *op = get_task();
			if (op->fileinfo.is_trailing_record()) {
				// Send the trailing zero record
				SDEBUG("Send the trailing record\n");
				is_trailing = true;
				FileInfoHeader h;
				memset(&h, 0, sizeof(h));
				sendn(sock, &h, sizeof(h), 0);
			} else {
				assert(op->fileinfo.name_length < 1024);
				// Send the file info structure
				uint8_t info_header[sizeof(FileInfoHeader) +
					op->fileinfo.name_length];
				FileInfoHeader *file_h =
					reinterpret_cast<FileInfoHeader *>(info_header);
				*file_h = op->fileinfo;
#ifndef NDEBUG
				SDEBUG("fileinfo: ");
				file_h->print();
#endif
				file_h->hton();
				memcpy(info_header + sizeof(FileInfoHeader), op->filename.c_str(),
				op->fileinfo.name_length);
				sendn(sock, info_header, sizeof(info_header), 0);

				if (op->fileinfo.type == resource_is_a_file) {
					DEBUG("Send file: %s\n", op->filename.c_str());
					// Send the file
					int write_result;
					if ((write_result = write_to_file(sock)) != 0) {
						// It's is the connection error throw appropriate exception
						throw ConnectionException(errno);
					}
					// Send the trailing character
					static const unsigned char zero = '\0';
					sendn(sock, &zero, 1, MSG_OOB);
					SDEBUG("Out-of-band data sent\n");
					// Send the checksum
#ifndef NDEBUG
					SDEBUG("Send the checksum: ");
					MD5sum::display_signature(stdout, checksum()->signature);
					printf("\n");
#endif
					sendn(sock, checksum()->signature, sizeof(checksum()->signature), 0);
				}
			}
			// Wait for an acknowledgement
			char *reply_message;
			ReplyHeader h = recv_reply(sock, &reply_message);
			w->status = h.status;
			DEBUG("Received acknowledgement with status %u\n", h.status);
			if (h.status == STATUS_OK) {
				// All ok
				SDEBUG("Successfull acknowledgement returned\n");
			} else if (h.status == STATUS_INCORRECT_CHECKSUM) {
				// Request for the file retransmission (done by the previous code)
				SERROR("Incorrect checksum, request the file retransmission\n");
			} else {
				// Fatal error occurred during the connection
				SDEBUG("Fatal error received\n");
				buff->unicast_sender.status = h.status;
				buff->unicast_sender.set_error(h.address, reply_message,
					h.msg_length);
				submit_task();
				close(sock);
				sock = -1;
				return -1;
			}
			submit_task();
		} while (!is_trailing);
	} catch (std::exception& e) {
		// Transmission error during session initialization
		try {
			DEBUG("Connection exception %s, Try to get an error\n", e.what());
			// Try to get an error from the immediate destination
			char *reply_message;
			ReplyHeader h = recv_reply(sock, &reply_message);
			if (h.status >= STATUS_FIRST_FATAL_ERROR) {
				buff->unicast_sender.set_error(h.address, reply_message,
					strlen(reply_message));
				buff->unicast_sender.status = h.status;
				submit_task();
				close(sock);
				sock = -1;
				return -1;
			}
		} catch(std::exception& e) {
			// Can't receive an error, generate it
		}
		register_error(STATUS_UNICAST_CONNECTION_ERROR,
			"Error during unicast transmission with the host %s: %s", e.what());
		close(sock);
		sock = -1;
		return -1;
	}

	close(sock);
	sock = -1;
	return 0;
}
