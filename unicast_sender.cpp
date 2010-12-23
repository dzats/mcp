#include <stdio.h>
#include <stdlib.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>

#include <exception>

#include "unicast_sender.h"
#include "log.h"

void UnicastSender::connect_to(in_addr_t addr) {
	if ((sock = socket(PF_INET, SOCK_STREAM, 0)) == -1) {
		//ERROR("Can't create socket: %d\n", sock);
		throw ConnectionException(errno);
	}

	struct sockaddr_in saddr;
	saddr.sin_family = AF_INET;
	saddr.sin_port = htons(DEFAULT_PORT);
	saddr.sin_addr.s_addr = htonl(addr);

	if (connect(sock, (struct sockaddr *)&saddr, sizeof(saddr)) != 0) {
		close(sock);
		sock = -1;
		throw ConnectionException(errno);
	}

	int buffsize = 66608;
	// This is work around some FreeBSD kernel bug in the sockatmark function.
	// The buffer size should be greater that 65536 bytes.
	// FIXME: don't shure that this is correct solution.
	if (setsockopt(sock, SOL_SOCKET, SO_SNDBUF, &buffsize,
			sizeof(buffsize)) != 0) {
		DEBUG("setsockopt error: %s\n", strerror(errno));
		perror("setsockopt error");
		throw ConnectionException(errno);
	}
}

void UnicastSender::send_initial_record(int nsources, char *path,
		MD5sum *checksum) {
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
		MD5sum *checksum) {
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

void UnicastSender::send_destination_trailing(MD5sum *checksum) {
	// Trailing record
	DestinationHeader dh = {0, 0};
	sendn(sock, &dh, sizeof(dh), 0);
	checksum->update((unsigned char *)&dh, sizeof(dh));
}

int UnicastSender::choose_destination(const std::vector<Destination>& dst) {
	// TODO: it could be useful to parse the local addresses and look for
	// the nearest one destination by the topoligy in the list 
	return 0;
}

int UnicastSender::session_init(const std::vector<Destination>& dst,
		int nsources) {
	SDEBUG("UnicastSender::session_init called\n");
	// Write the initial data of the session
	unsigned destination_index = choose_destination(dst);
	uint8_t status = 0;
	try {
		// Establish connection with the nearest neighbor
		int retries = MAX_INITIALIZATION_RETRIES;
		bool do_retry;
		connect_to(dst[destination_index].addr);
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
			recvn(sock, &status, sizeof(status), 0);
			DEBUG("Reply received, status: %d\n", status);
			if (status == STATUS_OK) {
				// All ok
			} else if (status == STATUS_INCORRECT_CHECKSUM) {
				if (--retries > 0) {
					do_retry = true;
					// Retransmit the session initialization message
					ERROR("Incorrect checksum, retransmit the session initialization "
						"message in the %d time\n", MAX_INITIALIZATION_RETRIES -
						retries);
				} else {
					// TODO: Return an error message
					return status;
				}
			} else {
				// Fatal error occurred during the connection establishement
				close(sock);
				sock = -1;
				return status;
			}
		} while (do_retry);
		SDEBUG("Connection established\n");
		return status;
	} catch (std::exception& e) {
		// Transmission error during session initialization, unrecoverable error
		// TODO: send the error to the source
		ERROR("Transmission error: %s\n", e.what());
		close(sock);
		exit(EXIT_FAILURE);
	}
}

int UnicastSender::session() {
	// Write the files and directories
	while (1) {
		// Get the operation from the queue
		Distributor::TaskHeader *op = get_task();
		if (op->fileinfo.is_trailing_record()) {
			break;
		}
		assert(op->fileinfo.name_length < 1024);
		// Send the file info structure
		uint8_t info_header[sizeof(FileInfoHeader) +
			op->fileinfo.name_length];
		FileInfoHeader *h = reinterpret_cast<FileInfoHeader *>(info_header);
		*h = op->fileinfo;
#ifndef NDEBUG
		SDEBUG("fileinfo: ");
		h->print();
#endif
		h->hton();
		memcpy(info_header + sizeof(FileInfoHeader), op->filename.c_str(),
			op->fileinfo.name_length);
		try {
			sendn(sock, info_header, sizeof(info_header), 0);

			if (op->fileinfo.type == resource_is_a_file) {
				DEBUG("Send file: %s\n", op->filename.c_str());
				// Send the file
				write_to_file(sock);
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
				sendn(sock, checksum()->signature,
					sizeof(checksum()->signature), 0);
				// Wait for an acknowledgement
				uint8_t status;
				recvn(sock, &status, sizeof(status), 0);
				w->status = status;
				DEBUG("Received acknowledgement with status %u\n", status);
				if (status == STATUS_OK) {
					// All ok
				} else if (status == STATUS_INCORRECT_CHECKSUM) {
					// Retransmit the file
					SERROR("Incorrect checksum, request the file retransmission\n");
				} else {
					// Fatal error occurred during the connection establishement
					close(sock);
					sock = -1;
					return status;
				}
				SDEBUG("Successfull acknowledgement returned\n");
			}
		} catch (std::exception& e) {
			// TODO: File transmission failed, possibly recoverable
			ERROR("Transmission error: %s\n", e.what());
			close(sock);
			return EXIT_FAILURE;
		}
		SDEBUG("Object is sent\n");
		submit_task();
	}

	// Send the trailing zero record
	SDEBUG("Send the trailing record\n");
	FileInfoHeader h;
	memset(&h, 0, sizeof(h));
	sendn(sock, &h, sizeof(h), 0);

	close(sock);
	return 0;
}
