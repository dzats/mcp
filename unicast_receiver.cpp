#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/param.h> // for MAXPATHLEN
#include <poll.h> // for poll

#include <fcntl.h>
#include <dirent.h>
#include <libgen.h>

#include <stack>

#include "unicast_receiver.h"
#include "log.h"

using namespace std;

// reads file from the socket 'fd' and pass it to the distributor
void UnicastReceiver::read_from_socket(const char *filename) {
	struct pollfd p;
	memset(&p, 0, sizeof(p));
	p.fd = sock;
	// FIXME: Don't shure that POLLPRI is really required
	p.events = POLLIN | POLLPRI;

	// FIXME: Remove total, it is for debugging only
	int total = 0;
	while(1) {
		int count = get_space();
		count = std::min(count, 4096); // to speed up start

#ifdef BUFFER_DEBUG
		DEBUG("Free space in the buffer: %d bytes\n", count);
#endif
		// The poll is used to catch the case when the first byte of
		// the received data is out-of-band
		if (poll(&p, 1, -1) <= 0) {
#if 0
			DEBUG("The poll call finished with error during "
				"transmission of %s: %s\n", filename, strerror(errno));
			register_error(STATUS_UNICAST_CONNECTION_ERROR,
				"The poll call finished with error during "
				"transmission of %s: %s", filename, strerror(errno));
#endif
			throw ConnectionException(errno);
		}
#ifdef BUFFER_DEBUG
		DEBUG("sockatmark test (%d, %d, %d)\n", p.revents, POLLIN, POLLPRI);
#endif
		if ((p.revents & POLLPRI) && sockatmark(sock)) {
			DEBUG("(1)End of file received, %d bytes read\n", total);
			uint8_t status;
			recvn(sock, &status, sizeof(status), 0);
			// File received
			DEBUG("status: %x\n", status);
			if (status != 0) {
/*
				DEBUG("Corrupted data received for the file %s\n", filename);
				register_error(STATUS_UNICAST_CONNECTION_ERROR,
					"Corrupted data received for the file %s", filename);
*/
				throw ConnectionException(ConnectionException::corrupted_data_received);
			} else {
				// All ok
				return;
			}
		}
		count = read(sock, rposition(), count);
#ifdef BUFFER_DEBUG
		DEBUG("%d bytes of data read\n", count);
#endif
		if (count > 0) {
			// Update the checksum
			checksum.update((unsigned char *)rposition(),
				count);
			update_reader_position(count);
			total += count;
		} else if(count == 0) {
			// End of file
			DEBUG("Unexpected end of transmission for the file %s\n", filename);
			register_error(STATUS_UNICAST_CONNECTION_ERROR,
				"Unexpected end of transmission for the file %s", filename);
			throw ConnectionException(ConnectionException::unexpected_end_of_input);
		} else {
			// An error occurred
#if 0
			DEBUG("Socket read error on %s: %s\n", filename, strerror(errno));
			register_error(STATUS_UNICAST_CONNECTION_ERROR,
				"Socket read error on %s: %s", filename, strerror(errno));
#endif
			throw ConnectionException(errno);
		}
	}
}

// Gets the initial record from the immediate source
int UnicastReceiver::get_initial(MD5sum *checksum) throw (ConnectionException) {
	// Get the number of sources
	UnicastSessionHeader ush;
	recvn(sock, &ush, sizeof(ush), 0);
	checksum->update(&ush, sizeof(ush));
	nsources = ush.get_nsources();
	register int path_len = ush.get_path_length();

	if (path_len > MAXPATHLEN) {
		throw ConnectionException(ConnectionException::corrupted_data_received);
	}

	DEBUG("number of sources: %d, path length: %d\n", nsources, path_len);
	if (path != NULL) {
		free(path);
	}
	path = (char *)malloc(path_len + 1);
	path[path_len] = 0;
	if (path_len > 0) {
		recvn(sock, path, path_len, 0);
		checksum->update(path, path_len);
		DEBUG("destination: %s\n", path);
	}

	char *error;
	path_type = get_path_type(path, &error, nsources);
	if (path_type == path_is_invalid) {
		register_error(STATUS_DISK_ERROR, error);
		free(error);
		return -1;
	}
	DEBUG("SocketReader::get_initial: The specified path is : %d\n", path_type);
	return 0;
}

// gets the destinations from the immediate source
void UnicastReceiver::get_destinations(MD5sum *checksum) {
	uint32_t record_header[2]; // addr, name len
	dst.clear();
	while(1) {
		recvn(sock, record_header, sizeof(record_header), 0);
		checksum->update(record_header, sizeof(record_header));
		// FIXME: rewrite using the DestinationHeader structure
		record_header[0] = ntohl(record_header[0]);
		record_header[1] = ntohl(record_header[1]);
		if (record_header[0] != 0) {
			DEBUG("addr: %x, length: %x\n", record_header[0], record_header[1]);
			if (record_header[1] >= MAXPATHLEN) {
				throw ConnectionException(ConnectionException::corrupted_data_received);
			}

			char *location = (char *)malloc(record_header[1] + 1);
			location[record_header[1]] = 0;
			if (record_header[1] > 0) {
				// Get the server's address
				recvn(sock, location, record_header[1], 0);
				checksum->update(location, record_header[1]);
			}
			// Get the destinations' list. Possibly memory leak here, check this
			dst.push_back(Destination(record_header[0],
				record_header[1] == 0 ? NULL : location));
		} else {
			// All the destinations are read
#ifndef NDEBUG
			SDEBUG("The destinations received: \n");
			for (std::vector<Destination>::const_iterator i = dst.begin();
					i != dst.end(); ++i) {
				printf("%d.", (*i).addr >> 24);
				printf("%d.", (*i).addr >> 16 & 0xFF);
				printf("%d.", (*i).addr >> 8 & 0xFF);
				printf("%d: ", (*i).addr & 0xFF);
				if (i->filename != NULL) {
					printf("%s\n", &*(i->filename));
				} else {
					printf("\n");
				}
			}
#endif
			return;
		}
	}
}

// Establishes the network session from the TCP connection 'sock'
int UnicastReceiver::session_init(int s) {
	sock = s;
	// Set the SO_OOBINLINE option, to determine the end of file
	// by the TCP URGENT pointer it should be done before
	// any OOB data can be received.
	int on = 1;
	if (setsockopt(sock, SOL_SOCKET, SO_OOBINLINE, &on, sizeof(on)) != 0) {
		DEBUG("Can't execute setsockopt call: %s\n", strerror(errno));
		register_error(STATUS_UNKNOWN_ERROR,
			"Can't execute setsockopt call: %s\n", strerror(errno));
		return -1;
	}

	try {
		// Get the session initialization record
		MD5sum checksum;
		repeat_session_initalization:
		if (get_initial(&checksum) != 0) {
			// The error has been already registered
			return -1;
		}
		// Get the destinations for the files
		get_destinations(&checksum);
		// Get the checksum and compare it with the calculated one
		checksum.final();
#ifndef NDEBUG
		SDEBUG("Calculated checksum: ");
		MD5sum::display_signature(stdout, checksum.signature);
		printf("\n");
#endif
		uint8_t received_checksum[sizeof(checksum.signature)];
		recvn(sock, received_checksum, sizeof(received_checksum), 0);
#ifndef NDEBUG
		SDEBUG("Received checksum:   ");
		MD5sum::display_signature(stdout, received_checksum);
		printf("\n");
#endif
		if (memcmp(checksum.signature, received_checksum,
				sizeof(checksum.signature)) != 0) {
			SERROR("Session inialization request with incorrect "
				"checksum received\n");
			// TODO: Read all the available data to synchronize connection
			// Send the retransmit request message
			send_retransmission_request(sock);
			goto repeat_session_initalization;
		} else {
			// Conform the session initialization (for this particula hop)
			SDEBUG("Session established\n");
			send_normal_conformation(sock);
		}
	} catch (std::exception& e) {
		DEBUG("Network error during session initialization: %s\n", e.what());
		register_error(STATUS_UNICAST_INIT_ERROR,
			"Network error during session initialization: %s\n", e.what());
		return -1;
	}
	return 0;
}

/*
	The main routine of the reader, that works with a unicast session.
	It reads files and directories and transmits them to the distributor
*/
int UnicastReceiver::session() {
	while (1) {
		FileInfoHeader finfo;
		try {
			recvn(sock, &finfo, sizeof(finfo), 0);
			if (finfo.is_trailing_record()) {
				SDEBUG("End of the transmission\n");
				if (finish_work() >= STATUS_FIRST_FATAL_ERROR) {
					// A fatal error occurred
					return -1;
				} else {
					// FIXME: What to do with the retransmission request in this case
					send_normal_conformation(sock);
					return 0;
				}
			}
			if (finfo.get_name_length() >= MAXPATHLEN) {
				throw ConnectionException(ConnectionException::corrupted_data_received);
			}
	
			// Read the file name
			char fname[finfo.get_name_length() + 1];
			recvn(sock, fname, finfo.get_name_length(), 0);
			fname[finfo.get_name_length()] = 0;
	
			int retries = 0;
			if (finfo.get_type() == resource_is_a_file) {
				DEBUG("File: %s(%s) (%o)\n", path, fname, finfo.get_mode());
	
				Distributor::TaskHeader op = {finfo, fname};
				// Add task for the senders (after buffer reinitialization);
				add_task(op);
	
				read_from_socket(fname);
				checksum.final();
	
				if (reader.status == STATUS_OK) {
					// All ok, get checksum for the received file and check it
					uint8_t signature[sizeof(checksum.signature)];
	
					recvn(sock, signature, sizeof(signature), 0);
	#ifndef NDEBUG
					DEBUG("Received checksum(%u): ", (unsigned)sizeof(signature));
					MD5sum::display_signature(stdout, signature);
					printf("\n");
					DEBUG("Calculated checksum(%u)  : ",
						(unsigned)sizeof(checksum.signature));
					MD5sum::display_signature(stdout, checksum.signature);
					printf("\n");
	#endif
				
					if (memcmp(signature, checksum.signature,
							sizeof(signature)) != 0) {
						SERROR("Received checksum differs from the calculated one\n");
						reader.status = STATUS_INCORRECT_CHECKSUM;
					}
				} else {
					// An error during trasmission, close the session
					send_error(sock, reader.status,
						reader.addr, reader.message_length,
						reader.message);
					DEBUG("UnicastReceiver finished with error: %s\n",
						reader.message);
					finish_task();
					finish_work();
					return -1;
				}
			} else {
				DEBUG("Directory: %s(%s) (%o)\n", path, fname, finfo.get_mode());
				// Add task for the senders
				Distributor::TaskHeader op = {finfo, fname};
				add_task(op);
				// TODO: Add checksum or something like this to the fileinfo header
				// No more actions are done for the directory
			}

			// The data has to be written, before we send the the reply.
			wait_for_filewriter();
			if (file_writer.is_present &&
					file_writer.status >= STATUS_FIRST_FATAL_ERROR) {
				// File writer finished with an error. This is the end.
				DEBUG("File writer finished with error: %s\n",
					file_writer.message);
				send_error(sock, file_writer.status,
					file_writer.addr,
					file_writer.message_length,
					file_writer.message);
				finish_task();
				finish_work();
				return -1;
			}

			uint8_t status = reader.status;
			// Send the reply to the file sender
			sendn(sock, &status, sizeof(status), 0);

			status = finish_task();

			if (status == STATUS_INCORRECT_CHECKSUM) {
				if (reader.status == STATUS_INCORRECT_CHECKSUM) {
					// Receive the retransmitted message
					continue;
				}
				assert(finfo.get_type() == resource_is_a_file);
				// Retransmit the previous file from the disk
				DEBUG("Retransmission %d for the  file %s\n",
					MAX_DATA_RETRANSMISSIONS - retries, fname);

				// Get name of the name written to disk
				const char *filename = get_targetfile_name(fname, path,
					path_type);

				struct stat fs;
				if (stat(filename, &fs) != 0) {
					DEBUG("Can't open the file %s for retransmission: %s\n", filename,
						strerror(errno));
					register_error(STATUS_DISK_ERROR, 
						"Can't open the file %s for retransmission: %s", filename,
						strerror(errno));
					finish_task();
					finish_work();
					free_targetfile_name(filename, path_type);
					return -1;
				}

				// Retransmit the file using the local copy on disk
				if (handle_file(filename, &fs, 0, true) != 0) {
					free_targetfile_name(filename, path_type);
					return -1;
				}

				free_targetfile_name(filename, path_type);
			} else if (status >= STATUS_FIRST_FATAL_ERROR) {
				finish_work();
				return -1;
			}
		} catch(ConnectionException& e) {
			DEBUG("Network error during transmission: %s\n", e.what());
			register_error(STATUS_UNICAST_CONNECTION_ERROR,
				"Network error during transmission: %s\n", e.what());
			return -1;
		}
	}
}
