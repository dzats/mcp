#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <signal.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <sys/param.h> // for MAXPATHLEN
#include <sys/wait.h>
#include <sys/ioctl.h>
#include <poll.h>
#include <pthread.h>

#include <fcntl.h>

#include <assert.h>

#include <vector>
#include <set>
#include <functional>
#include <algorithm>

#include "multicast_receiver.h"

#include "connection.h"
#include "log.h"

#ifndef INT32_MAX // should be included through stdint.h somehow else
#define INT32_MAX 0x7fffffff
#endif

using namespace std;

// Get next value for the message retransmission timeout
unsigned MulticastReceiver::get_packet_retrans_timeout(unsigned prev_timeout)
{
	// FIXME: figure out a better way
	double factor = (double)rand() / RAND_MAX;
	unsigned timeout = (unsigned)(prev_timeout + MIN_PACKET_RETRANS_TIMEOUT +
		MAX_PACKET_RETRANS_LINEAR_ADD * factor + prev_timeout * factor);

	if (timeout > MAX_PACKET_RETRANS_TIMEOUT) {
		timeout = MAX_PACKET_RETRANS_TIMEOUT;
	}
	return timeout;
}

// Get next value for the file retransmission timeout
unsigned MulticastReceiver::get_file_retrans_timeout(unsigned prev_timeout)
{
	// FIXME: figure out a better way
	double factor = (double)rand() / RAND_MAX;
	unsigned timeout = (unsigned)(prev_timeout + MIN_FILE_RETRANS_TIMEOUT +
		MAX_FILE_RETRANS_LINEAR_ADD * factor + prev_timeout * factor);

	if (timeout > MAX_FILE_RETRANS_TIMEOUT) {
		timeout = MAX_FILE_RETRANS_TIMEOUT;
	}
	return timeout;
}

// Sends a UDP datagram to the multicast sender
void MulticastReceiver::send_datagram(void *data, size_t length)
{
	MulticastMessageHeader *mmh = (MulticastMessageHeader *)data;
	mmh->set_responder(local_address);
	int sendto_result;
	do {
		sendto_result = sendto(sock, data, length, 0,
			(struct sockaddr *)&source_addr, sizeof(source_addr));
	} while (sendto_result < 0 && errno == ENOBUFS);
	if (sendto_result < 0) {
		ERROR("Can't send a reply: %s\n", strerror(errno));
		// FIXME: do something else here
		abort();
	}
}

// Send a reply back to the source
void MulticastReceiver::send_reply(uint32_t number)
{
	DEBUG("Send a reply for the packet %u\n", number);
	MulticastMessageHeader mmh(MULTICAST_RECEPTION_CONFORMATION, session_id);
	mmh.set_number(number);
	send_datagram(&mmh, sizeof(mmh));
}

// This method implements something like the TCP's TIME_WAIT state. It waits
// for possible retransmissions of the MULTICAST_TERMINATION_REQUEST message.
void MulticastReceiver::time_wait(uint8_t * const buffer)
{
	struct pollfd pfd;
	pfd.fd = sock;
	pfd.events = POLLIN;
	int poll_result;

	struct sockaddr_in client_addr;
	socklen_t client_addr_len = sizeof(client_addr);

	int timeout = MULTICAST_FINAL_TIMEOUT;
	struct timeval initial_time;
	gettimeofday(&initial_time, NULL);
	while (1) {
		DEBUG("Wait for a session termination message %u milliseconds\n",
			timeout);
		poll_result = poll(&pfd, 1, timeout);
		if (poll_result == 0) {
			// Timeout expired, transmission done
			pthread_exit(NULL);
		} else if (poll_result > 0) {
			// Skip everything except the MULTICAST_TERMINATION_REQUEST message
			int len = recvfrom(sock, buffer, UDP_MAX_LENGTH, 0,
				(struct sockaddr *)&client_addr, &client_addr_len);
			if (len >= (int)sizeof(MulticastMessageHeader)) {
				MulticastMessageHeader *mmh = (MulticastMessageHeader *)buffer;
				// Check the session id and the source address
				if (client_addr.sin_addr.s_addr == source_addr.sin_addr.s_addr &&
						mmh->get_session_id() == session_id &&
						mmh->get_message_type() == MULTICAST_TERMINATION_REQUEST) {
					// Search whether the local address contained in this message
					uint32_t *hosts_begin = (uint32_t *)(mmh + 1);
					uint32_t *hosts_end = (uint32_t *)(buffer + len); 
					uint32_t *i = find(hosts_begin, hosts_end,
						htonl(local_address));
					if (i != hosts_end) {
						// Reply to the message and reset the timeout
						DEBUG("Reply for the session termination message (%u)\n",
							mmh->get_number());
						send_reply(mmh->get_number());
						gettimeofday(&initial_time, NULL);
					}
				}
			} else if (len < 0) {
				perror("recv_from error");
				exit(EXIT_FAILURE);
			} else {
				DEBUG("Incorrect message of length %d received\n", len);
				// Simply ignore the message
				continue;
			}
		} else {
			perror("poll error");
		}
		// Recalculate the timeout value
		struct timeval now;
		gettimeofday(&now, NULL);
		timeout = MULTICAST_FINAL_TIMEOUT -
			(now.tv_sec - initial_time.tv_sec) * 1000 -
			(now.tv_usec - initial_time.tv_usec) / 1000;
		if (timeout < 0) {
			timeout = 0;
		}
	}
}

// This routine reads data from the connection and put it into message_queue
void MulticastReceiver::read_data()
{
	// Wait for incoming connections
	uint8_t * const buffer = (uint8_t *)malloc(UDP_MAX_LENGTH); // maximum UDP
	const auto_ptr<uint8_t> buffer_guard(buffer);
	
	// Join socket to the multicast group
	struct ip_mreq mreq;
	struct in_addr maddr;
	maddr.s_addr = inet_addr(DEFAULT_MULTICAST_ADDR);
	memcpy(&mreq.imr_multiaddr, &maddr, sizeof(maddr));
	mreq.imr_interface.s_addr = ntohl(interface_address);

	if (setsockopt(sock, IPPROTO_IP, IP_ADD_MEMBERSHIP, &mreq,
			sizeof(mreq)) != 0) {
		char addr[INET_ADDRSTRLEN];
		uint32_t i_addr = ntohl(interface_address);
		DEBUG("Can't join the multicast group " DEFAULT_MULTICAST_ADDR " on %s: %s",
			inet_ntop(AF_INET, &i_addr, addr, sizeof(addr)),
			strerror(errno));
		exit(EXIT_FAILURE);
	}

	struct pollfd pfd;
	pfd.fd = sock;
	pfd.events = POLLIN;
	int total_time_slept;
	bool is_termination_request_received = false;
	uint32_t termination_request_number;

	struct sockaddr_in client_addr;
	socklen_t client_addr_len = sizeof(client_addr);
	SDEBUG("read_data started\n");
	while (1) {
		if (is_termination_request_received && missed_packets.size() == 0) {
			message_queue.wait_termination_synchronization();
			if (error_queue.get_n_errors() == 0) {
				// All work is done.
				DEBUG("Reply for the session termination message (%u)\n",
					termination_request_number);
				send_reply(termination_request_number);
				// move to something like the TCP's TIME_WAIT state.
				time_wait(buffer);
			}
		}

		if (missed_packets.size() + error_queue.get_n_errors() > 0) {
			total_time_slept = 0;
			while(1) {
				// FIXME: error check ???
				int time_to_sleep;
				unsigned time_passed;
				enum WhatToSend {send_message_retrans_request, send_error};
				WhatToSend what_to_send;
				if (error_queue.get_n_errors() > 0) {
					what_to_send = send_error;
					list<MulticastErrorQueue::ErrorMessage*>::iterator it =
						error_queue.get_error();
					MulticastErrorQueue::ErrorMessage *frr = *it;
	
					struct timeval current_time;
					gettimeofday(&current_time, NULL);
					time_passed =
						(current_time.tv_sec - frr->timestamp.tv_sec) * 1000 +
						(current_time.tv_usec - frr->timestamp.tv_usec) / 1000;
		
					DEBUG("Time passed (error): %u\n", time_passed);
					if (time_passed > frr->retrans_timeout) {
						time_to_sleep = 0;
					} else {
						time_to_sleep = frr->retrans_timeout - time_passed;
					}
					DEBUG("Time before the next retransmission (error): %u\n",
						time_to_sleep);
				} else if (missed_packets.size() > 0) {
					what_to_send = send_message_retrans_request;
					PacketRetransRequest *p = &missed_packets.front();
					struct timeval current_time;
					gettimeofday(&current_time, NULL);
					time_passed =
						(current_time.tv_sec - p->timestamp.tv_sec) * 1000 +
						(current_time.tv_usec - p->timestamp.tv_usec) / 1000;
		
					DEBUG("Time passed (mp): %u\n", time_passed);
					if (time_passed > p->retrans_timeout) {
						time_to_sleep = 0;
					} else {
						time_to_sleep = p->retrans_timeout - time_passed;
					}
					DEBUG("Time before the next retransmission (mp): %u\n", time_to_sleep);
				}
	
				int poll_result;
				if (time_to_sleep == 0 ||
						(poll_result = poll(&pfd, 1, time_to_sleep)) == 0) {
					// Send a file/message retransmission request
					total_time_slept += time_to_sleep;
					if (total_time_slept >= (int)MAX_IDLE_TIME) {
						char addr[INET_ADDRSTRLEN];
						ERROR("Multicast connection with host the %s timed out\n",
							inet_ntop(AF_INET, &source_addr.sin_addr.s_addr, addr,
							sizeof(addr)));
						exit(EXIT_FAILURE);
					}
					if (what_to_send == send_message_retrans_request) {
						PacketRetransRequest *p = &missed_packets.front();
						MulticastMessageHeader mmh(MULTICAST_MESSAGE_RETRANS_REQUEST,
							session_id);
						mmh.set_number(p->number);
						mmh.set_responder(local_address);
						DEBUG("Send a retransmission request for the packet %u\n",
							p->number);
						send_datagram(&mmh, sizeof(mmh));

						gettimeofday(&p->timestamp, NULL);
						p->retrans_timeout = get_packet_retrans_timeout(p->retrans_timeout);
						DEBUG("New timeout: %u\n", p->retrans_timeout);
						missed_packets.push_back(*p);
						missed_packets.pop_front();
					} else {
						assert(what_to_send == send_error);
						list<MulticastErrorQueue::ErrorMessage*>::iterator it =
							error_queue.get_error();
						MulticastErrorQueue::ErrorMessage *frr = *it;
						DEBUG("Send a file retransmission request %u\n",
							((MulticastMessageHeader *)frr->message)->get_number());
						if (sendto(sock, frr->message, frr->message_size, 0,
							(struct sockaddr *)&source_addr, sizeof(source_addr)) < 0) {
							ERROR("Can't send a file retransmission request: %s\n",
								strerror(errno));
							abort();
						}
						gettimeofday(&frr->timestamp, NULL);
						frr->retrans_timeout = get_file_retrans_timeout(frr->retrans_timeout);
						DEBUG("New timeout: %u\n", frr->retrans_timeout);
						error_queue.move_back(it);
					}
				} else if (poll_result > 0) {
					// There are some input available, this is the only exit from
					// the infinite loop
					break;
				} else {
					perror("poll error");
				}

				assert(missed_packets.size() + error_queue.get_n_errors() > 0);
			}
		} else {
			// Poll here is to kill idle connections
			int poll_result;
			poll_result = poll(&pfd, 1, MAX_IDLE_TIME);
			if (poll_result <= 0) {
				if (poll_result < 0) {
					perror("poll error");
				} else {
					char addr[INET_ADDRSTRLEN];
					ERROR("Multicast connection with the host %s timed out\n",
						inet_ntop(AF_INET, &source_addr.sin_addr.s_addr, addr,
						sizeof(addr)));
					exit(EXIT_FAILURE);
				}
			}
		}

		int len = recvfrom(sock, buffer, UDP_MAX_LENGTH, 0,
			(struct sockaddr *)&client_addr, &client_addr_len);
		
		if (len >= (int)sizeof(MulticastMessageHeader)) {
			MulticastMessageHeader *mmh = (MulticastMessageHeader *)buffer;
#ifndef NDEBUG
			{
				char sender_a[INET_ADDRSTRLEN];
				char responder_a[INET_ADDRSTRLEN];
				in_addr responder_addr = {htonl(mmh->get_responder())};
				DEBUG("Received %u(%u):%s from %s, length: %d, port: %u\n",
					mmh->get_number(), next_message_expected,
					inet_ntop(AF_INET, &responder_addr, responder_a, sizeof(responder_a)),
					inet_ntop(AF_INET, &client_addr.sin_addr, sender_a, sizeof(sender_a)),
					len, ntohs(client_addr.sin_port));
			}
#endif

#if 0
		if(rand() % 17 == 1) {
			SDEBUG("Skip the message (for debugging purpose)\n");
			continue;
		}
#endif
			// Check the session id and the source address
			if (client_addr.sin_addr.s_addr != source_addr.sin_addr.s_addr ||
					mmh->get_session_id() != session_id) {
				DEBUG("Incorrect session id (%d) or source address in the message\n",
					mmh->get_session_id());
				continue;
			}

			if (mmh->get_message_type() == MULTICAST_RECEPTION_CONFORMATION) {
				// Remove the corresponding pendinig error
				DEBUG("Conformation for the message: %d\n", mmh->get_number());
				error_queue.remove(mmh->get_number());
				continue;
			}

			// Store the message to be processed (may be later)
			message_queue.put_message(buffer, len, mmh->get_number());

			uint32_t message_num = mmh->get_number();
			if (next_message_expected != message_num) {
				if (cyclic_less(message_num, next_message_expected)) {
					// A retransmission received
					list<PacketRetransRequest>::iterator i;
					DEBUG("Retransmission received, number "
						"of missed messages: %zu\n", missed_packets.size());
					if ((i = find(missed_packets.begin(), missed_packets.end(),
							PacketRetransRequest(message_num))) != missed_packets.end()) {
						missed_packets.erase(i);
					}
					// Send the pending replies
					while (!is_termination_request_received &&
							pending_replies.size() > 0 &&
							cyclic_less(pending_replies.front(),
							missed_packets.front().number)) {
						send_reply(pending_replies.front());
						pending_replies.pop_front();
					}
					DEBUG("missed messages after: %zu\n", missed_packets.size());
				} else {
					// Some packets lost
					for (; next_message_expected != message_num;
						++next_message_expected) {
						DEBUG("Packet %u lost\n", next_message_expected);
						missed_packets.push_back(PacketRetransRequest(next_message_expected));
						DEBUG("Number of pending retransmissions: %zu\n",
							missed_packets.size());
					}
					next_message_expected++;
				}
			} else {
				next_message_expected++;
			}

			DEBUG("type: %x (%x)\n", mmh->get_message_type(),
				MULTICAST_TERMINATION_REQUEST);
			if (mmh->get_message_type() == MULTICAST_TERMINATION_REQUEST) {
				// Search whether the local address contained in this message
				uint32_t *hosts_begin = (uint32_t *)(mmh + 1);
				uint32_t *hosts_end = (uint32_t *)(buffer + len); 
				uint32_t *i = find(hosts_begin, hosts_end,
					htonl(local_address));
				if (i != hosts_end) {
					// The host is supposed to reply to this message
					SDEBUG("Termination request received\n");
					is_termination_request_received = true;
					termination_request_number = mmh->get_number();
				}
			} else {
				// FIXME: check the message type here
				// Reply to the message or schedule the reply after some 
				// lost packets will be received.
				if (mmh->get_responder() == local_address &&
						!is_termination_request_received) {
					// Send the reply if there were no packets lost
					if (missed_packets.size() == 0 ||
							cyclic_greater(missed_packets.front().number, message_num)) {
						// Clear the pending replies
						while (pending_replies.size() > 0 &&
								cyclic_less(pending_replies.front(), message_num)) {
							pending_replies.pop_front();
						}
						send_reply(message_num);
					} else {
						// There are some lost packets, we can't send the reply now
						pending_replies.insert(
							find_if(pending_replies.begin(), pending_replies.end(),
							bind2nd(greater<uint32_t>(), message_num)),
							message_num);
					}
				}
			}
		} else if (len < 0) {
			perror("recv_from error");
			exit(EXIT_FAILURE);
		} else {
			DEBUG("Incorrect message of length %d received\n", len);
			// Simply ignore the message
			continue;
		}
	}
}

// Wrapper function to run the read_data method in a separate thread
void* MulticastReceiver::read_data_wrapper(void *arg)
{
	MulticastReceiver *mr = (MulticastReceiver *)arg;
	mr->read_data(); 

	SDEBUG("read_data_wrapper exited\n");
	return NULL;
}

// The routine reads data from the connection and pass it to the 
// process_data routine
void MulticastReceiver::session()
{
	// Start the read_data routine in a separate thread
	int error;
	pthread_t read_data_thread;
	error = pthread_create(&read_data_thread, NULL, read_data_wrapper, this);
	if (error != 0) {
		ERROR("Can't create a new thread: %s\n", strerror(errno));
		// TODO: Set the error here
		exit(EXIT_FAILURE);
	}

	// Read and process the data
	while (1) {
		size_t length;
		MulticastMessageHeader *mmh =
			(MulticastMessageHeader *)message_queue.get_message(&length);
		// Do something with the message
		switch (mmh->get_message_type()) {
			case MULTICAST_TARGET_PATHS: {
					// Search the target path
					SDEBUG("MULTICAST_TARGET_PATHS message received\n");
					if (path != NULL) {
						SDEBUG("Path is already detected, don't parse the message\n");
						break;
					}
					uint8_t *end_of_message = (uint8_t *)mmh + length;
					uint32_t *nsources_p = (uint32_t *)(mmh + 1);
					nsources = ntohl(*nsources_p);
					DestinationHeader *p = (DestinationHeader *)(nsources_p + 1);
					while((uint8_t *)p + sizeof(DestinationHeader) <= end_of_message) {
						DEBUG("%x (%x)\n", p->get_addr(), local_address);
						if (p->get_addr() == local_address) {
							// Figure out the local path
							assert(p->get_path_length() <=
								end_of_message - (uint8_t *)p - sizeof(*p));
							uint32_t pathlen = min(p->get_path_length(),
								(uint32_t)(end_of_message - (uint8_t *)p - sizeof(*p)));
							path = (char *)malloc(pathlen + 1);
							memcpy(path, p + 1, pathlen);
							path[pathlen] = '\0';
							char *error;
							path_type = get_path_type(path, &error, nsources);
							if (path_type == path_is_invalid) {
								//register_error(STATUS_FATAL_DISK_ERROR, error);
								DEBUG("%s\n", error);
								abort(); // Report about an error somehow
								free(error);
							}
							break;
						}
						p = (DestinationHeader *)((uint8_t *)p + sizeof(*p) +
							p->get_path_length());
					}
					break;
				}
			case MULTICAST_FILE_RECORD: {
					DEBUG("MULTICAST_FILE_RECORD(%d) message received\n",
						mmh->get_number());
					if (path == NULL) {
						SERROR("MULTICAST_TARGET_PATHS message has not beet received.\n");
						abort();
					}
					FileInfoHeader *fih = (FileInfoHeader *)(mmh + 1);
					file_info_header = *fih;
					if (fih->get_name_length() >= MAXPATHLEN) {
						throw ConnectionException(
							ConnectionException::corrupted_data_received);
					}
					// Read the file name
					if (filename != NULL) {
						free(filename);
					}
					filename = (char *)malloc(fih->get_name_length() + 1);
					memcpy(filename, (char *)(fih + 1), fih->get_name_length());
					filename[fih->get_name_length()] = 0;
			
					if (fih->get_type() == resource_is_a_file) {
						DEBUG("File: %s(%s:%u)\n", path, filename,
							fih->get_name_offset());
						const char *target_name = get_targetfile_name(
							filename + fih->get_name_offset(), path, path_type);

						// Open the output file
						DEBUG("open the file: %s\n", target_name);
						if (fd != -1) {
							close(fd);
						}
						fd = open(target_name, O_WRONLY | O_CREAT | O_TRUNC,
							fih->get_mode());
						int error = errno;
						if (fd == -1 && errno == EACCES && unlink(target_name) == 0) {
							// A read only file with the same name exists,
							// the default bahavior is to overwrite it.
							fd = open(target_name, O_WRONLY | O_CREAT | O_TRUNC,
								fih->get_mode());
						}
						if (fd == -1) {
							// Report about the error
							DEBUG("Can't open the output file %s(%o): %s\n", target_name,
								fih->get_mode(), strerror(error));
							//register_input_output_error("Can't open the output file %s: %s",
								//filename + fih->get_name_offset(), strerror(error));
							free_targetfile_name(target_name, path_type);
							assert(0);
						}
						free_targetfile_name(target_name, path_type);
					} else {
						DEBUG("Directory: %s(%s) (%o)\n", path,
							filename + fih->get_name_offset(), fih->get_mode());
						// TODO: Create the directory
						const char *dirname = get_targetdir_name(
							filename + fih->get_name_offset(), path, &path_type);
						if (dirname == NULL) {
							//register_input_output_error("The destination path %s is a file, "
								//"but source is a directory. Remove %s first", path, path);
							free_targetfile_name(dirname, path_type);
							assert(0);
						}
			
						// Create the directory
						if (mkdir(dirname, fih->get_mode()) != 0 && errno != EEXIST) {
							//register_input_output_error("Can't create directory: %s: %s",
							//	dirname, strerror(errno));
							free_targetfile_name(dirname, path_type);
							assert(0);
						}
						free_targetfile_name(dirname, path_type);
					}
				}
				break;
			case MULTICAST_FILE_DATA:
				DEBUG("MULTICAST_FILE_DATA(%d) message received\n",
					mmh->get_number());
				if (fd == -1) {
						// MULTICAST_FILE_RECORD message has not beet received
						assert(0);
				}
				if (length - sizeof(*mmh) > 0) {
					// Write the received data to the file
					size_t size = length - sizeof(*mmh);
					uint8_t *data = (uint8_t *)(mmh + 1);
					checksum.update(data, size);
					do {
						register int bytes_written = write(fd, data, size);
						if (bytes_written < 0) {
							ERROR("File write error: %s\n", strerror(errno));
							abort();
						} else {
							size -= bytes_written;
							data = data + bytes_written;
						}
					} while(size > 0);
				}
				break;
			case MULTICAST_FILE_TRAILING:
				DEBUG("MULTICAST_FILE_TRAILING(%d) message received\n",
					mmh->get_number());
				if (fd == -1) {
					SERROR("MULTICAST_FILE_TRAILING message without corresponding "
						"MULTICAST_FILE_RECORD message received\n");
					assert(0);
				}
				checksum.final();
				if (length < sizeof(*mmh) + sizeof(checksum.signature)) {
					SERROR("Incomplete MULTICAST_FILE_TRAILING message received\n");
					assert(0);
				}
#if 0
				if (rand() % 20 == 0) {
					*(uint8_t *)(mmh + 1) = '\0';
				}
#endif

#ifndef NDEBUG
				SDEBUG("Calculated checksum: ");
				checksum.display_signature(stdout, checksum.signature);
				printf("\n");
				SDEBUG("Received checksum: ");
				checksum.display_signature(stdout, (uint8_t *)(mmh + 1));
				printf("\n");
#endif
				if (memcmp(checksum.signature, (uint8_t *)(mmh + 1),
						sizeof(checksum.signature)) != 0) {
					// Request for the file retransmission
					ERROR("Incorrect checksum for the file %s, pending request for the "
						"retransmission\n", filename + file_info_header.get_name_offset());
					error_queue.add_retrans_request(filename, file_info_header,
						session_id, local_address);
				}
			
				close(fd);
				fd = -1;
				break;
			case MULTICAST_TERMINATION_REQUEST:
				// Finish work
				message_queue.signal_termination_synchronization();
				pthread_join(read_data_thread, NULL);
				return;
				break;
			default:
				SDEBUG("Error condition\n");
				abort();
		}
	}
}

