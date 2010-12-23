#include <string.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/param.h> // for MAXPATHLEN
#include <sys/time.h> // for gettimeofday
#include <poll.h>

#include <pthread.h>

#include <vector>
#include <algorithm>

using namespace std;

#include "multicast_sender.h"

// A helper function that chooses a UDP port and binds socket with it
uint16_t MulticastSender::choose_ephemeral_port() {
	uint16_t ephemeral_port;
	struct sockaddr_in ephemeral_addr;
	memset(&ephemeral_addr, 0, sizeof(ephemeral_addr));
	// FIXME: another address should be here
	ephemeral_addr.sin_family = AF_INET;
	ephemeral_addr.sin_addr.s_addr = htonl(INADDR_ANY);
	int bind_result;
	int tries = MAX_PORT_CHOOSING_TRIES;
	do {
		// Randomly choose the ephemeral UDP port to use
		// FIXME: It can be not the best decisition
		ephemeral_port = 49152 + rand() % (65536 - 49152);
		ephemeral_addr.sin_port = htons(ephemeral_port);
		DEBUG("try ephemeral UDP port: %d\n", ephemeral_port);

		bind_result = bind(sock, (struct sockaddr *)&ephemeral_addr,
			sizeof(ephemeral_addr));
	} while (bind_result != 0 && tries-- > 0 && errno == EADDRINUSE);
	return bind_result == 0 ? ephemeral_port : 0;
}

// Register an error and finish the current task
void MulticastSender::register_error(uint8_t status, const char *fmt,
		const char *error) {
	char *error_message = (char *)malloc(strlen(fmt) + strlen(error) + 1);
	sprintf((char *)error_message, fmt, error);
	DEBUG("register error: %s\n", error_message);
	buff->multicast_sender.set_error(INADDR_NONE, error_message,
		strlen(error_message));
	buff->multicast_sender.status = status;
	// Finish the multicast sender
	submit_task();
}

/*
	This is the initialization routine tries to establish the multicast
	session with the destinations specified in dst. The return value
	is a vector of destinations the connection has not been established
	with.
*/
std::vector<Destination>* MulticastSender::session_init(
		const std::vector<Destination>& dst, int nsources) {
	// Clear the previous connection targets
	targets.clear();
	
	// Fill up the multicast address to be used in the connection
	memset(&target_address, 0, sizeof(target_address));
	target_address.sin_family = AF_INET;
	target_address.sin_addr.s_addr = address;
	target_address.sin_port = htons(port);

	if ((sock = socket(AF_INET, SOCK_DGRAM, 0)) == -1) {
		ERROR("Can't create a UDP socket: %s\n", strerror(errno));
		register_error(STATUS_UNKNOWN_ERROR, "Can't create a UDP socket: %s",
			strerror(errno));
		return NULL;
	}

	// Choose a UDP port
	errno = 0;
	uint16_t ephemeral_port = choose_ephemeral_port();
	if (ephemeral_port == 0) {
		ERROR("Can't choose an ephemeral port: %s\n", strerror(errno));
		register_error(STATUS_UNKNOWN_ERROR,
			"Can't choose an ephemeral port: %s", strerror(errno));
		return NULL;
	}
	DEBUG("Ephemeral port for the new connection is: %u\n", ephemeral_port);

	// Compose the session initialization message
	unsigned init_message_length = sizeof(MulticastMessageHeader) +
		dst.size() * sizeof(MulticastHostRecord);
	uint8_t init_message[init_message_length];
	session_id = getpid();
	MulticastMessageHeader *mmh =
		new(init_message)MulticastMessageHeader(MULTICAST_INIT_REQUEST, session_id);

	MulticastHostRecord *hr = (MulticastHostRecord *)(mmh + 1);
	MulticastHostRecord *hr_begin = hr;
	for (uint32_t i = 0; i < dst.size(); ++i) {
		hr->set_addr(dst[i].addr);
		++hr;
	}
	MulticastHostRecord *hr_end = hr;

	// Start the session initialization procedure
	vector<Destination> *result = new vector<Destination>(dst);
	struct timeval tprev;
	struct timeval tcurr;
	gettimeofday(&tprev, NULL);
	--tprev.tv_sec;

	struct pollfd pfds;
	memset(&pfds, 0, sizeof(pfds));
	pfds.fd = sock;
	pfds.events = POLLIN;

	int replies_before = -1;
	int replies_now;
	// Send the session initialization message MAX_INITIALIZATION_RETRIES times
	// or unil there will be no replies by two successive session initialization
	// messages
	for(unsigned i = 0; i < MAX_INITIALIZATION_RETRIES; ++i) {
		replies_now = 0;
		gettimeofday(&tprev, NULL);
		udp_send(init_message, init_message_length, 0);

		// Send the init_message and wait for the replies
		do {
			gettimeofday(&tcurr, NULL);
			register unsigned time_difference =
				(tcurr.tv_sec - tprev.tv_sec) * 1000000 + tcurr.tv_usec - tprev.tv_usec;
			time_difference = INIT_RETRANSMISSION_RATE - time_difference;
			if (time_difference < 0) { time_difference = 0; }

			DEBUG("Time to sleep: %d\n", time_difference);
			int poll_result;
			if ((poll_result = poll(&pfds, 1, time_difference / 1000)) > 0) {
				uint8_t buffer[UDP_MAX_LENGTH];
				struct sockaddr_in client_addr;
				socklen_t client_addr_len = sizeof(client_addr);
				int length = recvfrom(sock, buffer, UDP_MAX_LENGTH, 0,
					(struct sockaddr*)&client_addr, &client_addr_len);
				if (length < (int)sizeof(MulticastMessageHeader)) {
					continue;
				}

				// TODO: do something with errors about used ports
				MulticastMessageHeader *mih = (MulticastMessageHeader *)buffer;
				if (mih->get_message_type() != MULTICAST_INIT_REPLY ||
						mih->get_session_id() != session_id) {
					DEBUG("Incorrect reply of length %d received\n", length);
					// Silently skip the message
					continue;
				} else {
					DEBUG("Received reply of length %d\n", length);
					++replies_now;
					// Parse the reply message and remove the received destinations
					uint32_t *p = (uint32_t *)(mih + 1);
					uint32_t *end = (uint32_t* )(buffer + length);
					// Need to store only the first address
					bool is_address_alredy_matched = false;
					do {
						MulticastHostRecord *found_record;
						MulticastHostRecord hr_p(ntohl(*p));
#ifndef NDEBUG
						char saddr[INET_ADDRSTRLEN];
						DEBUG("Reply from %s\n",
							inet_ntop(AF_INET, p, saddr, sizeof(saddr)));
#endif
						if ((found_record = find(hr_begin, hr_end, hr_p)) != hr_end) {
							if (!is_address_alredy_matched) {
								targets.push_back((*result)[found_record - hr_begin]);
								is_address_alredy_matched = true;
							}
							DEBUG("Host %s connected\n", saddr);
							// Exclude host from the message and the result
							--hr_end;
							swap(*found_record, *hr_end);
							swap((*result)[found_record - hr_begin], result->back());
							result->pop_back();
							init_message_length -= sizeof(MulticastHostRecord);
						}
						++p;
					} while (p < end);
					if (result->size() == 0) {
						// All the destinations responded
						goto finish_session_initialization;
					}
				}
			} else if (poll_result == 0) {
				// Time expired, send the next message
				break;
			} else {
				ERROR("poll: %s\n", strerror(errno));
				register_error(STATUS_UNKNOWN_ERROR, "poll: %s", strerror(errno));
				return NULL;
			}
		} while(1);
		// Finish procedure if there were no replies for two successive
		// retransmissions (for speed up reasons)
		if (replies_now == 0 && replies_before == 0) {
			break;
		} else {
			// TODO: correct the transmission rate using the number of replies
			// received
		}
		replies_before = replies_now;
	}

finish_session_initialization:
	// Set the new (ephemeral) port as the target port for the next messages
	target_address.sin_port = htons(ephemeral_port);

	if (send_queue != NULL) {
		delete(send_queue);
	}

	sort(result->begin(), result->end());
	sort(targets.begin(), targets.end());
	send_queue = new MulticastSendQueue(targets);

	return result;
}

// Routine that controls the multicast packets delivery,
// should be started in a separate thread
void MulticastSender::multicast_delivery_control() {
	uint8_t * const buffer = new uint8_t[UDP_MAX_LENGTH];
	const auto_ptr<uint8_t> buffer_guard(buffer);
	struct sockaddr_in client_addr;
	socklen_t client_addr_len = sizeof(client_addr);
	bool do_work = true;
	while (do_work) {
		int length = recvfrom(sock, buffer, UDP_MAX_LENGTH, 0,
			(struct sockaddr*)&client_addr, &client_addr_len);
		DEBUG("Received a message of length %d\n", length);
		if (length < 0) {
			// FIXME: do something else here, however this code is probably
			// unreachable
			ERROR("recvfrom returned: %s\n", strerror(errno));
			abort(); // FIXME: change this behavior
		} else if ((unsigned)length >= sizeof(MulticastMessageHeader)) {
			MulticastMessageHeader *mmh = (MulticastMessageHeader *)buffer;
			if (mmh->get_session_id() != session_id) {
				DEBUG("Incorrect session id: %u\n", mmh->get_session_id());
				continue;
			}

			vector<Destination>::const_iterator i = lower_bound(
				targets.begin(), targets.end(),
				Destination(mmh->get_responder(), NULL));
			if (i == targets.end() || i->addr != mmh->get_responder()) {
				char addr[INET_ADDRSTRLEN];
				uint32_t responder = htonl(mmh->get_responder());
				DEBUG("reply from an unknown host %s received\n",
					inet_ntop(AF_INET, &responder, addr, sizeof(addr)));
				continue;
			}
			switch (mmh->get_message_type()) {
				case MULTICAST_MESSAGE_RETRANS_REQUEST: {
						// TODO: somehow limit the rate of retransmissions
						DEBUG("Retransmission for the packet %u\n", mmh->get_number());
						size_t size;
						void *message = send_queue->get_message(&size, mmh->get_number());
						if (message != 0) {
							udp_send(message, size, 0);
						}
						break;
					}
				case MULTICAST_RECEPTION_CONFORMATION: {
						if (send_queue->acknowledge(mmh->get_number(),
								i - targets.begin()) != 0) {
							// Transmission finished
							do_work = false;
						}
						break;
					}
				default:
					DEBUG("Unexpected message type: %x\n", mmh->get_message_type());
					// FIXME: it should not be a fatal case
					continue;
			}
		} else {
			SDEBUG("Corrupted datagram received");
			continue;
		}
	}
}

// A wrapper function for multicast_delivery_control
void* MulticastSender::multicast_delivery_thread(void *arg) {
	MulticastSender *ms = (MulticastSender *)arg;
	ms->multicast_delivery_control();

	SDEBUG("multicast_delivery_control exited\n");
	return NULL;
}

// helper fuction that sends 'message' to the udp socket
void MulticastSender::udp_send(const void *message, int size, int flags) {
#ifndef NDEBUG
	char taddr[INET_ADDRSTRLEN];
	DEBUG("send a udp message %d, %d bytes to %s:%d\n",
		((MulticastMessageHeader *)message)->get_number(),
		size, inet_ntop(AF_INET, &target_address.sin_addr, taddr, sizeof(taddr)),
		ntohs(target_address.sin_port));
#endif
	int sendto_result;
	while ((sendto_result = sendto(sock, message, size, 0,
			(struct sockaddr *)&target_address, sizeof(target_address))) == -1 &&
			errno == ENOBUFS) {
		SDEBUG("ENOBUFS error occurred\n");
		usleep(1000); // FIXME: use poll or smth else her
	}
	if (sendto_result < 0) {
		ERROR("sendto returned: %s\n", strerror(errno));
		abort(); // FIXME: figure out something in this case
	}
}

// A helper fuction which reliably sends message to the multicast connection
void MulticastSender::mcast_send(const void *message,
		int size, int flags) {
#ifndef NDEBUG
	char dst_addr[INET_ADDRSTRLEN];
	DEBUG("Send %d bytes to %s:%d\n", size,
		inet_ntop(AF_INET, &target_address.sin_addr, dst_addr, sizeof(dst_addr)),
		ntohs(target_address.sin_port));
#endif

	MulticastMessageHeader *mmh = (MulticastMessageHeader *)message;
	mmh->set_number(next_message);
	++next_message;
	// Add noreply frames to implement a part of the control flow (don't shure
	// that it will be useful
	if (next_responder < targets.size()) {
		mmh->set_responder(targets[next_responder].addr);
	} else {
		mmh->set_responder(MULTICAST_UNEXISTING_RESPONDER);
	}
	next_responder = (next_responder + 1) % (targets.size() +
		2/* FIXME: definitely not 2 */);

	// Store message for the possibility of the future retransmissions.
	// (can block)
	struct timeval current_time;
	gettimeofday(&current_time, NULL);
	struct timespec next_retrans_time;
	TIMEVAL_TO_TIMESPEC(&current_time, &next_retrans_time);
	unsigned from_position = 0;
	while (send_queue->store_message(message, size, &next_retrans_time) !=
			0) {
		size_t size;
		void *message = send_queue->get_unacknowledged_message(&size,
			&from_position);
		if (message != NULL) {
			DEBUG("Retransmit message %u\n",
				((MulticastMessageHeader *)message)->get_number());
			udp_send(message, size, flags);
		} else {
			// All the retransmissions done, wait for the replies, than restart
			// the retransmissions
			gettimeofday(&current_time, NULL);
			TIMEVAL_TO_TIMESPEC(&current_time, &next_retrans_time);
			// FIXME: use a constant here 
			next_retrans_time.tv_nsec += 200000000;
			if (next_retrans_time.tv_nsec >= 1000000000) {
				next_retrans_time.tv_nsec = next_retrans_time.tv_nsec % 1000000000;
				next_retrans_time.tv_sec += 1;
			}
		}
	}
	udp_send(message, size, flags);
}

// Sends file through the multicast connection 
void MulticastSender::send_file() {
#ifndef NDEBUG
	int total = 0;
#endif
	unsigned count = get_data();
	count = std::min(count, (unsigned)(MAX_UDP_PACKET_SIZE -
		sizeof(MulticastMessageHeader))); 
	while (count > 0) {
#ifdef BUFFER_DEBUG
		DEBUG("Sending %d bytes of data\n", count);
#endif
		// TODO: attach a header
		uint32_t message[MAX_UDP_PACKET_SIZE];
		MulticastMessageHeader *mmh =
			new(message)MulticastMessageHeader(MULTICAST_FILE_DATA, session_id);
		memcpy(mmh + 1, pointer(), count);
		mcast_send(message, sizeof(MulticastMessageHeader) + count, 0);
#ifndef NDEBUG
		total += count;
#endif
#ifdef BUFFER_DEBUG
		DEBUG("overall (%d) bytes of data sent\n", total);
#endif
		update_position(count);

		count = get_data();
		count = std::min(count, (unsigned)(MAX_UDP_PACKET_SIZE -
			sizeof(MulticastMessageHeader)));
	}
	DEBUG("send_file: %d bytes wrote\n", total);
}

/*
	This is the main routine of the multicast sender. It sends
	files and directories to destinations.
*/
int MulticastSender::session() {
	int error;
	pthread_t routine;
	if ((error = pthread_create(&routine, NULL, multicast_delivery_thread,
			this)) != 0) {
		ERROR("Can't create a new thread: %s\n", strerror(error));
		// TODO: Set the error here
		return -1;
	}

	// Send nsources and the target paths
	// Compose the destinations path messages
	uint8_t targets_message[MAX_UDP_PACKET_SIZE];
	MulticastMessageHeader *mmh =
		new(targets_message) MulticastMessageHeader(MULTICAST_TARGET_PATHS,
		session_id);
	uint32_t *nsources_p = (uint32_t *)(mmh + 1);
	*nsources_p = htonl(nsources);
	uint8_t *t_curr = (uint8_t *)(nsources_p + 1);
	uint8_t *t_end = targets_message + sizeof(targets_message);
	for (unsigned i = 0; i < targets.size(); ++i) {
		size_t fname_length;
		if (targets[i].filename == NULL) {
			fname_length = 0;
		} else {
#ifdef HAS_TR1_MEMORY
			fname_length = strlen(targets[i].filename.get()); 
#else
			fname_length = strlen(targets[i].filename); 
#endif
		}
		unsigned length = sizeof(DestinationHeader) + fname_length;
		if (length > MAX_UDP_PACKET_SIZE - sizeof(MulticastMessageHeader)) {
#ifdef HAS_TR1_MEMORY
			ERROR("Filename %s is too long\n", targets[i].filename.get());
#else
			ERROR("Filename %s is too long\n", targets[i].filename);
#endif
			abort();
		}
		if (t_curr + length >= t_end) {
			// Send the message
			mcast_send(targets_message, t_curr - targets_message, 0);
			t_curr = (uint8_t *)(nsources_p + 1);
		}
		DestinationHeader *h = new(t_curr)
			DestinationHeader(targets[i].addr, fname_length);
		t_curr = (uint8_t *)(h + 1);
#ifdef HAS_TR1_MEMORY
		memcpy(t_curr, targets[i].filename.get(), fname_length);
#else
		memcpy(t_curr, targets[i].filename, fname_length);
#endif
		t_curr += fname_length;
	}
	if (t_curr != (uint8_t *)(nsources_p + 1)) {
		mcast_send(targets_message, t_curr - targets_message, 0);
	}
	SDEBUG("Destinations sent\n");

	try {
		bool is_trailing = false; // Whether the current task is the trailing one
		do {
			// Get the operation from the queue
			Distributor::TaskHeader *op = get_task();
			if (op->fileinfo.is_trailing_record()) {
				// Send the MULTICAST_TERMINATION_REQUEST message
				size_t size;
				void *message = send_queue->prepare_termination(&size, session_id);
				mcast_send(message, size, 0);

#define MAX_NUMBER_OF_TERMINATION_RETRANS 6
				int retrans_number = 1;
				struct timeval current_time;
				gettimeofday(&current_time, NULL);
				struct timespec next_retrans_time;
				TIMEVAL_TO_TIMESPEC(&current_time, &next_retrans_time);
				next_retrans_time.tv_nsec += TERMINATE_RETRANSMISSION_RATE;
				if (next_retrans_time.tv_nsec >= 1000000000) {
					next_retrans_time.tv_nsec = next_retrans_time.tv_nsec % 1000000000;
					next_retrans_time.tv_sec += 1;
				}
				while (send_queue->wait_for_destinations(&next_retrans_time) != 0 &&
						retrans_number <= MAX_NUMBER_OF_TERMINATION_RETRANS) {
					DEBUG("Retranssion %u of the MULTICAST_TERMINATION_REQUEST\n",
						retrans_number);
					udp_send(message, size, 0);
					++retrans_number;

					gettimeofday(&current_time, NULL);
					TIMEVAL_TO_TIMESPEC(&current_time, &next_retrans_time);
					next_retrans_time.tv_nsec += TERMINATE_RETRANSMISSION_RATE;
					if (next_retrans_time.tv_nsec >= 1000000000) {
						next_retrans_time.tv_nsec = next_retrans_time.tv_nsec % 1000000000;
						next_retrans_time.tv_sec += 1;
					}
				}

				if (retrans_number > MAX_NUMBER_OF_TERMINATION_RETRANS) {
					SERROR("Can't finish multicast session: too many retransmissions of "
						"the termination request message\n");
					register_error(STATUS_TOO_MANY_RETRANSMISSIONS,
						"Can't finish multicast session: %s",
						"too many retransmissions of the termination request message");
					return -1;
				}
				is_trailing = true;
			} else {
				assert(op->fileinfo.get_name_length() <= MAXPATHLEN);
			
				// Send the file info structure
				uint8_t file_record[sizeof(MulticastMessageHeader) +
					sizeof(FileInfoHeader) + op->fileinfo.get_name_length()];
				MulticastMessageHeader *mm_h =
					new(file_record)MulticastMessageHeader(MULTICAST_FILE_RECORD,
					session_id);
				FileInfoHeader *file_h = (FileInfoHeader *)(mm_h + 1);
				*file_h = op->fileinfo;
#ifndef NDEBUG
				SDEBUG("fileinfo: ");
				file_h->print();
#endif
				memcpy(file_h + 1, op->filename.c_str(),
					op->fileinfo.get_name_length());
				mcast_send(file_record, sizeof(file_record), 0);
				// TODO: send the dirname in this messsage (for possibility of
				// delayed retransmission or wait for some condition after the
				// if block

				if (op->fileinfo.get_type() == resource_is_a_file) {
					DEBUG("Send file: %s\n", op->filename.c_str());
					// Send the file
					send_file();
					// Send the file trailing record
#ifndef NDEBUG
					SDEBUG("Send the checksum: ");
					MD5sum::display_signature(stdout, checksum()->signature);
					printf("\n");
#endif
					uint8_t file_trailing[sizeof(MulticastMessageHeader) +
						sizeof(checksum()->signature)];
					MulticastMessageHeader *mmh = 
						new(file_trailing)MulticastMessageHeader(MULTICAST_FILE_TRAILING,
						session_id);
					memcpy(mmh + 1, checksum()->signature, sizeof(checksum()->signature));
					mcast_send(file_trailing, sizeof(file_trailing), 0);
				}
			}
			submit_task();
		} while (!is_trailing);
	} catch (std::exception& e) {
		DEBUG("Exception caught: %s\n", e.what());
		return -1;
	}
	return 0;
}
