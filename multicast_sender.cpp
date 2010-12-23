#include <string.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/param.h> // for MAXPATHLEN
#include <poll.h>

#include <pthread.h>

#include <vector>
#include <set>
#include <algorithm>

using namespace std;

#include "multicast_sender.h"

// A helper function that chooses a UDP port and binds socket with it
uint16_t MulticastSender::choose_ephemeral_port()
{
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
		// FIXME: It can be not very good decisition
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
		const char *error)
{
	char *error_message = (char *)malloc(strlen(fmt) + strlen(error) + 1);
	sprintf((char *)error_message, fmt, error);
	DEBUG("register error: %s\n", error_message);
	reader->errors.add(new Reader::SimpleError(status, INADDR_NONE,
		error_message, strlen(error_message)));
	if (reader->multicast_sender.status < status) {
		reader->multicast_sender.status = status;
	}
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
		const std::vector<Destination>& dst, int nsources)
{
	// Clear the previous connection targets
	targets.clear();
	
	// Fill up the multicast address to be used in the connection
	memset(&target_address, 0, sizeof(target_address));
	target_address.sin_family = AF_INET;
	target_address.sin_addr.s_addr = address;
	target_address.sin_port = htons(port);

	sock = socket(AF_INET, SOCK_DGRAM, 0);
	if (sock == -1) {
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
				register_error(STATUS_UNKNOWN_ERROR, "poll error: %s", strerror(errno));
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
void MulticastSender::multicast_delivery_control()
{
	uint8_t * const buffer = new uint8_t[UDP_MAX_LENGTH];
	const auto_ptr<uint8_t> buffer_guard(buffer);
	struct sockaddr_in client_addr;
	socklen_t client_addr_len = sizeof(client_addr);
	bool do_work = true;
	while (do_work) {
		int length = recvfrom(sock, buffer, UDP_MAX_LENGTH, 0,
			(struct sockaddr*)&client_addr, &client_addr_len);
		DEBUG("Received a message of length %d\n", length);
		if ((unsigned)length >= sizeof(MulticastMessageHeader)) {
			MulticastMessageHeader *mmh = (MulticastMessageHeader *)buffer;
			if (mmh->get_session_id() != session_id) {
				DEBUG("Incorrect session id: %u\n", mmh->get_session_id());
				continue;
			}

			vector<Destination>::const_iterator i = lower_bound(
				targets.begin(), targets.end(),
				Destination(mmh->get_responder(), NULL));
			if (i == targets.end() || i->addr != mmh->get_responder()) {
#ifndef NDEBUG
				char addr[INET_ADDRSTRLEN];
				uint32_t responder = htonl(mmh->get_responder());
				DEBUG("reply from an unknown host %s received\n",
					inet_ntop(AF_INET, &responder, addr, sizeof(addr)));
#endif
				continue;
			}
			switch (mmh->get_message_type()) {
				case MULTICAST_MESSAGE_RETRANS_REQUEST: {
						// TODO: somehow limit the rate of retransmissions
						DEBUG("Retransmission request for the packet %u\n",
							mmh->get_number());
						size_t size;
						void *message = send_queue->get_message_for_retransmission(&size,
							mmh->get_number());
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
				case MULTICAST_ERROR_MESSAGE:
				case MULTICAST_FILE_RETRANS_REQUEST: {
						struct timeval tv;
						gettimeofday(&tv, NULL);
						ErrorMessage em = ErrorMessage(mmh->get_number(),
							mmh->get_responder(), tv);
#ifndef NDEBUG
						char addr[INET_ADDRSTRLEN];
						uint32_t source = htonl(mmh->get_responder());
						DEBUG("Error message(%x) %s:%u received\n", mmh->get_message_type(),
							inet_ntop(AF_INET, &source, addr, sizeof(addr)),
							mmh->get_number());
#endif
						// Send reply to the source
						MulticastMessageHeader reply(MULTICAST_RECEPTION_CONFORMATION,
							session_id);
						reply.set_number(mmh->get_number());
						reply.set_responder(mmh->get_responder());
						struct sockaddr_in dest;
						dest.sin_family = AF_INET;
						dest.sin_addr = client_addr.sin_addr;
						dest.sin_port = target_address.sin_port;
						// FIXME: change sendto to something more general
						int sendto_result;
						do {
							sendto_result = sendto(sock, &reply, sizeof(reply), 0,
								(struct sockaddr *)&dest, sizeof(dest));
						} while (sendto_result < 0 && errno == ENOBUFS);
						if (sendto_result < 0) {
							ERROR("sendto error: %s\n", strerror(errno));
							abort();
						}
						if (received_errors.find(em) == received_errors.end()) {
							SDEBUG("Add the message into the error queue\n");
							received_errors.insert(em);
							if (received_errors.size() >
									targets.size() * MAX_ERROR_QUEUE_SIZE_MULTIPLICATOR) {
								// Error queue is overflowed, remove an outdated error
								// FIXME: Choose the error to delete somehow else using
								// the timestamps
								received_errors.erase(received_errors.begin());
							}
							// Add message into the error queue
							if (mmh->get_message_type() == MULTICAST_ERROR_MESSAGE) {
								ReplyHeader *rh = (ReplyHeader *)(mmh + 1);
								reader->errors.add(new Reader::SimpleError(rh->get_status(),
									rh->get_address(), (char *)(rh + 1), rh->get_msg_length()));
							} else {
								assert(mmh->get_message_type() ==
									MULTICAST_FILE_RETRANS_REQUEST);
								FileInfoHeader *fih = (FileInfoHeader *)(mmh + 1);
								reader->errors.add(new Reader::FileRetransRequest(
									(char *)(fih + 1), // file name
									*fih, mmh->get_responder(), vector<Destination>())); 
							}
						}
						break;
					}
				default:
					DEBUG("Unexpected message type: %x\n", mmh->get_message_type());
					// FIXME: it should not be a fatal case
					continue;
			}
		} else if (length >= 0) {
			SDEBUG("Corrupted datagram received\n");
			continue;
		} else {
			// FIXME: do something else here, however this code is probably
			// unreachable
			ERROR("recvfrom returned: %s\n", strerror(errno));
			abort(); // FIXME: change this behavior
		}
	}
}

// A wrapper function for multicast_delivery_control
void* MulticastSender::multicast_delivery_thread(void *arg)
{
	MulticastSender *ms = (MulticastSender *)arg;
	ms->multicast_delivery_control();

	SDEBUG("multicast_delivery_control exited\n");
	return NULL;
}

// helper fuction that sends 'message' to the udp socket
void MulticastSender::udp_send(const void *message, int size, int flags)
{
#ifndef NDEBUG
	char taddr[INET_ADDRSTRLEN];
	DEBUG("send a udp message %d, %d bytes to %s:%d\n",
		((MulticastMessageHeader *)message)->get_number(),
		size, inet_ntop(AF_INET, &target_address.sin_addr, taddr, sizeof(taddr)),
		ntohs(target_address.sin_port));
#endif
	int sendto_result;
	do {
		sendto_result = sendto(sock, message, size, 0,
			(struct sockaddr *)&target_address, sizeof(target_address));
#ifndef NDEBUG
		if (sendto_result == -1 && errno == ENOBUFS) {
			SDEBUG("ENOBUFS error occurred\n");
		}
#endif
	} while (sendto_result == -1 && errno == ENOBUFS);
	if (sendto_result < 0) {
		ERROR("sendto returned: %s\n", strerror(errno));
		abort(); // FIXME: figure out something in this case
	}
}

// A helper fuction which reliably sends message to the multicast connection
void MulticastSender::mcast_send(const void *message, int size)
{
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
		mmh->set_responder(INADDR_NONE);
	}
	// TODO: add some percentage of unacknowledged packets depending
	// on the window size
	next_responder = (next_responder + 1) % targets.size();

	// Store message for the possibility of the future retransmissions.
	// (can block)
	void *retrans_message;
	size_t retrans_message_size;
	while ((retrans_message = send_queue->store_message(message, size,
			&retrans_message_size)) != NULL) {
		ERROR("Retransmit message %u\n",
			((MulticastMessageHeader *)retrans_message)->get_number());
		udp_send(retrans_message, retrans_message_size, 0);
	}
	udp_send(message, size, 0);
}

// Sends file through the multicast connection 
void MulticastSender::send_file()
{
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
		mcast_send(message, sizeof(MulticastMessageHeader) + count);
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
int MulticastSender::session()
{
	int error;
	pthread_t routine;
	error = pthread_create(&routine, NULL, multicast_delivery_thread, this);
	if (error != 0) {
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
			mcast_send(targets_message, t_curr - targets_message);
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
		mcast_send(targets_message, t_curr - targets_message);
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
				mcast_send(message, size);

				unsigned retrans_number = 1;
				struct timeval current_time;
				gettimeofday(&current_time, NULL);
				struct timespec next_retrans_time;
				TIMEVAL_TO_TIMESPEC(&current_time, &next_retrans_time);
				unsigned max_rtt = send_queue->get_max_round_trip_time();
				if (max_rtt > 0) {
					next_retrans_time.tv_nsec += max_rtt;
				} else {
					next_retrans_time.tv_nsec += DEFAULT_TERMINATE_RETRANSMISSION_RATE;
				}
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
					max_rtt = send_queue->get_max_round_trip_time();
					if (max_rtt > 0) {
						next_retrans_time.tv_nsec += max_rtt;
					} else {
						next_retrans_time.tv_nsec += DEFAULT_TERMINATE_RETRANSMISSION_RATE;
					}
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
				DEBUG("Fileinfo: |%u| %o | %u / %u  |\n", file_h->get_type(),
				 	file_h->get_mode(), file_h->get_name_length(),
					file_h->get_name_offset());
				memcpy(file_h + 1, op->get_filename(), op->fileinfo.get_name_length());
				mcast_send(file_record, sizeof(file_record));

				if (op->fileinfo.get_type() == resource_is_a_file) {
					DEBUG("Send file: %s\n",
						op->get_filename() + op->fileinfo.get_name_offset());
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
					mcast_send(file_trailing, sizeof(file_trailing));
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
