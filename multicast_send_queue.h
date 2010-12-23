#ifndef MULTICAST_SEND_QUEUE_H_HEADER
#define MULTICAST_SEND_QUEUE_H_HEADER 1
#include <queue>

#include "connection.h"
#include "destination.h"

class MulticastSendQueue {
	struct MessageRecord {
		size_t size;
		uint8_t message[MAX_UDP_PACKET_SIZE];
	};
	static const unsigned DEFAULT_BUFFER_SCALE = 5; // buffer size / number
		// of destinations
	std::deque<MessageRecord*> buffer; // dequeue of the message number to
		// the message content
	volatile unsigned store_position;

	unsigned n_destinations;
	// Array of the offsets of the first unacknowledged messages in the buffer
	// Index in this array is the destination number
	unsigned *first_to_acknowledge;
	// IP addresses of the destinations
	uint32_t *target_addresses;

	// Session termination message
	uint8_t *termination_message;
	size_t termination_message_size;

	pthread_mutex_t _mutex;
	pthread_cond_t _space_ready_cond;
	pthread_cond_t _transmission_finished_cond;

public:
	MulticastSendQueue(const std::vector<Destination> targets);
	~MulticastSendQueue();

	// Add message to the queue
	int store_message(const void *message, size_t size,
		const struct timespec *timeout);
	// Acknowledge receiving all the messages till 'number' by 'destination'
	int acknowledge(uint32_t number, int destination);
	// Compose the session termination message and return it to the caller
	void* prepare_termination(size_t *size, uint32_t session_id);
#if 0
	// Acknowledge receiving all the messages in this session
	void final_acknowledgement(int destination);
#endif
	// Waits for the transmission termination. Returns addresses of the
	// hosts there were no final replies from.
	int wait_for_destinations(const struct timespec *timeout);

	// Get message whith the number 'number'
	void *get_message(size_t *size, uint32_t number) {
		pthread_mutex_lock(&_mutex);
		unsigned offset = number -
			((MulticastMessageHeader *)buffer.front()->message)->get_number();
		if (offset < 0) {
			offset += UINT_MAX + 1;
		}
		DEBUG("offset: %u, first: %u, number: %u\n", offset,
			((MulticastMessageHeader *)buffer.front()->message)->get_number(),
			number);
		if (offset >= buffer.size()) {
			return NULL;
		}
		register void *message = buffer[offset]->message;
		*size = buffer[offset]->size;
		pthread_mutex_unlock(&_mutex);
		return message;
	}

	// Get the first message that has not been acknowledged. Returns NULL if
	// there is no such message
	void *get_first_unacknowledged_message(size_t *size);

	void lock_queue() {
		pthread_mutex_lock(&_mutex);
	}
	void release_queue() {
		pthread_mutex_unlock(&_mutex);
	}
};
#endif
