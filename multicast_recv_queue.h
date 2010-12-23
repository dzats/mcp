#ifndef MULTICAST_RECV_QUEUE_H_HEADER
#define MULTICAST_RECV_QUEUE_H_HEADER 1
#include <pthread.h>

#include <queue>

#include "connection.h"

class MulticastRecvQueue {
	struct MessageRecord {
		size_t length; // zero means that the message doesn't present in the queue
		uint8_t message[MAX_UDP_PACKET_SIZE];
		MessageRecord() {}
	};
	static const int DEFAULT_BUFFER_SIZE = 4;
	std::deque<MessageRecord*> buffer; // dequeue of the message number to
	MessageRecord *swapper;

	uint32_t first_num; // Number of the first message in the buffer
	uint32_t last_num; // Number of the last message in the buffer
	unsigned n_messages; // Index of the first buffer's free  element

	pthread_mutex_t _mutex;
	pthread_cond_t _data_ready_cond;
	//pthread_cond_t _transmission_finished_cond;

public:
	MulticastRecvQueue() : buffer(DEFAULT_BUFFER_SIZE) {
		for (unsigned i = 0; i < buffer.size(); ++i) {
			buffer[i] = new MessageRecord;
			buffer[i]->length = 0;
		}
		swapper = new MessageRecord;
		first_num = 0; // FIXME: This is not very obvious
		last_num = UINT32_MAX; // FIXME: This is not very obvious
		n_messages = 0;
		pthread_mutex_init(&_mutex, NULL);
		pthread_cond_init(&_data_ready_cond, NULL);
	}
	~MulticastRecvQueue() {
		for (unsigned i = 0; i < buffer.size(); ++i) {
			free(buffer[i]);
		}
		delete swapper;
		pthread_cond_destroy(&_data_ready_cond);
		pthread_mutex_destroy(&_mutex);
	}

	// Add message to the queue
	int put_message(const void *message, size_t length, uint32_t number);
	// Get message from the queue
	void* get_message(size_t *length);
};
#endif
