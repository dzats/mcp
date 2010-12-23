#include <stdint.h>

#include <assert.h>

#include "multicast_recv_queue.h"
#include "connection.h"
#include "log.h"

// Add message to the queue
int MulticastRecvQueue::put_message(const void *message, size_t length,
		uint32_t number) {
	assert(length <= MAX_UDP_PACKET_SIZE);
	pthread_mutex_lock(&_mutex);
	DEBUG("%u, %u, %u\n", last_num, first_num, n_messages);
	if (cyclic_greater(number, last_num)) {
		bool do_wake_reader = n_messages == 0 && number == last_num + 1;
		// Check space in the buffer
		if (cyclic_less_or_equal(first_num + buffer.size(), number)) {
			// Enlarge the buffer, FIXME: some limit check required here (may be)
			unsigned element = first_num + buffer.size() - 1;
			while (element != number) {
				SDEBUG("Enlarge the buffer\n");
				buffer.push_back(new MessageRecord());
				++element;
			}
		}

		// Add the skipped messages
		uint32_t num = last_num + 1;
		while (num != number) {
			buffer[n_messages]->length = 0;
			++num;
			++n_messages;
		}

		// Put the message into the buffer
		buffer[n_messages]->length = length;
		memcpy(buffer[n_messages]->message, message, length);
		++n_messages;
		last_num = number;
		first_num = number - n_messages + 1;
		DEBUG("%u, %u, %u\n", last_num, first_num, n_messages);
		if (do_wake_reader) {
			pthread_cond_signal(&_data_ready_cond);
		}
	} else {
		// Retransmission received
		if (cyclic_less(number, first_num) || n_messages == 0) {
			SDEBUG("Retransmission for the already processed packet received\n");
			pthread_mutex_unlock(&_mutex);
			return 0;
		}
		if (buffer[0]->length == 0 && number == first_num) {
			// Wake the receiving process up
			pthread_cond_signal(&_data_ready_cond);
		}
		if (buffer[number - first_num]->length == 0) {
			// Store message if it has been missed
			buffer[number - first_num]->length = length;
			memcpy(buffer[number - first_num]->message, message, length);
		}
	}
	pthread_mutex_unlock(&_mutex);
	return 0;
}

// Get message from the queue
void* MulticastRecvQueue::get_message(size_t *length) {
	pthread_mutex_lock(&_mutex);
	while (n_messages == 0 || buffer[0]->length == 0) {
		pthread_cond_wait(&_data_ready_cond, &_mutex);
	}

	DEBUG("Get the message: %u\n", first_num);
	swapper->length = 0;
	buffer.push_back(swapper);
	swapper = buffer.front();
	buffer.pop_front();
	--n_messages;
	if (n_messages != 0) {
		first_num++;
	}
	*length = swapper->length;
	DEBUG("Get the message: %u\n", first_num);
	pthread_mutex_unlock(&_mutex);
	return swapper->message;
}
