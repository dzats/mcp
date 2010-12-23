#include <assert.h>
#include <pthread.h>

#include <algorithm>

#include "multicast_send_queue.h"

using namespace std;

#ifndef INT32_MAX // should be included through stdint.h somehow else
#define INT32_MAX 0x7fffffff
#endif

MulticastSendQueue::MulticastSendQueue(
		const std::vector<Destination> targets) : store_position(0) {
	n_destinations = targets.size();
	// FIXME: this is should be changed
	buffer = std::deque<MessageRecord*>(n_destinations * DEFAULT_BUFFER_SCALE);
	for (unsigned i = 0; i < buffer.size(); ++i) {
		buffer[i] = new MessageRecord;
	}
	target_addresses = new uint32_t[n_destinations];
	for (unsigned i = 0; i < n_destinations; ++i) {
		target_addresses[i] = targets[i].addr;
	}
	first_to_acknowledge = new unsigned[n_destinations];
	memset(first_to_acknowledge, 0, sizeof(unsigned) * n_destinations);
	termination_message = NULL;
		
	pthread_mutex_init(&_mutex, NULL);
	pthread_cond_init(&_space_ready_cond, NULL);
	pthread_cond_init(&_transmission_finished_cond, NULL);
}

MulticastSendQueue::~MulticastSendQueue() {
	for (unsigned i = 0; i < buffer.size(); ++i) {
		delete buffer[i];
	}
	delete first_to_acknowledge;
	delete target_addresses;
	if (termination_message != NULL) {
		delete termination_message;
	}

	pthread_mutex_destroy(&_mutex);
	pthread_cond_destroy(&_space_ready_cond);
	pthread_cond_destroy(&_transmission_finished_cond);
}

// Add message to the queue
int MulticastSendQueue::store_message(const void *message, size_t size,
		const struct timespec *timeout) {
	assert(size <= MAX_UDP_PACKET_SIZE);
	pthread_mutex_lock(&_mutex);
	if (store_position == buffer.size()) {
		// Buffer is full, wait for acknowledgements
		SDEBUG("Buffer is full\n");
		int error = pthread_cond_timedwait(&_space_ready_cond, &_mutex, timeout);
		if (error != 0) {
			if (error != ETIMEDOUT) {
				ERROR("pthread_cond_timedwait error: %s\n", strerror(error));
			} else {
				SDEBUG("pthread_cond_timedwait timeout expired\n");
			}
			pthread_mutex_unlock(&_mutex);
			return error;
		}
	}
	// Put message into the buffer
	buffer[store_position]->size = size;
	memcpy(buffer[store_position]->message, message, size);
	++store_position;
	pthread_mutex_unlock(&_mutex);
	return 0;
}

// Acknowledge receiving all the messages till 'number' by 'destination'
// Return 0 if it is not the last expected acknowledgement
int MulticastSendQueue::acknowledge(uint32_t number, int destination) {
	DEBUG("MulticastSendQueue::acknowledge (%u, %d) (%u, %zu)\n", 
		number, destination, store_position, buffer.size());
	pthread_mutex_lock(&_mutex);
	unsigned offset = number + 1 -
		((MulticastMessageHeader *)buffer[0]->message)->get_number();
	DEBUG("offset: %d\n", offset);

	// one here is imoprtant for the session termination conformation
	if (offset > store_position + 1) {
		// Ignore this conformation
		DEBUG("Ignore retransmitted conformation %u from %u\n",
			number, destination);
		pthread_mutex_unlock(&_mutex);
		return 0;
	}
	assert(offset <= store_position ||
		offset == store_position + 1 && termination_message != NULL);
	first_to_acknowledge[destination] =
		offset <= store_position ? offset : store_position;
	unsigned min_to_acknowledge =
		*min_element(first_to_acknowledge, first_to_acknowledge + n_destinations);

	if (termination_message != 0 &&
			first_to_acknowledge[destination] >= store_position) {
		// Remove distination from 'termination_message'
		uint32_t *hosts_begin = (uint32_t *)((uint8_t *)termination_message +
			sizeof(MulticastMessageHeader)); 
		uint32_t *hosts_end = (uint32_t *)((uint8_t *)termination_message +
			termination_message_size); 
		uint32_t *i = find(hosts_begin, hosts_end,
			target_addresses[destination]);
		if (i != hosts_end) {
			*i = *(hosts_end - 1);
			--termination_message_size;
		}
	}

	if (min_to_acknowledge > 0) {
		for (unsigned i = 0; i < n_destinations; ++i) {
			first_to_acknowledge[i] -= min_to_acknowledge;
		}
		// Move all the acknowledged elements to the queue's tail
		buffer.insert(buffer.end(), buffer.begin(),
			buffer.begin() + min_to_acknowledge);
		buffer.erase(buffer.begin(), buffer.begin() + min_to_acknowledge);
		if (store_position == buffer.size()) {
			SDEBUG("Wake up the multicast sender\n");
			pthread_cond_signal(&_space_ready_cond);
		}
		store_position -= min_to_acknowledge;
	}

	if (termination_message != NULL && store_position == 0) {
		SDEBUG("Multicast transmission finished\n");
		pthread_cond_signal(&_transmission_finished_cond);
		pthread_mutex_unlock(&_mutex);
		return 1;
	}
	pthread_mutex_unlock(&_mutex);
	return 0;
}

// Compose the session termination message and return it to the caller
void *MulticastSendQueue::prepare_termination(size_t *size,
		uint32_t session_id) {
	if (termination_message != NULL) {
		delete termination_message;
	}
	// Compose the termination message
	termination_message_size = sizeof(MulticastMessageHeader) +
		n_destinations * sizeof(uint32_t);
	termination_message = new uint8_t[termination_message_size];
	MulticastMessageHeader *mmh = new(termination_message)
		MulticastMessageHeader(MULTICAST_TERMINATION_REQUEST, session_id);
	uint32_t *p = (uint32_t *)(mmh + 1);
	for (unsigned i = 0; i < n_destinations; ++i) {
		*p++ = htonl(target_addresses[i]);
	}
	*size = termination_message_size;
	return termination_message;
}

// Waits for the transmission termination. Returns 0 if all the
// destinations have finished and -1 otherwise
int MulticastSendQueue::wait_for_destinations(const struct timespec *timeout) {
	pthread_mutex_lock(&_mutex);
	if (store_position != 0) {
		SDEBUG("Wait for the replies from destinations\n");
		int error = pthread_cond_timedwait(&_transmission_finished_cond, &_mutex,
			timeout);
		if (error != 0) {
			if (error != ETIMEDOUT) {
				ERROR("pthread_cond_timedwait error: %s\n", strerror(error));
				abort();
			} else {
				SDEBUG("pthread_cond_timedwait timeout expired\n");
			}
			pthread_mutex_unlock(&_mutex);
			return -1;
		}
		// Transmission is done
	}
	pthread_mutex_unlock(&_mutex);
	return 0;
}

// Get the first message that has not been acknowledged. Returns NULL if
// there is no such message
// FIXME: untested
void* MulticastSendQueue::get_first_unacknowledged_message(size_t *size) {
	pthread_mutex_lock(&_mutex);
	for (unsigned i = 0; i < store_position; ++i) {
		MulticastMessageHeader *mmh =
			((MulticastMessageHeader *)buffer[i]->message);
		if (mmh->get_responder() != MULTICAST_UNEXISTING_RESPONDER) {
			// FIXME: use lower_bound here
			uint32_t *t = lower_bound(target_addresses,
				target_addresses + n_destinations, mmh->get_responder());
			if (t != target_addresses + n_destinations &&
					*t == mmh->get_responder() &&
					first_to_acknowledge[t - target_addresses] <= i) {
				*size = buffer[i]->size;
				register void *message = buffer[i]->message;
				pthread_mutex_unlock(&_mutex);
				return message;
			}
		}
	}
	pthread_mutex_unlock(&_mutex);
	return NULL;
}
