#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <assert.h>

#include <algorithm>

#include "distributor.h"
#include "log.h"

Distributor::Distributor() : is_reader_awaiting(false),
		are_writers_awaiting(false) {
	// Set the buffer to the default state
	reader.is_present = true;
	reader.is_done = true;
	reader.status = STATUS_OK;
	reader.offset = 0;

	file_writer.is_present = false;
	file_writer.is_done = true;
	file_writer.status = STATUS_OK;
	file_writer.offset = 0;

	unicast_sender.is_present = false;
	unicast_sender.is_done = true;
	unicast_sender.status = STATUS_OK;
	unicast_sender.offset = 0;

	multicast_sender.is_present = false;
	multicast_sender.is_done = true;
	multicast_sender.status = STATUS_OK;
	multicast_sender.offset = 0;

	buffer = (uint8_t*)malloc(DEFAULT_BUFFER_SIZE);
	pthread_mutex_init(&_mutex, NULL);
	pthread_cond_init(&space_ready_cond, NULL);
	pthread_cond_init(&data_ready_cond, NULL);
	pthread_cond_init(&operation_ready_cond, NULL);
	pthread_cond_init(&writers_finished_cond, NULL);
	pthread_cond_init(&filewriter_done_cond, NULL);
}

void Distributor::add_task(const TaskHeader& op) {
	pthread_mutex_lock(&_mutex);
	operation = op;
	// Set the buffer to the default state
	assert(reader.is_present);
	reader.is_done = false;
	reader.status = STATUS_OK;
	reader.offset = 0;

	if (file_writer.is_present) {
		file_writer.is_done = false;
		file_writer.status = STATUS_OK;
		file_writer.offset = 0;
	}

	if (unicast_sender.is_present) {
		unicast_sender.is_done = false;
		unicast_sender.status = STATUS_OK;
		unicast_sender.offset = 0;
	}

	if (multicast_sender.is_present) {
		multicast_sender.is_done = false;
		multicast_sender.status = STATUS_OK;
		multicast_sender.offset = 0;
	}
	// Wake up the readers
	pthread_cond_broadcast(&operation_ready_cond);
	pthread_mutex_unlock(&_mutex);
}

uint8_t Distributor::finish_task() {
	pthread_mutex_lock(&_mutex);
	reader.is_done = true;

	// Wake up the writers
#ifdef BUFFER_DEBUG
	SDEBUG("finish_buffer_operation: wake readers\n");
#endif
	pthread_cond_broadcast(&data_ready_cond);

	// Wait until the readers accomplish the task
	while (!all_done()) {
		pthread_cond_wait(&writers_finished_cond, &_mutex);
	}

	// Get the operation status
	uint8_t status = reader.status;
	if (file_writer.status > status) {
		status = file_writer.status;
	}
	if (unicast_sender.status > status) {
		status = unicast_sender.status;
	}
	if (multicast_sender.status > status) {
		status = multicast_sender.status;
	}

	pthread_mutex_unlock(&_mutex);
	return status;
}

void Distributor::finish_work() {
	pthread_mutex_lock(&_mutex);
	while (!all_done()) {
		// Wait until the readers accomplish the previous task
		pthread_cond_wait(&writers_finished_cond, &_mutex);
	}
	// Move the trailing record to the operation->fileinfo
	memset(&operation.fileinfo, 0, sizeof(operation.fileinfo));
	// Set the buffer to the default state, except the reader.is_done
	if (file_writer.is_present) {
		file_writer.is_done = false;
		file_writer.status = STATUS_OK;
		file_writer.offset = 0;
	}

	if (unicast_sender.is_present) {
		unicast_sender.is_done = false;
		unicast_sender.status = STATUS_OK;
		unicast_sender.offset = 0;
	}

	if (multicast_sender.is_present) {
		multicast_sender.is_done = false;
		multicast_sender.status = STATUS_OK;
		multicast_sender.offset = 0;
	}

	// Wake up the writers
	pthread_cond_broadcast(&operation_ready_cond);
	pthread_mutex_unlock(&_mutex);
}

int Distributor::get_data(Client *w) {
	int count;
	pthread_mutex_lock(&_mutex);
	count = _get_data(w->offset);
	while (count == 0) {
		if (reader.is_done) {
#ifdef BUFFER_DEBUG
			SDEBUG("writer %p is done, wake up reader\n", w);
#endif
			if (w == &file_writer) {
				pthread_cond_signal(&filewriter_done_cond);
			}
			pthread_mutex_unlock(&_mutex);
			return 0;
		} else {
#ifdef BUFFER_DEBUG
			SDEBUG("writer %p is going to sleep\n", w);
#endif
			are_writers_awaiting = true;
			pthread_cond_wait(&data_ready_cond, &_mutex);
			count = _get_data(w->offset);
#ifdef BUFFER_DEBUG
			DEBUG("awake attempt for the reader %p (%d)\n", w, count);
#endif
		}
	}
	pthread_mutex_unlock(&_mutex);
	assert(count > 0);
	return count;
}

int Distributor::get_space() {
	int count;
	pthread_mutex_lock(&_mutex);
	count = _get_space();
	while (count == 0) {
#ifdef BUFFER_DEBUG
		SDEBUG("get_space: reader is going to sleep\n");
#endif
		is_reader_awaiting = true;
		pthread_cond_wait(&space_ready_cond, &_mutex);
		count = _get_space();
		is_reader_awaiting = false;
#ifdef BUFFER_DEBUG
		DEBUG("get_space: awake attempt for the reader (%d)\n", count);
#endif
	}
	pthread_mutex_unlock(&_mutex);
	assert(count > 0);
	assert((reader.offset & DEFAULT_BUFFER_MASK) + count <= DEFAULT_BUFFER_SIZE);
	return count;
}

void Distributor::update_reader_position(int count) {
	pthread_mutex_lock(&_mutex);
#ifdef BUFFER_DEBUG
	DEBUG("rposition update: %d -> ", reader.offset);
#endif
	reader.offset = reader.offset + count;
	if (reader.offset >= DEFAULT_BUFFER_SIZE * 2) {
		reader.offset = DEFAULT_BUFFER_SIZE + (reader.offset & DEFAULT_BUFFER_MASK);
		file_writer.offset &= DEFAULT_BUFFER_MASK;
		unicast_sender.offset &= DEFAULT_BUFFER_MASK;
		multicast_sender.offset &= DEFAULT_BUFFER_MASK;
	}
#ifdef BUFFER_DEBUG
	DEBUG("%d\n", reader.offset);
#endif
	if (are_writers_awaiting) {
#ifdef BUFFER_DEBUG
		SDEBUG("wake up writers\n");
#endif
		are_writers_awaiting = false;
		pthread_cond_broadcast(&data_ready_cond);
	}
	pthread_mutex_unlock(&_mutex);
}

void Distributor::update_writer_position(int count, Client *w) {
	pthread_mutex_lock(&_mutex);
#ifdef BUFFER_DEBUG
	DEBUG("update_writer_position: %d -> ", w->offset);
#endif
	w->offset += count;
#ifdef BUFFER_DEBUG
	DEBUG("%d\n", w->offset);
#endif
	if (is_reader_awaiting) {
#ifdef BUFFER_DEBUG
		SDEBUG("update_writer_position: wake up reader\n");
#endif
		pthread_cond_signal(&space_ready_cond);
	}
	pthread_mutex_unlock(&_mutex);
}

void Distributor::put_data(void *data, int size) {
	do {
		int count = get_space();
		count = std::min(count, size);
	
		memcpy(rposition(), data, count);
		// Update the checksum
		checksum.update((unsigned char *)rposition(), count);
		update_reader_position(count);
		size -= count;
		data = (uint8_t *)data + count;
	} while (size > 0);
}

void Distributor::put_data_without_checksum_update(void *data, int size) {
	do {
		int count = get_space();
		count = std::min(count, size);
	
		memcpy(rposition(), data, count);
		update_reader_position(count);
		size -= count;
		data = (uint8_t *)data + count;
	} while (size > 0);
}

void Distributor::Writer::write_to_file(int fd) {
	int total = 0;
	int count = get_data();
	count = std::min(count, 16384); 
	while (count > 0) {
#ifdef BUFFER_DEBUG
		DEBUG("Sending %d bytes of data\n", count);
#endif
		count = write(fd, pointer(), count);
		total += count;
#ifdef BUFFER_DEBUG
		DEBUG("%d (%d) bytes of data sent\n", count, total);
#endif
		if (count > 0) {
			update_position(count);
		} else {
			DEBUG("!!!!!! count == 0, total == %d\n", total);
			// FIXME: Behavior here should be a bit less cruel
			perror("Input/Output error 2");
			exit(EXIT_FAILURE);
		}
		count = get_data();
		count = std::min(count, 16384);
	}
	DEBUG("write_to_file: %d bytes wrote\n", total);
}

