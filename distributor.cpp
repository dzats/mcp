#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <assert.h>

#include <algorithm>

#include "distributor.h"
#include "log.h"

// Displays the fatal error registered by the writers (unicast sender
// and multicast sender or by the reader
void Distributor::display_error() {
	if (reader.status >= STATUS_FIRST_FATAL_ERROR) {
		ERROR("%s\n", reader.message);
	} else if (unicast_sender.is_present &&
			unicast_sender.status >= STATUS_FIRST_FATAL_ERROR) {
		if (unicast_sender.addr != INADDR_NONE) {
			struct in_addr addr = {unicast_sender.addr};
			char host_addr[INET_ADDRSTRLEN];
			ERROR("from host %s: %s\n",
				inet_ntop(AF_INET, &addr, host_addr, sizeof(host_addr)),
				unicast_sender.message);
		} else {
			ERROR("%s\n", unicast_sender.message);
		}
	} else if (multicast_sender.is_present &&
			multicast_sender.status >= STATUS_FIRST_FATAL_ERROR) {
		// TODO: display errors
	}
}

// Sends the occurred error to the imediate source
void Distributor::send_fatal_error(int sock) {
	try {
		// Global error occured, send it to the immediate source
		if (reader.status >= STATUS_FIRST_FATAL_ERROR) {
			DEBUG("Reader finished with the error: %s\n", reader.message);
			send_error(sock, reader.status, reader.addr, reader.message_length,
				reader.message);
		}
		else if (unicast_sender.is_present &&
				unicast_sender.status >= STATUS_FIRST_FATAL_ERROR) {
			struct in_addr addr = {unicast_sender.addr};
#ifndef NDEBUG
			char host_addr[INET_ADDRSTRLEN];
			DEBUG("Unicast Sender finished with the error: (%s) %s\n",
				inet_ntop(AF_INET, &addr, host_addr, sizeof(host_addr)),
				unicast_sender.message);
#endif
			send_error(sock, unicast_sender.status,
				unicast_sender.addr, unicast_sender.message_length,
				unicast_sender.message);
		} else if (file_writer.is_present &&
				file_writer.status >= STATUS_FIRST_FATAL_ERROR) {
			DEBUG("File writer finished with the error: %s\n", file_writer.message);
			send_error(sock, file_writer.status, file_writer.addr,
				file_writer.message_length, file_writer.message);
		} else {
			assert(multicast_sender.is_present &&
				multicast_sender.status >= STATUS_FIRST_FATAL_ERROR);
			// TODO: Display the multicast error
			SERROR("multicast error\n");
		}
	} catch (ConnectionException& e) {
		ERROR("Can't send error to the source: %s\n", e.what());
	}
}

Distributor::Distributor() : is_reader_awaiting(false),
		are_writers_awaiting(false) {
	// The reader is always present. FIXME: It may worth to join
	// the reader and the distributor
	reader.is_present = true;

	buffer = (uint8_t*)malloc(DEFAULT_BUFFER_SIZE);
	pthread_mutex_init(&_mutex, NULL);
	pthread_cond_init(&space_ready_cond, NULL);
	pthread_cond_init(&data_ready_cond, NULL);
	pthread_cond_init(&operation_ready_cond, NULL);
	pthread_cond_init(&writers_finished_cond, NULL);
	pthread_cond_init(&filewriter_done_cond, NULL);
}

// Adds a new task to the buffer, block until the previous task finished
void Distributor::add_task(const TaskHeader& op) {
	pthread_mutex_lock(&_mutex);
	operation = op;
	// Set the buffer to the default state
	assert(reader.is_present);
	reader.is_done = false;
	reader.status = STATUS_OK;
	reader.offset = 0;

	if (file_writer.is_present &&
			file_writer.status < STATUS_FIRST_FATAL_ERROR) {
		file_writer.is_done = false;
		file_writer.offset = 0;
	}

	if (unicast_sender.is_present &&
			unicast_sender.status < STATUS_FIRST_FATAL_ERROR) {
		unicast_sender.is_done = false;
		unicast_sender.offset = 0;
	}

	if (multicast_sender.is_present &&
			multicast_sender.status < STATUS_FIRST_FATAL_ERROR) {
		multicast_sender.is_done = false;
		multicast_sender.offset = 0;
	}
	// Wake up the readers
	pthread_cond_broadcast(&operation_ready_cond);
	pthread_mutex_unlock(&_mutex);
}

// Signal that the reader has finished the task and wait for the readers
// Returns class of the most critical error
uint8_t Distributor::finish_task() {
	pthread_mutex_lock(&_mutex);
	reader.is_done = true;

	// Wake up the writers
#ifdef BUFFER_DEBUG
	SDEBUG("finish_buffer_operation: wake readers\n");
#endif
	pthread_cond_broadcast(&data_ready_cond);

	// Wait until the writers accomplish the task
	while (!all_done()) {
		pthread_cond_wait(&writers_finished_cond, &_mutex);
	}

	// Get the task status
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

// It is combination of the add_task for trailing task and finish_task
uint8_t Distributor::finish_work() {
	pthread_mutex_lock(&_mutex);
	// Warning the task must be finished
	while (!all_done()) {
		// Wait until the readers accomplish the previous task
		pthread_cond_wait(&writers_finished_cond, &_mutex);
	}
	SDEBUG("Set the trailing task\n");
	// Move the trailing record to the operation->fileinfo
	memset(&operation.fileinfo, 0, sizeof(operation.fileinfo));

	// Don't wait for the file writer
	if (file_writer.is_present &&
			file_writer.status < STATUS_FIRST_FATAL_ERROR) {
		file_writer.is_done = false;
		file_writer.offset = 0;
	}

	if (unicast_sender.is_present &&
			unicast_sender.status < STATUS_FIRST_FATAL_ERROR) {
		unicast_sender.is_done = false;
		unicast_sender.offset = 0;
	}

	if (multicast_sender.is_present &&
			multicast_sender.status < STATUS_FIRST_FATAL_ERROR) {
		multicast_sender.is_done = false;
		multicast_sender.offset = 0;
	}

	SDEBUG("Wait for the writers\n");
	// Wake up the writers
	pthread_cond_broadcast(&operation_ready_cond);

	// Wait until the writers accomplish the task
	while (!all_done()) {
		pthread_cond_wait(&writers_finished_cond, &_mutex);
	}

	// Get the task status
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

	SDEBUG("The finish_work method exited\n");
	pthread_mutex_unlock(&_mutex);
	return status;
}


/*
	Return free space available for read, blocks until
	the space will be available. Can be called only by the
	reader.
*/
int Distributor::get_data(Client *w) {
	int count;
	pthread_mutex_lock(&_mutex);
	count = _get_data(w->offset);
	while (count == 0) {
		if (reader.is_done) {
#ifdef BUFFER_DEBUG
			SDEBUG("writer %p is done, wake up reader\n", w);
#endif
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

/*
	Return free space available for write, blocks until
	the space will be available. Can be called only by the
	reader.
*/
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

// Move w->offset to the count bytes left (cyclic)
// Count should not be greater than zero.
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

// Put data into the buffer
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

// Put data into the buffer, checksum is not changed
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

// Writes data from the distributor to fd. Returns 0 on success or
// errno of the last write call on failure
int Distributor::Writer::write_to_file(int fd) {
#ifndef NDEBUG
	int total = 0;
#endif
	int count = get_data();
	count = std::min(count, 16384); 
	while (count > 0) {
#ifdef BUFFER_DEBUG
		DEBUG("Sending %d bytes of data\n", count);
#endif
		count = write(fd, pointer(), count);
#ifndef NDEBUG
		total += count;
#endif
#ifdef BUFFER_DEBUG
		DEBUG("%d (%d) bytes of data sent\n", count, total);
#endif
		if (count > 0) {
			update_position(count);
		} else {
			if (errno == ENOBUFS) {
				SDEBUG("ENOBUFS error occurred\n");
				usleep(200000);
				continue;
			}
			return errno;
		}

		count = get_data();
		count = std::min(count, 16384);
	}
	DEBUG("write_to_file: %d bytes wrote\n", total);
	return 0;
}
