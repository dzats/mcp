#ifndef DISTRIBUTOR_H_HEADER
#define DISTRIBUTOR_H_HEADER 1
#include <assert.h>
#include <algorithm>
#include <string>

#include "connection.h"
#include "md5.h"

// An object that deliver tasks (copy of file or directory) from one reader
// to up to three personalized writers
class Distributor {
public:
	// Structure describing a task for the distributor
	struct TaskHeader {
		FileInfoHeader fileinfo;
		std::string filename; // carefully check this
	};
	
	// Structure describing status of the reader or a writer
	struct Client {
		bool is_present; // flag indicating whether the object is active
		bool is_done; // flag indicating whether the current task is done by
			// this object
		uint8_t status; // status of the last task, see connection.h
		unsigned offset; // data offet in the distributor's buffer

		// Fields used to store errors
		uint32_t addr; // Address ot the host, caused the error.
			// INADDR_NONE means that the error occured on this host
		char *message; // network message to be sent to the immediate
			// source in the case of unrecoverable error 
		uint32_t message_length;

		void set_error(int address, char *msg, uint32_t msg_length) volatile {
			if (message != NULL) {
				free(message);
			}
			addr = address;
			message = msg;
			message_length = msg_length;
		}

		Client() : is_present(false), is_done(true), status(STATUS_OK),
			offset(0), message(NULL) {}
		~Client() {
			if (message != NULL) {
				free(message);
			}
		}
	};

	class Writer;
private:
	TaskHeader operation;
	uint8_t *buffer;
public:
	// Objects connected to the distributor
	volatile Client reader; // The object that reads tasks and data from disk or
		// network and passes them to the distributor
	volatile Client file_writer; // the file reader object
	volatile Client unicast_sender; // the unicast sender object
	volatile Client multicast_sender; // the multicast sender object

	static const unsigned DEFAULT_BUFFER_MASK = 0xFFFF; // must be equal
		// to 2^n - 1
	static const unsigned DEFAULT_BUFFER_SIZE = (DEFAULT_BUFFER_MASK + 1);
	MD5sum checksum;

private:
	// State signaling variables
	pthread_mutex_t _mutex; // mutex that protect changes of offset and state
	pthread_cond_t space_ready_cond; // State change condition
	volatile bool is_reader_awaiting;
	pthread_cond_t data_ready_cond; // State change condition
	volatile bool are_writers_awaiting;
	pthread_cond_t operation_ready_cond; // New operation is ready
	pthread_cond_t writers_finished_cond; // The current operation is finished
		// by the all the writers
	pthread_cond_t filewriter_done_cond; // The file_writer has finished the
		// operation

	// protect from copying
	Distributor(const Distributor&);
	Distributor& operator=(const Distributor&);

	// This is experimental routine used for file retransmissions in the
	// case of data corruption
	void wait_for_filewriter() {
		pthread_mutex_lock(&_mutex);
		reader.is_done = true;
	
		// Wake up the writers
		pthread_cond_broadcast(&data_ready_cond);
	
		if (file_writer.is_present && !file_writer.is_done) {
			pthread_cond_wait(&filewriter_done_cond, &_mutex);
		}
		pthread_mutex_unlock(&_mutex);
	}

	/*
		Internal funcion that return space available for write
		_mutex should be locked.
	*/
	int _get_space();

	/*
		Internal funcion that return space available for read
		_mutex should be locked.
	*/
	inline int _get_data(int offset);

	bool all_writers_done() {
		return (!file_writer.is_present || file_writer.is_done) &&
			(!unicast_sender.is_present || unicast_sender.is_done) &&
			(!multicast_sender.is_present || multicast_sender.is_done);
	}

	bool all_done() {
		return reader.is_done && all_writers_done();
	}

	/*
		Return free space available for write, blocks until
		the space will be available. Can be called only by the
		reader.
	*/
	int get_space();

	/*
		Return free space available for read, blocks until
		the space will be available. Can be called only by the
		reader.
	*/
	int get_data(Client *w);

	/*
		Move reader.offset to the count bytes right (cyclic)
		Count should not be greater than zero.
	*/
	void update_reader_position(int count);

	void *rposition() {
		return buffer + (reader.offset & DEFAULT_BUFFER_MASK);
	}

	// Move w->offset to the count bytes left (cyclic)
	// Count should not be greater than zero.
	void update_writer_position(int count, Client *w);

	void *wposition(Client *w) {
		return buffer + (w->offset & DEFAULT_BUFFER_MASK);
	}

	// Adds a new task to the buffer, block until the previous task finished
	void add_task(const TaskHeader& op);

	// Restart the same task (for retransmission)
	void restart_task();

	// Signal that the reader has finished the task and wait for the readers
	// Returns class of the most critical error
	uint8_t finish_task();

	// It is almost equivalent to add_task(operation with empty fileinfo), finish
	// task
	uint8_t finish_work();

	// Put data into the buffer
	void put_data(void *data, int size);
	// Put data into the buffer, checksum is not changed
	void put_data_without_checksum_update(void *data, int size);

public:
	Distributor();

	~Distributor() {
		free(buffer);
		pthread_cond_destroy(&filewriter_done_cond);
		pthread_cond_destroy(&writers_finished_cond);
		pthread_cond_destroy(&operation_ready_cond);
		pthread_cond_destroy(&data_ready_cond);
		pthread_cond_destroy(&space_ready_cond);
		pthread_mutex_destroy(&_mutex);
	}

	// Displays the fatal error registered by the writers (unicast sender
	// and multicast sender or by the reader
	void display_error();

	// Sends the occurred error to the imediate source
	void send_fatal_error(int sock);

	friend class Reader;
};

// The Writer class describing work of an abstract writer with the buffer
class Distributor::Writer {
protected:
	Distributor *buff;
	Distributor::Client *w;

	Writer(Distributor *b, Distributor::Client *worker) : buff(b),
			w(worker) {
		assert(!w->is_present);
		w->is_present = true;
	}

	Distributor::TaskHeader* get_task() {
		pthread_mutex_lock(&buff->_mutex);
		while (w->is_done) {
			// Wait for the new task become available
			pthread_cond_wait(&buff->operation_ready_cond, &buff->_mutex);
		}
		pthread_mutex_unlock(&buff->_mutex);
		return &buff->operation;
	}

	void submit_task() {
		pthread_mutex_lock(&buff->_mutex);
		w->is_done = true;
		if (w->status != STATUS_OK) {
			pthread_cond_signal(&buff->space_ready_cond);
		}
		if (w == &buff->file_writer) {
			pthread_cond_signal(&buff->filewriter_done_cond);
		}
		if (buff->all_writers_done()) {
			pthread_cond_signal(&buff->writers_finished_cond);
		}
		pthread_mutex_unlock(&buff->_mutex);
	}

	int get_data() { return buff->get_data(w); }
	void update_position(int count) {
		return buff->update_writer_position(count, w);
	}
	void *pointer() { return buff->wposition(w); }
	// Checksum of the last file received
	MD5sum* checksum() { return &buff->checksum; }

	// Writes data from the distributor to fd. Returns 0 on success or
	// errno of the last write call on failure
	int write_to_file(int fd);
};

/*
	Internal funcion that return space available for read
	_mutex should be locked.
*/
inline int Distributor::_get_data(int offset) {
	register unsigned count = DEFAULT_BUFFER_SIZE - offset;
	if (count > 0) {
		count = std::min(count, reader.offset - offset);
	} else {
		count = reader.offset - offset;
	}
	return count;
}

/*
	Internal funcion that return space available for write
	_mutex should be locked.
*/
inline int Distributor::_get_space() {
	register int reader_offset = reader.offset;
	unsigned count = DEFAULT_BUFFER_SIZE - (reader_offset & DEFAULT_BUFFER_MASK);
	if (file_writer.is_present && !file_writer.is_done) {
		count = std::min(count, DEFAULT_BUFFER_SIZE - (reader_offset -
			file_writer.offset));
	}
	if (unicast_sender.is_present && !unicast_sender.is_done) {
		count = std::min(count, DEFAULT_BUFFER_SIZE - (reader_offset -
			unicast_sender.offset));
	}
	if (multicast_sender.is_present && !multicast_sender.is_done) {
		count = std::min(count, DEFAULT_BUFFER_SIZE - (reader_offset -
			unicast_sender.offset));
	}
	return count;
}
#endif
