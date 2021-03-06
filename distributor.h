#ifndef DISTRIBUTOR_H_HEADER
#define DISTRIBUTOR_H_HEADER 1
#include <assert.h>

#include <algorithm>
#include <string>
#include <list>

#include "connection.h"
#include "md5.h"
#include "errors.h"

// An object that deliver tasks (copy of file or directory) from one reader
// to up to three personalized writers
class Distributor
{
public:
  // Structure describing a task for the distributor
  struct TaskHeader
  {
  private:
    char *filename;

    // Prohibit coping for this object
    TaskHeader(const TaskHeader& t);
  public:
    FileInfoHeader fileinfo;

    TaskHeader() : filename(NULL) {}
    ~TaskHeader() {
      if (filename != NULL) {
        free(filename);
      }
    }

    void set_filename(const char *fname) {
      if (filename != NULL) {
        free(filename);
      }
      filename = strdup(fname);
    }

    char *get_filename() {
      return filename;
    }
  };
  
  // Structure describing status of the reader or a writer
  struct Client
  {
    bool is_present; // flag indicating whether the object is active
    bool is_done; // flag indicating whether the current task is done by
      // this object
    uint8_t status; // status of the last task, see connection.h
    unsigned offset; // data offet in the distributor's buffer

    Client() : is_present(false), is_done(true), status(STATUS_OK), offset(0) {}
  };

private:
  // Internal distributor's data
  Errors errors;
  TaskHeader operation;
  uint8_t *buffer;

  static const unsigned DEFAULT_BUFFER_MASK = 0xFFFFF; // must be equal
    // to 2^n - 1
  static const unsigned DEFAULT_BUFFER_SIZE = (DEFAULT_BUFFER_MASK + 1);

  // State signaling variables
  pthread_mutex_t mutex; // mutex that protect changes of offset and state
  pthread_cond_t space_ready_cond; // State change condition
  volatile bool is_reader_awaiting;
  pthread_cond_t data_ready_cond; // State change condition
  volatile bool are_writers_awaiting;
  pthread_cond_t operation_ready_cond; // New operation is ready
  pthread_cond_t writers_finished_cond; // The current operation is finished
    // by the all the writers

  // Internal distributor's routines

  // Returns space available for write. Mutex should be locked.
  int _get_space() const;
  // Returns space available for read. Mutex should be locked.
  inline int _get_data(int offset) const;
  // Return true if all the writers are finished.
  inline bool all_writers_done() const;
  // Return true if all the writers and the reader are finished.
  bool all_done() const { return reader.is_done && all_writers_done(); }

protected:
  // Reader's interfaces

  volatile Client reader; // The object that reads tasks and data from disk or
    // network and passes them to the distributor
  MD5sum checksum;

  // Return free space available for write, blocks until
  // the space will be available. Can be called only by the reader.
  int get_space();

  /*
    Move reader.offset to the count bytes right (cyclic)
    Count should not be greater than zero.
  */
  void update_reader_position(int count);

  void *rposition()
  {
    return buffer + (reader.offset & DEFAULT_BUFFER_MASK);
  }

  // Adds a new task to the buffer, block until the previous task finished
  void add_task(const FileInfoHeader& fileinfo, const char* filename);

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

  Distributor();

  ~Distributor()
  {
    free(buffer);
    pthread_cond_destroy(&writers_finished_cond);
    pthread_cond_destroy(&operation_ready_cond);
    pthread_cond_destroy(&data_ready_cond);
    pthread_cond_destroy(&space_ready_cond);
    pthread_mutex_destroy(&mutex);
  }

public:
  // Writers' interfaces
  volatile Client file_writer; // the file reader object
  volatile Client unicast_sender; // the unicast sender object
  volatile Client multicast_sender; // the multicast sender object

  // Drop one of the writers. It can be useful if some fatal error occurred.
  inline void drop_writer(Client* w);
  // Get new task for the current writer.
  inline TaskHeader * get_writer_task(Client * w);
  // Submit that all the data has been written.
  inline void submit_writer_task(Client * w);

  // Return the space available for read, blocks until
  // the space will be available. Can be called only by the
  // reader.
  int get_data(Client *w);

  // Get position for the current writer
  void *wposition(Client *w)
  {
    return buffer + (w->offset & DEFAULT_BUFFER_MASK);
  }

  // Move w->offset to the count bytes left (cyclic)
  // Count should not be greater than zero.
  void update_writer_position(int count, Client *w);

  // Updates status of the multicast sender (thread safe)
  void update_multicast_sender_status(uint8_t status) {
    pthread_mutex_lock(&mutex);
    if (multicast_sender.status < status) {
      multicast_sender.status = status;
    }
    pthread_mutex_unlock(&mutex);
  }

  // Return pointer to the calculated checksum
  const MD5sum* get_checksum() const { return &checksum; }

  // Wrapper funtions for the errors object.
  inline void add_error(ErrorMessage * error_message)
  {
    errors.add(error_message);
  };
  inline void send_errors(int sock) { errors.send(sock); }
  inline void send_first_error(int sock) { errors.send_first(sock); }
  inline void display_errors() { errors.display(); }
  inline void delete_recoverable_errors()
  {
    errors.delete_recoverable_errors();
  }
  inline bool is_unrecoverable_error_occurred()
  {
    return errors.is_unrecoverable_error_occurred();
  }
  inline bool is_server_busy() { return errors.is_server_busy(); }
  inline char** get_retransmissions(std::vector<Destination> * dest,
      int **filename_offsets) const
  {
    return errors.get_retransmissions(dest, filename_offsets);
  };

private:
  // protect from copying
  Distributor(const Distributor&);
  Distributor& operator=(const Distributor&);
};

// Internal funcion that return space available for read
// mutex should be locked.
inline int Distributor::_get_data(int offset) const
{
  register unsigned count = DEFAULT_BUFFER_SIZE - offset;
  if (count > 0) {
    count = std::min(count, reader.offset - offset);
  } else {
    count = reader.offset - offset;
  }
  return count;
}

// Internal funcion that return space available for write
// mutex should be locked.
inline int Distributor::_get_space() const
{
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
      multicast_sender.offset));
  }
  return count;
}

// Return true if all the writers are finished.
inline bool Distributor::all_writers_done() const {
  return (!file_writer.is_present || file_writer.is_done) &&
    (!unicast_sender.is_present || unicast_sender.is_done) &&
    (!multicast_sender.is_present || multicast_sender.is_done);
}

// Drop one of the writers. It can be useful if some fatal error occurred.
inline void Distributor::drop_writer(Distributor::Client* w)
{
  pthread_mutex_lock(&mutex);
  w->is_present = false;
  w->status = STATUS_OK;
  w->is_done = true;
  pthread_cond_signal(&space_ready_cond);
  if (all_writers_done()) {
    pthread_cond_signal(&writers_finished_cond);
  }
  pthread_mutex_unlock(&mutex);
}

// Get new task for the current writer.
inline Distributor::TaskHeader* Distributor::get_writer_task(
    Distributor::Client* w)
{
  pthread_mutex_lock(&mutex);
  while (w->is_done && w->status < STATUS_FIRST_FATAL_ERROR) {
    // Wait for the new task become available
    pthread_cond_wait(&operation_ready_cond, &mutex);
  }
  pthread_mutex_unlock(&mutex);
  return &operation;
}

// Submit that all the data has been written.
inline void Distributor::submit_writer_task(Distributor::Client* w)
{
  pthread_mutex_lock(&mutex);
  w->is_done = true;
  if (w->status != STATUS_OK) {
    pthread_cond_signal(&space_ready_cond);
  }
  if (all_writers_done()) {
    pthread_cond_signal(&writers_finished_cond);
  }
  pthread_mutex_unlock(&mutex);
}
#endif
