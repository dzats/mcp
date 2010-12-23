#ifndef DISTRIBUTOR_H_HEADER
#define DISTRIBUTOR_H_HEADER 1
#include <assert.h>

#include <algorithm>
#include <string>
#include <list>

#include "connection.h"
#include "md5.h"

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

  class Errors;

  class ErrorMessage
  {
    ErrorMessage(const ErrorMessage&); // Protect from coping
  protected:
    uint8_t status; // status of the last task, see connection.h
  public:
    ErrorMessage(uint8_t s) : status(s) {}
    virtual ~ErrorMessage() {}
  
    virtual void display() const = 0;
    virtual void send(int sock) = 0;

    friend class Errors;
  };
  
  class SimpleError : public ErrorMessage
  {
    uint32_t address; // Address ot the host, caused the error.
      // INADDR_NONE means that the error occured on this host
    uint32_t message_length;
    uint8_t *message; // network message to be sent to the immediate

    SimpleError(const ErrorMessage&); // Protect from coping
  public:
    SimpleError(uint8_t status, uint32_t address, char *message,
        uint32_t message_length) : ErrorMessage(status)
    {
      this->address = address;
      this->message_length = message_length;
      if (message_length != 0) {
        this->message = (uint8_t *)strdup(message);
      } else {
        this->message = NULL;
      }
    }
    ~SimpleError()
    {
      if (message != NULL) { free(message); }
    }
    void display() const;
    void send(int sock);
  };

  class FileRetransRequest : public ErrorMessage
  {
    char *filename;
    FileInfoHeader file_info_header;
    uint32_t address;
    std::vector<Destination> destinations;
  public:
    FileRetransRequest(const char *fname, const FileInfoHeader& finfo,
        uint32_t addr, const std::vector<Destination>& dst) :
        ErrorMessage(STATUS_INCORRECT_CHECKSUM), file_info_header(finfo),
        address(addr), destinations(dst)
    {
      size_t filename_length = file_info_header.get_name_length();
      filename = (char *)malloc(filename_length + 1);
      filename[filename_length] = '\0';
      memcpy(filename, fname, filename_length);
    }
    FileRetransRequest(uint32_t addr, const void *message,
      size_t message_length);
    ~FileRetransRequest()
    {
      free(filename);
    }

    void display() const;
    void send(int sock);

    friend class Errors;
  };

  class Errors
  {
    pthread_mutex_t mutex;
    std::list<ErrorMessage*> errors;
  public:
    Errors()
    {
      pthread_mutex_init(&mutex, NULL);
    }
    ~Errors()
    {
      while (errors.size() > 0) {
        delete errors.front();
        errors.pop_front();
      }
      pthread_mutex_destroy(&mutex);
    }

    void add(ErrorMessage *error_message)
    {
      pthread_mutex_lock(&mutex);
      errors.push_back(error_message);
      pthread_mutex_unlock(&mutex);
    }

    // Displays the registered errors 
    void display();

    // Delete the STATUS_SERVER_IS_BUSY errors
    void delete_server_is_busy_errors() {
      for(std::list<ErrorMessage*>::iterator i = errors.begin();
          i != errors.end(); ++i) {
        if ((*i)->status == STATUS_SERVER_IS_BUSY) {
          delete(*i);
          std::list<ErrorMessage*>::iterator next = i;
          ++next;
          errors.erase(i);
          i = next;
        }
      }
    }

    // Returns true if some of the errors is unrecoverable (even if it is not
    // fatal) and false otherwise
    bool is_unrecoverable_error_occurred();

    // Sends the occurred error to the imediate unicast source
    void send(int sock);
    // Sends the first occurred error to the imediate unicast source
    void send_first(int sock);

    // Get the files that should be retransmitted. dest is a value/result
    // argument.
    char** get_retransmissions(std::vector<Destination> *dest,
      int **filename_offsets);
  };

  Errors errors;

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

  static const unsigned DEFAULT_BUFFER_MASK = 0xFFFFF; // must be equal
    // to 2^n - 1
  static const unsigned DEFAULT_BUFFER_SIZE = (DEFAULT_BUFFER_MASK + 1);
  MD5sum checksum;

protected:
  // State signaling variables
  pthread_mutex_t mutex; // mutex that protect changes of offset and state
  pthread_cond_t space_ready_cond; // State change condition
  volatile bool is_reader_awaiting;
  pthread_cond_t data_ready_cond; // State change condition
  volatile bool are_writers_awaiting;
  pthread_cond_t operation_ready_cond; // New operation is ready
  pthread_cond_t writers_finished_cond; // The current operation is finished
    // by the all the writers

  // protect from copying
  Distributor(const Distributor&);
  Distributor& operator=(const Distributor&);

  /*
    Internal funcion that return space available for write
    mutex should be locked.
  */
  int _get_space();

  /*
    Internal funcion that return space available for read
    mutex should be locked.
  */
  inline int _get_data(int offset);

  bool all_writers_done()
  {
    return (!file_writer.is_present || file_writer.is_done) &&
      (!unicast_sender.is_present || unicast_sender.is_done) &&
      (!multicast_sender.is_present || multicast_sender.is_done);
  }

  bool all_done()
  {
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

  void *rposition()
  {
    return buffer + (reader.offset & DEFAULT_BUFFER_MASK);
  }

  // Move w->offset to the count bytes left (cyclic)
  // Count should not be greater than zero.
  void update_writer_position(int count, Client *w);

  void *wposition(Client *w)
  {
    return buffer + (w->offset & DEFAULT_BUFFER_MASK);
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

public:
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

  // Updates status of the multicast sender (thread safe)
  void update_multicast_sender_status(uint8_t status) {
    pthread_mutex_lock(&mutex);
    if (multicast_sender.status < status) {
      multicast_sender.status = status;
    }
    pthread_mutex_unlock(&mutex);
  }


  // Sends the occurred error to the imediate source
  void send_errors(int sock)
  {
    errors.send(sock);
  }

  // Displays the registered errors 
  void display_errors()
  {
    errors.display();
  }

  // Delete the STATUS_SERVER_IS_BUSY errors
  void delete_server_is_busy_errors() {
    errors.delete_server_is_busy_errors();
  }

  // Wrapper for the Errors.is_unrecoverable_error_occurred
  bool is_unrecoverable_error_occurred()
  {
    return errors.is_unrecoverable_error_occurred();
  }

  friend class Writer;
};

/*
  Internal funcion that return space available for read
  mutex should be locked.
*/
inline int Distributor::_get_data(int offset)
{
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
  mutex should be locked.
*/
inline int Distributor::_get_space()
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
#endif
