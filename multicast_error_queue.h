#ifndef MULTICAST_ERROR_QUEUE_H
#define MULTICAST_ERROR_QUEUE_H 1
#include <sys/time.h> // for struct timeval

#include <list>

#include "connection.h"

// Queue of the pending errors, used by the MulticastReceiver
class MulticastErrorQueue
{
public:
  // Structure describing one multicast error to be send
  struct ErrorMessage
  {
    void *message; // File retransmission request message
      // (MulticastMessageHeader + FileInfoHeader + filename)
    unsigned message_size;
    struct timeval timestamp; // timestamp when the last message of this
    unsigned retrans_timeout; // retransmission timeout for this packet

    bool operator==(const ErrorMessage& second) const
    {
      register unsigned offset = sizeof(MulticastMessageHeader) +
          sizeof(FileInfoHeader);
      register unsigned size = message_size - offset;
      return message_size == second.message_size &&
        !memcmp((uint8_t *)message + offset,
          (uint8_t *)second.message + offset, size);
    }
    bool operator==(const char* second) const
    {
      register unsigned offset = sizeof(MulticastMessageHeader) +
          sizeof(FileInfoHeader);
      register unsigned size = message_size - offset;
      return message_size == strlen(second) &&
        !memcmp((uint8_t *)message + offset, second, size);
    }

    virtual ~ErrorMessage() {}
  };

  // Structure describing a file retransmission request
  struct FileRetransRequest : public ErrorMessage
  {
    FileRetransRequest (const char *filename, const FileInfoHeader& finfo,
      uint32_t session_id, uint32_t number, uint32_t local_address);
    ~FileRetransRequest()
    {
      free(message);
    }
  };

  // Structure describing an error message (all except the file retransmission
  // requests)
  struct TextError : public ErrorMessage
  {
    TextError(uint8_t status, const char *error, uint32_t session_id,
      uint32_t number, uint32_t local_address);
    ~TextError()
    {
      free(message);
    }
  };

private:
  uint32_t next_packet_number; // number of the next file retransmission
    // request used to implement protection against replies 
  pthread_mutex_t mutex;
  std::list<ErrorMessage*> errors;
  volatile unsigned n_errors;
public:

  MulticastErrorQueue() : next_packet_number(0), n_errors(0)
  {
    pthread_mutex_init(&mutex, NULL);
  }
  ~MulticastErrorQueue()
  {
    pthread_mutex_destroy(&mutex);
  }

  unsigned get_n_errors()
  {
    return n_errors;
  }

  // Add retransmission for the file 'fimename' to the queue
  void add_retrans_request(const char *filename, const FileInfoHeader& finfo,
      uint32_t session_id, uint32_t local_address);

  // Move retransmission for the file 'fimename' in the end of the queue
  std::list<ErrorMessage*>::iterator get_error();

  // Move the error message pointed by 'arg' to the end of the queue
  void move_back(const std::list<ErrorMessage*>::iterator& arg);

  // Move the error message with the number 'number' from the queue
  void remove(uint32_t number);
};
#endif

