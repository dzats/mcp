#ifndef MULTICAST_RECV_QUEUE_H_HEADER
#define MULTICAST_RECV_QUEUE_H_HEADER 1
#include <pthread.h>

#include <queue>

#include "connection.h"

class MulticastRecvQueue
{
  struct MessageRecord
  {
    size_t length; // zero means that the message doesn't present in the queue
    uint8_t message[MAX_UDP_PACKET_SIZE];
    MessageRecord() {}
  };
  static const int DEFAULT_BUFFER_SIZE = 100;
  static const int MAX_QUEUE_SIZE = 6000; // Maximum number of messages in
    // the queue
  std::deque<MessageRecord*> buffer; // dequeue of the message number to
  MessageRecord *swapper;

  uint32_t first_num; // Number of the first message in the buffer
  uint32_t last_num; // Number of the last message in the buffer
  unsigned n_messages; // Index of the first buffer's free  element,
    // value UINT_MAX means that the queue is in error state

  pthread_mutex_t mutex;
  pthread_cond_t data_ready_cond;
  pthread_cond_t termination_cond;
  //pthread_cond_t _transmission_finished_cond;

public:
  MulticastRecvQueue();
  ~MulticastRecvQueue();

  // Wait until the MULTICAST_TERMINATION_REQUEST message will be processed
  void wait_termination_synchronization();
  // Signal that the MULTICAST_TERMINATION_REQUEST message has been processed
  void signal_termination_synchronization();
  // Set queue to the error state
  void set_fatal_error();

  // Add message to the queue
  int put_message(const void *message, size_t length, uint32_t number);
  // Get message from the queue
  void* get_message(size_t *length);
};
#endif
