#ifndef MULTICAST_SEND_QUEUE_H_HEADER
#define MULTICAST_SEND_QUEUE_H_HEADER 1
#include <time.h>

#include <queue>
#include <algorithm>

#include "connection.h"
#include "destination.h"

class MulticastSendQueue
{
  struct MessageRecord
  {
    size_t size; // size of the field 'message'
    struct timeval timestamp; // time when this packet has been send or
      // 0 if some retransmissions have been send for this message
    uint8_t message[MAX_UDP_PACKET_SIZE];
  };
  static const unsigned DEFAULT_BUFFER_SCALE = 3; // buffer size / number
  static const unsigned DEFAULT_ROUND_TRIP_TIME = 160000; // Time before
    // the first retransmissioin (while actual RTT has not been detected yet)
    // (in microseconds)
  static const unsigned INITIAL_WINDOW_SIZE = MAX_UDP_PACKET_SIZE * 3;
  std::deque<MessageRecord*> buffer; // dequeue of the message number to
    // the message content
  unsigned store_position;
  unsigned first_unacknowledged_packet; // offset of the first packet,
    // that has not been acknowledged by any destination
  unsigned unacknowledged_data; // data (in bytes) in the buffer, which has
    // not been acknowledged by any destination

  unsigned n_destinations;
  unsigned *first_to_acknowledge; // Array of the offsets of the first
  // unacknowledged messages in the buffer. Index in this array
  // is the destination number
  uint32_t *target_addresses; // IP addresses of the destinations
  unsigned *round_trip_times; // Round trip timesin microseconds

  unsigned window_size; // data window size (something like the TCP's cwnd)

  unsigned ssthresh; // Slow start threshold, is not very
    // required here (see RFC 2581)

  // Session termination message
  uint8_t *termination_message;
  size_t termination_message_size;

  pthread_mutex_t _mutex;
  pthread_cond_t space_ready_cond;
  pthread_cond_t _transmission_finished_cond;

public:
  MulticastSendQueue(const std::vector<Destination> targets);
  ~MulticastSendQueue();

  unsigned get_window_size() const { return window_size; }
  unsigned get_max_round_trip_time() const
  {
    return *std::max_element(round_trip_times,
      round_trip_times + n_destinations);
  }

  // Add message to the queue. Returns NULL on success and message
  // that should be retransmitted if the sending window is full
  void* store_message(const void *message, size_t size,
    size_t *retrans_message_size);
  // Acknowledge receiving all the messages till 'number' by 'destination'
  int acknowledge(uint32_t number, int destination);
  // Compose the session termination message and return it to the caller
  void* prepare_termination(size_t *size, uint32_t session_id);

  // Waits for the transmission termination. Returns addresses of the
  // hosts there were no final replies from.
  int wait_for_destinations(const struct timespec *timeout);

  // Get message with the number 'number' and set timestamp for this message
  // to 0 (can be called only by the thread controlling the message delivery)
  void *get_message_for_retransmission(size_t *size, uint32_t number);

#if 0
  // Get the first message (starting from 'from_position' in the queue) that
  // has not been acknowledged. Returns NULL if there is no such message
  void* get_unacknowledged_message(size_t *size, unsigned *from_position);

  void lock_queue()
  {
    pthread_mutex_lock(&_mutex);
  }
  void release_queue()
  {
    pthread_mutex_unlock(&_mutex);
  }
#endif
};
#endif
