#ifndef MULTICAST_SEND_QUEUE_H_HEADER
#define MULTICAST_SEND_QUEUE_H_HEADER 1
#include <time.h>

#include <queue>
#include <set>
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
  unsigned store_position; // first free position in the buffer

  unsigned data_on_flow; // data that is expected to be somewhere
    // inside the channel
  struct timeval last_data_on_flow_evaluation; // timestamp when the last
    // packet has been sent

  unsigned n_destinations;
  unsigned *first_to_acknowledge; // Array of the offsets of the first
  // unacknowledged messages in the buffer. Index in this array
  // is the destination number
  uint32_t *target_addresses; // IP addresses of the destinations
  unsigned *round_trip_times; // Round trip timesin microseconds
  unsigned max_round_trip_time; // Maximum round trip time in microseconds

  unsigned window_size; // data window size (something like the TCP's cwnd)

  unsigned ssthresh; // Slow start threshold, is not very
    // required here (see RFC 2581)

  std::set<uint32_t> missed_packets; // Packets that has been missed on some of
    // the destinations

  volatile bool is_queue_full;

  // Session termination message
  uint8_t *termination_message;
  size_t termination_message_size;
  bool is_some_destinations_replied; // Whether some of the destinations
    // replied somehow to the session termination request message

  pthread_mutex_t mutex;
  pthread_cond_t space_ready_cond;
  pthread_cond_t transmission_finished_cond;

public:
  MulticastSendQueue(const std::vector<Destination> targets, unsigned *rtts);
  ~MulticastSendQueue();

  unsigned get_window_size() const { return window_size; }
  unsigned get_max_round_trip_time() const
  {
    return *std::max_element(round_trip_times,
      round_trip_times + n_destinations);
  }

  // Wait for free space in the buffer
  int wait_for_space(unsigned timeout);

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

  // Function registers that some of the packets has been missed
  void add_missed_packets(uint32_t number, uint32_t destination,
    uint32_t *numbers, uint32_t *end);
};
#endif
