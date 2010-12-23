#ifndef MULTICAST_SEND_QUEUE_H_HEADER
#define MULTICAST_SEND_QUEUE_H_HEADER 1
#include <time.h>

#include <queue>
#include <set>
#include <algorithm>
#include <exception>

#include "connection.h"
#include "destination.h"

class MulticastSendQueue
{
public:
  static const unsigned MAX_EXPECTED_WINDOW_SIZE = 4096; // (in packets)
    // there will be no significant harm if this limit is too low.

  // The exception indicating that some of the multicast destinations
  // returned an error.
  struct MulticastException : public std::exception {
    enum Reason { fatal_error_received, connection_timed_out };
    Reason reason;
    uint32_t culprit;
    MulticastException(Reason r, uint32_t c) : reason(r), culprit(c)  {}
  };
  // See Equation-based Congestion Control for details
  struct LossInformation {
    static const unsigned MAX_INTERVAL_LENGTH = 16000; // Max number of packets
      // that are taken into account during loss rate evaluation
#if 0
    static const unsigned LOSS_INTERVALS_ACCOUNTED = 8; // Don't change this
      // value unless you've appropriately changed the add_missed method
    unsigned intervals[LOSS_INTERVALS_ACCOUNTED]; // intervals that
      // has not been lost by the particular destinations
    int first_interval; // first element in the loss_interval array
    double average_loss_interval; // Weighted average loss interval
#endif
    double loss_event_rate; // lost packets / sent packets
    uint32_t last_packet; // Last packet that has been received by the
      // destinatin
    double measured_interval; // Length of the interval that
      // has been taken into account
    unsigned n_errors_previous; // Number of errors on the previous
      // controlling interval

    LossInformation() : loss_event_rate(0), last_packet(0),
        n_errors_previous(0) {
#if 0
      first_interval = 0;
      average_loss_interval = 0.0;
      memset(intervals, 0, sizeof(intervals));
#endif
    }
  };
private:
  struct MessageRecord
  {
    size_t size; // size of the field 'message'
    struct timeval timestamp; // time when this packet has been send or
      // 0 if some retransmissions have been send for this message
    uint8_t message[MAX_UDP_PACKET_SIZE];
  };
  static const unsigned DEFAULT_BUFFER_SCALE = 3; // buffer size / number
    // the first retransmissioin (while actual RTT has not been detected yet)
    // (in microseconds)
  static const unsigned INITIAL_WINDOW_SIZE = MAX_UDP_PACKET_SIZE * 3;

#ifdef NDEBUG
  static const unsigned MAX_DESTINATION_IDLE_TIME = 30; // (in seconds)
#else
  static const unsigned MAX_DESTINATION_IDLE_TIME = 4; // (in seconds)
#endif

  std::deque<MessageRecord*> buffer; // dequeue of the message number to
    // the message content
  unsigned store_position; // first free position in the buffer

  volatile unsigned data_on_flow; // data that is expected to be somewhere
    // inside the channel
  struct timeval last_data_on_flow_evaluation; // timestamp when the last
    // packet has been sent

  unsigned n_destinations;
  unsigned *first_to_acknowledge; // Array of the offsets of the first
  // unacknowledged messages in the buffer. Index in this array
  // is the destination number
  LossInformation *losses; // Information about loss rate
    // for each of the destinations;
  double max_loss_event_rate; // Maximum of the loss_event_rate values
    // for all the destinations
  unsigned destination_with_max_loss_event_rate; // number of the destination
    // with maximum loss_event_rate

  uint32_t *target_addresses; // IP addresses of the destinations
  unsigned *round_trip_times; // Round trip timesin microseconds
  unsigned max_round_trip_time; // Maximum round trip time in microseconds

  // A group of fields used to detect the case when one of the destinations
  // is not responding too long
  uint32_t last_retransmission;
  struct timeval last_retransmission_time;

  unsigned window_size; // data window size (something like the TCP's cwnd)

  unsigned ssthresh; // Slow start threshold, is not very
    // required here (see RFC 2581)

  std::set<uint32_t> missed_packets; // Packets that has been missed on some of
    // the destinations
  uint32_t last_packet_caused_congestion; // Last packet among the data
    // caused congestion

  volatile bool is_queue_full;
  volatile bool is_fatal_error_occurred;

  // Session termination message
  uint8_t *termination_message;
  volatile size_t termination_message_size;
  bool is_some_destinations_replied; // Whether some of the destinations
    // replied somehow to the session termination request message

  pthread_mutex_t mutex;
  pthread_cond_t space_ready_cond;
  pthread_cond_t transmission_finished_cond;

public:
  MulticastSendQueue(const std::vector<Destination> targets, unsigned *rtts);
  ~MulticastSendQueue();

  unsigned get_window_size() const { return window_size; }
  unsigned get_max_round_trip_time() const { return max_round_trip_time; }
  unsigned get_termination_message_size() const
  {
    return termination_message_size;
  }
  void register_fatal_error() { is_fatal_error_occurred = true; }

  // Wait for free space in the buffer
  int wait_for_space(unsigned timeout);

  // Add message to the queue. Returns NULL on success and message
  // that should be retransmitted if the sending window is full
  const void* store_message(const void *message, size_t size,
    size_t *retrans_message_size);
  // Acknowledge receiving all the messages till 'number' by 'destination'
  int acknowledge(uint32_t number, int destination);
  // Compose the session termination message and return it to the caller
  void* prepare_termination(uint32_t session_id);

  // Changes the loss event rate for particular destination and possibly
  // the max_loss_event_rate
  void adjust_loss_rate(int destination, double new_rate,
    unsigned new_interval);

  // Waits for the transmission termination. Returns addresses of the
  // hosts there were no final replies from.
  int wait_for_destinations(const struct timespec *timeout);

  // Function registers that some of the packets has been missed
  void add_missed_packets(uint32_t number, int destination,
    uint32_t *numbers, uint32_t *end);
};
#endif
