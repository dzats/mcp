#include <assert.h>
#include <pthread.h>
#include <sys/time.h>

#include "multicast_send_queue.h"

using namespace std;

#ifndef INT32_MAX // should be included through stdint.h somehow else
#define INT32_MAX 0x7fffffff
#endif

MulticastSendQueue::MulticastSendQueue(const vector<Destination> targets,
    unsigned *rtts) : store_position(0), data_on_flow(0),
    window_size(INITIAL_WINDOW_SIZE), ssthresh(UINT_MAX),
    last_packet_caused_congestion(UINT32_MAX),
    is_queue_full(false), is_fatal_error_occurred(false)
{
  n_destinations = targets.size();
  buffer = std::deque<MessageRecord*>(max(n_destinations * DEFAULT_BUFFER_SCALE,
    INITIAL_WINDOW_SIZE * 2 / MAX_UDP_PACKET_SIZE));
  for (unsigned i = 0; i < buffer.size(); ++i) {
    buffer[i] = new MessageRecord;
  }
  target_addresses = new uint32_t[n_destinations];
  round_trip_times = new unsigned[n_destinations];
  max_round_trip_time = 0;
  for (unsigned i = 0; i < n_destinations; ++i) {
    target_addresses[i] = targets[i].addr;
    round_trip_times[i] = rtts[i];
    if (max_round_trip_time < rtts[i]) {
      max_round_trip_time = rtts[i];
    }
  }
  last_retransmission = UINT32_MAX;
  gettimeofday(&last_retransmission_time, NULL);
  
  first_to_acknowledge = new unsigned[n_destinations];
  memset(first_to_acknowledge, 0, sizeof(unsigned) * n_destinations);
  termination_message = NULL;

  gettimeofday(&last_data_on_flow_evaluation, NULL);
    
  pthread_mutex_init(&mutex, NULL);
  pthread_cond_init(&space_ready_cond, NULL);
  pthread_cond_init(&transmission_finished_cond, NULL);
}

MulticastSendQueue::~MulticastSendQueue()
{
  for (unsigned i = 0; i < buffer.size(); ++i) {
    delete buffer[i];
  }
  delete first_to_acknowledge;
  delete target_addresses;
  delete round_trip_times;
  if (termination_message != NULL) {
    delete termination_message;
  }

  pthread_mutex_destroy(&mutex);
  pthread_cond_destroy(&space_ready_cond);
  pthread_cond_destroy(&transmission_finished_cond);
}

// Wait for free space in the buffer, mutex should be locked
int MulticastSendQueue::wait_for_space(unsigned timeout) {
  struct timeval current_time;
  struct timespec till_time;
  gettimeofday(&current_time, NULL);
  TIMEVAL_TO_TIMESPEC(&current_time, &till_time);
  till_time.tv_sec += timeout / 1000000;
  till_time.tv_nsec += (timeout % 1000000) * 1000;
  if (till_time.tv_nsec > 1000000000) {
    till_time.tv_nsec -= 1000000000;
    till_time.tv_sec += 1;
  }
  is_queue_full = true;
  int wait_result = pthread_cond_timedwait(&space_ready_cond,
    &mutex, &till_time);
  is_queue_full = false;
  if (wait_result == 0) {
    // Some reply received and space've been freed
    return 0;
  } else {
    assert(wait_result == ETIMEDOUT);
    // Timeout expired
    SDEBUG("Timeout expired\n");
    return ETIMEDOUT;
  }
}

// Add message to the queue
const void* MulticastSendQueue::store_message(const void *message, size_t size,
    size_t *retrans_message_size)
{
  assert(size <= MAX_UDP_PACKET_SIZE);
  struct timeval current_time;
  if (is_fatal_error_occurred) {
    throw MulticastException(MulticastException::fatal_error_received,
      INADDR_NONE);
  }
  pthread_mutex_lock(&mutex);
  DEBUG("missed: %zu, max_rtt: %u\n", missed_packets.size(),
    max_round_trip_time);

  const void *message_to_send = message;
  while (missed_packets.size() > 0) {
    // Some packets have been missed
    unsigned number = *missed_packets.begin();
    missed_packets.erase(missed_packets.begin());

    unsigned offset = number -
      ((MulticastMessageHeader *)buffer[0]->message)->get_number();
    DEBUG("error offset: %d\n", offset);

    if (offset < store_position) {
      message_to_send = buffer[offset]->message;
      *retrans_message_size = buffer[offset]->size;
      buffer[offset]->timestamp.tv_sec = 0;
      break;
    } else {
      DEBUG("Retransmission request for %u is outdated\n", number);
    }
  }

  if (message_to_send == message &&
      store_position > n_destinations * 2 + (window_size >> 9) + 7) {
    /*
      The previlus rude expression means n_destinations * 2 + channel capacity.
      Buffers is filled, perform retransmission for the first unacknowledged
      message, that has not been retransmitted yet, or for the first
      unacknowledged message if there were retransmissions for all
      the messages in the buffer. TODO: In the latter case delay the
      retransmission by max_round_trip_time * 2
    */
    SDEBUG("Buffer is full\n");
    unsigned min_unaknowledged = UINT_MAX;
    unsigned i = 0;
    bool is_message_for_retransmission_choosen = false;
    for (; i < store_position; ++i) {
      MulticastMessageHeader *mmh =
          (MulticastMessageHeader *)buffer[i]->message;
      if (mmh->get_responder() != INADDR_NONE) {
        uint32_t *t = lower_bound(target_addresses,
          target_addresses + n_destinations, mmh->get_responder());
        if (t != target_addresses + n_destinations &&
            *t == mmh->get_responder() &&
            first_to_acknowledge[t - target_addresses] <= i) {
          if (buffer[i]->timestamp.tv_sec > 0) {
            // The next search should start with the next position
            *retrans_message_size = buffer[i]->size;
            message_to_send = buffer[i]->message;
            is_message_for_retransmission_choosen = true;
            gettimeofday(&current_time, NULL);
            struct timeval packet_send_time = buffer[i]->timestamp;
            unsigned timeout = (round_trip_times[t - target_addresses] << 1) -
              ((current_time.tv_sec - buffer[i]->timestamp.tv_sec) * 1000000 +
              current_time.tv_usec - buffer[i]->timestamp.tv_usec);
            assert(timeout < 1000000 || timeout > INT_MAX);
            if (timeout < 1000000) {
              if (wait_for_space(timeout) == 0) {
                // Space has been freed
                SDEBUG("Space has been freed\n");
                message_to_send = message;
              } else {
                // Timeout expired
                buffer[i]->timestamp.tv_sec = 0;
              }
            } else {
              // Timeout has already expired
              buffer[i]->timestamp.tv_sec = 0;
            }
            break;
          } else {
            if (min_unaknowledged == UINT_MAX) {
              min_unaknowledged = i;
            }
          }
        }
      }
    }
    if (!is_message_for_retransmission_choosen) {
      SDEBUG("At least one retransmission performed for all packets\n");
      assert(min_unaknowledged != UINT_MAX);
      *retrans_message_size = buffer[min_unaknowledged]->size;
      message_to_send = buffer[min_unaknowledged]->message;

      // Delay retransmission for MAX_RTT
      if (wait_for_space(max_round_trip_time) == 0) {
        // Space has been freed
        message_to_send = message;
      } else {
        // Timeout expired
        MulticastMessageHeader *mmh =
          (MulticastMessageHeader *)buffer[min_unaknowledged]->message;
        if (mmh->get_number() == last_retransmission) {
          SDEBUG("Reapeated retransmission\n");
          struct timeval current_time;
          gettimeofday(&current_time, NULL);
          if ((unsigned)(current_time.tv_sec -
              last_retransmission_time.tv_sec) > MAX_DESTINATION_IDLE_TIME) {
            // The destination has not replied too long, terminate the
            // connection
            pthread_mutex_unlock(&mutex);
            throw MulticastException(MulticastException::connection_timed_out,
              mmh->get_responder());
          }
        } else {
          last_retransmission = mmh->get_number();
          gettimeofday(&last_retransmission_time, NULL);
        }
        buffer[min_unaknowledged]->timestamp.tv_sec = 0;
      }
    }
  }

  // Rate control code
  // Adjust amount of data that is currently on flow
  gettimeofday(&current_time, NULL);
  // FIXME: Be careful to avoid overflows
  unsigned data_delivered = ((uint64_t)window_size *
    ((current_time.tv_sec - last_data_on_flow_evaluation.tv_sec) * 1000000 +
    current_time.tv_usec - last_data_on_flow_evaluation.tv_usec)) /
    max_round_trip_time;
  if (data_delivered > data_on_flow) {
    data_delivered = data_on_flow;
  }
  // Some limitation to avoid unlimited grouth of window_size
  if (window_size < INT32_MAX) {
    // Change the window size
    if (ssthresh == UINT_MAX) {
      // Slow start
      window_size += data_delivered;
    } else {
      // Congestion avoidance
      window_size += (MAX_UDP_PACKET_SIZE * data_delivered) / window_size;
    }
    DEBUG("New window size is %u (%u/%u)\n", window_size, data_delivered,
      data_on_flow);
  }
  last_data_on_flow_evaluation = current_time;
  data_on_flow -= data_delivered;
  
  if (message_to_send == message) {
    data_on_flow += size;
    if (message == NULL) {
      // The request is simpy a check, whether some messages
      // should be retransmitted.
      pthread_mutex_unlock(&mutex);
      return NULL;
    }
  } else {
    data_on_flow += *retrans_message_size;
  }

  if (data_on_flow > window_size) {
    // Delay the packet to avoid congestion
    // FIXME: Be careful to avoid overflow in the following expression
    useconds_t delay = max_round_trip_time *
      (((data_on_flow - window_size) << 12) / window_size) >> 12;
    // FIXME: Check whether this kind of protection is really required
    if (delay > 1000000) {
      delay = 1000000;
    }
    pthread_mutex_unlock(&mutex);
    usleep(delay);
    pthread_mutex_lock(&mutex);
  }

  if (message_to_send == message) {
    if (store_position == buffer.size()) {
      // Increase the buffer size instead
      buffer.push_back(new MessageRecord);
      SDEBUG("Increase the multicast send buffer\n");
    }

    // Put message into the buffer
    assert(store_position < buffer.size());
    gettimeofday(&current_time, NULL);
    buffer[store_position]->size = size;
    buffer[store_position]->timestamp = current_time;
    memcpy(buffer[store_position]->message, message, size);
    ++store_position;
  }
  pthread_mutex_unlock(&mutex);
  return message_to_send;
}

// Acknowledge receiving all the messages till 'number' by 'destination'
// Return 0 if it is not the last expected acknowledgement
int MulticastSendQueue::acknowledge(uint32_t number, int destination)
{
  struct timeval current_time;
  gettimeofday(&current_time, NULL);
  pthread_mutex_lock(&mutex);
  DEBUG("MulticastSendQueue::acknowledge (%u, %d) (%u, %zu)\n", 
    number, destination, store_position, buffer.size());

  unsigned offset = number -
    ((MulticastMessageHeader *)buffer[0]->message)->get_number();
  DEBUG("offset: %d\n", offset);

  if (offset >= store_position) {
    // Ignore this conformation
    DEBUG("Ignore retransmitted conformation %u from %u\n",
      number, destination);
    pthread_mutex_unlock(&mutex);
    return 0;
  }
  first_to_acknowledge[destination] = offset + 1;

  // Update the Round-Trip Time information, if there were no
  // retransmissions for this packet
  if (buffer[offset]->timestamp.tv_sec > 0) {
    struct timeval current_time;
    gettimeofday(&current_time, NULL);
    unsigned received_rtt =
      (current_time.tv_sec - buffer[offset]->timestamp.tv_sec) * 1000000 +
      current_time.tv_usec - buffer[offset]->timestamp.tv_usec;
    DEBUG("Received round trip time: %d\n", received_rtt);
    unsigned previous_rtt = round_trip_times[destination];
    if (round_trip_times[destination] * 2 > received_rtt) {
      round_trip_times[destination] = received_rtt;
    } else {
      round_trip_times[destination] += round_trip_times[destination] >> 2;
    }

    if (max_round_trip_time < round_trip_times[destination]) {
      max_round_trip_time = round_trip_times[destination];
    } else if (max_round_trip_time == previous_rtt) {
      max_round_trip_time = *max_element(round_trip_times,
        round_trip_times + n_destinations);
    }
    //assert(max_round_trip_time < 200000);
    buffer[offset]->timestamp.tv_sec = 0;
  }

  if (termination_message != 0 &&
      first_to_acknowledge[destination] >= store_position) {
    // Remove distination from 'termination_message'
    uint32_t *hosts_begin = (uint32_t *)((uint8_t *)termination_message +
      sizeof(MulticastMessageHeader)); 
    uint32_t *hosts_end = (uint32_t *)((uint8_t *)termination_message +
      termination_message_size); 
    uint32_t *i = find(hosts_begin, hosts_end,
      target_addresses[destination]);
    if (i != hosts_end) {
      *i = *(hosts_end - 1);
      --termination_message_size;
    }
  }

  unsigned min_to_acknowledge =
    *min_element(first_to_acknowledge, first_to_acknowledge + n_destinations);
  if (min_to_acknowledge > 0) {
    for (unsigned i = 0; i < n_destinations; ++i) {
      first_to_acknowledge[i] -= min_to_acknowledge;
      assert(first_to_acknowledge[i] < INT_MAX);
    }
    // Move all the acknowledged elements to the queue's tail
    unsigned i = min_to_acknowledge;
    for (;i > 0; --i) {
      MessageRecord *swp = buffer.front();
      buffer.pop_front();
      buffer.push_back(swp);
    }
    store_position -= min_to_acknowledge;
    assert(store_position < INT_MAX);
    if (is_queue_full) {
       SDEBUG("Wake up the sender\n");
       pthread_cond_signal(&space_ready_cond);
    }
  }

  if (termination_message != NULL) {
    if (store_position == 0) {
      SDEBUG("Multicast transmission finished\n");
      pthread_cond_signal(&transmission_finished_cond);
      pthread_mutex_unlock(&mutex);
      return 1;
    } else {
      is_some_destinations_replied = true;
    }
  }
  pthread_mutex_unlock(&mutex);
  return 0;
}

// Function registers that some of the packets has been missed
void MulticastSendQueue::add_missed_packets(uint32_t number,
    int destination, uint32_t *numbers, uint32_t *end)
{
  DEBUG("add_missed_packets: %p, %p\n", numbers, end);

  // Update the Round-Trip Time information, if there were no
  // retransmissions for this packet
  unsigned offset = number -
    ((MulticastMessageHeader *)buffer[0]->message)->get_number();
  DEBUG("offset: %u, first: %u, number: %u\n", offset,
    ((MulticastMessageHeader *)buffer.front()->message)->get_number(),
    number);
  assert(offset < store_position);
  if (offset < store_position && buffer[offset]->timestamp.tv_sec) {
    struct timeval current_time;
    gettimeofday(&current_time, NULL);
    unsigned received_rtt =
      (current_time.tv_sec - buffer[offset]->timestamp.tv_sec) * 1000000 +
      current_time.tv_usec - buffer[offset]->timestamp.tv_usec;
    DEBUG("Received round trip time: %d\n", received_rtt);
    unsigned previous_rtt = round_trip_times[destination];
    if (round_trip_times[destination] * 2 > received_rtt) {
      round_trip_times[destination] = received_rtt;
    } else {
      round_trip_times[destination] += round_trip_times[destination] >> 2;
    }

    if (max_round_trip_time < round_trip_times[destination]) {
      max_round_trip_time = round_trip_times[destination];
    } else if (max_round_trip_time == previous_rtt) {
      max_round_trip_time = *max_element(round_trip_times,
        round_trip_times + n_destinations);
    }
    //assert(max_round_trip_time < 200000);
    //buffer[offset]->timestamp.tv_sec = 0;
  }

  if (numbers != end) {
    // Change window size if there were no messages about missed packets
    // recently (during RTT)
  	// FIXME: Check whether this algorithm is appropriate
#if 0
    if (number - last_packet_caused_congestion <
        UINT32_MAX - MAX_EXPECTED_WINDOW_SIZE) {
#endif
      ssthresh = max(data_on_flow / 2, (unsigned)MAX_UDP_PACKET_SIZE * 2);
      window_size = ssthresh + (ssthresh >> 1) + MAX_UDP_PACKET_SIZE * 3;
      DEBUG("New window size: %u\n", window_size);
#if 0
      last_packet_caused_congestion = 
        ((MulticastMessageHeader *)buffer[store_position - 1]->message)->get_number();
    }
#endif

    end -= 2;
    pthread_mutex_lock(&mutex);
    while (numbers <= end) {
      uint32_t i = ntohl(*numbers);
      ++numbers;
      for (; i <= ntohl(*numbers); ++i) {
#ifndef NDEBUG
        uint32_t dest_addr = htonl(target_addresses[destination]);
        char dest_name[INET_ADDRSTRLEN];
        DEBUG("Packet %d has not been delivered to %s\n", i,
          inet_ntop(AF_INET, &dest_addr, dest_name, sizeof(dest_name)));
#endif
        missed_packets.insert(i);
      }
      ++numbers;
    }
  }
  is_some_destinations_replied = true;
  pthread_mutex_unlock(&mutex);
}

// Compose the session termination message and return it to the caller
void *MulticastSendQueue::prepare_termination(size_t *size,
    uint32_t session_id)
{
  if (termination_message != NULL) {
    delete termination_message;
  }
  // Compose the termination message
  termination_message_size = sizeof(MulticastMessageHeader) +
    n_destinations * sizeof(uint32_t);
  termination_message = new uint8_t[termination_message_size];
  MulticastMessageHeader *mmh = new(termination_message)
    MulticastMessageHeader(MULTICAST_TERMINATION_REQUEST, session_id);
  uint32_t *p = (uint32_t *)(mmh + 1);
  for (unsigned i = 0; i < n_destinations; ++i) {
    *p++ = htonl(target_addresses[i]);
  }
  *size = termination_message_size;
  return termination_message;
}

// Waits for the transmission termination. Returns 0 if all the
// destinations have finished, 1 if some of the destinations
// have send replies and -1 otherwise
int MulticastSendQueue::wait_for_destinations(const struct timespec *till_time)
{
  pthread_mutex_lock(&mutex);
  is_some_destinations_replied = false;
  if (store_position != 0) {
    SDEBUG("Wait for the replies from destinations\n");
    int error = pthread_cond_timedwait(&transmission_finished_cond, &mutex,
      till_time);
    if (error != 0) {
      if (error != ETIMEDOUT) {
        ERROR("pthread_cond_timedwait error: %s\n", strerror(error));
        abort();
      } else {
        SDEBUG("pthread_cond_timedwait timeout expired\n");
      }
      pthread_mutex_unlock(&mutex);
      if (is_some_destinations_replied) {
        return 1;
      } else {
        return -1;
      }
    }
    // Transmission is done
  }
  pthread_mutex_unlock(&mutex);
  return 0;
}

