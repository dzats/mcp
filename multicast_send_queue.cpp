#include <assert.h>
#include <pthread.h>
#include <sys/time.h>

#include "multicast_send_queue.h"

using namespace std;

#ifndef INT32_MAX // should be included through stdint.h somehow else
#define INT32_MAX 0x7fffffff
#endif

MulticastSendQueue::MulticastSendQueue(
    const std::vector<Destination> targets) : store_position(0),
    first_unacknowledged_packet(0), unacknowledged_data(0),
    window_size(INITIAL_WINDOW_SIZE), ssthresh(UINT_MAX)
{
  n_destinations = targets.size();
  buffer = std::deque<MessageRecord*>(max(n_destinations * DEFAULT_BUFFER_SCALE,
    INITIAL_WINDOW_SIZE * 2 / MAX_UDP_PACKET_SIZE));
  for (unsigned i = 0; i < buffer.size(); ++i) {
    buffer[i] = new MessageRecord;
  }
  target_addresses = new uint32_t[n_destinations];
  round_trip_times = new unsigned[n_destinations];
  for (unsigned i = 0; i < n_destinations; ++i) {
    target_addresses[i] = targets[i].addr;
    round_trip_times[i] = 0;
  }
  first_to_acknowledge = new unsigned[n_destinations];
  memset(first_to_acknowledge, 0, sizeof(unsigned) * n_destinations);
  termination_message = NULL;
    
  pthread_mutex_init(&_mutex, NULL);
  pthread_cond_init(&space_ready_cond, NULL);
  pthread_cond_init(&_transmission_finished_cond, NULL);
}

MulticastSendQueue::~MulticastSendQueue()
{
  for (unsigned i = 0; i < buffer.size(); ++i) {
    delete buffer[i];
  }
  delete first_to_acknowledge;
  delete target_addresses;
  if (termination_message != NULL) {
    delete termination_message;
  }

  pthread_mutex_destroy(&_mutex);
  pthread_cond_destroy(&space_ready_cond);
  pthread_cond_destroy(&_transmission_finished_cond);
}

// Add message to the queue
void* MulticastSendQueue::store_message(const void *message, size_t size,
    size_t *retrans_message_size)
{
  assert(size <= MAX_UDP_PACKET_SIZE);
  struct timeval current_time;
  pthread_mutex_lock(&_mutex);
  unacknowledged_data += size;

  if (unacknowledged_data > window_size) {
    // Window is filled, wait for acknowledgements
    while(unacknowledged_data > window_size) {
      // Get the time when the first unacknowledged message has been send
      unsigned i = first_unacknowledged_packet;
      unsigned number;
      uint32_t responder = INADDR_NONE;
      for (; i < store_position; ++i) {
        responder =
          ((MulticastMessageHeader *)buffer[i]->message)->get_responder();
  
        if (responder != INADDR_NONE && buffer[i]->timestamp.tv_sec > 0) {
          current_time = buffer[i]->timestamp;
          number = ((MulticastMessageHeader *)buffer[i]->message)->get_number();
          break;
        }
      }
      if (i == store_position) {
        // There are no message in the queue for which there were no
        // retransmissions. Initializa timeout from the currect time
        // and retransmit the last message if the timeout expired.
        gettimeofday(&current_time, NULL);
        number = ((MulticastMessageHeader *)buffer[store_position -
          1]->message)->get_number();
        --i;
      }
      
#if 0
      DEBUG("max_round_trip_time: %d\n", max_round_trip_time);
#endif
      // Calculate time when the next retransmission should take place
      uint32_t *t = lower_bound(target_addresses,
        target_addresses + n_destinations, responder);
      if (responder != INADDR_NONE &&
          round_trip_times[t - target_addresses] > 0) {
        current_time.tv_usec += round_trip_times[t - target_addresses];
#if 0
      } else if (max_round_trip_time > 0) {
        current_time.tv_usec += max_round_trip_time;
#endif
      } else {
        current_time.tv_usec += DEFAULT_ROUND_TRIP_TIME;
      }
      if (current_time.tv_usec >= 1000000) {
        current_time.tv_sec += current_time.tv_usec / 1000000;
        current_time.tv_usec %= 1000000;
      }
  
      struct timespec wait_till_time;
      TIMEVAL_TO_TIMESPEC(&current_time, &wait_till_time);
  
      int error = pthread_cond_timedwait(&space_ready_cond, &_mutex,
        &wait_till_time);
      if (error != 0) {
        if (error != ETIMEDOUT) {
          ERROR("pthread_cond_timedwait error: %s\n", strerror(error));
          abort();
        } else {
          SDEBUG("pthread_cond_timedwait timeout expired\n");
          if (i < store_position &&
              ((MulticastMessageHeader *)buffer[i]->message)->get_number() == number) {
            // Retransmission for some packet required
            unacknowledged_data -= size;
            void *result = buffer[i]->message;
            *retrans_message_size = buffer[i]->size;
            buffer[i]->timestamp = (struct timeval) {0, 0};
            pthread_mutex_unlock(&_mutex);
            return result;
          } else {
            // Some acknowledgements have been received, we should
            // restart the procedure (wait for round trip time for some other
            // packet)
            continue;
          }
        }
      }
      SDEBUG("Space ready signal received\n");
      break;
    }
  }

  if (store_position == buffer.size()) {
    // Increase the buffer size instead
    buffer.push_back(new MessageRecord);
    SDEBUG("Increase the multicast send buffer\n");
  }

  // Put message into the buffer
  gettimeofday(&current_time, NULL);
  buffer[store_position]->size = size;
  buffer[store_position]->timestamp = current_time;
  memcpy(buffer[store_position]->message, message, size);
  ++store_position;
  pthread_mutex_unlock(&_mutex);
  return NULL;
}

// Acknowledge receiving all the messages till 'number' by 'destination'
// Return 0 if it is not the last expected acknowledgement
int MulticastSendQueue::acknowledge(uint32_t number, int destination)
{
  struct timeval current_time;
  gettimeofday(&current_time, NULL);
  pthread_mutex_lock(&_mutex);
  DEBUG("MulticastSendQueue::acknowledge (%u, %d) (%u, %u, %zu)\n", 
    number, destination, store_position, first_unacknowledged_packet,
    buffer.size());

  unsigned offset = number -
    ((MulticastMessageHeader *)buffer[0]->message)->get_number();
  DEBUG("offset: %d\n", offset);

  // greater instead of greater or equal here for correct session
  // termination conformation processing
  if (offset >= store_position) {
    // Ignore this conformation
    DEBUG("Ignore retransmitted conformation %u from %u\n",
      number, destination);
    pthread_mutex_unlock(&_mutex);
    return 0;
  }
  assert(offset < store_position);
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
#if 0
    unsigned previous_rtt = round_trip_times[destination];
#endif
    round_trip_times[destination] = received_rtt + (received_rtt >> 1);

#if 0
    if (max_round_trip_time < round_trip_times[destination]) {
      max_round_trip_time = round_trip_times[destination];
    } else if (max_round_trip_time == previous_rtt) {
      max_round_trip_time = *max_element(round_trip_times,
        round_trip_times + n_destinations);
    }
#endif
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

  if (offset >= first_unacknowledged_packet) {
    unsigned acknowledged_data = 0; // only by one of the destinations
    for (unsigned i = first_unacknowledged_packet; i <= offset; ++i) {
      acknowledged_data += buffer[i]->size;
    }

    unsigned old_window_size = window_size;
    // Change the window size
    if (ssthresh == UINT_MAX) {
      // Slow start + additional limitation on window size
      if (unacknowledged_data * 2 > window_size) {
        window_size += acknowledged_data;
      }
    } else {
      // Conjenction avoidance + additional limitation on window size
      if (unacknowledged_data * 2 > window_size) {
        window_size += acknowledged_data * MAX_UDP_PACKET_SIZE / window_size;
      }
    }

    DEBUG("New window size is %u (%u)\n", window_size, unacknowledged_data);

    if (unacknowledged_data > old_window_size &&
        unacknowledged_data - acknowledged_data <= window_size) {
      SDEBUG("Wake up the multicast sender\n");
      assert(pthread_cond_signal(&space_ready_cond) == 0);
    }
    unacknowledged_data -= acknowledged_data;
    first_unacknowledged_packet = offset + 1;
  }

  unsigned min_to_acknowledge =
    *min_element(first_to_acknowledge, first_to_acknowledge + n_destinations);
  if (min_to_acknowledge > 0) {
    for (unsigned i = 0; i < n_destinations; ++i) {
      first_to_acknowledge[i] -= min_to_acknowledge;
    }
    // Move all the acknowledged elements to the queue's tail
    buffer.insert(buffer.end(), buffer.begin(),
      buffer.begin() + min_to_acknowledge);
    buffer.erase(buffer.begin(), buffer.begin() + min_to_acknowledge);
    store_position -= min_to_acknowledge;
    first_unacknowledged_packet -= min_to_acknowledge;
  }

  if (termination_message != NULL && store_position == 0) {
    SDEBUG("Multicast transmission finished\n");
    pthread_cond_signal(&_transmission_finished_cond);
    pthread_mutex_unlock(&_mutex);
    return 1;
  }
  pthread_mutex_unlock(&_mutex);
  return 0;
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
// destinations have finished and -1 otherwise
int MulticastSendQueue::wait_for_destinations(const struct timespec *timeout)
{
  pthread_mutex_lock(&_mutex);
  if (store_position != 0) {
    SDEBUG("Wait for the replies from destinations\n");
    int error = pthread_cond_timedwait(&_transmission_finished_cond, &_mutex,
      timeout);
    if (error != 0) {
      if (error != ETIMEDOUT) {
        ERROR("pthread_cond_timedwait error: %s\n", strerror(error));
        abort();
      } else {
        SDEBUG("pthread_cond_timedwait timeout expired\n");
      }
      pthread_mutex_unlock(&_mutex);
      return -1;
    }
    // Transmission is done
  }
  pthread_mutex_unlock(&_mutex);
  return 0;
}

// Get message with the number 'number' and set timestamp for this message
// to 0 (can be called only by the thread controlling the message delivery)
void* MulticastSendQueue::get_message_for_retransmission(size_t *size,
    uint32_t number)
{
  pthread_mutex_lock(&_mutex);
  unsigned offset = number -
    ((MulticastMessageHeader *)buffer.front()->message)->get_number();
  DEBUG("offset: %u, first: %u, number: %u\n", offset,
    ((MulticastMessageHeader *)buffer.front()->message)->get_number(),
    number);
  if (offset >= buffer.size()) {
    return NULL;
  }
  register void *message = buffer[offset]->message;
  *size = buffer[offset]->size;
  buffer[offset]->timestamp = (struct timeval){0, 0};
  ssthresh = max(unacknowledged_data / 2, (unsigned)MAX_UDP_PACKET_SIZE * 2);
  // May be excessive condition
  DEBUG("Unacknowledged data: %u (%u)\n", unacknowledged_data, window_size);
  if (unacknowledged_data > window_size &&
      unacknowledged_data <= ssthresh + MAX_UDP_PACKET_SIZE * 3) {
    pthread_cond_signal(&space_ready_cond);
  }

  window_size = ssthresh + MAX_UDP_PACKET_SIZE * 3;
  DEBUG("Packet %u lost, new window size: %u\n", number, window_size);
  pthread_mutex_unlock(&_mutex);
  return message;
}

#if 0
// Get the first message (starting from 'from_position' in the queue) that
// has not been acknowledged. Returns NULL if there is no such message
void* MulticastSendQueue::get_unacknowledged_message(size_t *size,
    unsigned *from_number)
{
  pthread_mutex_lock(&_mutex);
  for (unsigned i = *from_position; i < store_position; ++i) {
    MulticastMessageHeader *mmh =
      ((MulticastMessageHeader *)buffer[i]->message);
    if (mmh->get_responder() != INADDR_NONE) {
      uint32_t *t = lower_bound(target_addresses,
        target_addresses + n_destinations, mmh->get_responder());
      if (t != target_addresses + n_destinations &&
          *t == mmh->get_responder() &&
          first_to_acknowledge[t - target_addresses] <= i) {
        *size = buffer[i]->size;
        register void *message = buffer[i]->message;
        pthread_mutex_unlock(&_mutex);
        // The next search should start whith the next position
        *from_position = i + 1;
        return message;
      }
    }
  }
  pthread_mutex_unlock(&_mutex);
  *from_position = 0;
  return NULL;
}
#endif
