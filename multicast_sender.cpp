#include <string.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/param.h> // for MAXPATHLEN
#include <sys/time.h>
#include <poll.h>

#include <pthread.h>

#include <memory>
#include <vector>
#include <set>
#include <map>
#include <algorithm>

using namespace std;

#include "multicast_sender.h"
#include "multicast_send_queue.h"
#include "destination.h"

// A helper function that chooses a UDP port and binds socket with it
uint16_t MulticastSender::choose_ephemeral_port(bool use_global_multicast)
{
  uint16_t ephemeral_port;
  struct sockaddr_in ephemeral_addr;
  memset(&ephemeral_addr, 0, sizeof(ephemeral_addr));
  ephemeral_addr.sin_family = AF_INET;
  ephemeral_addr.sin_addr.s_addr = htonl(local_address);
  int bind_result;
  int tries = MAX_PORT_CHOOSING_TRIES;

  // Randomly choose an ephemeral UDP port for the multicast connection
  // FIXME: It's not a very good decisition
  ephemeral_port = 49152 + rand() % (58000 - 49152);
  do {
    --tries;
    ephemeral_addr.sin_port = htons(ephemeral_port);
    DEBUG("Try ephemeral UDP port: %d\n", ephemeral_port);

    bind_result = bind(sock, (struct sockaddr *)&ephemeral_addr,
      sizeof(ephemeral_addr));

    if (bind_result != 0) {
      ephemeral_port = ephemeral_port + EPHEMERAL_PORT_CHOOSING_STEP;
      if (ephemeral_port >= 58000) {
        ephemeral_port = ephemeral_port - 58000 + 49152;
      }
      continue;
    } else {
      // Set default interface for multicast traffic
      if (setsockopt(sock, IPPROTO_IP, IP_MULTICAST_IF,
          &ephemeral_addr.sin_addr, sizeof(ephemeral_addr.sin_addr)) != 0) {
        ERROR("Can't set default interface "
          "for outgoing multicast traffic: %s\n", strerror(errno));
        exit(EXIT_FAILURE);
      }
  
      if (use_global_multicast) {
        // Set TTL for multicast packets
        unsigned char ttl = 31;
        if (setsockopt(sock, IPPROTO_IP, IP_MULTICAST_TTL,
            &ttl, sizeof(ttl)) != 0) {
          ERROR("Can't set TTL for outgoing multicast packets: %s\n",
            strerror(errno));
          exit(EXIT_FAILURE);
        }
      }
    }
  } while (tries > 0 && errno == EADDRINUSE);
  return bind_result == 0 ? ephemeral_port : 0;
}

// Register an error and finish the current task
void MulticastSender::register_error(uint8_t status, const char *fmt,
    const char *error)
{
  char *error_message = (char *)malloc(strlen(fmt) + strlen(error) + 1);
  sprintf((char *)error_message, fmt, error);
  DEBUG("register error: %s\n", error_message);
  reader->add_error(new SimpleError(status, INADDR_NONE,
    error_message, strlen(error_message)));
  reader->update_multicast_sender_status(status);
  // Finish the multicast sender
  submit_task();
}

// Abnormal multicast connection termination
// FIXME: Rewrite this routine to something synchronious,
// like it is done for the session initialization routine
void MulticastSender::abnormal_termination() {
  SDEBUG("Abnormal termination of the multicast connection\n");
  unsigned max_rtt;
  if (send_queue != NULL) {
    max_rtt = send_queue->get_max_round_trip_time();
    if (max_rtt == 0) {
      max_rtt = DEFAULT_ROUND_TRIP_TIME;
    }
  } else {
    max_rtt = DEFAULT_ROUND_TRIP_TIME;
  }
  // Compose the multicast abnormal termination request message
  unsigned message_length = sizeof(MulticastMessageHeader) +
    sizeof(MulticastHostRecord) * targets.size();
  uint8_t message[message_length];
  MulticastMessageHeader *mmh = new(message)
    MulticastMessageHeader(MULTICAST_ABNORMAL_TERMINATION_REQUEST, session_id);
  mmh->set_number(max_rtt);

  MulticastHostRecord *hr = (MulticastHostRecord *)(mmh + 1);
  for (uint32_t i = 0; i < targets.size(); ++i) {
    hr->set_addr(targets[i].addr);
    ++hr;
  }

  try {
    for (unsigned i = 0; i < MAX_ABNORMAL_TERMINATION_RETRIES; ++i) {
      DEBUG("Send MULTICAST_ABNORMAL_TERMINATION_REQUEST %u\n", i);
      udp_send(message, message_length, 0);
      usleep(max_rtt >> 1);
    }
  } catch (ConnectionException& e) {
    ERROR("Can't send MULTICAST_ABNORMAL_TERMINATION_REQUEST: %s\n", e.what());
    return;
  }
}

// Routine that controls the multicast packets delivery,
// should be started in a separate thread
void MulticastSender::multicast_delivery_control()
{
  uint8_t * const buffer = new uint8_t[UDP_MAX_LENGTH];
  const auto_ptr<uint8_t> buffer_guard(buffer);
  struct sockaddr_in client_addr;
  socklen_t client_addr_len = sizeof(client_addr);
  bool do_work = true;
  while (do_work) {
    int length = recvfrom(sock, buffer, UDP_MAX_LENGTH, 0,
      (struct sockaddr*)&client_addr, &client_addr_len);
#if defined(DETAILED_MULTICAST_DEBUG) && !defined(NDEBUG)
    char caddr[INET_ADDRSTRLEN];
    DEBUG("Received a message from %s (%d)\n",
      inet_ntop(AF_INET, &client_addr.sin_addr.s_addr, caddr, sizeof(caddr)),
      length);
#endif
    if ((unsigned)length >= sizeof(MulticastMessageHeader)) {
      MulticastMessageHeader *mmh = (MulticastMessageHeader *)buffer;
      if (mmh->get_session_id() != session_id) {
        DEBUG("Incorrect session id: %u\n", mmh->get_session_id());
        continue;
      }

      vector<Destination>::const_iterator i = lower_bound(
        targets.begin(), targets.end(),
        Destination(mmh->get_responder(), NULL));
      if (i == targets.end() || i->addr != mmh->get_responder()) {
#ifndef NDEBUG
        char addr[INET_ADDRSTRLEN];
        uint32_t responder = htonl(mmh->get_responder());
        DEBUG("reply from an unknown host %s received\n",
          inet_ntop(AF_INET, &responder, addr, sizeof(addr)));
#endif
        continue;
      }
      switch (mmh->get_message_type()) {
        case MULTICAST_MESSAGE_RETRANS_REQUEST: {
            // TODO: somehow limit the rate of retransmissions
#ifndef NDEBUG
            char resp_str[INET_ADDRSTRLEN];
            uint32_t resp_addr = htonl(mmh->get_responder());
            DEBUG("Retransmission request from %s(%u)\n",
              inet_ntop(AF_INET, &resp_addr, resp_str, sizeof(resp_str)),
              mmh->get_number());
#endif
            send_queue->add_missed_packets(mmh->get_number(),
              i - targets.begin(), (uint32_t *)(mmh + 1),
              (uint32_t *)(buffer + length));
            break;
          }
        case MULTICAST_RECEPTION_CONFORMATION: {
            if (send_queue->acknowledge(mmh->get_number(),
                i - targets.begin()) != 0) {
              // Transmission finished
              do_work = false;
            }
            break;
          }
        case MULTICAST_ERROR_MESSAGE:
        case MULTICAST_FILE_RETRANS_REQUEST: {
            struct timeval tv;
            gettimeofday(&tv, NULL);
            ErrorMessage em = ErrorMessage(mmh->get_number(),
              mmh->get_responder(), tv);
#ifndef NDEBUG
            char addr[INET_ADDRSTRLEN];
            uint32_t source = htonl(mmh->get_responder());
            DEBUG("Error message(%x) %s:%u received\n", mmh->get_message_type(),
              inet_ntop(AF_INET, &source, addr, sizeof(addr)),
              mmh->get_number());
#endif
            // Send reply to the source
            MulticastMessageHeader reply(MULTICAST_RECEPTION_CONFORMATION,
              session_id);
            reply.set_number(mmh->get_number());
            reply.set_responder(mmh->get_responder());
            struct sockaddr_in dest;
            dest.sin_family = AF_INET;
            dest.sin_addr = client_addr.sin_addr;
            dest.sin_port = target_address.sin_port;

            // Send reply to the destination
            int sendto_result;
            do {
              sendto_result = sendto(sock, &reply, sizeof(reply), 0,
                (struct sockaddr *)&dest, sizeof(dest));
            } while (sendto_result < 0 && errno == ENOBUFS);
            if (sendto_result < 0) {
              // Some internal error occurred
              ERROR("Can't send a UDP datagram: %s\n", strerror(errno));
              register_error(STATUS_MULTICAST_CONNECTION_ERROR,
                "Can't send a UDP datagram: %s", strerror(errno));
              send_queue->register_fatal_error();
              abnormal_termination();
              pthread_exit(NULL);
            }

            if (received_errors.find(em) == received_errors.end()) {
              SDEBUG("Add the message into the error queue\n");
              received_errors.insert(em);
              if (received_errors.size() >
                  targets.size() * MAX_ERROR_QUEUE_SIZE_MULTIPLICATOR) {
                // Error queue is overflowed, remove an outdated error
                // FIXME: Choose the error to delete somehow else using
                // the timestamps
                received_errors.erase(received_errors.begin());
              }
              // Add message into the error queue
              if (mmh->get_message_type() == MULTICAST_ERROR_MESSAGE) {
                ReplyHeader *rh = (ReplyHeader *)(mmh + 1);
                reader->add_error(new SimpleError(rh->get_status(),
                  rh->get_address(), (char *)(rh + 1), rh->get_msg_length()));
                reader->update_multicast_sender_status(rh->get_status());
                if (rh->get_status() >= STATUS_FIRST_FATAL_ERROR) {
                  send_queue->register_fatal_error();
                  submit_task();
                  abnormal_termination();
                  pthread_exit(NULL);
                }
              } else {
                assert(mmh->get_message_type() ==
                  MULTICAST_FILE_RETRANS_REQUEST);
                FileInfoHeader *fih = (FileInfoHeader *)(mmh + 1);
                reader->add_error(new FileRetransRequest(
                  (char *)(fih + 1), // file name
                  *fih, mmh->get_responder(), vector<Destination>())); 
                reader->update_multicast_sender_status(
                  STATUS_INCORRECT_CHECKSUM);
              }
            }
            break;
          }
        default:
          DEBUG("Unexpected message type: %x\n", mmh->get_message_type());
          continue;
      }
    } else if (length >= 0) {
      SDEBUG("Corrupted datagram received\n");
      continue;
    } else {
      // Some internal error occurred
      ERROR("recvfrom returned: %s\n", strerror(errno));
      register_error(STATUS_MULTICAST_CONNECTION_ERROR,
        "recvfrom returned: %s", strerror(errno));
      send_queue->register_fatal_error();
      abnormal_termination();
      pthread_exit(NULL);
    }
  }
}

// A wrapper function for multicast_delivery_control
void* MulticastSender::multicast_delivery_control_wrapper(void *arg)
{
  MulticastSender *ms = (MulticastSender *)arg;
  ms->multicast_delivery_control();

  SDEBUG("multicast_delivery_control exited\n");
  return NULL;
}

// Helper fuction that sends 'message' to the udp socket.
void MulticastSender::udp_send(const void *message, int size, int flags)
{
#ifndef NDEBUG
  total_bytes_sent += size;
#endif
#if defined(DETAILED_MULTICAST_DEBUG) && !defined(NDEBUG)
  char taddr[INET_ADDRSTRLEN];
  struct timeval current_time;
  gettimeofday(&current_time, NULL);
  DEBUG("(%u, %u) udp_send %d, %d(%d) bytes to %s:%d\n",
    (unsigned)current_time.tv_sec, (unsigned)current_time.tv_usec,
    ((MulticastMessageHeader *)message)->get_number(),
    size, total_bytes_sent,
    inet_ntop(AF_INET, &target_address.sin_addr, taddr, sizeof(taddr)),
    ntohs(target_address.sin_port));
#endif

  if (bandwidth != 0) {
    // Bandwidth limitation enabled
    struct timeval current_timestamp;
    gettimeofday(&current_timestamp, NULL);
    allowed_to_send += ((int64_t)bandwidth *
      (current_timestamp.tv_usec - bandwidth_estimation_timestamp.tv_usec)) >>
      20;
    allowed_to_send += (bandwidth  - (bandwidth >> 5) - (bandwidth >> 6)) *
      (current_timestamp.tv_sec - bandwidth_estimation_timestamp.tv_sec);
#ifdef DETAILED_MULTICAST_DEBUG
    DEBUG("size: %u, allowed_to_send: %llu\n", size, allowed_to_send);
#endif
    if (allowed_to_send > 8LL * 1024 * 1024) {
      allowed_to_send = 4LL * 1024 * 1024;
    }
    bandwidth_estimation_timestamp = current_timestamp;
    if ((unsigned)size > allowed_to_send) {
#ifdef DETAILED_MULTICAST_DEBUG
      DEBUG("Sleep for %llu\n",
        (((int64_t)size - allowed_to_send) << 20) / bandwidth);
#endif
      internal_usleep((((int64_t)size - allowed_to_send) << 20) / bandwidth);
    }
    allowed_to_send -= size;
  }
  
  int sendto_result;
  do {
    sendto_result = sendto(sock, message, size, 0,
      (struct sockaddr *)&target_address, sizeof(target_address));
#if defined(DETAILED_MULTICAST_DEBUG) && !defined(NDEBUG)
    if (sendto_result == -1 && errno == ENOBUFS) {
      SDEBUG("ENOBUFS error occurred\n");
    }
#endif
  } while (sendto_result == -1 && errno == ENOBUFS);
  if (sendto_result < 0) {
    ERROR("sendto returned: %s\n", strerror(errno));
    throw ConnectionException(errno);
  }
}

// Helper fuction which reliably sends message to the multicast connection.
void MulticastSender::mcast_send(const void *message, int size)
{
#if defined(DETAILED_MULTICAST_DEBUG) && !defined(NDEBUG)
  char dst_addr[INET_ADDRSTRLEN];
  DEBUG("Send %d bytes to %s:%d\n", size,
    inet_ntop(AF_INET, &target_address.sin_addr, dst_addr, sizeof(dst_addr)),
    ntohs(target_address.sin_port));
#endif

  MulticastMessageHeader *mmh = (MulticastMessageHeader *)message;
  mmh->set_number(next_message);
  ++next_message;
  // Add noreply frames to decrease the number of acknowledgements
  if ((next_responder & 1) == 0) {
    mmh->set_responder(targets[next_responder >> 1].addr);
  } else {
    mmh->set_responder(INADDR_NONE);
  }
  next_responder = (next_responder + 1) % (targets.size() << 1);

  // Store message for the possibility of the future retransmissions.
  // (can block)
  const void *retrans_message;
  size_t retrans_message_size;
  while ((retrans_message = send_queue->store_message(message, size,
      &retrans_message_size)) != message) {
    DEBUG("Retransmit message %u\n",
      ((MulticastMessageHeader *)retrans_message)->get_number());
    udp_send(retrans_message, retrans_message_size, 0);
  }
  udp_send(message, size, 0);
}

// Sends file through the multicast connection 
void MulticastSender::send_file()
{
#ifndef NDEBUG
  int total = 0;
#endif
  unsigned count = get_data();
  count = std::min(count, (unsigned)(MAX_UDP_PACKET_SIZE -
    sizeof(MulticastMessageHeader))); 
  while (count > 0) {
#ifdef BUFFER_DEBUG
    DEBUG("Sending %d bytes of data\n", count);
#endif
    // TODO: attach a header
    uint32_t message[MAX_UDP_PACKET_SIZE];
    MulticastMessageHeader *mmh =
      new((void * const)message)MulticastMessageHeader(MULTICAST_FILE_DATA, session_id);
    memcpy(mmh + 1, pointer(), count);
    mcast_send(message, sizeof(MulticastMessageHeader) + count);
#ifndef NDEBUG
    total += count;
#endif
#ifdef BUFFER_DEBUG
    DEBUG("overall (%d) bytes of data sent\n", total);
#endif
    update_position(count);

    count = get_data();
    count = std::min(count, (unsigned)(MAX_UDP_PACKET_SIZE -
      sizeof(MulticastMessageHeader)));
  }
  DEBUG("send_file: %d bytes wrote\n", total);
}

/*
  This routine analyses targets, then creates and initializes
  multicast sender for link-local targets, if it is reasonable
  in the particular case.  If some error occurred remaining_dst
  is set to NULL.
*/
MulticastSender *MulticastSender::create_and_initialize(
    const vector<Destination>& all_destinations,
    const vector<Destination> **remaining_dst,
    uint32_t n_sources,
    bool is_multicast_only,
    bool use_global_multicast,
    uint32_t multicast_interface,
    Reader *reader,
    Mode mode,
    uint16_t multicast_port,
    unsigned bandwidth,
    bool use_fixed_rate_multicast,
    unsigned n_retransmissions)
{
  MulticastSender *multicast_sender = NULL;
  *remaining_dst = NULL;
  if (!use_global_multicast) {
    vector<uint32_t> local_addresses;
    vector<uint32_t> masks;
    // Get the local ip addresses and network masks for them
    int temporary_sock;
    temporary_sock = socket(PF_INET, SOCK_DGRAM, 0);
    if (temporary_sock == -1) {
      ERROR("Can't create a UDP socket: %s\n", strerror(errno));
      return NULL;
    }
    if (get_local_addresses(temporary_sock, &local_addresses, &masks) != 0) {
      close(temporary_sock);
      return NULL;
    }
    close(temporary_sock);
    // Figure out which interface should be used for multicast connections
    for (unsigned i = 0; i < local_addresses.size(); ++i) {
#ifndef NDEBUG
      char saddr[INET_ADDRSTRLEN];
      char smask[INET_ADDRSTRLEN];
      uint32_t addr = htonl(local_addresses[i]);
      uint32_t mask = htonl(masks[i]);
      DEBUG("Address: %s (%s)\n", 
        inet_ntop(AF_INET, &addr, saddr, sizeof(saddr)),
        inet_ntop(AF_INET, &mask, smask, sizeof(smask)));
#endif
      vector<Destination> local_destinations;
      uint32_t nonlocal_destination = INADDR_NONE;
      for (unsigned j = 0; j < all_destinations.size(); ++j) {
        if ((local_addresses[i] & masks[i]) ==
            (all_destinations[j].addr & masks[i])) {
          if (nonlocal_destination != INADDR_NONE) {
            char dest_name[INET_ADDRSTRLEN];
            uint32_t dest_addr = ntohl(nonlocal_destination);
            ERROR("Destination %s is not link-local. Try to use -g option.\n",
              inet_ntop(AF_INET, &dest_addr, dest_name, sizeof(dest_name)));
            return NULL;
          }
#ifndef NDEBUG
          char dest_name[INET_ADDRSTRLEN];
          uint32_t dest_addr = ntohl(all_destinations[j].addr);
          DEBUG("Destination %s is link-local\n",
            inet_ntop(AF_INET, &dest_addr, dest_name, sizeof(dest_name)));
#endif
          if (is_multicast_only &&
              local_addresses[i] == all_destinations[j].addr) {
            char dest_name[INET_ADDRSTRLEN];
            uint32_t dest_addr = ntohl(all_destinations[j].addr);
            ERROR("Can't use multicast transfer for localhost: %s\n",
              inet_ntop(AF_INET, &dest_addr, dest_name, sizeof(dest_name)));
            return NULL;
          }
          local_destinations.push_back(all_destinations[j]);
        } else if (is_multicast_only) {
          if (local_destinations.size() == 0) {
            nonlocal_destination = all_destinations[j].addr;
          } else {
            char dest_name[INET_ADDRSTRLEN];
            uint32_t dest_addr = ntohl(all_destinations[j].addr);
            ERROR("Destination %s is not link-local. Try to use -g option.\n",
              inet_ntop(AF_INET, &dest_addr, dest_name, sizeof(dest_name)));
            return NULL;
          }
        }
      }
#ifdef NDEBUG
      if (local_destinations.size() > 2 ||
          (local_destinations.size() > 0 && is_multicast_only))
#else
      if (local_destinations.size() > 0)
#endif
      {
        int si_result;
        unsigned port_tries = 0;
        do {
          // Create the multicast sender
          delete multicast_sender;
          multicast_sender = new MulticastSender(reader, mode, multicast_port,
            n_sources, n_retransmissions, bandwidth);
          // Establish the multicast session
          si_result = multicast_sender->session_init(local_addresses[i],
            local_destinations, use_global_multicast, use_fixed_rate_multicast);
          ++port_tries;
        } while(si_result == STATUS_PORT_IN_USE &&
            port_tries <= MAX_REMOTE_PORT_TRIES);
        if (si_result != 0) {
          // A fatal error occurred
          delete multicast_sender;
          return NULL;
        } else if (multicast_sender->targets.size() == 0) {
          // No connection has been established
          delete multicast_sender;
          continue;
        } else {
          // Multicast connection has been established with some hosts
          // TODO: close the connection here if there are not many such
          // hosts
          vector<Destination> *new_dst = new vector<Destination>;
          if (multicast_sender->targets.size() > 0) {
            DEBUG("%zu hosts connected:\n",
              multicast_sender->targets.size());
            for (vector<Destination>::const_iterator i =
              all_destinations.begin();
                i != all_destinations.end(); ++i) {
              vector<Destination>::const_iterator j = lower_bound(
                multicast_sender->targets.begin(),
                multicast_sender->targets.end(), *i);
              if (j == multicast_sender->targets.end() || *j != *i) {
                new_dst->push_back(*i);
              }
#ifndef NDEBUG
              else {
                char dest_name[INET_ADDRSTRLEN];
                uint32_t dest_addr = ntohl((*i).addr);
                DEBUG("%s\n", inet_ntop(AF_INET, &dest_addr, dest_name,
                  sizeof(dest_name)));
              }
#endif
            }
          }
          // Some packets sent to a newly created multicast group are lost.
          // Sleep here is to avoid/decrease this.
          // FIXME: A better approach should be developed
          usleep(20000);
          *remaining_dst = new_dst;
          return multicast_sender;
        }
      }
    }
  } else if (all_destinations.size() > 2 || is_multicast_only) {
    // Use global multicast
    int si_result;
    unsigned port_tries = 0;
    do {
      // Create the multicast sender
      delete multicast_sender;
      multicast_sender = new MulticastSender(reader,
        mode, multicast_port, n_sources, n_retransmissions, bandwidth);
      // Establish the multicast session
      si_result = multicast_sender->session_init(multicast_interface,
        all_destinations, use_global_multicast, use_fixed_rate_multicast);
      ++port_tries;
    } while(si_result == STATUS_PORT_IN_USE &&
        port_tries <= MAX_REMOTE_PORT_TRIES);
    if (si_result != 0) {
      // A fatal error occurred
      delete multicast_sender;
      return NULL;
    } else if (multicast_sender->targets.size() == 0) {
      // No connection has been established
      delete multicast_sender;
      *remaining_dst = &all_destinations;
      return NULL;
    } else {
      // Multicast connection has been established with some hosts
      vector<Destination> *new_dst = new vector<Destination>;
      if (multicast_sender->targets.size() > 0) {
        DEBUG("%zu hosts connected:\n",
          multicast_sender->targets.size());
        for (vector<Destination>::const_iterator i = all_destinations.begin();
            i != all_destinations.end(); ++i) {
          vector<Destination>::const_iterator j = lower_bound(
            multicast_sender->targets.begin(),
            multicast_sender->targets.end(), *i);
          if (j == multicast_sender->targets.end() || *j != *i) {
            new_dst->push_back(*i);
          }
#ifndef NDEBUG
          else {
            char dest_name[INET_ADDRSTRLEN];
            uint32_t dest_addr = ntohl((*i).addr);
            DEBUG("%s\n", inet_ntop(AF_INET, &dest_addr, dest_name,
              sizeof(dest_name)));
          }
#endif
        }
      }
      // Some packets sent to a newly created multicast group are lost.
      // Sleep here is to avoid/decrease this.
      // FIXME: A better approach should be developed
      usleep(20000);
      *remaining_dst = new_dst;
      return multicast_sender;
    }
  }
  *remaining_dst = &all_destinations;
  return NULL;
}

/*
  This initialization routine tries to establish a multicast
  session with the destinations specified in dst.
  The return value is 0 on success, -1 in the case of local error and
  error status received in the reply if some remote error occurred.
*/
int MulticastSender::session_init(
    uint32_t local_addr,
    const std::vector<Destination>& dst,
    bool use_global_multicast,
    bool use_fixed_rate_multicast)
{
  // Clear the previous connection targets
  local_address = local_addr;
  targets.clear();
  map<uint32_t, unsigned> round_trip_times;
  
  allowed_to_send = MAX_UDP_PACKET_SIZE;
  gettimeofday(&bandwidth_estimation_timestamp, NULL);
  
  // Fill up the multicast address to be used in the connection
  memset(&target_address, 0, sizeof(target_address));
  target_address.sin_family = AF_INET;
  target_address.sin_addr.s_addr = address;
  target_address.sin_port = htons(port);

  sock = socket(AF_INET, SOCK_DGRAM, 0);
  if (sock == -1) {
    ERROR("Can't create a UDP socket: %s\n", strerror(errno));
    register_error(STATUS_UNKNOWN_ERROR, "Can't create a UDP socket: %s",
      strerror(errno));
    return -1;
  }

  // Choose an ephemeral UDP port
  errno = 0;
  uint16_t ephemeral_port = choose_ephemeral_port(use_global_multicast);
  if (ephemeral_port == 0) {
    ERROR("Can't choose an ephemeral port: %s\n", strerror(errno));
    register_error(STATUS_UNKNOWN_ERROR,
      "Can't choose an ephemeral port: %s", strerror(errno));
    return -1;
  }
  DEBUG("Ephemeral port for the new connection is: %u\n", ephemeral_port);

  // Choose an ephemeral multicast address
  in_addr_t ephemeral_addr; 
  ephemeral_addr = inet_addr(MULTICAST_EPHEMERAL_ADDR_RANGE);
  in_addr_t ephemeral_mask; 
  ephemeral_mask = inet_addr(MULTICAST_EPHEMERAL_ADDR_MASK);
  uint32_t mask = ntohl(ephemeral_mask);
  ephemeral_addr = htonl(ntohl(ephemeral_addr) & mask);
  uint32_t interface_id = rand() & ~mask;
  if ((interface_id & 0xFF) == 0 || (interface_id & 0xFF) == 0xFF) {
    interface_id = 0x11111111 & ~mask;
  }
  ephemeral_addr |= htonl(interface_id);

#ifndef NDEBUG
  char e_addr[INET_ADDRSTRLEN];
  DEBUG("Ephemeral multicast address: %s\n",
    inet_ntop(AF_INET, &ephemeral_addr, e_addr, sizeof(e_addr)));
#endif

  // Compose the session initialization message
  unsigned init_message_length = sizeof(MulticastMessageHeader) +
    sizeof(MulticastInitData) + dst.size() * sizeof(MulticastHostRecord);
  uint8_t init_message[init_message_length];
  MulticastMessageHeader *mmh = new(init_message)
    MulticastMessageHeader(MULTICAST_INIT_REQUEST, session_id);

  // Fill up the message data
  MulticastInitData *mid =
    new(mmh + 1) MulticastInitData(ntohl(ephemeral_addr));

  // Fill up the host records
  MulticastHostRecord *hr = (MulticastHostRecord *)(mid + 1);
  MulticastHostRecord *hr_begin = hr;
  for (uint32_t i = 0; i < dst.size(); ++i) {
    hr->set_addr(dst[i].addr);
    ++hr;
  }
  MulticastHostRecord *hr_end = hr;

  // Start the session initialization procedure
  vector<Destination> *remaining_dst = new vector<Destination>(dst);
  struct timeval tprev;
  struct timeval tcurr;
  gettimeofday(&tprev, NULL);
  --tprev.tv_sec;

  struct pollfd pfds;
  memset(&pfds, 0, sizeof(pfds));
  pfds.fd = sock;
  pfds.events = POLLIN;

  unsigned n_unreplied_init_requests = 0;
  unsigned replies_now;
  // Send the session initialization message up to MAX_INITIALIZATION_RETRIES
  // times or unil there will be no replies by two successive session
  // initialization messages
  try {
    for(unsigned i = 0; i < MAX_INITIALIZATION_RETRIES; ++i) {
      mmh->set_number(i);
      replies_now = 0;
      gettimeofday(&tprev, NULL);
      udp_send(init_message, init_message_length, 0);
  
      // Send the init_message and wait for the replies
      do {
        gettimeofday(&tcurr, NULL);
        register unsigned time_difference =
          (tcurr.tv_sec - tprev.tv_sec) * 1000000 +
          tcurr.tv_usec - tprev.tv_usec;
        time_difference = INIT_RETRANSMISSION_RATE - time_difference;
        if (time_difference < 0) { time_difference = 0; }
  
#ifdef DETAILED_MULTICAST_DEBUG
        DEBUG("Time to sleep: %d\n", time_difference);
#endif
        int poll_result;
        if ((poll_result = poll(&pfds, 1, time_difference / 1000)) > 0) {
          uint8_t buffer[UDP_MAX_LENGTH];
          struct sockaddr_in client_addr;
          socklen_t client_addr_len = sizeof(client_addr);
          int length = recvfrom(sock, buffer, UDP_MAX_LENGTH, 0,
            (struct sockaddr*)&client_addr, &client_addr_len);
          if (length < (int)sizeof(MulticastMessageHeader)) {
            continue;
          }
  
          // TODO: Do something in the case if the port is already in use
          // on some destination
          MulticastMessageHeader *mih = (MulticastMessageHeader *)buffer;
          if (mih->get_session_id() != session_id) {
            DEBUG("Wrong session id (%x) in the received reply\n",
              mih->get_session_id());
            // Silently skip the message
            continue;
          } else if (mih->get_message_type() == MULTICAST_INIT_REPLY) {
            DEBUG("Received reply of length %d\n", length);
            ++replies_now;
            n_unreplied_init_requests = 0;
            // Parse the reply message and remove the received destinations
            uint32_t *p = (uint32_t *)(mih + 1);
            uint32_t *end = (uint32_t* )(buffer + length);
            // Need to store only the first address
            bool is_address_alredy_matched = false;
            while (p < end) {
              MulticastHostRecord *found_record;
              MulticastHostRecord hr_p(ntohl(*p));
#ifndef NDEBUG
              char saddr[INET_ADDRSTRLEN];
              DEBUG("Reply from %s\n",
                inet_ntop(AF_INET, p, saddr, sizeof(saddr)));
#endif
              if ((found_record = find(hr_begin, hr_end, hr_p)) != hr_end) {
                if (!is_address_alredy_matched) {
                  targets.push_back((*remaining_dst)[found_record - hr_begin]);
                  // FIXME: the following equation is wrong if the
                  // retransmission rate will vary
                  struct timeval current_time;
                  gettimeofday(&current_time, NULL);
                  round_trip_times[found_record->get_addr()] =
                    (i - mih->get_number()) * INIT_RETRANSMISSION_RATE +
                    (current_time.tv_sec - tprev.tv_sec) * 1000000 +
                    current_time.tv_usec - tprev.tv_usec ;
                  is_address_alredy_matched = true;
                }
                DEBUG("Host %s connected\n", saddr);
                // Exclude host from the message and the remaining_dst
                --hr_end;
                swap(*found_record, *hr_end);
                swap((*remaining_dst)[found_record - hr_begin],
                  remaining_dst->back());
                remaining_dst->pop_back();
                init_message_length -= sizeof(MulticastHostRecord);
              }
              ++p;
            }
            if (remaining_dst->size() == 0) {
              // All the destinations responded
              goto finish_session_initialization;
            }
          } else if (mih->get_message_type() == MULTICAST_ERROR_MESSAGE) {
            ReplyHeader *rh = (ReplyHeader *)(mih + 1);
            MulticastHostRecord hr_p(rh->get_address());
#ifndef NDEBUG
            uint32_t source_ip = htonl(rh->get_address());
            char source_addr[INET_ADDRSTRLEN];
            DEBUG("Error %u(%s) received from %s\n", rh->get_status(),
              get_reply_status_description(rh->get_status()),
              inet_ntop(AF_INET, &source_ip, source_addr, sizeof(source_addr)));
#endif
            if (find(hr_begin, hr_end, hr_p) != hr_end) {
              reader->add_error(new SimpleError(rh->get_status(),
                rh->get_address(), (char *)(rh + 1), rh->get_msg_length()));
              reader->update_multicast_sender_status(rh->get_status());
              if (rh->get_status() >= STATUS_FIRST_FATAL_ERROR) {
                // Terminate the connection
                address = ephemeral_addr;
                target_address.sin_addr.s_addr = address;
                target_address.sin_port = htons(ephemeral_port);
                sort(targets.begin(), targets.end());
                abnormal_termination();
                return rh->get_status();
              }
            }
          } else {
            DEBUG("Message of unknown type %x received\n",
              mih->get_message_type());
          }
        } else if (poll_result == 0) {
          // Time expired, send the next message
          break;
        } else {
          ERROR("poll error: %s\n", strerror(errno));
          register_error(STATUS_UNKNOWN_ERROR, "poll error: %s",
            strerror(errno));
          return -1;
        }
      } while(1);
      // Finish procedure if there were no replies for two successive
      // retransmissions (for speed up reasons)
      if (replies_now == 0) { ++n_unreplied_init_requests; }
      if (n_unreplied_init_requests > MAX_UNREPLID_INIT_RETRIES) { break; }

      // TODO: correct the transmission rate using the number of replies
      // received
    }
  } catch (ConnectionException& e) {
    DEBUG("Can't send a UDP datagram: %s\n", e.what());
    register_error(STATUS_MULTICAST_INIT_ERROR,
      "Can't send a UDP datagram: %s", e.what());
    return -1;
  }

finish_session_initialization:
  // Set the new (ephemeral) port as the target port for the next messages
  address = ephemeral_addr;
  target_address.sin_addr.s_addr = address;
  target_address.sin_port = htons(ephemeral_port);

  delete send_queue;
  delete remaining_dst;
  sort(targets.begin(), targets.end());
  unsigned rtts[targets.size()];
  for (unsigned i = 0; i < targets.size(); ++i) {
    rtts[i] = round_trip_times[targets[i].addr];
  }
  send_queue = new MulticastSendQueue(targets, rtts, use_fixed_rate_multicast);

  return 0;
}

/*
  This is the main routine of the multicast sender. It sends
  files and directories to destinations.
*/
int MulticastSender::session()
{
  int error;
  pthread_t multicast_delivery_thread;
  error = pthread_create(&multicast_delivery_thread, NULL,
    multicast_delivery_control_wrapper, this);
  if (error != 0) {
    ERROR("Can't create a new thread: %s\n", strerror(error));
    // TODO: Set the error here
    return -1;
  }

  try {
    // Send n_sources and the target paths
    // Compose the destinations path messages
    uint8_t targets_message[MAX_UDP_PACKET_SIZE];
    MulticastMessageHeader *mmh =
      new(targets_message) MulticastMessageHeader(MULTICAST_TARGET_PATHS,
      session_id);
    uint32_t *nsources_p = (uint32_t *)(mmh + 1);
    *nsources_p = htonl(n_sources);
    uint8_t *t_curr = (uint8_t *)(nsources_p + 1);
    uint8_t *t_end = targets_message + sizeof(targets_message);
    for (unsigned i = 0; i < targets.size(); ++i) {
      unsigned length = sizeof(DestinationHeader) + targets[i].filename.size();
      if (length > MAX_UDP_PACKET_SIZE - sizeof(MulticastMessageHeader)) {
        ERROR("Filename %s is too long\n", targets[i].filename.c_str());
        register_error(STATUS_UNKNOWN_ERROR,
          "Filename %s is too long", targets[i].filename.c_str());
        pthread_cancel(multicast_delivery_thread);
        //pthread_join(multicast_delivery_thread, NULL)
        return -1;
      }
      if (t_curr + length >= t_end) {
        // Send the message
        mcast_send(targets_message, t_curr - targets_message);
        t_curr = (uint8_t *)(nsources_p + 1);
      }
      DestinationHeader *h = new(t_curr)
        DestinationHeader(targets[i].addr, targets[i].filename.size());
      t_curr = (uint8_t *)(h + 1);
      if (targets[i].filename.size() > 0) {
        memcpy(t_curr, targets[i].filename.c_str(),
          targets[i].filename.size());
      }
      t_curr += targets[i].filename.size();
    }
    if (t_curr != (uint8_t *)(nsources_p + 1)) {
      mcast_send(targets_message, t_curr - targets_message);
    }
    SDEBUG("Destinations sent\n");
  
    bool is_trailing = false; // Whether the current task is the trailing one
    do {
      // Get the operation from the queue
      Distributor::TaskHeader *op = get_task();
      if (reader->multicast_sender.status >= STATUS_FIRST_FATAL_ERROR) {
        SDEBUG("Some multicast destinations returned a fatal error\n");
        pthread_join(multicast_delivery_thread, NULL);
        return -1;
      }
      if (op->fileinfo.is_trailing_record()) {
        // Send the MULTICAST_TERMINATION_REQUEST message
        void *message = send_queue->prepare_termination(session_id);
        mcast_send(message, send_queue->get_termination_message_size());

        unsigned retrans_number = 1;
        struct timeval current_time;
        gettimeofday(&current_time, NULL);
        struct timespec next_retrans_time;
        TIMEVAL_TO_TIMESPEC(&current_time, &next_retrans_time);
        unsigned max_rtt = send_queue->get_max_round_trip_time();
        struct timespec retrans_timeout;
        if (max_rtt > 0) {
          retrans_timeout.tv_sec = (max_rtt / 500000) * 2;
          retrans_timeout.tv_nsec = ((max_rtt % 500000) * 2) * 1000;
        } else {
          retrans_timeout.tv_sec =
            DEFAULT_TERMINATE_RETRANSMISSION_RATE / 1000000000;
          retrans_timeout.tv_nsec =
            DEFAULT_TERMINATE_RETRANSMISSION_RATE % 1000000000;
        }
        next_retrans_time.tv_sec += retrans_timeout.tv_sec;
        next_retrans_time.tv_nsec += retrans_timeout.tv_nsec;
        if (next_retrans_time.tv_nsec >= 1000000000) {
          next_retrans_time.tv_nsec = next_retrans_time.tv_nsec % 1000000000;
          next_retrans_time.tv_sec = next_retrans_time.tv_nsec / 1000000000;
        }

        while (retrans_number <= MAX_NUMBER_OF_TERMINATION_RETRANS) {
          int wait_result = send_queue->wait_for_destinations(
            &next_retrans_time);
          if (wait_result < 0) {
            ++retrans_number;
          } else if (wait_result == 0) {
            break;
          }

          // Retransmit some error messages if required
          const void *retrans_message;
          size_t retrans_message_size = 0;
          retrans_message = send_queue->store_message(NULL, 0,
            &retrans_message_size);
          while (retrans_message != NULL) {
            DEBUG("Retransmit message %u\n",
              ((MulticastMessageHeader *)retrans_message)->get_number());
            udp_send(retrans_message, retrans_message_size, 0);
            retrans_message_size = 0;
            retrans_message = send_queue->store_message(NULL, 0,
              &retrans_message_size);
          }

          // Retransmit the session termination request
          udp_send(message, send_queue->get_termination_message_size(), 0);

          gettimeofday(&current_time, NULL);
          DEBUG("Retranssion %u of the MULTICAST_TERMINATION_REQUEST: %u, %u\n",
            retrans_number, (unsigned)current_time.tv_sec,
            (unsigned)current_time.tv_usec);
          TIMEVAL_TO_TIMESPEC(&current_time, &next_retrans_time);
          if (wait_result < 0 &&
              (unsigned)retrans_timeout.tv_sec < MAX_RETRANSMISSION_TIMEOUT) {
            // Increase the retransmission timeout if there were no reply
            retrans_timeout.tv_sec += (retrans_timeout.tv_sec >> 1);
            retrans_timeout.tv_nsec += (retrans_timeout.tv_nsec >> 1);
            if (retrans_timeout.tv_nsec >= 1000000000) {
              retrans_timeout.tv_nsec = next_retrans_time.tv_nsec % 1000000000;
              retrans_timeout.tv_sec += 1;
            }
          }
          next_retrans_time.tv_sec += retrans_timeout.tv_sec;
          next_retrans_time.tv_nsec += retrans_timeout.tv_nsec;
          if (next_retrans_time.tv_nsec >= 1000000000) {
            next_retrans_time.tv_nsec = next_retrans_time.tv_nsec % 1000000000;
            next_retrans_time.tv_sec += 1;
          }
        }

        if (retrans_number > MAX_NUMBER_OF_TERMINATION_RETRANS) {
          SERROR("Can't finish multicast session: too many retransmissions of "
            "the termination request message\n");
          register_error(STATUS_TOO_MANY_RETRANSMISSIONS,
            "Can't finish multicast session: %s",
            "too many retransmissions of the termination request message");
          return -1;
        } else {
          DEBUG("Transmission is over, %u bytes sent\n", total_bytes_sent);
        }
        is_trailing = true;
      } else {
        assert(op->fileinfo.get_name_length() <= MAXPATHLEN);
      
        // Send the file info structure
        uint8_t file_record[sizeof(MulticastMessageHeader) +
          sizeof(FileInfoHeader) + op->fileinfo.get_name_length()];
        MulticastMessageHeader *mm_h =
          new(file_record)MulticastMessageHeader(MULTICAST_FILE_RECORD,
          session_id);
        FileInfoHeader *file_h = (FileInfoHeader *)(mm_h + 1);
        *file_h = op->fileinfo;
        DEBUG("Fileinfo: |%u| %o | %u / %u  |\n", file_h->get_type(),
           file_h->get_mode(), file_h->get_name_length(),
          file_h->get_name_offset());
        memcpy(file_h + 1, op->get_filename(), op->fileinfo.get_name_length());
        mcast_send(file_record, sizeof(file_record));

        if (op->fileinfo.get_type() == resource_is_a_file) {
          DEBUG("Send file: %s\n",
            op->get_filename() + op->fileinfo.get_name_offset());
          // Send the file
          send_file();
          // Send the file trailing record
#ifndef NDEBUG
          SDEBUG("Send the checksum: ");
          MD5sum::display_signature(stdout, checksum()->signature);
          printf("\n");
#endif
          uint8_t file_trailing[sizeof(MulticastMessageHeader) +
            sizeof(checksum()->signature)];
          MulticastMessageHeader *mmh = new(file_trailing)
            MulticastMessageHeader(MULTICAST_FILE_TRAILING, session_id);
          memcpy(mmh + 1, checksum()->signature, sizeof(checksum()->signature));
          mcast_send(file_trailing, sizeof(file_trailing));
        }
      }
      submit_task();
    } while (!is_trailing);
  } catch (ConnectionException& e) {
    DEBUG("Can't send a UDP datagram: %s\n", e.what());
    register_error(STATUS_MULTICAST_CONNECTION_ERROR,
      "Can't send a UDP datagram: %s", e.what());
    pthread_cancel(multicast_delivery_thread);
    //pthread_join(multicast_delivery_thread, NULL)
    return -1;
  } catch (MulticastSendQueue::MulticastException& e) {
    if (e.reason ==
        MulticastSendQueue::MulticastException::fatal_error_received) {
      SDEBUG("Some multicast destinations returned a fatal error\n");
      pthread_join(multicast_delivery_thread, NULL);
    } else {
      assert(e.reason ==
        MulticastSendQueue::MulticastException::connection_timed_out);
      char host[INET_ADDRSTRLEN];
      uint32_t addr = htonl(e.culprit);
      DEBUG("Multicast connection with %s timed out\n",
        inet_ntop(AF_INET, &addr, host, sizeof(host)));
      register_error(STATUS_MULTICAST_CONNECTION_ERROR,
        "Multicast connection with %s timed out",
        inet_ntop(AF_INET, &addr, host, sizeof(host)));
      pthread_cancel(multicast_delivery_thread);
      //pthread_join(multicast_delivery_thread, NULL);
      abnormal_termination();
    }
    return -1;
  }
  return 0;
}
