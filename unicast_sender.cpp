#include <stdio.h>
#include <stdlib.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>

#include <exception>

#include "unicast_sender.h"
#include "log.h"

// Tries to establish TCP connection with the host 'addr'
void UnicastSender::connect_to(in_addr_t addr) throw (ConnectionException)
{
  sock = socket(PF_INET, SOCK_STREAM, 0);
  if (sock == -1) {
    //ERROR("Can't create socket: %d\n", sock);
    throw ConnectionException(errno);
  }

  struct sockaddr_in saddr;
  saddr.sin_family = AF_INET;
  saddr.sin_port = htons(port);
  saddr.sin_addr.s_addr = htonl(addr);

  if (connect(sock, (struct sockaddr *)&saddr, sizeof(saddr)) != 0) {
    close(sock);
    sock = -1;
    throw ConnectionException(errno);
  }
}

// Send the Unicast Session Initialization record
void UnicastSender::send_initial_record(int nsources, char *path,
    MD5sum *checksum) throw (ConnectionException)
{
  uint32_t path_len = path == NULL ? 0 : strlen(path);
  UnicastSessionHeader ush(flags, nsources, path_len);
  sendn(sock, &ush, sizeof(ush), 0);
  checksum->update(&ush, sizeof(ush));
  if (path_len > 0) {
    sendn(sock, path, path_len, 0);
    checksum->update(path, path_len);
  }
}

// Send the destinations from *i to *_end
void UnicastSender::send_destinations(
    std::vector<Destination>::const_iterator i,
    const std::vector<Destination>::const_iterator& _end,
    MD5sum *checksum) throw (ConnectionException)
{
  for(; i != _end; ++i) {
#ifdef HAS_TR1_MEMORY
    int path_len = &*i->filename == NULL ? 0 : strlen(i->filename.get());
#else
    int path_len = i->filename == NULL ? 0 : strlen(i->filename);
#endif
    DestinationHeader dh(i->addr, path_len);
    sendn(sock, &dh, sizeof(dh), 0);
    checksum->update(&dh, sizeof(dh));
    if (path_len > 0) {
#ifdef HAS_TR1_MEMORY
      sendn(sock, i->filename.get(), path_len, 0);
      checksum->update(i->filename.get(), path_len);
#else
      sendn(sock, i->filename, path_len, 0);
      checksum->update(i->filename, path_len);
#endif
    }
  }
}

// Send the trailing record and checksum for all the previously sent data
void UnicastSender::send_destination_trailing(
    MD5sum *checksum) throw (ConnectionException)
{
  // Trailing record
  DestinationHeader dh(0, 0);
  sendn(sock, &dh, sizeof(dh), 0);
  checksum->update(&dh, sizeof(dh));
}

// Chooses the next destination 
int UnicastSender::choose_destination(const std::vector<Destination>& dst)
{
  // TODO: it could be useful to parse the local addresses and look for
  // the nearest one destination by the topoligy in the list 
  return 0;
}

// Register an error and finish the current task
void UnicastSender::register_error(uint8_t status, uint32_t address,
    const char *fmt, const char *error)
{
  struct in_addr addr;
  addr.s_addr = ntohl(address);
  char hostaddr[INET_ADDRSTRLEN];
  inet_ntop(AF_INET, &addr, hostaddr, sizeof(hostaddr));
  // FIXME: use the strerror_r instead of the
  // corresponding one without the _r suffix.
  char *error_message = (char *)malloc(strlen(fmt) + strlen(hostaddr) +
    strlen(error) + 1); // a bit more that required
  sprintf((char *)error_message, fmt, hostaddr, error);
  DEBUG("register error: %s\n", error_message);
  reader->errors.add(new Reader::SimpleError(status, INADDR_NONE,
    error_message, strlen(error_message)));
  reader->unicast_sender.status = status;
  // Finish the unicast sender
  submit_task();
}

// Writes data (not more that size) from the distributor to sock.
// Returns the number of bytes sent or (size + 1) on failure
uint64_t UnicastSender::write_to_socket(int sock, uint64_t size)
{
  DEBUG("Write file of size %zu to the socket\n", (size_t)size);
  uint64_t total = 0;
  int count = get_data();
  while (count > 0) {
#ifdef BUFFER_DEBUG
    DEBUG("Sending %d bytes of data\n", count);
#endif
    count = write(sock, pointer(), count);
#ifdef BUFFER_DEBUG
    DEBUG("%d (%zu) bytes of data sent\n", count, total);
#endif
    if (count > 0) {
      total += count;
      assert(total <= size);
      update_position(count);
    } else {
      if (errno == ENOBUFS) {
        SDEBUG("ENOBUFS error occurred\n");
        usleep(200000);
        continue;
      }
      return size + 1;
    }

    count = get_data();
  }
  DEBUG("write_to_file: %zu bytes've been written\n", (size_t)total);
  return total;
}

/*
  This is the initialization routine which establish the unicast
  session with one of the destinations.
*/
int UnicastSender::session_init(const std::vector<Destination>& dst,
    int nsources)
{
  SDEBUG("UnicastSender::session_init called\n");
  // Write the initial data of the session
  unsigned destination_index = choose_destination(dst);
  target_address = dst[destination_index].addr;
  try {
    // Establish connection with the nearest neighbor
    int retries_remaining = MAX_INITIALIZATION_RETRIES;
    bool is_retransmission_required;
    connect_to(target_address);
    do {
      MD5sum checksum;
      is_retransmission_required = false;
#ifdef HAS_TR1_MEMORY
      send_initial_record(nsources, dst[destination_index].filename.get(),
        &checksum);
#else
      send_initial_record(nsources, dst[destination_index].filename,
        &checksum);
#endif
      send_destinations(dst.begin(), dst.begin() + destination_index,
        &checksum);
      if (dst.size() > destination_index + 1) {
        send_destinations(dst.begin() + destination_index + 1, dst.end(),
          &checksum);
      }
      
      send_destination_trailing(&checksum);
      // Send the checksum calculated on the initial record
      checksum.final();
      sendn(sock, checksum.signature, sizeof(checksum.signature), 0);
      SDEBUG("Destinations sent\n");
      // Get the reply
      char *reply_message;
      ReplyHeader h;
      h.recv_reply(sock, &reply_message, 0);
      DEBUG("Reply received, status: %d\n", h.get_status());
      if (h.get_status() == STATUS_OK) {
        // All ok
      } else if (h.get_status() == STATUS_INCORRECT_CHECKSUM) {
        if (--retries_remaining > 0) {
          is_retransmission_required = true;
          // Retransmit the session initialization message
          ERROR("Incorrect checksum, retransmit the session initialization "
            "message in the %d time\n", MAX_INITIALIZATION_RETRIES -
            retries_remaining);
        } else {
          // Fatal error: Too many retransmissions
          register_error(STATUS_UNICAST_INIT_ERROR, target_address,
            "Can't establish unicast connection with the host %s: %s",
            "Too many retransmissions");
          close(sock);
          sock = -1;
          return -1;
        }
      } else {
        // Fatal error occurred during the session establishing
        DEBUG("Fatal error received: %s\n", reply_message);
        // Register the error
        reader->errors.add(new Reader::SimpleError(h.get_status(),
          h.get_address(), reply_message, h.get_msg_length()));
        if (reader->unicast_sender.status < h.get_status()) {
          reader->unicast_sender.status = h.get_status();
        }
        submit_task();
        close(sock);
        sock = -1;
        return -1;
      }
    } while (is_retransmission_required);
    SDEBUG("Connection established\n");
    return 0;
  } catch (ConnectionException& e) {
    // Transmission error during session initialization
    try {
      // Try to get an error from the immediate destination
      DEBUG("Connection exception %s, Try to get an error\n", e.what());
      char *reply_message;
      ReplyHeader h;
      h.recv_reply(sock, &reply_message, 0);
      if (h.get_status() >= STATUS_FIRST_FATAL_ERROR) {
        reader->errors.add(new Reader::SimpleError(h.get_status(),
          h.get_address(), reply_message, strlen(reply_message)));
        reader->unicast_sender.status = h.get_status();
        submit_task();
        close(sock);
        sock = -1;
        return -1;
      }
    } catch(std::exception& e) {
      // Can't receive an error, generate it
    }
    register_error(STATUS_UNICAST_INIT_ERROR, target_address,
      "Can't establish unicast connection with the host %s: %s", e.what());
    close(sock);
    sock = -1;
    return -1;
  }
}

/*
  This is the main routine of the unicast sender. This routine sends
  the files and directories to the next destination.
*/
int UnicastSender::session()
{
  try {
    bool is_trailing = false; // Whether the current task is the trailing one
    do {
      // Get the operation from the queue
      Distributor::TaskHeader *op = get_task();
      if (op->fileinfo.is_trailing_record()) {
        // Send the trailing zero record
        SDEBUG("Send the trailing record\n");
        is_trailing = true;
        FileInfoHeader fih;
        sendn(sock, &fih, sizeof(fih), 0);
        // Wait for the session termination
        char *reply_message;
        ReplyHeader h;
        while (h.recv_reply(sock, &reply_message, 0) == 0) {
          // Some reply received
          if (w->status < h.get_status()) {
            w->status = h.get_status();
          }
          if (h.get_status() == STATUS_OK) {
            // All done
            break;
          } else if (h.get_status() == STATUS_INCORRECT_CHECKSUM) {
            // Request for the file retransmission (done by the previous code)
            SDEBUG("Incorrect checksum, request for a file retransmission\n");
            reader->errors.add(new Reader::FileRetransRequest(h.get_address(),
              reply_message, h.get_msg_length()));
            if (reader->unicast_sender.status < h.get_status()) {
              reader->unicast_sender.status = h.get_status();
            }
          } else if(h.get_status() != STATUS_OK) {
            // Fatal error occurred during the connection
            SDEBUG("An error received\n");
            reader->errors.add(new Reader::SimpleError(h.get_status(),
              h.get_address(), reply_message, h.get_msg_length()));
            if (reader->unicast_sender.status < h.get_status()) {
              reader->unicast_sender.status = h.get_status();
            }
            if (h.get_status() >= STATUS_FIRST_FATAL_ERROR) {
              // Finish work in the case of a fatal error
              submit_task();
              close(sock);
              sock = -1;
              return -1;
            }
          }
        }
      } else {
        assert(op->fileinfo.get_name_length() < 1024);
        // Send the file info structure
        uint8_t info_header[sizeof(FileInfoHeader) +
          op->fileinfo.get_name_length()];
        FileInfoHeader *file_h =
          reinterpret_cast<FileInfoHeader *>(info_header);
        *file_h = op->fileinfo;
        DEBUG("Fileinfo: |%u| %o | %u / %u  |\n", file_h->get_type(),
           file_h->get_mode(), file_h->get_name_length(),
          file_h->get_name_offset());
        memcpy(info_header + sizeof(FileInfoHeader), op->get_filename(),
        op->fileinfo.get_name_length());
        sendn(sock, info_header, sizeof(info_header), 0);

        if (op->fileinfo.get_type() == resource_is_a_file) {
          DEBUG("Send file: %s\n",
            op->get_filename() + op->fileinfo.get_name_offset());
          // Send the file
          uint64_t write_result;
          if ((write_result = write_to_socket(sock,
              op->fileinfo.get_file_size())) > op->fileinfo.get_file_size()) {
            // It's is the connection error throw appropriate exception
            throw ConnectionException(errno);
          }
          if (write_result != op->fileinfo.get_file_size()) {
            // File has been shrinked or enlarged during the transmission
            static const uint8_t trailing = OOB_FILE_SIZE_CHANGED;
            sendn(sock, &trailing, 1, MSG_OOB);
            SDEBUG("Out-of-band data 1 sent\n");
            uint64_t n_bytes_delivered;
            do {
              recvn(sock, &n_bytes_delivered, sizeof(n_bytes_delivered), 0);
              n_bytes_delivered = ntoh64(n_bytes_delivered);
            } while (n_bytes_delivered != write_result);
            // Send the trailing character OOB character
            static const uint8_t final = OOB_FILE_TRANSMISSION_DONE;
            sendn(sock, &final, 1, MSG_OOB);
            SDEBUG("Out-of-band data 2 sent\n");
            // Waiting for acknowledgement of the trailing OOB date reception
            uint8_t byte;
            recvn(sock, &byte, sizeof(byte), 0);
            if (byte != 0) {
              throw ConnectionException(
                ConnectionException::corrupted_data_received);
            }
          }
          // Send the checksum
#ifndef NDEBUG
          SDEBUG("Send the checksum: ");
          MD5sum::display_signature(stdout, checksum()->signature);
          printf("\n");
#endif
          sendn(sock, checksum()->signature, sizeof(checksum()->signature), 0);
        }
        // Check whether some errors occurrred
        char *reply_message;
        ReplyHeader h;
        if (h.recv_reply(sock, &reply_message, MSG_DONTWAIT) == 0) {
          // Some reply received
          if (w->status < h.get_status()) {
            w->status = h.get_status();
          }
          if (h.get_status() == STATUS_INCORRECT_CHECKSUM) {
            // Request for the file retransmission (done by the previous code)
            SDEBUG("Incorrect checksum, request for a file retransmission\n");
            reader->errors.add(new Reader::FileRetransRequest(h.get_address(),
              reply_message, h.get_msg_length()));
            if (reader->unicast_sender.status < h.get_status()) {
              reader->unicast_sender.status = h.get_status();
            }
          } else if(h.get_status() != STATUS_OK) {
            // An error occurred during the connection
            SDEBUG("An error received\n");
            if (reader->unicast_sender.status < h.get_status()) {
              reader->unicast_sender.status = h.get_status();
            }
            if (h.get_status() >= STATUS_FIRST_FATAL_ERROR) {
              // Finish work in the case of a fatal error
              reader->errors.add(new Reader::SimpleError(h.get_status(),
                h.get_address(), reply_message, h.get_msg_length()));
              submit_task();
              close(sock);
              sock = -1;
              return -1;
            } else {
              if (mode == client_mode) {
                // Display non-fatal errors immediately
                Reader::SimpleError(h.get_status(), h.get_address(),
                  reply_message, h.get_msg_length()).display();
              } else {
                reader->errors.add(new Reader::SimpleError(h.get_status(),
                  h.get_address(), reply_message, h.get_msg_length()));
              }
            }
          }
        }
      }
      submit_task();
    } while (!is_trailing);
  } catch (std::exception& e) {
    // An Error during transmission
    try {
      DEBUG("Connection exception %s, Try to get an error\n", e.what());
      // Try to get an error from the immediate destination
      // FIXME: rewrite this (errors can come from both directions)
      char *reply_message;
      ReplyHeader h;
      h.recv_reply(sock, &reply_message, 0);
      if (h.get_status() >= STATUS_FIRST_FATAL_ERROR) {
        reader->errors.add(new Reader::SimpleError(h.get_status(),
          h.get_address(), reply_message, strlen(reply_message)));
        reader->unicast_sender.status = h.get_status();
        submit_task();
        close(sock);
        sock = -1;
        return -1;
      }
    } catch(std::exception& e) {
      // Can't receive an error, generate it
    }
    register_error(STATUS_UNICAST_CONNECTION_ERROR, target_address,
      "Error during unicast transmission with the host %s: %s", e.what());
    close(sock);
    sock = -1;
    return -1;
  }

  close(sock);
  sock = -1;
  return 0;
}
