#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/param.h> // for MAXPATHLEN
#include <poll.h> // for poll

#include <fcntl.h>
#include <dirent.h>
#include <libgen.h>

#ifndef linux
#include <machine/atomic.h>
#endif

#include <stack>

#include "unicast_receiver.h"
#include "log.h"

using namespace std;

// reads file from the socket 'fd' and pass it to the distributor
void UnicastReceiver::read_from_socket(const char *filename, uint64_t size)
{
  struct pollfd p;
  p.fd = sock;
  p.events = POLLIN | POLLPRI;

  uint64_t total = 0;
  int flags = 0;
  bool has_file_been_shrinked = false; // whether the file has been shrinked
    // during the transmission
  unsigned previous_bytes_received = *bytes_received;
  struct timeval previous_timestamp;
  struct timeval current_timestamp;
  gettimeofday(&previous_timestamp, NULL);
  int64_t allowed_to_recv = 4096;
  int remaining = 0;
  int count = 0;

  while(1) {
    if (remaining < 16384) {
      //SDEBUG("First case\n");
      count = get_space();
      remaining = count;
    } else {
      //SDEBUG("Second case\n");
      count = 16384;
    }

#ifdef BUFFER_DEBUG
    DEBUG("Free space in the buffer: %d bytes\n", count);
#endif
    if (!has_file_been_shrinked && total + count > size) {
      count = size - total;
      if (count == 0) {
        break;
      }
    }

    int poll_result;
    if ((poll_result = poll(&p, 1, -1)) < 0) {
      DEBUG("Poll call finished with error: %s\n", strerror(errno));
#if 0
        register_error(STATUS_UNICAST_CONNECTION_ERROR,
          "The poll call finished with error during "
          "transmission of %s: %s", filename, strerror(errno));
        throw BrokenInputException();
#endif
      throw ConnectionException(errno);
    }
    if (p.revents & POLLPRI) {
      // Try to get the out of band data
      uint8_t oob_data;
      int count = recv(sock, &oob_data, sizeof(oob_data),
        MSG_OOB | MSG_DONTWAIT);
      if (count > 0) {
        // the out-of-band data has been received
        if (oob_data == (uint8_t)OOB_FILE_SIZE_CHANGED) {
          // This is signal to switch to 'shrinked file' mode
          has_file_been_shrinked = true;
          SDEBUG("OOB data received: File has been shrinked\n");
          // Send the total numbers of bytes read back to the source
          total = hton64(total);
          sendn(sock, &total, sizeof(total), 0);
          total = ntoh64(total);
          continue;
        } else if (oob_data == (uint8_t)OOB_FILE_TRANSMISSION_DONE) {
          // The end of the file transmission
          SDEBUG("OOB data received: Transmission is done\n");
          uint8_t zero = 0;
          sendn(sock, &zero, sizeof(zero), 0);
          return;
        } else {
          // Unexpected Out-of-Band data received
          DEBUG("Unexpected Out-of-Band data received: %d\n", oob_data);
          continue;
        }
      } else {
        if (errno == EAGAIN) {
          flags |= MSG_DONTWAIT;
          SDEBUG("Out of band data is not ready\n");
        } else if (errno == EINVAL) {
          flags |= MSG_DONTWAIT;
          usleep(200000);
          SDEBUG("recv returned EINVAL\n");
        } else {
          // An error occurred
          DEBUG("Socket read error on %s: (%u)%s\n", filename, errno,
            strerror(errno));
#if 0
          register_error(STATUS_UNICAST_CONNECTION_ERROR,
            "Socket read error on %s: %s", filename, strerror(errno));
#endif
          throw ConnectionException(errno);
        }
      }
    }

    if (bandwidth != 0) {
      gettimeofday(&current_timestamp, NULL);
      unsigned data_received = *bytes_received;

      allowed_to_recv += ((int64_t)bandwidth *
        (current_timestamp.tv_usec - previous_timestamp.tv_usec)) >> 20;
      allowed_to_recv += (bandwidth  - (bandwidth >> 5) - (bandwidth >> 6)) *
        (current_timestamp.tv_sec - previous_timestamp.tv_sec);
      allowed_to_recv -= data_received - previous_bytes_received;
      previous_bytes_received = data_received;
      previous_timestamp = current_timestamp;

      if (allowed_to_recv > 8LL * 1024 * 1024) {
        allowed_to_recv = 4LL * 1024 * 1024;
      }

      //DEBUG("allowed_to_recv: %llu\n", allowed_to_recv);
      if (allowed_to_recv > 1000) {
        count = min(count, (int)allowed_to_recv);
      } else if ((unsigned)count > allowed_to_recv) {
        unsigned time_to_sleep = (((int64_t)count - allowed_to_recv) << 20) /
          bandwidth;
        //DEBUG("Sleep for %u\n", time_to_sleep);
        internal_usleep(time_to_sleep);
      }
    }

    // Normal data received
    count = recv(sock, rposition(), count, flags);
    if (count > 0) {
      // Update the checksum
      checksum.update((unsigned char *)rposition(), count);
      update_reader_position(count);
      total += count;
      remaining -= count;
#ifndef linux
      atomic_add_int(bytes_received, count);
#else
      bytes_received += count;
#endif
      //DEBUG("total: %zu, size: %zu\n", (size_t)total, (size_t)size);
      if (has_file_been_shrinked) {
        // Send the total numbers of bytes back read to the source
        total = hton64(total);
        sendn(sock, &total, sizeof(total), 0);
        total = ntoh64(total);
      } else {
        if (total == size) {
          // The end of the file transmission
          DEBUG("%lu bytes received\n", (long unsigned)total);
          return;
        }
      }
    } else if (count == 0) {
      // End of file
      DEBUG("Unexpected end of transmission for the file %s\n", filename);
      throw ConnectionException(ConnectionException::unexpected_end_of_input);
    } else {
      if (errno == EAGAIN) {
        // Data is not ready
        continue;
      }
      // An error occurred
      DEBUG("Socket read error on %s: %s\n", filename, strerror(errno));
#if 0
      register_error(STATUS_UNICAST_CONNECTION_ERROR,
        "Socket read error on %s: %s", filename, strerror(errno));
#endif
      throw ConnectionException(errno);
    }
  }
}

// Gets the initial record from the immediate source
int UnicastReceiver::get_initial(MD5sum *checksum) throw (ConnectionException)
{
  // Get the number of sources
  UnicastSessionHeader ush;
  recvn(sock, &ush, sizeof(ush));
  checksum->update(&ush, sizeof(ush));
  n_sources = ush.get_nsources();
  flags = ush.get_flags();
  register int path_len = ush.get_path_length();

  if (path_len > MAXPATHLEN) {
    throw ConnectionException(ConnectionException::corrupted_data_received);
  }

  DEBUG("number of sources: %d, path length: %d\n", n_sources, path_len);
  if (path != NULL) {
    free(path);
  }
  path = (char *)malloc(path_len + 1);
  path[path_len] = 0;
  if (path_len > 0) {
    recvn(sock, path, path_len);
    checksum->update(path, path_len);
    DEBUG("destination: %s\n", path);
  }

  char *error;
  path_type = get_path_type(path, &error, n_sources);
  if (path_type == path_is_invalid) {
    register_error(STATUS_FATAL_DISK_ERROR, error);
    free(error);
    return -1;
  }
  DEBUG("SocketReader::get_initial: The specified path is : %d\n", path_type);
  return 0;
}

// gets the destinations from the immediate source
void UnicastReceiver::get_destinations(MD5sum *checksum)
{
  uint32_t record_header[2]; // addr, name len
  destinations.clear();
  while(1) {
    recvn(sock, record_header, sizeof(record_header));
    checksum->update(record_header, sizeof(record_header));
    // FIXME: rewrite using the DestinationHeader structure
    record_header[0] = ntohl(record_header[0]);
    record_header[1] = ntohl(record_header[1]);
    if (record_header[0] != 0) {
      DEBUG("addr: %x, length: %x\n", record_header[0], record_header[1]);
      if (record_header[1] >= MAXPATHLEN) {
        throw ConnectionException(ConnectionException::corrupted_data_received);
      }

      char *location = (char *)malloc(record_header[1] + 1);
      location[record_header[1]] = 0;
      if (record_header[1] > 0) {
        // Get the server's address
        recvn(sock, location, record_header[1]);
        checksum->update(location, record_header[1]);
      }
      // Get the destinations' list. Possibly memory leak here, check this
      destinations.push_back(Destination(record_header[0],
        record_header[1] == 0 ? NULL : location));
    } else {
      // All the destinations are read
#ifndef NDEBUG
      SDEBUG("The destinations received: \n");
      for (std::vector<Destination>::const_iterator i = destinations.begin();
          i != destinations.end(); ++i) {
        printf("%d.", (*i).addr >> 24);
        printf("%d.", (*i).addr >> 16 & 0xFF);
        printf("%d.", (*i).addr >> 8 & 0xFF);
        printf("%d: ", (*i).addr & 0xFF);
        if (i->filename.size() > 0) {
          printf("%s\n", &*(i->filename.c_str()));
        }
      }
#endif
      return;
    }
  }
}

// Establishes the network session from the TCP connection 'sock'
int UnicastReceiver::session_init(int s)
{
  sock = s;
  struct sockaddr_in saddr;
  socklen_t saddr_len;
  if(getsockname(sock, (struct sockaddr *)&saddr, &saddr_len) != 0) {
    register_error(STATUS_UNICAST_INIT_ERROR,
      "Can't get local address from the socket: %s\n", strerror(errno));
    return -1;
  }
  local_address = ntohl(saddr.sin_addr.s_addr);
  try {
    // Get the session initialization record
    MD5sum checksum;
    bool is_initialization_finished = false;
    do {
      if (get_initial(&checksum) != 0) {
        // The error has been already registered
        return -1;
      }
      // Get the destinations for the files
      get_destinations(&checksum);
      // Get the checksum and compare it with the calculated one
      checksum.final();
#ifndef NDEBUG
      SDEBUG("Calculated checksum: ");
      MD5sum::display_signature(stdout, checksum.signature);
      printf("\n");
#endif
      uint8_t received_checksum[sizeof(checksum.signature)];
      recvn(sock, received_checksum, sizeof(received_checksum));
#ifndef NDEBUG
      SDEBUG("Received checksum:   ");
      MD5sum::display_signature(stdout, received_checksum);
      printf("\n");
#endif
      if (memcmp(checksum.signature, received_checksum,
          sizeof(checksum.signature)) != 0) {
        SERROR("Incorrect checksum in the session inialization request\n");
        // TODO: Read all the available data to synchronize connection
        send_incorrect_checksum(sock, local_address);
        // Wait for a retransmission of the session initialization data
      } else {
        // Conform the session initialization (for this particular hop)
        SDEBUG("Session established\n");
        send_normal_conformation(sock, local_address);
        is_initialization_finished = true;
      }
    } while(!is_initialization_finished);
  } catch (std::exception& e) {
    ERROR("Network error during session initialization: %s\n", e.what());
    register_error(STATUS_UNICAST_INIT_ERROR,
      "Network error during session initialization: %s\n", e.what());
    return -1;
  }
  return 0;
}

/*
  The main routine of the reader, that works with the unicast session.
  It reads files and directories and transmits them to the distributor
*/
int UnicastReceiver::session()
{
  while (1) {
    FileInfoHeader finfo;
    try {
      recvn(sock, &finfo, sizeof(finfo));
      if (finfo.is_trailing_record()) {
        SDEBUG("End of the transmission\n");
        if (finish_work() >= STATUS_FIRST_FATAL_ERROR) {
          // A fatal error occurred
          send_errors(sock);
          return EXIT_FAILURE;
        } else {
          // Send the errors occurred , if any
          send_errors(sock);
          send_normal_conformation(sock, local_address);
          return EXIT_SUCCESS;
        }
      }
      if (finfo.get_name_length() >= MAXPATHLEN) {
        throw ConnectionException(ConnectionException::corrupted_data_received);
      }
  
      // Read the file name
      char fname[finfo.get_name_length() + 1];
      recvn(sock, fname, finfo.get_name_length());
      fname[finfo.get_name_length()] = 0;
  
      if (finfo.get_type() == resource_is_a_file) {
        DEBUG("File: %s(%s) (%o, %zu)\n", path, fname + finfo.get_name_offset(),
          finfo.get_mode(), (size_t)finfo.get_file_size());
  
        // Add task for the senders (after buffer reinitialization);
        add_task(finfo, fname);
  
        read_from_socket(fname + finfo.get_name_offset(),
          finfo.get_file_size());
        checksum.final();

        if (reader.status == STATUS_OK) {
          // All ok, get checksum for the received file and check it
          uint8_t signature[sizeof(checksum.signature)];
  
          recvn(sock, signature, sizeof(signature));
#ifndef NDEBUG
          SDEBUG("Received checksum  : ");
          MD5sum::display_signature(stdout, signature);
          printf("\n");
          SDEBUG("Calculated checksum: ");
          MD5sum::display_signature(stdout, checksum.signature);
          printf("\n");
#endif
      
          if (memcmp(signature, checksum.signature,
              sizeof(signature)) != 0) {
            SERROR("Received checksum differs from the calculated one\n");
            add_error(new FileRetransRequest(
              fname, finfo, local_address, destinations));
            reader.status = STATUS_INCORRECT_CHECKSUM;
          }
        } else {
          // An error during trasmission, close the session
          SDEBUG("UnicastReceiver finished with error\n");
          finish_task();
          finish_work();
          return EXIT_FAILURE;
        }
      } else {
        DEBUG("Directory: %s(%s) (%o)\n", path,
          fname + finfo.get_name_offset(), finfo.get_mode());
        // Add task for the senders
        add_task(finfo, fname);
        // TODO: Add checksum or something like this to the fileinfo header
        // No more actions are done for the directory
      }

      uint8_t status;
      status = finish_task();
      // Send errors to the source
      if (status != STATUS_OK) {
        DEBUG("Some destinations finished with errors (%d)\n", status);
        send_first_error(sock);
        if (status >= STATUS_FIRST_FATAL_ERROR) {
          send_errors(sock);
          finish_work();
          return EXIT_FAILURE;
        }
      }
    } catch(ConnectionException& e) {
      ERROR("Network error during transmission: %s\n", e.what());
      checksum.final();
      register_error(STATUS_UNICAST_CONNECTION_ERROR,
        "Network error during transmission: %s\n", e.what());
      send_errors(sock);
      return EXIT_FAILURE;
    }
  }
}
