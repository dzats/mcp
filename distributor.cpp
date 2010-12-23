#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <assert.h>

#include <algorithm>
#include <set>
#include <map>

#include "distributor.h"
#include "log.h"

using namespace std;

void Distributor::SimpleError::display() const
{
  if (address != INADDR_NONE) {
    char host[INET_ADDRSTRLEN];
    uint32_t addr = htonl(address);
    ERROR("from host %s: %s\n",
      inet_ntop(AF_INET, &addr, host, sizeof(host)), message);
  } else {
    ERROR("%s\n", message);
  }
}

void Distributor::SimpleError::send(int sock)
{
  try {
    if (address == INADDR_NONE) {
      // Get address from the current connection
      struct sockaddr_in addr;
      socklen_t addr_len = sizeof(addr);
      
      if(getsockname(sock, (struct sockaddr *)&addr, &addr_len) != 0) {
        ERROR("Can't get address from socket: %s", strerror(errno));
      } else {
        address = ntohl(addr.sin_addr.s_addr);
      }
    }
  
    ReplyHeader h(status, address, message_length);
    sendn(sock, &h, sizeof(h), 0);
    sendn(sock, message, message_length, 0);
  } catch (ConnectionException& e) {
    ERROR("Can't send error to the source: %s\n", e.what());
  }
}

Distributor::FileRetransRequest::FileRetransRequest(uint32_t addr,
    const void *message, size_t message_length) :
    ErrorMessage(STATUS_INCORRECT_CHECKSUM) {
  assert(message_length > sizeof(FileInfoHeader));
  if (message_length < sizeof(FileInfoHeader)) {
    throw ConnectionException(ConnectionException::corrupted_data_received);
  }
  FileInfoHeader *fih = (FileInfoHeader *)message;
  file_info_header = *fih;
  assert(file_info_header.get_name_length() <= message_length -
    sizeof(FileInfoHeader) - sizeof(uint32_t));
  if (file_info_header.get_name_length() > message_length -
      sizeof(FileInfoHeader) - sizeof(uint32_t)) {
    throw ConnectionException(ConnectionException::corrupted_data_received);
  }
  filename = (char *)malloc(file_info_header.get_name_length());
  memcpy(filename, fih + 1, file_info_header.get_name_length());
  uint32_t *p = (uint32_t *)((uint8_t *)(fih + 1) +
    file_info_header.get_name_length());
  address = ntohl(*p);
  while (p <=
      (uint32_t *)((uint8_t *)message + message_length - sizeof(uint32_t))) {
    destinations.push_back(Destination(ntohl(*p), NULL));
    ++p;
  }
}

void Distributor::FileRetransRequest::display() const
{
  if (address != INADDR_NONE) {
    char host_addr[INET_ADDRSTRLEN];
    uint32_t h_addr = htonl(address);
    ERROR("Retransmission request for %s from %s\n",
      filename + file_info_header.get_name_offset(),
      inet_ntop(AF_INET, &h_addr, host_addr, sizeof(host_addr)));
  } else {
    ERROR("%s\n", filename + file_info_header.get_name_offset());
  }
}

void Distributor::FileRetransRequest::send(int sock)
{
  if (address == INADDR_NONE) {
    // Get address from the current connection
    struct sockaddr_in addr;
    socklen_t addr_len = sizeof(addr);
    
    if(getsockname(sock, (struct sockaddr *)&addr, &addr_len) != 0) {
      ERROR("Can't get address from socket: %s", strerror(errno));
    } else {
      address = ntohl(addr.sin_addr.s_addr);
    }
  }

  size_t message_length = sizeof(struct FileInfoHeader) + strlen(filename) +
    sizeof(uint32_t) + destinations.size()  * sizeof(uint32_t);
  uint8_t *message = (uint8_t *)malloc(message_length);

  FileInfoHeader *fih = new(message) FileInfoHeader(file_info_header);
  memcpy(fih + 1, filename, strlen(filename));
  register uint32_t *n_destinations = (uint32_t *)((uint8_t*)(fih + 1) +
    strlen(filename)); 
  *n_destinations = destinations.size() + 1;
  register uint32_t *addr = n_destinations + 1;
  *addr = htonl(address);
  ++addr;
  for (vector<Destination>::const_iterator i = destinations.begin();
      i != destinations.end(); ++i) {
    *addr = htonl(i->addr);
    addr++;
  }
  *addr = INADDR_NONE;
  try {
    ReplyHeader h(status, address, message_length);
    sendn(sock, &h, sizeof(h), 0);
    sendn(sock, message, message_length, 0);
  } catch (ConnectionException& e) {
    ERROR("Can't file retransmission request to the source: %s\n", e.what());
  }
  free(message);
}

// Displays the fatal error registered by the writers (unicast sender
// and multicast sender or by the reader
void Distributor::Errors::display()
{
  pthread_mutex_lock(&mutex);
  for (list<ErrorMessage*>::const_iterator i = errors.begin();
      i != errors.end(); ++i) {
    (*i)->display();
  }
  pthread_mutex_unlock(&mutex);
}

// Returns true if some of the errors is unrecoverable (even if it is not
// fatal) and false otherwise
bool Distributor::Errors::is_unrecoverable_error_occurred()
{
  pthread_mutex_lock(&mutex);
  bool result = false;
  for (list<ErrorMessage*>::const_iterator i = errors.begin();
      i != errors.end(); ++i) {
    if ((*i)->status != STATUS_INCORRECT_CHECKSUM) {
      result = true;
      break;
    }
  }
  pthread_mutex_unlock(&mutex);
  return result;
}

// Sends the occurred error to the imediate unicast source
void Distributor::Errors::send(int sock)
{
  pthread_mutex_lock(&mutex);
  while (errors.size() > 0) {
    errors.front()->send(sock);
    delete errors.front();
    errors.pop_front();
  }
  pthread_mutex_unlock(&mutex);
}

// Get the files that should be retransmitted. dest is a value/result
// argument.
char** Distributor::Errors::get_retransmissions(vector<Destination> *dest,
    int **filename_offsets)
{
  set<uint32_t> addresses;
  set<string> filenames;
  map<string, int> offsets;
  pthread_mutex_lock(&mutex);
  for (list<ErrorMessage*>::const_iterator i = errors.begin();
      i != errors.end(); ++i) {
    if ((*i)->status == STATUS_INCORRECT_CHECKSUM) {
      FileRetransRequest *frr = static_cast<FileRetransRequest*>(*i);
      string fname(frr->filename);
      filenames.insert(fname);
      offsets[fname] = frr->file_info_header.get_name_offset();
      if (frr->address != INADDR_NONE) {
        addresses.insert(frr->address);
      }
    }
  }
  // Remove the excess destinations
  unsigned i = 0;
  while (i < dest->size()) {
    if (addresses.find((*dest)[i].addr) == addresses.end()) {
      (*dest)[i] = (*dest).back();
      dest->pop_back();
    } else {
      ++i;
    }
  }
  // Get the filenames (cases with more than one file to retransmit are
  // almost impossible)
  char **result = (char **)malloc(sizeof(char *) * (filenames.size() + 1));
  *filename_offsets = (int *)malloc(sizeof(int) * filenames.size());
  int j = 0;
  for (set<string>::const_iterator i = filenames.begin();
      i != filenames.end(); ++i) {
    result[j] = strdup(i->c_str());
    (*filename_offsets)[j] = offsets[*i];
    DEBUG("Pending retransmission for %s:%d\n", i->c_str(),
      (*filename_offsets)[j]);
    ++j;
  }

  result[j] = NULL;
  pthread_mutex_unlock(&mutex);
  return result;
}

Distributor::Distributor() : is_reader_awaiting(false),
    are_writers_awaiting(false)
{
  // The reader is always present
  reader.is_present = true;

  buffer = (uint8_t*)malloc(DEFAULT_BUFFER_SIZE);
  pthread_mutex_init(&mutex, NULL);
  pthread_cond_init(&space_ready_cond, NULL);
  pthread_cond_init(&data_ready_cond, NULL);
  pthread_cond_init(&operation_ready_cond, NULL);
  pthread_cond_init(&writers_finished_cond, NULL);
}

// Adds a new task to the buffer, block until the previous task finished
void Distributor::add_task(const FileInfoHeader& fileinfo,
    const char* filename)
{
  pthread_mutex_lock(&mutex);
  operation.fileinfo = fileinfo;
  operation.set_filename(filename);
  // Set the buffer to the default state
  assert(reader.is_present);
  reader.is_done = false;
  reader.status = STATUS_OK;
  reader.offset = 0;

  if (file_writer.is_present &&
      file_writer.status < STATUS_FIRST_FATAL_ERROR) {
    file_writer.is_done = false;
    file_writer.offset = 0;
  }

  if (unicast_sender.is_present &&
      unicast_sender.status < STATUS_FIRST_FATAL_ERROR) {
    unicast_sender.is_done = false;
    unicast_sender.offset = 0;
  }

  if (multicast_sender.is_present &&
      multicast_sender.status < STATUS_FIRST_FATAL_ERROR) {
    multicast_sender.is_done = false;
    multicast_sender.offset = 0;
  }
  // Wake up the readers
  pthread_cond_broadcast(&operation_ready_cond);
  pthread_mutex_unlock(&mutex);
}

// Signal that the reader has finished the task and wait for the readers
// Returns class of the most critical error
uint8_t Distributor::finish_task()
{
  pthread_mutex_lock(&mutex);
  reader.is_done = true;

  // Wake up the writers
#ifdef BUFFER_DEBUG
  SDEBUG("finish_buffer_operation: wake readers\n");
#endif
  pthread_cond_broadcast(&data_ready_cond);

  // Wait until the writers accomplish the task
  while (!all_done()) {
    pthread_cond_wait(&writers_finished_cond, &mutex);
  }

  // Get the task status
  uint8_t status = reader.status;
  if (file_writer.status > status) {
    status = file_writer.status;
  }
  if (unicast_sender.status > status) {
    status = unicast_sender.status;
  }
  if (multicast_sender.status > status) {
    status = multicast_sender.status;
  }

  pthread_mutex_unlock(&mutex);
  return status;
}

// It is combination of the add_task for trailing task and finish_task
uint8_t Distributor::finish_work()
{
  pthread_mutex_lock(&mutex);
  // Warning the task must be finished
  while (!all_done()) {
    // Wait until the readers accomplish the previous task
    pthread_cond_wait(&writers_finished_cond, &mutex);
  }
  SDEBUG("Set the trailing task\n");
  // Move the trailing record to the operation->fileinfo
  memset(&operation.fileinfo, 0, sizeof(operation.fileinfo));

  if (file_writer.is_present &&
      file_writer.status < STATUS_FIRST_FATAL_ERROR) {
    file_writer.is_done = false;
    file_writer.offset = 0;
  }

  if (unicast_sender.is_present &&
      unicast_sender.status < STATUS_FIRST_FATAL_ERROR) {
    unicast_sender.is_done = false;
    unicast_sender.offset = 0;
  }

  if (multicast_sender.is_present &&
      multicast_sender.status < STATUS_FIRST_FATAL_ERROR) {
    multicast_sender.is_done = false;
    multicast_sender.offset = 0;
  }

  SDEBUG("Wait for the writers\n");
  // Wake up the writers
  pthread_cond_broadcast(&operation_ready_cond);

  // Wait until the writers accomplish the task
  while (!all_done()) {
    pthread_cond_wait(&writers_finished_cond, &mutex);
  }

  // Additional wake up for the multithreaded multicast sender
  pthread_cond_broadcast(&operation_ready_cond);

  // Get the task status
  uint8_t status = reader.status;
  if (file_writer.status > status) {
    status = file_writer.status;
  }
  if (unicast_sender.status > status) {
    status = unicast_sender.status;
  }
  if (multicast_sender.status > status) {
    status = multicast_sender.status;
  }

  SDEBUG("The finish_work method exited\n");
  pthread_mutex_unlock(&mutex);
  return status;
}


/*
  Return free space available for read, blocks until
  the space will be available. Can be called only by the
  reader.
*/
int Distributor::get_data(Client *w)
{
  int count;
  pthread_mutex_lock(&mutex);
  count = _get_data(w->offset);
  while (count == 0) {
    if (reader.is_done) {
#ifdef BUFFER_DEBUG
      SDEBUG("writer %p is done, wake up reader\n", w);
#endif
      pthread_mutex_unlock(&mutex);
      return 0;
    } else {
#ifdef BUFFER_DEBUG
      SDEBUG("writer %p is going to sleep\n", w);
#endif
      are_writers_awaiting = true;
      pthread_cond_wait(&data_ready_cond, &mutex);
      count = _get_data(w->offset);
#ifdef BUFFER_DEBUG
      DEBUG("awake attempt for the reader %p (%d)\n", w, count);
#endif
    }
  }
  pthread_mutex_unlock(&mutex);
  assert(count > 0);
  return count;
}

/*
  Return free space available for write, blocks until
  the space will be available. Can be called only by the
  reader.
*/
int Distributor::get_space()
{
  int count;
  pthread_mutex_lock(&mutex);
  count = _get_space();
  while (count == 0) {
#ifdef BUFFER_DEBUG
    SDEBUG("get_space: reader is going to sleep\n");
#endif
    is_reader_awaiting = true;
    pthread_cond_wait(&space_ready_cond, &mutex);
    count = _get_space();
    is_reader_awaiting = false;
#ifdef BUFFER_DEBUG
    DEBUG("get_space: awake attempt for the reader (%d)\n", count);
#endif
  }
  pthread_mutex_unlock(&mutex);
  assert(count > 0);
  assert((reader.offset & DEFAULT_BUFFER_MASK) + count <= DEFAULT_BUFFER_SIZE);
  return count;
}

void Distributor::update_reader_position(int count)
{
  pthread_mutex_lock(&mutex);
#ifdef BUFFER_DEBUG
  DEBUG("rposition update: %d -> ", reader.offset);
#endif
  reader.offset = reader.offset + count;
  if (reader.offset >= DEFAULT_BUFFER_SIZE * 2) {
    reader.offset = DEFAULT_BUFFER_SIZE + (reader.offset & DEFAULT_BUFFER_MASK);
    file_writer.offset &= DEFAULT_BUFFER_MASK;
    unicast_sender.offset &= DEFAULT_BUFFER_MASK;
    multicast_sender.offset &= DEFAULT_BUFFER_MASK;
  }
#ifdef BUFFER_DEBUG
  DEBUG("%d\n", reader.offset);
#endif
  if (are_writers_awaiting) {
#ifdef BUFFER_DEBUG
    SDEBUG("wake up writers\n");
#endif
    are_writers_awaiting = false;
    pthread_cond_broadcast(&data_ready_cond);
  }
  pthread_mutex_unlock(&mutex);
}

// Move w->offset to the count bytes left (cyclic)
// Count should not be greater than zero.
void Distributor::update_writer_position(int count, Client *w)
{
  pthread_mutex_lock(&mutex);
#ifdef BUFFER_DEBUG
  DEBUG("update_writer_position: %d -> ", w->offset);
#endif
  w->offset += count;
#ifdef BUFFER_DEBUG
  DEBUG("%d\n", w->offset);
#endif
  if (is_reader_awaiting) {
#ifdef BUFFER_DEBUG
    SDEBUG("update_writer_position: wake up reader\n");
#endif
    pthread_cond_signal(&space_ready_cond);
  }
  pthread_mutex_unlock(&mutex);
}

// Put data into the buffer
void Distributor::put_data(void *data, int size)
{
  do {
    int count = get_space();
    count = std::min(count, size);
  
    memcpy(rposition(), data, count);
    // Update the checksum
    checksum.update((unsigned char *)rposition(), count);
    update_reader_position(count);
    size -= count;
    data = (uint8_t *)data + count;
  } while (size > 0);
}

// Put data into the buffer, checksum is not changed
void Distributor::put_data_without_checksum_update(void *data, int size)
{
  do {
    int count = get_space();
    count = std::min(count, size);
  
    memcpy(rposition(), data, count);
    update_reader_position(count);
    size -= count;
    data = (uint8_t *)data + count;
  } while (size > 0);
}

