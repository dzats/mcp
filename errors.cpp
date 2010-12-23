#include <assert.h>

#include <string>
#include <vector>

#include "errors.h"

using namespace std;

void SimpleError::display() const
{
  if (address != INADDR_NONE) {
    char host[INET_ADDRSTRLEN];
    uint32_t addr = htonl(address);
    if (message_length != 0) {
      ERROR("from host %s: %s\n",
        inet_ntop(AF_INET, &addr, host, sizeof(host)), message);
    } else if (status == STATUS_SERVER_IS_BUSY) {
      ERROR("Server %s is busy\n",
        inet_ntop(AF_INET, &addr, host, sizeof(host)));
    } else {
      ERROR("Error %u(%s) received from %s\n", status,
        get_reply_status_description(status),
        inet_ntop(AF_INET, &addr, host, sizeof(host)));
    }
  } else {
    ERROR("%s\n", message);
  }
}

void SimpleError::send(int sock)
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
    if (message_length != 0) {
      sendn(sock, message, message_length, 0);
    }
  } catch (ConnectionException& e) {
    ERROR("Can't send an error to the source: %s\n", e.what());
  }
}

FileRetransRequest::FileRetransRequest(uint32_t addr,
    const void *message, size_t message_length) :
    ErrorMessage(STATUS_INCORRECT_CHECKSUM)
{
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
  filename = (char *)malloc(file_info_header.get_name_length() + 1);
  memcpy(filename, fih + 1, file_info_header.get_name_length());
  filename[file_info_header.get_name_length()] = '\0';
  uint32_t *n_destinations = (uint32_t *)((uint8_t *)(fih + 1) +
    file_info_header.get_name_length());
  if (n_destinations >=
      (uint32_t *)((uint8_t *)message + message_length - sizeof(uint32_t)) ||
      n_destinations + ntohl(*n_destinations) + 1 >
      (uint32_t *)((uint8_t *)message + message_length)) {
    throw ConnectionException(ConnectionException::corrupted_data_received);
  }

  uint32_t *p = n_destinations + 1;
  address = ntohl(*p);

  for (unsigned i = 0; i < ntohl(*n_destinations); ++i) {
    destinations.push_back(Destination(ntohl(p[i]), NULL));
  }
}

void FileRetransRequest::display() const
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

void FileRetransRequest::send(int sock)
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
    sizeof(uint32_t) + (destinations.size() + 1)  * sizeof(uint32_t);
  uint8_t *message = (uint8_t *)malloc(message_length);

  FileInfoHeader *fih = new(message) FileInfoHeader(file_info_header);
  memcpy(fih + 1, filename, strlen(filename));
  register uint32_t *n_destinations = (uint32_t *)((uint8_t*)(fih + 1) +
    strlen(filename)); 
  *n_destinations = htonl(destinations.size() + 1);
  register uint32_t *addr = n_destinations + 1;
  *addr = htonl(address);
  ++addr;
  for (vector<Destination>::const_iterator i = destinations.begin();
      i != destinations.end(); ++i) {
    *addr = htonl(i->addr);
    addr++;
  }
  try {
    ReplyHeader h(status, address, message_length);
    sendn(sock, &h, sizeof(h), 0);
    sendn(sock, message, message_length, 0);
  } catch (ConnectionException& e) {
    ERROR("Can't send a file retransmission request to the source: %s\n",
      e.what());
  }
  free(message);
}

// Get the retransmission data for Errors::get_retransmissions
void FileRetransRequest::get_data(std::set<std::string>* filenames,
    std::map<std::string, int>* offsets,
    std::set<uint32_t>* addresses) const
{
  string fname(filename);
  filenames->insert(fname);
  (*offsets)[fname] = file_info_header.get_name_offset();
  if (address != INADDR_NONE) {
    addresses->insert(address);
  }
  for (std::vector<Destination>::const_iterator i = destinations.begin();
      i != destinations.end(); ++i) {
    if (i->addr != INADDR_NONE) {
      addresses->insert(i->addr);
    }
  }
}

// Displays the fatal error registered by the writers (unicast sender
// and multicast sender or by the reader
void Errors::display() const
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
bool Errors::is_unrecoverable_error_occurred() const
{
  pthread_mutex_lock(&mutex);
  bool result = false;
  for (list<ErrorMessage*>::const_iterator i = errors.begin();
      i != errors.end(); ++i) {
    if ((*i)->get_status() != STATUS_INCORRECT_CHECKSUM &&
        (*i)->get_status() != STATUS_SERVER_IS_BUSY &&
        (*i)->get_status() != STATUS_PORT_IN_USE) {
      result = true;
      break;
    }
  }
  pthread_mutex_unlock(&mutex);
  return result;
}

// Returns true if the STATUS_SERVER_IS_BUSY error occurred
bool Errors::is_server_busy() const
{
  pthread_mutex_lock(&mutex);
  bool result = false;
  for (list<ErrorMessage*>::const_iterator i = errors.begin();
      i != errors.end(); ++i) {
    if ((*i)->get_status() == STATUS_SERVER_IS_BUSY) {
      result = true;
      break;
    }
  }
  pthread_mutex_unlock(&mutex);
  return result;
}

// Sends occurred errors to the imediate unicast source
void Errors::send(int sock)
{
  pthread_mutex_lock(&mutex);
  while (errors.size() > 0) {
    errors.front()->send(sock);
    delete errors.front();
    errors.pop_front();
  }
  pthread_mutex_unlock(&mutex);
}

// Sends the first occurred error to the imediate unicast source
void Errors::send_first(int sock)
{
  pthread_mutex_lock(&mutex);
  if (errors.size() > 0) {
    errors.front()->send(sock);
    delete errors.front();
    errors.pop_front();
  }
  pthread_mutex_unlock(&mutex);
}

// Get the files that should be retransmitted. dest is a value/result
// argument.
char** Errors::get_retransmissions(vector<Destination> *dest,
    int **filename_offsets) const
{
  set<uint32_t> addresses;
  set<string> filenames;
  map<string, int> offsets;
  pthread_mutex_lock(&mutex);
  for (list<ErrorMessage*>::const_iterator i = errors.begin();
      i != errors.end(); ++i) {
    if ((*i)->get_status() == STATUS_INCORRECT_CHECKSUM) {
      FileRetransRequest *frr = static_cast<FileRetransRequest*>(*i);
      frr->get_data(&filenames, &offsets, &addresses);
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
    DEBUG("Pending retransmission for %s:%d\n", result[j],
      (*filename_offsets)[j]);
    ++j;
  }

  result[j] = NULL;
  pthread_mutex_unlock(&mutex);
  return result;
}

