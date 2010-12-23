#ifndef ERRORS_H_HEADER
#define ERRORS_H_HEADER 1
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <inttypes.h>

#include <list>
#include <set>
#include <map>

#include "connection.h"

class ErrorMessage
{
protected:
  uint8_t status; // status of the last task, see connection.h
public:
  ErrorMessage(uint8_t s) : status(s) {}
  virtual ~ErrorMessage() {}

  virtual void display() const = 0;
  virtual void send(int sock) = 0;

  uint8_t get_status() const { return status; }

private:
  // Protect from coping
  ErrorMessage(const ErrorMessage&);
  ErrorMessage& operator=(const ErrorMessage&);
};

class SimpleError : public ErrorMessage
{
  uint32_t address; // Address ot the host, caused the error.
    // INADDR_NONE means that the error occured on this host
  uint32_t message_length;
  uint8_t *message; // network message to be sent to the immediate
public:
  SimpleError(uint8_t status, uint32_t address, char *message,
      uint32_t message_length) : ErrorMessage(status)
  {
    this->address = address;
    this->message_length = message_length;
    if (message_length != 0) {
      this->message = (uint8_t *)strdup(message);
    } else {
      this->message = NULL;
    }
  }
  ~SimpleError()
  {
    if (message != NULL) { free(message); }
  }
  void display() const;
  void send(int sock);

private:
  // Protect from coping
  SimpleError(const SimpleError&);
  SimpleError& operator=(const SimpleError&);
};

class FileRetransRequest : public ErrorMessage
{
  char *filename;
  FileInfoHeader file_info_header;
  uint32_t address;
  std::vector<Destination> destinations;
public:
  FileRetransRequest(const char *fname, const FileInfoHeader& finfo,
      uint32_t addr, const std::vector<Destination>& dst) :
      ErrorMessage(STATUS_INCORRECT_CHECKSUM), file_info_header(finfo),
      address(addr), destinations(dst)
  {
    size_t filename_length = file_info_header.get_name_length();
    filename = (char *)malloc(filename_length + 1);
    filename[filename_length] = '\0';
    memcpy(filename, fname, filename_length);
  }
  FileRetransRequest(uint32_t addr, const void *message,
    size_t message_length);
  ~FileRetransRequest()
  {
    free(filename);
  }

  void display() const;
  void send(int sock);

  // Get the retransmission data for Errors::get_retransmissions
  void get_data(std::set<std::string>* filenames,
      std::map<std::string, int>* offsets,
      std::set<uint32_t>* addresses) const;

private:
  // Protect from coping
  FileRetransRequest(const FileRetransRequest&);
  FileRetransRequest& operator=(const FileRetransRequest&);
};

class Errors
{
  mutable pthread_mutex_t mutex;
  std::list<ErrorMessage*> errors;
public:
  Errors()
  {
    pthread_mutex_init(&mutex, NULL);
  }
  ~Errors()
  {
    while (errors.size() > 0) {
      delete errors.front();
      errors.pop_front();
    }
    pthread_mutex_destroy(&mutex);
  }

  void add(ErrorMessage *error_message)
  {
    pthread_mutex_lock(&mutex);
    errors.push_back(error_message);
    pthread_mutex_unlock(&mutex);
  }

  // Displays the registered errors 
  void display() const;

  // Delete the recoverable errors
  void delete_recoverable_errors() {
    for(std::list<ErrorMessage*>::iterator i = errors.begin();
        i != errors.end(); ++i) {
      if ((*i)->get_status() == STATUS_SERVER_IS_BUSY ||
          (*i)->get_status() == STATUS_PORT_IN_USE) {
        delete(*i);
        std::list<ErrorMessage*>::iterator next = i;
        ++next;
        errors.erase(i);
        i = next;
      }
    }
  }

  // Returns true if some of the errors is unrecoverable (even if it is not
  // fatal) and false otherwise
  bool is_unrecoverable_error_occurred() const;

  // Returns true if the STATUS_SERVER_IS_BUSY error occurred
  bool is_server_busy() const;

  // Sends the occurred error to the imediate unicast source
  void send(int sock);
  // Sends the first occurred error to the imediate unicast source
  void send_first(int sock);

  // Get the files that should be retransmitted. dest is a value/result
  // argument.
  char** get_retransmissions(std::vector<Destination> *dest,
    int **filename_offsets) const;
};

#endif
