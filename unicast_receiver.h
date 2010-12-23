#ifndef UNICAST_RECEIVER_H_HEADER
#define UNICAST_RECEIVER_H_HEADER 1

#include <sys/param.h> // for MAXPATHLEN

#include <exception>
#include <vector>

#include "reader.h"
#include "path.h"

// class that reads sources from disk or from the unicast network connection
class UnicastReceiver : public Reader
{
  int sock;

  // Prohibit coping for objects of this class
  UnicastReceiver(const UnicastReceiver&);
  UnicastReceiver& operator=(const UnicastReceiver&);
public:
  std::vector<Destination> destinations;
  uint32_t flags;
  int n_sources;
  char *path;
  uint32_t local_address;
  PathType path_type;

  UnicastReceiver() : sock(-1), path(NULL) {}
  ~UnicastReceiver()
  {
    if (path != NULL) {
      free(path);
    }
  }

  
  // Establishes the network session from the TCP connection 'sock'
  int session_init(int sock);
  /*
    The main routine of the reader, that works with a unicast session.
    It reads files and directories and transmits them to the distributor
  */
  int session();
private:

  // Gets the initial record from the immediate source
  int get_initial(MD5sum *checksum) throw (ConnectionException);

  // Gets the destinations from the immediate source
  void get_destinations(MD5sum *checksum);

  // reads file from the socket 'fd' and pass it to the distributor
  void read_from_socket(const char *filename, uint64_t size);
};
#endif
