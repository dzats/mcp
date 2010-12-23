#ifndef FILE_WRITER_H_HEADER
#define FILE_WRITER_H_HEADER 1

#include "destination.h"
#include "writer.h"
#include "unicast_receiver.h"
#include "connection.h"
#include "path.h"

// Class that writes files and directories to the disk
class FileWriter : private Writer
{
  static const size_t FILE_READ_BUFFER_SIZE = 65536;

  char *path;
  PathType path_type;
  uint32_t n_sources;

  // Prohibit coping for objects of this class
  FileWriter(const FileWriter&);
  FileWriter& operator=(const FileWriter&);
public:
  uint32_t flags;
  UnicastReceiver *unicast_receiver;
  FileWriter(UnicastReceiver* ur, uint32_t f) : Writer(ur,
    (UnicastReceiver::Client *)&ur->file_writer), path(NULL), flags(f),
    unicast_receiver(ur) {}
  
  // Initialization routine of the file writer class
  void init(char* path, PathType path_type, uint32_t n_sources)
  {
    this->path = path;
    this->path_type = path_type;
    this->n_sources = n_sources;
  }

  // The main routine of the FileWriter class. It reads files and
  // directories from the distributor and writes them to disk.
  int session();

private:
  // Register an input/ouput error and finish the current task
  void register_error(uint8_t status, const char *fmt,
    const char *filename, const char *error);

  // Writes data from the distributor to fd. Returns 0 on success or
  // errno of the last write call on failure
  int write_to_file(int fd);
};
#endif
