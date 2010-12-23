#ifndef FILE_READER_H_HEADER
#define FILE_READER_H_HEADER 1

#include <sys/param.h> // for MAXPATHLEN

#include <exception>
#include <vector>

#include "distributor.h"
#include "reader.h"

class BrokenInputException : public std::exception {};

// class that reads sources from disk or from the unicast network connection
class FileReader : public Reader
{
  static const int MAX_DATA_RETRANSMISSIONS = 3;

  // Prohibit coping for objects of this class
  FileReader(const FileReader&);
  FileReader& operator=(const FileReader&);
public:

  /*
    The main routine of the mcp programm, it reads sources from
    the disk and passes them to the distributor. Returns
    status of the crudest error or STATUS_OK, if there were no errors.
  */
  uint8_t read_sources(char **filenames, int *filename_offsets);

  FileReader() {}
private:
  // Reads the file 'filename' and pass it to the distributor
  // Returns status of the crudest error or STATUS_OK,
  // if there were no errors.
  uint8_t handle_file(const char *filename, struct stat *statp,
    int basename_offset, bool error_if_cant_open);

  // Reads data from fd (till the end of file) and passes it to
  // the distributor. Returns 0 on success and errno on failure.
  int read_from_file(int fd, uint64_t size);

  // Reads information about the directory 'dirname' and pass it to
  // the distributor.
  uint8_t handle_directory(char *dirname, struct stat *statp,
    int rootdir_basename_offset);

  // Implements recursive behavior for the directory 'name'.
  // Uses the handle_file and handle_directory functions.
  uint8_t handle_directory_with_content(char *name, struct stat *statp);
};
#endif
