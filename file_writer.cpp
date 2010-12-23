#include <stdio.h>
#include <fcntl.h> // for stat

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/param.h> // for MAXPATHLEN
#ifndef MAXPATHLEN
#define MAXPATHLEN 1100
#endif

#include <netinet/in.h> // for INADDR_NONE

#include "log.h"
#include "file_writer.h"

// Register an input/ouput error and finish the current task
void FileWriter::register_error(uint8_t status, const char *fmt,
    const char *filename, const char *error)
{
  int error_length = strlen(fmt) + strlen(filename) +
    strlen(error) + 1; // a bit more that required
  char *error_message = (char *)malloc(error_length);
  sprintf((char *)error_message, fmt, filename, error);
  DEBUG("%s\n", error_message);
  reader->add_error(new SimpleError(status, INADDR_NONE,
    error_message, strlen(error_message)));
  reader->file_writer.status = status;
  // Finish the file writer
  submit_task();
}

// Writes data from the distributor to fd. Returns 0 on success or
// errno of the last write call on failure
int FileWriter::write_to_file(int fd)
{
#ifndef NDEBUG
  int total = 0;
#endif
  int count = get_data();
  count = std::min(count, 16384); 
  while (count > 0) {
#ifdef BUFFER_DEBUG
    DEBUG("Sending %d bytes of data\n", count);
#endif
    count = write(fd, pointer(), count);
#ifndef NDEBUG
    total += count;
#endif
#ifdef BUFFER_DEBUG
    DEBUG("%d (%d) bytes of data sent\n", count, total);
#endif
    if (count > 0) {
      update_position(count);
    } else {
      if (errno == ENOBUFS) {
        SDEBUG("ENOBUFS error occurred\n");
        usleep(200000);
        continue;
      }
      return errno;
    }

    count = get_data();
    count = std::min(count, 16384);
  }
  DEBUG("write_to_file: %llu bytes wrote\n", (long long unsigned)total);
  return 0;
}

// The main routine of the FileWriter class.
// It reads files and directories
// from the distributor and writes them to the disk
int FileWriter::session()
{
  assert(path != NULL);
  while (1) {
    // Get an operation from the distributor
    Distributor::TaskHeader *op = get_task();
    if (op->fileinfo.is_trailing_record()) {
      submit_task();
      break;
    }
    assert(op->fileinfo.get_name_length() <= MAXPATHLEN);
    if (op->fileinfo.get_type() == resource_is_a_file) {
      DEBUG("Entry for a file %s(%s): |%u| %o | %u / %u  |\n",
        op->get_filename() + op->fileinfo.get_name_offset(),
        path, op->fileinfo.get_type(), op->fileinfo.get_mode(),
        op->fileinfo.get_name_length(), op->fileinfo.get_name_offset());
      const char *filename = get_targetfile_name(
        op->get_filename() + op->fileinfo.get_name_offset(), path, path_type,
        n_sources);

      // Open the output file
      DEBUG("open the file: %s\n", filename);
      int fd;
      fd = open(filename, O_WRONLY | O_CREAT | O_TRUNC,
        op->fileinfo.get_mode());
      int error = errno;
      if (fd == -1 && errno == EACCES && unlink(filename) == 0) {
        // A read only file with the same name exists,
        // the default bahavior is to overwrite it.
        fd = open(filename, O_WRONLY | O_CREAT | O_TRUNC,
          op->fileinfo.get_mode());
      }
      if (fd == -1) {
        // Report about the error
        register_error(STATUS_NOT_FATAL_DISK_ERROR,
          "Can't open the output file %s: %s", filename, strerror(error));
        free_targetfile_name(filename, path_type);
        continue;
      }

      // Write content into the file
      int write_result;
      if ((write_result = write_to_file(fd)) != 0) {
        // Report about the error
        register_error(STATUS_NOT_FATAL_DISK_ERROR,
          "Write error for the file %s: %s", filename, strerror(errno));
        close(fd);
        free_targetfile_name(filename, path_type);
        continue;
      }
  
      close(fd);
      if ((flags & VERIFY_CHECKSUMS_TWISE_FLAG) != 0) {
        // Verify the checksum in the second time
        int fd = open(filename, O_RDONLY); 
        if (fd == -1) {
          // Report about the error
          register_error(STATUS_NOT_FATAL_DISK_ERROR,
            "Can't open the file %s for reading: %s", filename,
            strerror(error));
          free_targetfile_name(filename, path_type);
          continue;
        }
        char *buffer = (char *)malloc(FILE_READ_BUFFER_SIZE);
        MD5sum checksum;
        int read_result;
        while ((read_result = read(fd, buffer, FILE_READ_BUFFER_SIZE)) > 0) {
          checksum.update(buffer, read_result);
        }
        checksum.final();
        free(buffer);
        close(fd);
        if (read_result < 0) {
          register_error(STATUS_NOT_FATAL_DISK_ERROR,  
            "Read error for the file %s: %s", filename, strerror(errno));
          free_targetfile_name(filename, path_type);
          continue;
        }
        if (memcmp(checksum.signature, this->checksum()->signature,
            sizeof(checksum.signature)) != 0) {
          reader->add_error(new FileRetransRequest(
            op->get_filename(), op->fileinfo, unicast_receiver->local_address,
            unicast_receiver->destinations));
          reader->file_writer.status = STATUS_INCORRECT_CHECKSUM;
        }
      }

      DEBUG("File %s has been written\n", filename);
      free_targetfile_name(filename, path_type);
    } else {
      DEBUG("Entry for a directory %s(%s): |%u| %o | %u / %u  |\n",
        op->get_filename() + op->fileinfo.get_name_offset(),
        path, op->fileinfo.get_type(), op->fileinfo.get_mode(),
        op->fileinfo.get_name_length(), op->fileinfo.get_name_offset());
      const char *dirname = get_targetdir_name(
        op->get_filename() + op->fileinfo.get_name_offset(), path,
        &path_type);
      if (dirname == NULL) {
        register_error(STATUS_FATAL_DISK_ERROR,
          "The destination path %s is a file, but source is a directory. "
          "Remove %s first", path, path);
        free_targetfile_name(dirname, path_type);
        return -1;
      }

      // Create the directory
      if (mkdir(dirname, op->fileinfo.get_mode()) != 0 && errno != EEXIST) {
        register_error(STATUS_FATAL_DISK_ERROR,
          "Can't create directory: %s: %s", dirname, strerror(errno));
        free_targetfile_name(dirname, path_type);
        return -1;
      }
      free_targetfile_name(dirname, path_type);
    }

    submit_task();
  }
  return 0;
}
