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
void FileWriter::register_input_output_error(const char *fmt,
		const char *filename, const char *error) {
	int error_length = strlen(fmt) + strlen(filename) +
		strlen(error) + 1; // a bit more that required
	char *error_message = (char *)malloc(error_length);
	sprintf((char *)error_message, fmt, filename, error);
	ERROR("%s\n", error_message);
	buff->file_writer.set_error(INADDR_NONE, error_message,
		strlen(error_message));
	buff->file_writer.status = STATUS_DISK_ERROR;
	// Finish the file writer
	submit_task();
}

// The main routine of the FileWriter class.
// It reads files and directories
// from the distributor and writes them to the disk
int FileWriter::session() {
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
#ifndef NDEBUG
			DEBUG("Entry for a file (%s)%s: ", path, op->filename.c_str());
			op->fileinfo.print();
#endif
			const char *filename = get_targetfile_name(op->filename.c_str(), path,
				path_type);

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
				register_input_output_error("Can't open the output file %s: %s",
					filename, strerror(error));
				free_targetfile_name(filename, path_type);
				return -1;
			}

			// Write content into the file
			int write_result;
			if ((write_result = write_to_file(fd)) != 0) {
				// Report about the error
				register_input_output_error("Write error for the file %s: %s",
					filename, strerror(errno));
				close(fd);
				free_targetfile_name(filename, path_type);
				return -1;
			}
	
			close(fd);
			// TODO: Verify the checksum again, if required

			DEBUG("File %s has been written\n", filename);
			free_targetfile_name(filename, path_type);
		} else {
#ifndef NDEBUG
			DEBUG("Entry for a directory %s received: ", op->filename.c_str());
			op->fileinfo.print();
#endif
			const char *dirname = get_targetdir_name(op->filename.c_str(), path,
				&path_type);
			if (dirname == NULL) {
				register_input_output_error("The destination path %s is a file, "
					"but source is a directory. Remove %s first", path, path);
				free_targetfile_name(dirname, path_type);
				return -1;
			}

			// Create the directory
			if (mkdir(dirname, op->fileinfo.get_mode()) != 0 && errno != EEXIST) {
				register_input_output_error("Can't create directory: %s: %s",
					dirname, strerror(errno));
				free_targetfile_name(dirname, path_type);
				return -1;
			}
			free_targetfile_name(dirname, path_type);
		}

		submit_task();
	}
	return 0;
}
