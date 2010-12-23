#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/param.h> // for MAXPATHLEN
#include <poll.h> // for poll

#include <fcntl.h>
#include <dirent.h>
#include <libgen.h>

#include <stack>

#include "reader.h"
#include "file_writer.h" // for FileWriter::get_targetfile_name
#include "log.h"

using namespace std;

// Register an error and finish the current task
void Reader::register_error(uint8_t status, const char *fmt, ...) {
	va_list args;
	va_start(args, fmt);
	char *error_message = (char *)malloc(MAX_ERROR_MESSAGE_SIZE);
	vsnprintf(error_message, MAX_ERROR_MESSAGE_SIZE, fmt, args);
	reader.set_error(INADDR_NONE, error_message,
		strlen(error_message));
	reader.status = status;
	// Finish the unicast sender
	finish_task();
	finish_work();
	va_end(args);
}

// Reads data from fd (till the end of file) and passes it to
// the distributor. Returns 0 on success and errno on failure.
int Reader::read_from_file(int fd) {
	// FIXME: Remove total, it is for debugging only
	unsigned long total = 0;
	while(1) {
		int count = get_space();
		count = std::min(count, 4096);
	
#ifdef BUFFER_DEBUG
		DEBUG("Free space in the buffer: %d bytes\n", count);
#endif
		count = read(fd, rposition(), count);
		if (count > 0) {
			// Update the checksum
			checksum.update((unsigned char *)rposition(), count);
			update_reader_position(count);
			total += count;
#ifdef BUFFER_DEBUG
			DEBUG("%d (%lu) bytes of data read\n", count, total);
#endif
		} else if(count == 0) {
			// End of file
			DEBUG("read_file: %lu bytes long\n", total);
			checksum.final();
			return 0;
		} else {
			return errno;
		}
	}
}

// Reads the file 'filename' and pass it to the distributor
int Reader::handle_file(const char *filename, struct stat *statp,
		int basename_offset, bool error_if_cant_open) {
	int retries = MAX_DATA_RETRANSMISSIONS;
	retransmit_file:
	/* Open the input file */
	int fd;
	if ((fd = open(filename, O_RDONLY)) == -1) {
		// Should be non-fatal in the recursive case
		DEBUG("Can't open the file %s: %s\n", filename, strerror(errno));
		if (error_if_cant_open) {
			register_error(STATUS_DISK_ERROR, "Can't open the file %s: %s\n",
				filename, strerror(errno));
			return -1;
		} else {
			return 0;
		}
	}

	// Start the file transfert operation for the distributor
	unsigned filename_length = strlen(filename + basename_offset);
	FileInfoHeader f_info(resource_is_a_file,
		statp->st_mode & ~S_IFMT, filename_length);
	Distributor::TaskHeader op = {f_info, filename + basename_offset};
	add_task(op);

	// Read file from the disk
	int read_result;
	read_result = read_from_file(fd);
	if (read_result != 0) {
		DEBUG("Read error for the file %s: %s\n", filename, strerror(errno));
		register_error(STATUS_DISK_ERROR, "Read error for the file %s: %s\n",
			filename, strerror(errno));
		return -1;
	}
	close(fd);
	uint8_t status = finish_task();

	// Check the result of writers
	if (status == STATUS_OK) {
		return 0;
	} else if (status == STATUS_INCORRECT_CHECKSUM) {
		if (retries > 0) {
			// Retransmit the file
			--retries;
			DEBUG("Retransmit file %s in the %d time\n", filename,
				MAX_DATA_RETRANSMISSIONS - retries);
			goto retransmit_file;
		} else {
			DEBUG("Too many retransmissions for file %s\n", filename);
			register_error(STATUS_UNICAST_CONNECTION_ERROR,
				"Too many retransmissions for file %s\n", filename);
			return -1;
		}
	} else {
		// One of the writers finished with a fatal error
		finish_work();
		return -1;
	}
}
