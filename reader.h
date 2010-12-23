#ifndef READER_H_HEADER
#define READER_H_HEADER 1

#include <sys/param.h> // for MAXPATHLEN

#include <exception>
#include <vector>

#include "distributor.h"

// object that reads sources from the disk or from the unicast network
// connection
class Reader : public Distributor {
protected:
	static const int MAX_DATA_RETRANSMISSIONS = 3;
	static const int MAX_ERROR_MESSAGE_SIZE = MAXPATHLEN + 160;

	Reader() {}

	// Registers an error and finish the current task
	void register_error(uint8_t status, const char *fmt, ...);

	// Reads the file 'filename' and pass it to the distributor
	// Returns 0 on success and -1 otherwise
	int handle_file(const char *filename, struct stat *statp,
			int basename_offset, bool error_if_cant_open);

private:
	// Prohibit coping for objects of this class
	Reader(const Reader&);
	Reader& operator=(const Reader&);

	// Reads data from 'fd' and pass it to the distributor
	int read_from_file(int fd);
};
#endif
