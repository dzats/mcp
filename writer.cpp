#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <assert.h>

#include <algorithm>

#include "writer.h"
#include "log.h"

// Writes data from the distributor to fd. Returns 0 on success or
// errno of the last write call on failure
int Writer::write_to_file(int fd) {
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
	DEBUG("write_to_file: %d bytes wrote\n", total);
	return 0;
}
