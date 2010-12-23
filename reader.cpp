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

/*
	The main routine of the mcp programm, it reads sources from
	the disk and passes them to the distributor. Returns 0
	on suceess and something else otherwise (error can be
	detected by the distributor's state).
*/
int Reader::read_sources(char **filenames) {
	while(*filenames != NULL) {
		// Detect whether *filenames is a file or a directory
		struct stat fs;
		if (stat(*filenames, &fs) != 0) {
			ERROR("Can't access the file %s: %s\n", *filenames,
				strerror(errno));
			return -1;
		}

		if (S_ISDIR(fs.st_mode)) {
			// Recursive behaviour for directories
			handle_directory_with_content(*filenames, &fs);
		} else if (S_ISREG(fs.st_mode)) {
			// *filenames should be already fixed up by the 
			// prepare_to_basename routine (mcp.cpp)
			char *basename;
			int basename_offset;
			if ((basename = strrchr(*filenames, '/')) != NULL) {
				basename_offset = basename - *filenames + 1;
			} else {
				basename_offset = 0;
			}
			handle_file(*filenames, &fs, basename_offset, true);
		} else {
			ERROR("%s is not a regular file or directory\n", *filenames);
			return -1;
		}
		++filenames;
	}

	SDEBUG("All sources read, finish the task\n");
	return buff->finish_work();
}

// Register an connection error and finish the current task
void Reader::register_error(uint8_t status, const char *fmt, ...) {
	va_list args;
	va_start(args, fmt);
	char *error_message = (char *)malloc(MAX_ERROR_MESSAGE_SIZE);
	vsnprintf(error_message, MAX_ERROR_MESSAGE_SIZE, fmt, args);
	buff->reader.set_error(INADDR_NONE, error_message,
		strlen(error_message));
	buff->reader.status = status;
	// Finish the unicast sender
	buff->finish_task();
	buff->finish_work();
	va_end(args);
}

// Reads data from fd (till the end of file) and passes it to
// the distributor. Returns 0 on success and errno on failure.
int Reader::read_from_file(int fd) {
	// FIXME: Remove total, it is for debugging only
	unsigned long total = 0;
	while(1) {
		int count = buff->get_space();
		count = std::min(count, 4096);
	
#ifdef BUFFER_DEBUG
		DEBUG("Free space in the buffer: %d bytes\n", count);
#endif
		count = read(fd, buff->rposition(), count);
		if (count > 0) {
			// Update the checksum
			buff->checksum.update((unsigned char *)buff->rposition(), count);
			buff->update_reader_position(count);
			total += count;
#ifdef BUFFER_DEBUG
			DEBUG("%d (%lu) bytes of data read\n", count, total);
#endif
		} else if(count == 0) {
			// End of file
			DEBUG("read_file: %lu bytes long\n", total);
			buff->checksum.final();
			return 0;
		} else {
			return errno;
		}
	}
}

// reads file from the socket 'fd' and pass it to the distributor
void Reader::read_from_socket(int fd, const char *filename) {
	struct pollfd p;
	memset(&p, 0, sizeof(p));
	p.fd = sock;
	// FIXME: Don't shure that POLLPRI is really required
	p.events = POLLIN | POLLPRI;

	// FIXME: Remove total, it is for debugging only
	int total = 0;
	while(1) {
		int count = buff->get_space();
		count = std::min(count, 4096); // to speed up start

#ifdef BUFFER_DEBUG
		DEBUG("Free space in the buffer: %d bytes\n", count);
#endif
		// The poll is used to catch the case when the first byte of
		// the received data is out-of-band
		if (poll(&p, 1, -1) <= 0) {
#if 0
			DEBUG("The poll call finished with error during "
				"transmission of %s: %s\n", filename, strerror(errno));
			register_error(STATUS_UNICAST_CONNECTION_ERROR,
				"The poll call finished with error during "
				"transmission of %s: %s", filename, strerror(errno));
			throw BrokenInputException();
#endif
			throw ConnectionException(errno);
		}
#ifdef BUFFER_DEBUG
		DEBUG("sockatmark test (%d, %d, %d)\n", p.revents, POLLIN, POLLPRI);
#endif
		if ((p.revents & POLLPRI) && sockatmark(sock)) {
			DEBUG("(1)End of file received, %d bytes read\n", total);
			uint8_t status;
			recvn(sock, &status, sizeof(status), 0);
			// File received
			DEBUG("status: %x\n", status);
			if (status != 0) {
				DEBUG("Corrupted data received for the file %s\n", filename);
				register_error(STATUS_UNICAST_CONNECTION_ERROR,
					"Corrupted data received for the file %s", filename);
				throw BrokenInputException();
			} else {
				// All ok
				return;
			}
		}
		count = read(sock, buff->rposition(), count);
#ifdef BUFFER_DEBUG
		DEBUG("%d bytes of data read\n", count);
#endif
		if (count > 0) {
			// Update the checksum
			buff->checksum.update((unsigned char *)buff->rposition(), count);
			buff->update_reader_position(count);
			total += count;
		} else if(count == 0) {
			// End of file
			DEBUG("Unexpected end of transmission for the file %s\n", filename);
			register_error(STATUS_UNICAST_CONNECTION_ERROR,
				"Unexpected end of transmission for the file %s", filename);
			throw BrokenInputException();
		} else {
			// An error occurred
#if 0
			DEBUG("Socket read error on %s: %s\n", filename, strerror(errno));
			register_error(STATUS_UNICAST_CONNECTION_ERROR,
				"Socket read error on %s: %s", filename, strerror(errno));
#endif
			throw ConnectionException(errno);
		}
	}
}

// Reads the file 'filename' and pass it to the distributor
void Reader::handle_file(const char *filename, struct stat *statp,
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
			throw BrokenInputException();
		} else {
			return;
		}
	}

	// Start the file transfert operation for the distributor
	int filename_length = strlen(filename + basename_offset);
	FileInfoHeader f_info = {0, resource_is_a_file,
		statp->st_mode & ~S_IFMT, filename_length};
	Distributor::TaskHeader op = {f_info, filename + basename_offset};
	buff->add_task(op);

	// Read file from the disk
	int read_result;
	read_result = read_from_file(fd);
	if (read_result != 0) {
		DEBUG("Read error for the file %s: %s\n", filename, strerror(errno));
		register_error(STATUS_DISK_ERROR, "Read error for the file %s: %s\n",
			filename, strerror(errno));
		throw BrokenInputException();
	}
	close(fd);
	uint8_t status = buff->finish_task();

	// Check the result of writers
	if (status == STATUS_OK) {
		return;
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
			throw BrokenInputException();
		}
	} else {
		// One of the writers finished with a fatal error
		buff->finish_work();
		throw BrokenInputException();
	}
}

// Reads information about the directory 'dirname' and pass it to
// the distributor
void Reader::handle_directory(char *dirname, struct stat *statp,
		int rootdir_basename_offset) {
	int dirname_length = strlen(dirname + rootdir_basename_offset);
	FileInfoHeader d_info = {0, resource_is_a_directory,
		statp->st_mode & ~S_IFMT, dirname_length};
	Distributor::TaskHeader op = {d_info, dirname + rootdir_basename_offset};
	buff->add_task(op);
	if (buff->finish_task() >= STATUS_FIRST_FATAL_ERROR) {
		buff->finish_work();
		throw BrokenInputException();
	}
}

// Implements recursive behavior for the directory 'name'.
// Uses the handle_file and handle_directory functions.
void Reader::handle_directory_with_content(char *name,
		struct stat *statp) {
	DIR *dirp;
	struct dirent *dp;

	// Assume that there will be no slashes at the end of name
	int rootdir_basename_offset;
	char *base = strrchr(name, '/');
	if (base != NULL)  {
		rootdir_basename_offset = base - name + 1;
	} else {
		rootdir_basename_offset = 0;
	}

	struct stat tstat;

	// FIXME: The level of subdirectories should be limited to handle with 
	// loops in symlinks.
	stack<char *> dirs;

	// Condition for the case if someone wish to copy the root directory
	if (*(name + rootdir_basename_offset) != '\0') {
		DEBUG("Send directory: %s", name + rootdir_basename_offset);
		handle_directory(name, statp, rootdir_basename_offset);
		// to count one more slash
	} else {
		DEBUG("Send the '/' directory (nsources == %d)", nsources);
		// FIXME: need to figure out something for command ./mcp / img000:y
		// skip additional shash beween the root directory and its subdirectory
		++rootdir_basename_offset;
	}
	dirs.push(strdup(name));

	while (dirs.size() > 0) {
		// Get the next unprocessed directory
		char * dirname = dirs.top();
		dirs.pop();
		if ((dirp = opendir(dirname)) == NULL) {
			// Errors are allowed here (EACCES) for example, but these
			// errors should be reported about.
			ERROR("%s: %s", dirname, strerror(errno));
			continue;
		}

		while ((dp = readdir(dirp)) != NULL) {
			if (dp->d_ino == 0)
				continue;
			if (!strcmp(dp->d_name, ".") || !strcmp(dp->d_name, ".."))
				continue;
			int path_size = strlen(dirname) + 1 + strlen(dp->d_name) + 1;
			if (strlen(dirname) + 1 + strlen(dp->d_name) > MAXPATHLEN) {
				ERROR("%s/%s: name is too long", dirname, dp->d_name);
				continue;
			}

			char path[path_size];
			sprintf(path, "%s/%s", dirname, dp->d_name);

			if (stat(path, &tstat) != 0) {
				ERROR("Can't get attributes for file/directory %s: %s\n", path,
					strerror(errno));
				continue;
			}
			if (S_ISDIR(tstat.st_mode)) {
				DEBUG("Send directory: %s", path + rootdir_basename_offset);
				handle_directory(path, statp, rootdir_basename_offset);
				// add directory to the stack
				dirs.push(strdup(path));
			} else if (S_ISREG(tstat.st_mode)) {
				DEBUG("Send file: %s", path + rootdir_basename_offset);
				handle_file(path, &tstat, rootdir_basename_offset, false);
			} else {
				// Skip the object if it is not a regular file or directory
				ERROR("Error: %s is not a regular file\n", path);
			}
		}
		free(dirname);
		closedir(dirp);
	}
}

// Gets the initial record from the immediate source
int Reader::get_initial(MD5sum *checksum) throw (ConnectionException) {
	// Get the number of sources
	UnicastSessionHeader ush;
	recvn(sock, &ush, sizeof(ush), 0);
	checksum->update(&ush, sizeof(ush));
	ush.ntoh();
	nsources = ush.nsources;
	register int path_len = ush.path_length;

	if (path_len > MAXPATHLEN) {
		throw ConnectionException(ConnectionException::corrupted_data_received);
	}

	DEBUG("number of sources: %d, path length: %d\n", nsources, path_len);
	if (path != NULL) {
		free(path);
	}
	path = (char *)malloc(path_len + 1);
	path[path_len] = 0;
	if (path_len > 0) {
		recvn(sock, path, path_len, 0);
		checksum->update(path, path_len);
		DEBUG("destination: %s\n", path);
	}

	// Detect the type of the path specified
	if (path_len == 0) {
		// The path is not specified
		path_type = path_is_default_directory;
	} else {
		struct stat s;
		int stat_result = stat(path, &s);
		if (stat_result == 0) {
			DEBUG("Path %s exists\n", path);
			if (S_ISDIR(s.st_mode)) {
				path_type = path_is_directory;
			} else if (S_ISREG(s.st_mode)) {
				if (nsources > 1) {
					SDEBUG("Multiple sources specified, the path can't be a file\n");
					register_error(STATUS_DISK_ERROR,
						"Multiple sources specified, the path can't be a file\n");
					return -1;
				}
				path_type = path_is_regular_file;
			} else {
				DEBUG("Incorrect path specified\n");
				register_error(STATUS_DISK_ERROR,
					"Multiple sources specified, the path can't be a file\n");
				return -1;
			}
		} else if(errno == ENOENT) {
			if (nsources > 1) {
				// Create the directory with the name 'path'
				if (mkdir(path, S_IRWXU | S_IRGRP | S_IXGRP |
						S_IROTH | S_IXOTH) != 0) {
					DEBUG("Can't create directory %s: %s\n", path, strerror(errno));
					register_error(STATUS_DISK_ERROR, "Can't create directory %s: %s\n",
						path, strerror(errno));
					return -1;
				}
				path_type = path_is_directory;
			} else {
				path_type = path_is_nonexisting_object;
			}
		} else {
			DEBUG("Incorrect path: %s\n", path);
			register_error(STATUS_DISK_ERROR, "Incorrect path: %s\n", path);
			return -1;
		}
	}
	DEBUG("SocketReader::get_initial: The specified path is : %d\n", path_type);
	return 0;
}

// gets the destinations from the immediate source
void Reader::get_destinations(MD5sum *checksum) {
	uint32_t record_header[2]; // addr, name len
	dst.clear();
	while(1) {
		recvn(sock, record_header, sizeof(record_header), 0);
		checksum->update(record_header, sizeof(record_header));
		// FIXME: rewrite using the DestinationHeader structure
		record_header[0] = ntohl(record_header[0]);
		record_header[1] = ntohl(record_header[1]);
		if (record_header[0] != 0) {
			DEBUG("addr: %x, length: %x\n", record_header[0], record_header[1]);
			if (record_header[1] >= MAXPATHLEN) {
				throw ConnectionException(ConnectionException::corrupted_data_received);
			}

			char *location = (char *)malloc(record_header[1] + 1);
			location[record_header[1]] = 0;
			if (record_header[1] > 0) {
				// Get the server's address
				recvn(sock, location, record_header[1], 0);
				checksum->update(location, record_header[1]);
			}
			// Get the destinations' list. Possibly memory leak here, check this
			dst.push_back(Destination(record_header[0],
				record_header[1] == 0 ? NULL : location));
		} else {
			// All the destinations are read
#ifndef NDEBUG
			SDEBUG("The destinations received: \n");
			for (std::vector<Destination>::const_iterator i = dst.begin();
					i != dst.end(); ++i) {
				printf("%d.", (*i).addr >> 24);
				printf("%d.", (*i).addr >> 16 & 0xFF);
				printf("%d.", (*i).addr >> 8 & 0xFF);
				printf("%d: ", (*i).addr & 0xFF);
				if (i->filename != NULL) {
					printf("%s\n", &*(i->filename));
				} else {
					printf("\n");
				}
			}
#endif
			return;
		}
	}
}

// Establishes the network session from the TCP connection 'sock'
int Reader::session_init(int s) {
	sock = s;
	// Set the SO_OOBINLINE option, to determine the end of file
	// by the TCP URGENT pointer it should be done before
	// any OOB data can be received.
	int on = 1;
	if (setsockopt(sock, SOL_SOCKET, SO_OOBINLINE, &on, sizeof(on)) != 0) {
		DEBUG("Can't execute setsockopt call: %s\n", strerror(errno));
		register_error(STATUS_UNKNOWN_ERROR,
			"Can't execute setsockopt call: %s\n", strerror(errno));
		return -1;
	}

	try {
		// Get the session initialization record
		MD5sum checksum;
		repeat_session_initalization:
		if (get_initial(&checksum) != 0) {
			// The error has been already registered
			return -1;
		}
		// Get the destinations for the files
		get_destinations(&checksum);
		// Get the checksum and compare it with the calculated one
		checksum.final();
#ifndef NDEBUG
		SDEBUG("Calculated checksum: ");
		MD5sum::display_signature(stdout, checksum.signature);
		printf("\n");
#endif
		uint8_t received_checksum[sizeof(checksum.signature)];
		recvn(sock, received_checksum, sizeof(received_checksum), 0);
#ifndef NDEBUG
		SDEBUG("Received checksum:   ");
		MD5sum::display_signature(stdout, received_checksum);
		printf("\n");
#endif
		if (memcmp(checksum.signature, received_checksum,
				sizeof(checksum.signature)) != 0) {
			SERROR("Session inialization request with incorrect "
				"checksum received\n");
			// TODO: Read all the available data to synchronize connection
			// Send the retransmit request message
			send_retransmission_request(sock);
			goto repeat_session_initalization;
		} else {
			// Conform the session initialization (for this particula hop)
			SDEBUG("Session established\n");
			send_normal_conformation(sock);
		}
	} catch (std::exception& e) {
		DEBUG("Network error during session initialization: %s\n", e.what());
		register_error(STATUS_UNICAST_INIT_ERROR,
			"Network error during session initialization: %s\n", e.what());
		return -1;
	}
	return 0;
}

/*
	The main routine of the reader, that works with a unicast session.
	It reads files and directories and transmits them to the distributor
*/
int Reader::session() {
	while (1) {
		FileInfoHeader finfo;
		try {
			recvn(sock, &finfo, sizeof(finfo), 0);
			finfo.ntoh();
			if (finfo.is_trailing_record()) {
				SDEBUG("End of the transmission\n");
				if (buff->finish_work() >= STATUS_FIRST_FATAL_ERROR) {
					// A fatal error occurred
					return -1;
				} else {
					// FIXME: What to do with the retransmission request in this case
					send_normal_conformation(sock);
					return 0;
				}
			}
			if (finfo.name_length >= MAXPATHLEN) {
				throw ConnectionException(ConnectionException::corrupted_data_received);
			}
	
			// Read the file name
			char fname[finfo.name_length + 1];
			recvn(sock, fname, finfo.name_length, 0);
			fname[finfo.name_length] = 0;
	
			int retries = 0;
			if (finfo.type == resource_is_a_file) {
				DEBUG("File: %s(%s) (%o)\n", path, fname, finfo.mode);
	
				Distributor::TaskHeader op = {finfo, fname};
				// Add task for the senders (after buffer reinitialization);
				buff->add_task(op);
	
				read_from_socket(sock, fname);
				buff->checksum.final();
	
				if (buff->reader.status == STATUS_OK) {
					// All ok, get checksum for the received file and check it
					uint8_t signature[sizeof(buff->checksum.signature)];
	
					recvn(sock, signature, sizeof(signature), 0);
	#ifndef NDEBUG
					DEBUG("Calculated checksum(%u): ", (unsigned)sizeof(signature));
					MD5sum::display_signature(stdout, signature);
					printf("\n");
					DEBUG("Received checksum(%u)  : ",
						(unsigned)sizeof(buff->checksum.signature));
					MD5sum::display_signature(stdout, buff->checksum.signature);
					printf("\n");
	#endif
				
					if (memcmp(signature, buff->checksum.signature, sizeof(signature))
							!= 0) {
						SERROR("Received checksum differs from the calculated one\n");
						buff->reader.status = STATUS_INCORRECT_CHECKSUM;
					}
				} else {
					// An error during trasmission, close the session
					send_error(sock, buff->reader.status,
						buff->reader.addr, buff->reader.message_length,
						buff->reader.message);
					DEBUG("Reader finished with error: %s\n",
						buff->reader.message);
					buff->finish_task();
					buff->finish_work();
					return -1;
				}
			} else {
				DEBUG("Directory: %s(%s) (%o)\n", path, fname, finfo.mode);
				// Add task for the senders
				Distributor::TaskHeader op = {finfo, fname};
				buff->add_task(op);
				// TODO: Add checksum or something like this to the fileinfo header
				// No more actions are done for the directory
			}
	
			// The data has to be written, before we send the the reply.
			buff->wait_for_filewriter();
			if (buff->file_writer.is_present &&
					buff->file_writer.status >= STATUS_FIRST_FATAL_ERROR) {
				// File writer finished with an error. This is the end.
				DEBUG("File writer finished with error: %s\n",
					buff->file_writer.message);
				send_error(sock, buff->file_writer.status, buff->file_writer.addr,
					buff->file_writer.message_length, buff->file_writer.message);
				buff->finish_task();
				buff->finish_work();
				return -1;
			}

			uint8_t status = buff->reader.status;
			// Send the reply to the file sender
			sendn(sock, &status, sizeof(status), 0);

			status = buff->finish_task();

			if (status == STATUS_INCORRECT_CHECKSUM) {
				assert(finfo.type != resource_is_a_file);
				// Retransmit the previous file from the disk
				DEBUG("Retransmission %d for the  file %s\n",
					MAX_DATA_RETRANSMISSIONS - retries, fname);

				// Get name of the name written to disk
				const char *filename = FileWriter::get_targetfile_name(fname, path,
					path_type);

				struct stat fs;
				if (stat(filename, &fs) != 0) {
					DEBUG("Can't open the file %s for retransmission: %s\n", filename,
						strerror(errno));
					register_error(STATUS_DISK_ERROR, 
						"Can't open the file %s for retransmission: %s", filename,
						strerror(errno));
					buff->finish_task();
					buff->finish_work();
					return -1;
				}

				// Retransmit the file using the local copy on disk
				handle_file(filename, &fs, 0, true);

				FileWriter::free_targetfile_name(filename, path_type);
			} else if (status != STATUS_OK) {
				buff->finish_work();
				throw BrokenInputException();
			}
		} catch(ConnectionException& e) {
			DEBUG("Network error during transmission: %s\n", e.what());
			register_error(STATUS_UNICAST_CONNECTION_ERROR,
				"Network error during transmission: %s\n", e.what());
			return -1;
		} catch(BrokenInputException& e) {
			return -1;
		}
	}
}
