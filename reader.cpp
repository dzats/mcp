#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/param.h> // for MAXPATHLEN, but don't sure that this is right
#include <poll.h> // for poll

#include <fcntl.h>
#include <dirent.h>
#include <libgen.h>

#include <stack>

#include "reader.h"
#include "log.h"

using namespace std;

int Reader::read_sources(char **filenames) {
	while(*filenames != NULL) {
		// Detect whether it is a file or directory
		struct stat fs;

		if (stat(*filenames, &fs) != 0) {
			ERROR("Can't open the file %s: %s\n", *filenames,
				strerror(errno));
			return -1;
		}

		if (S_ISDIR(fs.st_mode)) {
			// Recursive behrior for directories
			if (handle_directory_with_content(*filenames, &fs) != 0) {
				ERROR("Can't copy the %s directory\n", *filenames);
				exit(EXIT_FAILURE);
			}
		} else if (S_ISREG(fs.st_mode)) {
			// Assume that there will be no slashes at the end of *filenames
			char *basename;
			int basename_offset;
			if ((basename = strrchr(*filenames, '/')) != NULL) {
				basename_offset = basename - *filenames + 1;
			} else {
				basename_offset = 0;
			}
			if (handle_file(*filenames, &fs, basename_offset) != 0) {
				SERROR("Error while file read\n");
				exit(EXIT_FAILURE);
			}
		} else {
			ERROR("%s is not a regular file or directory\n", *filenames);
			exit(EXIT_FAILURE);
		}
		++filenames;
	}

	SDEBUG("All sources read, finish the task\n");
	// finish the task
	buff->finish_work();
	SDEBUG("Task finished\n");

	return 0;
}

int Reader::read_from_file(int fd) {
	// Remove total
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
			DEBUG("read_file: %lu bytes read\n", total);
			buff->checksum.final();
			return 0;
		} else {
			return errno;
		}
	}
}

int Reader::read_from_socket(int fd) {
	struct pollfd p;
	memset(&p, 0, sizeof(p));
	p.fd = sock;
	p.events = POLLIN | POLLPRI;

	// FIXME: remove total
	int total = 0;
	while(1) {
		int count = buff->get_space();
		count = std::min(count, 4096); // for speed up

#ifdef BUFFER_DEBUG
		DEBUG("Free space in the buffer: %d bytes\n", count);
#endif
		// We need the poll call in the case of the first byte of
		// the received data is out-of-band
		if (poll(&p, 1, -1) <= 0) {
			perror("poll error");
			abort();
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
			return -status;
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
			SDEBUG("Unexpected end of file\n");
			return -1;
		} else {
			// FIXME: Behavior here should be a bit less fatal
			DEBUG("Input/Output error: %s", strerror(errno));
			return errno;
		}
	}
}

int Reader::handle_file(char *filename, struct stat *statp,
		int basename_offset) {
	int retries = MAX_DATA_RETRANSMISSIONS;
	retransmit_file:
	/* Open the input file */
	int fd;
	if ((fd = open(filename, O_RDONLY)) == -1) {
		// Should be non-fatal in the recursive case
		ERROR("Can't open the file %s: %s\n", filename, strerror(errno));
		return -1;
	}

	int filename_length = strlen(filename + basename_offset);
	// FIXME: Filename length should be checked here
	FileInfoHeader f_info = {0, resource_is_a_file,
		statp->st_mode & ~S_IFMT, filename_length};

	// The order is important

	Distributor::TaskHeader op = {f_info, filename + basename_offset};
	buff->add_task(op);

	int read_result;
	read_result = read_from_file(fd);
	if (read_result != 0) {
		ERROR("%s error during read of %s\n", strerror(read_result), filename);
		// This is a fatal error, exit from the program
		// TODO: It worth to send something to the destinations (may be)
		exit(EXIT_FAILURE);
	}
	close(fd);
	uint8_t status = buff->finish_task();

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
			// TODO: Unrecoverable error (too many retransmissions), close the file
			abort();
		}
	} else {
		// TODO Unrecoverable error, close the session
		abort();
	}

#if 0
	if (operation_result != no_errors_occured) {
		// Restart the operation, if possible, otherwise return an error
		if ((operation_result & file_read_error) != 0) {
			// TODO: Think about this situation, in general we should
			// try to fix the connection and send the retransmission request
			abort();
		} else if ((operation_result & file_write_error) != 0) {
			// TODO: send the retransmission request.
		} else if ((operation_result & file_transmission_error) != 0) {
			// TODO: send the file from disk again
		}
	}
#endif
	return 0;
}

int Reader::handle_directory(char *dirname, struct stat *statp,
		int rootdir_basename_offset) {
	int dirname_length = strlen(dirname + rootdir_basename_offset);
	// FIXME: Filename length should be checked here or a bit earlier
	FileInfoHeader d_info = {0, resource_is_a_directory,
		statp->st_mode & ~S_IFMT, dirname_length};
	Distributor::TaskHeader op = {d_info, dirname + rootdir_basename_offset};
	buff->add_task(op);
	// No more actions are done for directories
	buff->finish_task();
	return 0;
}

int Reader::handle_directory_with_content(char *name,
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

	// FIXME: The level of subdirectories should be limited to avoid 
	// loops in symlinks.
	stack<char *> dirs;

	// Condition for the case if someone wish to copy the root directory
	if (*(name + rootdir_basename_offset) != '\0') {
		puts(name + rootdir_basename_offset);
		handle_directory(name, statp, rootdir_basename_offset);
		// to count one more slash
	} else {
		// skip additional shash beween the root directory and its subdirectory
		++rootdir_basename_offset;
	}
	dirs.push(strdup(name));

	while (dirs.size() > 0) {
		// Get the next unprocessed directory
		char * dirname = dirs.top();
		dirs.pop();
		if ((dirp = opendir(dirname)) == NULL) {
			// Some errors are allowed here, such as EACCES (permission denied)
			ERROR("%s: %s", dirname, strerror(errno));
			continue;
		}

		while ((dp = readdir(dirp)) != NULL) {
			if (dp->d_ino == 0)
				continue;
			if (!strcmp(dp->d_name, ".") || !strcmp(dp->d_name, ".."))
				continue;
			// FIXME: check for the max path name
			int path_size = strlen(dirname) + 1 + strlen(dp->d_name) + 1;
			// TODO: if (strlen(dirname) + 1 + strlen(dp->d_name) >= ...) {
				//ERROR("%s/%s: name too long", dirname, dp->d_name);
				//continue;
			//}
			char path[path_size];

			sprintf(path, "%s/%s", dirname, dp->d_name);

			if (stat(path, &tstat) != 0) {
				ERROR("Can't get attributes for file/directory %s: %s\n", path,
					strerror(errno));
				continue;
			}
			if (S_ISDIR(tstat.st_mode)) {
				puts(path + rootdir_basename_offset);
				handle_directory(path, statp, rootdir_basename_offset);
				// add directory to the stack
				dirs.push(strdup(path));
			} else if (S_ISREG(tstat.st_mode)) {
				puts(path + rootdir_basename_offset);
				handle_file(path, &tstat, rootdir_basename_offset);
			} else {
				ERROR("Error: %s is not a regular file\n", path);
				// TODO: this error should be accouted somehow
			}
		}
		free(dirname);
		closedir(dirp);
	}
	return 0;
}

void Reader::get_initial(MD5sum *checksum) {
	// Get the number of sources
	UnicastSessionHeader ush;
	recvn(sock, &ush, sizeof(ush), 0);
	checksum->update(&ush, sizeof(ush));
	ush.ntoh();
	nsources = ush.nsources;
	register int path_len = ush.path_length;

	assert(path_len <= MAXPATHLEN);

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
					fprintf(stderr, "Multiple sources specified, "
						"path shoud be a directory\n");
					abort();
				}
				path_type = path_is_regular_file;
			} else {
				fprintf(stderr, "Incorrect path specified\n");
				abort();
			}
		} else if(errno == ENOENT) {
			if (nsources > 1) {
				// Create the directory with the name 'path'
				if (mkdir(path, S_IRWXU | S_IRGRP | S_IXGRP |
						S_IROTH | S_IXOTH) != 0) {
					fprintf(stderr, "Can't create directory %s: %s\n", path,
						strerror(errno));
					abort();
				}
				path_type = path_is_directory;
			} else {
				path_type = path_is_nonexisting_object;
			}
		} else {
			fprintf(stderr, "Incorrect path: %s\n", path);
			abort();
		}
	}
	DEBUG("SocketReader::get_initial: The specified path is : %d\n", path_type);
}

void Reader::get_destinations(MD5sum *checksum) {
	uint32_t record_header[2]; // addr, name len
	dst.clear();
	while(1) {
		recvn(sock, record_header, sizeof(record_header), 0);
		checksum->update(record_header, sizeof(record_header));
		// FIXME: something more readable required here
		record_header[0] = ntohl(record_header[0]);
		record_header[1] = ntohl(record_header[1]);
		if (record_header[0] != 0) {
			DEBUG("addr: %x, length: %x\n", record_header[0], record_header[1]);

			assert(record_header[1] <= MAXPATHLEN);

			char *location = (char *)malloc(record_header[1] + 1);
			location[record_header[1]] = 0;
			if (record_header[1] > 0) {
				// Get the server's address
				recvn(sock, location, record_header[1], 0);
				checksum->update(location, record_header[1]);
			}
			// Get the destinations' list
			// Possibly memory leak here, check this
			dst.push_back(Destination(record_header[0],
				record_header[1] == 0 ? NULL : location));
		} else {
			// All the destinations are read
			//sort(dst.begin(), dst.end());
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

int Reader::session_init(int s) {
	sock = s;
	// Set the SO_OOBINLINE option, to determin the end of file
	// by the TCP URGENT pointer it should be done before
	// any OOB data can be received.
	int on = 1;
	if (setsockopt(sock, SOL_SOCKET, SO_OOBINLINE, &on, sizeof(on)) != 0) {
		perror("setsockopt error\n");
		abort();
		return -1;
	}
	int buffsize;
	// This is work around some the FreeBSD bug in the sockatmark function.
	// The buffer size should be less that 65536 bytes.

	buffsize = 64560;
	if (setsockopt(sock, SOL_SOCKET, SO_RCVBUF, &buffsize,
			sizeof(buffsize)) != 0) {
		perror("setsockopt error\n");
		abort();
		return -1;
	}
#ifndef NDEBUG
	socklen_t len = sizeof(buffsize);
	if (getsockopt(sock, SOL_SOCKET, SO_RCVBUF, &buffsize,
			&len) != 0) {
		perror("setsockopt error\n");
		abort();
		return -1;
	}
	DEBUG("socket buffer size: %d\n", buffsize);
#endif
	try {
		// Get the session initialization record
		MD5sum checksum;
		repeat_session_initalization:
		get_initial(&checksum);
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
			SERROR("Session inialization request with incorrect checksum received\n");
			// TODO: Read all the available data to synchronize connection
			// Send the retransmit request message
			send_retransmission_request(sock);
			goto repeat_session_initalization;
		} else {
			// All ok
			// Conform the session initialization (for this particula hop)
			SDEBUG("Session established\n");
		}
	} catch (std::exception& e) {
		ERROR("Network error during session initialization: %s\n", e.what());
		// TODO: send error about the broken connection
		return -1;
	}
	return 0;
}

int Reader::session() {
	while (1) {
		FileInfoHeader finfo;
		try {
			recvn(sock, &finfo, sizeof(finfo), 0);
			finfo.ntoh();
			if (finfo.is_trailing_record()) {
				SDEBUG("End of the transmission\n");
				buff->finish_work();
				return 0;
			}
	
			assert(finfo.name_length < 1100);
	
			// Read the file name
			char fname[finfo.name_length + 1];
			recvn(sock, fname, finfo.name_length, 0);
			fname[finfo.name_length] = 0;
	
			if (finfo.type == resource_is_a_file) {
				int retries = 0;
				DEBUG("File: %s(%s) (%o)\n", path, fname, finfo.mode);
	
				Distributor::TaskHeader op = {finfo, fname};
				// Add task for the senders (after buffer reinitialization);
				buff->add_task(op);
	
				int read_result;
				read_result = read_from_socket(sock);
				buff->checksum.final();
	
				// Work with the reply status
				uint8_t reply = STATUS_OK;
				if (read_result == STATUS_OK) {
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
						fprintf(stderr, "\n");
						reply = STATUS_INCORRECT_CHECKSUM;
					}
				} else {
					// An error during trasmission, close the session
					if (read_result > 0) {
						ERROR("%s error during read of %s\n", strerror(read_result),
							fname);
					} else {
						ERROR("Unknown error during read of %s\n", fname);
					}
					// This is a fatal error, exit from the program
					// TODO: It worth to send something to the destinations (may be)
					exit(EXIT_FAILURE);
				}
	
				// The data has to be written, before we send the the reply.
				buff->wait_for_filewriter();
				if (buff->file_writer.is_present &&
						buff->file_writer.status != STATUS_OK) {
					// TODO: do fatal error, but a bit less fatal
					abort();
				}
	
				// Send the reply to the file sender
				sendn(sock, &reply, sizeof(reply), 0);
	
				uint8_t status = buff->finish_task();
	
				if (status == STATUS_INCORRECT_CHECKSUM) {
					// Retransmit the file
					DEBUG("Retransmit file %s in the %d time\n", fname,
						MAX_DATA_RETRANSMISSIONS - retries);
	
					// Figure out the file name
					// FIXME: This piece of code coincides with the one from file_writer.cc
					char *filename;
					if (path_type == Reader::path_is_default_directory) {
							filename = const_cast<char *>(fname);
					} else if (path_type == Reader::path_is_directory) {
							filename = (char *)malloc(strlen(path) + 1 /* slash */ +
								strlen(fname) + 1);
							sprintf(filename, "%s/%s", path, fname);
					} else if (path_type == Reader::path_is_regular_file ||
							path_type == Reader::path_is_nonexisting_object) {
						filename = path;
					} else if (path_type == Reader::path_is_subtsituted_directory) {
						int pathlen = strlen(path);
						filename = (char *)malloc(pathlen + 1 /* slash */ +
							strlen(fname) + 1);
						memcpy(filename, path, pathlen);
						char *slash = strchr(fname, '/');
						assert(slash != NULL);
						strcpy(filename + pathlen, slash);
					} else {
						abort();
					}
	
					struct stat fs;
					if (stat(filename, &fs) != 0) {
						ERROR("Can't open the file %s: %s\n", filename,
							strerror(errno));
						abort();
					}
	
					handle_file(filename, &fs, 0);
					if (path_type == Reader::path_is_directory ||
							path_type == Reader::path_is_subtsituted_directory) {
						free(filename);
					}
				} else if (status != STATUS_OK) {
					// TODO Unrecoverable error, close the session
					abort();
				}
				SDEBUG("File read\n");
			} else {
				DEBUG("Directory: %s(%s) (%o)\n", path, fname, finfo.mode);
				// The file_writer will create it
	
				// Add task for the senders (after buffer reinitialization);
				Distributor::TaskHeader op = {finfo, fname};
				buff->add_task(op);
				// No more actions are done for the directory
				buff->finish_task();
			}
		} catch(std::exception& e) {
			ERROR("Network error during transmission: %s\n", e.what());
			return -1;
		}
	}
}
