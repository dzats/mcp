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

#include "file_reader.h"
#include "file_writer.h" // for FileWriter::get_targetfile_name
#include "log.h"

using namespace std;

/*
	The main routine of the mcp programm, it reads sources from
	the disk and passes them to the distributor. Returns 0
	on suceess and something else otherwise (error can be
	detected by the distributor's state).
*/
int FileReader::read_sources(char **filenames, int *filename_offsets)
{
	int i = 0;
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
			if (handle_directory_with_content(*filenames, &fs) != 0) {
				return -1;
			}
		} else if (S_ISREG(fs.st_mode)) {
			// *filenames should be already fixed up by the 
			// prepare_to_basename routine (mcp.cpp)
			char *basename;
			int basename_offset = filename_offsets[i];
			if (basename_offset == 0 &&
					(basename = strrchr(*filenames, '/')) != NULL) {
				basename_offset = basename - *filenames + 1;
			}
			if (handle_file(*filenames, &fs, basename_offset, true) != 0) {
				return -1;
			}
		} else {
			ERROR("%s is not a regular file or directory\n", *filenames);
			return -1;
		}
		++filenames;
		++i;
	}

	SDEBUG("All sources read, finish the task\n");
	return finish_work();
}

// Reads data from fd (till the end of file) and passes it to
// the distributor. Returns 0 on success and errno on failure.
int FileReader::read_from_file(int fd, off_t size)
{
	while(size > 0) {
		unsigned count = get_space();
		count = std::min(count, (unsigned)4096);
		if (count > size) {
			count = size;
		}
	
#ifdef BUFFER_DEBUG
		DEBUG("Free space in the buffer: %d bytes\n", count);
#endif
		count = read(fd, rposition(), count);
		if (count > 0) {
			// Update the checksum
			checksum.update((unsigned char *)rposition(), count);
			update_reader_position(count);
			size -= count;
#ifdef BUFFER_DEBUG
			DEBUG("%d (%lu) bytes of data read\n", count, size);
#endif
		} else if(count == 0) {
			// End of file encountered
			DEBUG("read_file: %zu bytes have not been read\n", (size_t)size);
			checksum.final();
			return 0;
		} else {
			return errno;
		}
	}

	// File has been read
	SDEBUG("file has been completely read\n");
	checksum.final();
	return 0;
}

// Reads the file 'filename' and pass it to the distributor
int FileReader::handle_file(const char *filename, struct stat *statp,
		int basename_offset, bool error_if_cant_open)
{
	/* Open the input file */
	int fd;
	fd = open(filename, O_RDONLY);
	if (fd == -1) {
		// Should be non-fatal in the recursive case
		DEBUG("Can't open the file %s: %s\n", filename, strerror(errno));
		if (error_if_cant_open) {
			DEBUG("Can't open the file %s: %s\n", filename, strerror(errno));
			register_error(STATUS_FATAL_DISK_ERROR,
				"Can't open the file %s: %s\n", filename, strerror(errno));
			return -1;
		} else {
			ERROR("Can't open the file %s: %s\n", filename, strerror(errno));
			return 0;
		}
	}

	// Start the file transfert operation for the distributor
	FileInfoHeader f_info(resource_is_a_file,
		statp->st_mode & ~S_IFMT, strlen(filename), basename_offset,
		statp->st_size);
	add_task(f_info, filename);

	// Read file from the disk
	int read_result;
	read_result = read_from_file(fd, statp->st_size);
	if (read_result != 0) {
		DEBUG("Read error for the file %s: %s\n", filename, strerror(errno));
		register_error(STATUS_FATAL_DISK_ERROR, "Read error for the file %s: %s\n",
			filename, strerror(errno));
		return -1;
	}
	close(fd);
	uint8_t status = finish_task();

	// Check the result of writers
	if (status >= STATUS_FIRST_FATAL_ERROR) {
		// One of the writers finished with a fatal error
		finish_work();
		return -1;
	} else {
		return 0;
	}
}

// Reads information about the directory 'dirname' and pass it to
// the distributor
int FileReader::handle_directory(char *dirname, struct stat *statp,
		int rootdir_basename_offset)
{
	FileInfoHeader d_info(resource_is_a_directory,
		statp->st_mode & ~S_IFMT, strlen(dirname), rootdir_basename_offset, 0);
	add_task(d_info, dirname);
	if (finish_task() >= STATUS_FIRST_FATAL_ERROR) {
		finish_work();
		return -1;
	}
	return 0;
}

// Implements recursive behavior for the directory 'name'.
// Uses the handle_file and handle_directory functions.
int FileReader::handle_directory_with_content(char *name,
		struct stat *statp)
{
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
		DEBUG("Send directory: %s\n", name + rootdir_basename_offset);
		handle_directory(name, statp, rootdir_basename_offset);
		// to count one more slash
	} else {
		SDEBUG("Send the '/' directory\n");
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
			ERROR("%s: %s\n", dirname, strerror(errno));
			continue;
		}

		while ((dp = readdir(dirp)) != NULL) {
			if (dp->d_ino == 0)
				continue;
			if (!strcmp(dp->d_name, ".") || !strcmp(dp->d_name, ".."))
				continue;
			int path_size = strlen(dirname) + 1 + strlen(dp->d_name) + 1;
			if (strlen(dirname) + 1 + strlen(dp->d_name) > MAXPATHLEN) {
				ERROR("%s/%s: name is too long\n", dirname, dp->d_name);
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
				DEBUG("Send directory: %s\n", path + rootdir_basename_offset);
				if (handle_directory(path, statp, rootdir_basename_offset) != 0) {
					return -1;
				}
				// add directory to the stack
				dirs.push(strdup(path));
			} else if (S_ISREG(tstat.st_mode)) {
				DEBUG("Send file: %s\n", path + rootdir_basename_offset);
				if (handle_file(path, &tstat, rootdir_basename_offset, false) != 0) {
					return -1;
				}
			} else {
				// Skip the object if it is not a regular file or directory
				ERROR("Error: %s is not a regular file\n", path);
			}
		}
		free(dirname);
		closedir(dirp);
	}
	return 0;
}
