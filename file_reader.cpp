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
int FileReader::read_sources(char **filenames) {
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
			int basename_offset;
			if ((basename = strrchr(*filenames, '/')) != NULL) {
				basename_offset = basename - *filenames + 1;
			} else {
				basename_offset = 0;
			}
			if (handle_file(*filenames, &fs, basename_offset, true) != 0) {
				return -1;
			}
		} else {
			ERROR("%s is not a regular file or directory\n", *filenames);
			return -1;
		}
		++filenames;
	}

	SDEBUG("All sources read, finish the task\n");
	return finish_work();
}

// Reads information about the directory 'dirname' and pass it to
// the distributor
int FileReader::handle_directory(char *dirname, struct stat *statp,
		int rootdir_basename_offset) {
	int dirname_length = strlen(dirname + rootdir_basename_offset);
	FileInfoHeader d_info(resource_is_a_directory,
		statp->st_mode & ~S_IFMT, dirname_length);
	Distributor::TaskHeader op = {d_info, dirname + rootdir_basename_offset};
	add_task(op);
	if (finish_task() >= STATUS_FIRST_FATAL_ERROR) {
		finish_work();
		return -1;
	}
	return 0;
}

// Implements recursive behavior for the directory 'name'.
// Uses the handle_file and handle_directory functions.
int FileReader::handle_directory_with_content(char *name,
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
