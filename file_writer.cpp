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

// Returns the name of the target file
const char* FileWriter::get_targetfile_name(const char *source_name,
		const char *path, Reader::PathType path_type) {
	// Figure out the file name
	if (path_type == Reader::path_is_default_directory) {
		return source_name;
	} else if (path_type == Reader::path_is_directory) {
		char *filename = (char *)malloc(strlen(path) + 1 /* slash */ +
			strlen(source_name) + 1);
		sprintf(filename, "%s/%s", path, source_name);
		return filename;
	} else if (path_type == Reader::path_is_regular_file ||
			path_type == Reader::path_is_nonexisting_object) {
		return path;
	} else {
		assert(path_type == Reader::path_is_substituted_directory);
		int pathlen = strlen(path);
		char *filename = (char *)malloc(pathlen + 1 /* slash */ +
			strlen(source_name) + 1);
		memcpy(filename, path, pathlen);
		char *slash = strchr(source_name, '/');
		assert(slash != NULL);
		strcpy(filename + pathlen, slash);
		return filename;
	}
}

// Release the name returned by the get_targetfile_name function
void FileWriter::free_targetfile_name(const char *filename,
		Reader::PathType path_type) {
	if (path_type == Reader::path_is_directory ||
			path_type == Reader::path_is_substituted_directory) {
		free(const_cast<char *>(filename));
	}
}

// Returns the name of the target directory. path_type
// is a value-result argument
const char* FileWriter::get_targetdir_name(const char *source_name,
		const char *path, Reader::PathType *path_type) {
	// Figure out the directory name
	if (*path_type == Reader::path_is_default_directory) {
		return source_name;
	} else if (*path_type == Reader::path_is_directory) {
		char *dirname = (char *)malloc(strlen(path) + 1 /* slash */ +
			strlen(source_name) + 1);
		sprintf(dirname, "%s/%s", path, source_name);
		return dirname;
	} else if (*path_type == Reader::path_is_regular_file) {
		return NULL;
	} else if (*path_type == Reader::path_is_nonexisting_object) {
		*path_type = Reader::path_is_substituted_directory;
		// strdup is required for subsequent free_targetfile_name
		return strdup(path);
	} else {
		assert(*path_type == Reader::path_is_substituted_directory);
		int pathlen = strlen(path);
		char *dirname = (char *)malloc(pathlen + 1 /* slash */ +
			strlen(source_name) + 1);
		memcpy(dirname, path, pathlen);
		char *slash = strchr(source_name, '/');
		assert(slash != NULL);
		strcpy(dirname + pathlen, slash);
		return dirname;
	}
}

// Release the name returned by the get_targetdir_name function
void FileWriter::free_targetdir_name(const char *dirname,
		Reader::PathType path_type) {
	if (path_type == Reader::path_is_directory ||
			path_type == Reader::path_is_substituted_directory) {
		free(const_cast<char *>(dirname));
	}
}

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
		assert(op->fileinfo.name_length <= MAXPATHLEN);
		if (op->fileinfo.type == resource_is_a_file) {
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
				op->fileinfo.mode);
			int error = errno;
			if (fd == -1 && errno == EACCES && unlink(filename) == 0) {
				// A read only file with the same name exists,
				// the default bahavior is to overwrite it.
				fd = open(filename, O_WRONLY | O_CREAT | O_TRUNC,
					op->fileinfo.mode);
			}
			if (fd == -1) {
				// Report about the error
				register_input_output_error("Can't open the output file %s: %s",
					filename, strerror(error));
				close(fd);
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
			if (mkdir(dirname, op->fileinfo.mode) != 0 && errno != EEXIST) {
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
