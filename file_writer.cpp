#include <stdio.h>
#include <fcntl.h>

#include <sys/types.h>
#include <sys/stat.h>

#include "log.h"
#include "file_writer.h"

// The main routine of the FileWriter class. It reads files and directories
// from the distributor and writes them to the disk
int FileWriter::session() {
	assert(path != NULL);
	while (1) {
		// Get an operation from the queue
		Distributor::TaskHeader *op = get_task();
		if (op->fileinfo.is_trailing_record()) {
			break;
		}
		assert(op->fileinfo.name_length < 1024);
		const char *source_name = op->filename.c_str();
		if (op->fileinfo.type == resource_is_a_file) {
#ifndef NDEBUG
			DEBUG("Entry for a file (%s)%s: ", path, op->filename.c_str());
			op->fileinfo.print();
#endif
			// Figure out the file name
			char *filename;
			if (path_type == Reader::path_is_default_directory) {
					filename = const_cast<char *>(source_name);
			} else if (path_type == Reader::path_is_directory) {
					filename = (char *)malloc(strlen(path) + 1 /* slash */ +
						strlen(source_name) + 1);
					sprintf(filename, "%s/%s", path, source_name);
			} else if (path_type == Reader::path_is_regular_file ||
					path_type == Reader::path_is_nonexisting_object) {
				filename = path;
			} else if (path_type == Reader::path_is_subtsituted_directory) {
				int pathlen = strlen(path);
				filename = (char *)malloc(pathlen + 1 /* slash */ +
					strlen(source_name) + 1);
				memcpy(filename, path, pathlen);
				char *slash = strchr(source_name, '/');
				assert(slash != NULL);
				strcpy(filename + pathlen, slash);
			} else {
				abort();
			}

			// Open the output file
			DEBUG("open the file: %s\n", filename);
			int fd;
			do {
				fd = open(filename, O_WRONLY | O_CREAT | O_TRUNC,
					op->fileinfo.mode);
				if (fd == -1) {
					// FIXME: probably check for EPERM is excessive
					if ((errno == EACCES || errno == EPERM) && unlink(filename) == 0) {
						// A read only file with the same name exists,
						// the default bahavior is to overwrite it.
						continue;
					}
					ERROR("Can't open the output file %s: %s\n", filename,
						strerror(errno));
					abort();
				}
			} while(fd == -1 && (errno == EACCES || errno == EPERM));

			// Write the file content into the file
			write_to_file(fd);
	
			close(fd);
			DEBUG("File %s has been written\n", filename);
			if (path_type == Reader::path_is_directory ||
					path_type == Reader::path_is_subtsituted_directory) {
				free(filename);
			}
		} else {
#ifndef NDEBUG
			DEBUG("Entry for a directory %s received: ", source_name);
			op->fileinfo.print();
#endif
			// Figure out the directory name
			char *dirname;
			if (path_type == Reader::path_is_default_directory) {
					dirname = const_cast<char *>(source_name);
			} else if (path_type == Reader::path_is_directory) {
					dirname = (char *)malloc(strlen(path) + 1 /* slash */ +
						strlen(source_name) + 1);
					sprintf(dirname, "%s/%s", path, source_name);
			} else if (path_type == Reader::path_is_regular_file) {
				ERROR("The path %s is a file, but source is a directory", path);
				abort();
			} else if (path_type == Reader::path_is_nonexisting_object) {
				dirname = strdup(path);
				path_type = Reader::path_is_subtsituted_directory;
			} else if (path_type == Reader::path_is_subtsituted_directory) {
				int pathlen = strlen(path);
				dirname = (char *)malloc(pathlen + 1 /* slash */ +
					strlen(source_name) + 1);
				memcpy(dirname, path, pathlen);
				char *slash = strchr(source_name, '/');
				assert(slash != NULL);
				strcpy(dirname + pathlen, slash);
			} else {
				abort();
			}

			// Create the directory
			if (mkdir(dirname, op->fileinfo.mode) != 0 && errno != EEXIST) {
				ERROR("Can't create directory: %s: %s", dirname,
					strerror(errno));
				abort();
			}
			if (path_type == Reader::path_is_directory ||
					path_type == Reader::path_is_subtsituted_directory) {
				free(dirname);
			}
		}

		// TODO: Verify the checksum again, if required
		submit_task();
	}
	return 0;
}
