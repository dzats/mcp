#ifndef READER_H_HEADER
#define READER_H_HEADER 1

#include <sys/param.h> // for MAXPATHLEN

#include <exception>
#include <vector>

#include "distributor.h"

class BrokenInputException : public std::exception {};

// class that reads sources from disk or from the unicast network connection
class Reader {
	Distributor* buff;
	int sock;

	// Prohibit coping for objects of this class
	Reader(const Reader&);
	Reader& operator=(const Reader&);
public:
	std::vector<Destination> dst;
	int nsources;
	char *path;
	enum PathType {path_is_default_directory,
		path_is_directory,
		path_is_substituted_directory, // for example mcp /etc ...:1
		path_is_regular_file,
		path_is_nonexisting_object} path_type;

	static const int MAX_DATA_RETRANSMISSIONS = 3;
	static const int MAX_ERROR_MESSAGE_SIZE = MAXPATHLEN + 160;

	// Structure that is passed to the Reader thread routine
	struct ThreadArgs {
		Reader *source_reader;
		char **filenames;
	};

	Reader(Distributor* b) : buff(b), sock(-1), path(NULL) {}
	~Reader() {
		if (path != NULL) {
			free(path);
		}
	}
	/*
		The main routine of the mcp programm, it reads sources from
		the disk and passes them to the distributor. Returns 0
		on suceess and something else otherwise (error can be
		detected by the distributor's state).
	*/
	int read_sources(char **filenames);
	
	// Establishes the network session from the TCP connection 'sock'
	int session_init(int sock);
	/*
		The main routine of the reader, that works with a unicast session.
		It reads files and directories and transmits them to the distributor
	*/
	int session();
private:

	// Registers an error and finish the current task
	void register_error(uint8_t status, const char *fmt, ...);

	// Reads the file 'filename' and pass it to the distributor
	void handle_file(const char *filename, struct stat *statp,
			int basename_offset, bool error_if_cant_open);

	// Reads information about the directory 'dirname' and pass it to
	// the distributor
	void handle_directory(char *dirname, struct stat *statp,
			int rootdir_basename_offset);

	// Implements recursive behavior for the directory 'name'.
	// Uses the handle_file and handle_directory functions.
	void handle_directory_with_content(char *name, struct stat *statp);

	// Gets the initial record from the immediate source
	int get_initial(MD5sum *checksum) throw (ConnectionException);

	// Gets the destinations from the immediate source
	void get_destinations(MD5sum *checksum);

	// Reads data from 'fd' and pass it to the distributor
	int read_from_file(int fd);

	// Reads file from the socket 'fd' and pass it to the distributor
	void read_from_socket(int fd, const char *filename);
};
#endif
