#ifndef READER_H_HEADER
#define READER_H_HEADER 1

#include "distributor.h"

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
		path_is_subtsituted_directory, // for example mcp /etc ...:1
		path_is_regular_file,
		path_is_nonexisting_object} path_type;

	static const int MAX_DATA_RETRANSMISSIONS = 3;

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
	int read_sources(char **filenames); // read sources from the disk
	
	int session_init(int sock); // establish network session
	int session(); // read sources from the network session

private:
	int handle_file(char *filename, struct stat *statp,
			int basename_offset); // reads the file 'filename' and pass it to
			// the distributor
	int handle_directory(char *dirname, struct stat *statp,
			int rootdir_basename_offset); // reads the directory 'dirname' and
			// pass it to the distributor
	int handle_directory_with_content(char *name,
			struct stat *statp); // implements recursive behavior for the directory
			// name, uses the handle_file and handle_directory functions

	void get_initial(MD5sum *checksum); // get the initial record
			// from the connection session
	void get_destinations(MD5sum *checksum); // get the destinations
			// from the connection session

	int read_from_file(int fd); // read data from 'fd' and pass it
		// to the distributor
	int read_from_socket(int fd); // reads data from the socket 'fd'
		// and pass it to the distributor
};
#endif
