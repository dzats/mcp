#ifndef FILE_READER_H_HEADER
#define FILE_READER_H_HEADER 1

#include <sys/param.h> // for MAXPATHLEN

#include <exception>
#include <vector>

#include "distributor.h"
#include "reader.h"

class BrokenInputException : public std::exception {};

// class that reads sources from disk or from the unicast network connection
class FileReader : public Reader {

	// Prohibit coping for objects of this class
	FileReader(const FileReader&);
	FileReader& operator=(const FileReader&);
public:

	/*
		The main routine of the mcp programm, it reads sources from
		the disk and passes them to the distributor. Returns 0
		on suceess and something else otherwise (error can be
		detected by the distributor's state).
	*/
	int read_sources(char **filenames);

	FileReader() {}
private:

	// Reads information about the directory 'dirname' and pass it to
	// the distributor
	int handle_directory(char *dirname, struct stat *statp,
			int rootdir_basename_offset);

	// Implements recursive behavior for the directory 'name'.
	// Uses the handle_file and handle_directory functions.
	int handle_directory_with_content(char *name, struct stat *statp);
};
#endif
