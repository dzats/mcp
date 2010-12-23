#ifndef FILE_WRITER_H_HEADER
#define FILE_WRITER_H_HEADER 1

#include "destination.h"
#include "distributor.h"
#include "connection.h"
#include "reader.h" // for Reader::PathType

// Class that writes files and directories to the disk
class FileWriter : public Distributor::Writer {
	char *path;
	Reader::PathType path_type;

	// Prohibit coping for objects of this class
	FileWriter(const FileWriter&);
	FileWriter& operator=(const FileWriter&);
public:
	FileWriter(Distributor* b) : Distributor::Writer(b,
			(Distributor::Client *)&b->file_writer), path(NULL) {}
	
	// Initialization routine of the file writer class
	void init(char* p, Reader::PathType pt) {
		path = p;
		path_type = pt;
	}

	// The main routine of the FileWriter class. It reads files and
	// directories from the distributor and writes them to disk.
	int session();

	// Returns the name of the target file
	static const char *get_targetfile_name(const char *source_name,
		const char *path, Reader::PathType path_type);
	// Release the name returned by the get_targetfile_name function
	static void free_targetfile_name(const char *filename,
		Reader::PathType path_type);
	// Returns the name of the target directory. path_type
	// is a value-result argument
	static const char *get_targetdir_name(const char *source_name,
		const char *path, Reader::PathType *path_type);
	// Release the name returned by the get_targetdir_name function
	static void free_targetdir_name(const char *dirname,
		Reader::PathType path_type);
private:
	// Register an input/ouput error and finish the current task
	void register_input_output_error(const char *fmt,
		const char *filename, const char *error);
};
#endif
