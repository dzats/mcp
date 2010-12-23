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
			(Distributor::Client *)&b->file_writer) {}
	
	void init(char* p, Reader::PathType pt) {
		path = p;
		path_type = pt;
	}
	int session();
};
#endif
