#ifndef DESTINATION_H_HEADER
#define DESTINATION_H_HEADER 1
#include <inttypes.h>
#include <tr1/memory>

// Structrure describing a destination (host to send files to and location
// on this host)
struct Destination {
	uint32_t addr;
	std::tr1::shared_ptr<char> filename;
	//char *filename;
	Destination(int s, char *f) : addr(s), filename(f) {}
	bool operator<(const Destination& arg) const {
		return addr < arg.addr;
	}
};
#endif
