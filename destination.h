#ifndef DESTINATION_H_HEADER
#define DESTINATION_H_HEADER 1
#include <inttypes.h>
#define HAS_TR1_MEMORY
#ifdef HAS_TR1_MEMORY
#include <tr1/memory>
#endif

// Structrure describing a destination (host to send files to and location
// on this host)
struct Destination
{
	uint32_t addr;
#ifdef HAS_TR1_MEMORY
	std::tr1::shared_ptr<char> filename;
#else
	char *filename;
#endif
	Destination(int s, char *f) : addr(s), filename(f) {}
	Destination(const Destination& src)
	{
		addr = src.addr;
		filename = src.filename;
	}
	Destination& operator=(const Destination& src)
	{
		if (this != &src) {
			addr = src.addr;
			filename = src.filename;
		}
		return *this;
	}
	bool operator<(const Destination& arg) const
	{
		return addr < arg.addr;
	}
};
#endif
