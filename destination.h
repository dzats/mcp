#ifndef DESTINATION_H_HEADER
#define DESTINATION_H_HEADER 1
#include <inttypes.h>
#include <string>

// Structrure describing a destination (host to send files to and location
// on this host)
struct Destination
{
  uint32_t addr;
  std::string filename;
  Destination(int s, const char *f) : addr(s)
  {
    if (f != NULL) {
      filename.assign(f);
    }
  }
  Destination(const Destination& src)
  {
    addr = src.addr;
    filename.assign(src.filename);
  }
  Destination& operator=(const Destination& src)
  {
    if (this != &src) {
      addr = src.addr;
      filename.assign(src.filename);
    }
    return *this;
  }
  bool operator<(const Destination& second) const
  {
    return addr < second.addr;
  }
  bool operator!=(const Destination& second) const
  {
    return addr != second.addr;
  }
  bool operator==(const Destination& second) const
  {
    return addr == second.addr;
  }
};
#endif
