#ifndef READER_H_HEADER
#define READER_H_HEADER 1

#include <sys/param.h> // for MAXPATHLEN

#include <exception>
#include <vector>

#include "distributor.h"

// object that reads sources from the disk or from the unicast network
// connection
class Reader : public Distributor
{
protected:
  Reader() {}

  // Registers an error and finish the current task
  void register_error(uint8_t status, const char *fmt, ...);

private:
  // Prohibit coping for objects of this class
  Reader(const Reader&);
  Reader& operator=(const Reader&);
};
#endif
