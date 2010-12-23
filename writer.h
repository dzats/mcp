#ifndef WRITER_H_HEADER
#define WRITER_H_HEADER 1
#include <assert.h>
#include <algorithm>
#include <string>

#include "reader.h"

// The Writer class describing work of an abstract writer with the buffer
class Writer
{
protected:
  Reader *reader;
  Reader::Client *w;

  Writer(Reader *b, Reader::Client *worker) : reader(b),
      w(worker)
  {
    assert(!w->is_present);
    w->is_present = true;
  }
  ~Writer()
  {
    reader->drop_writer(w);
  }

  Reader::TaskHeader* get_task()
  {
    return reader->get_writer_task(w);
  }

  void submit_task()
  {
    return reader->submit_writer_task(w);
  }

  int get_data() { return reader->get_data(w); }
  void update_position(int count)
  {
    return reader->update_writer_position(count, w);
  }
  void *pointer() { return reader->wposition(w); }
  // Checksum of the last file received
  const MD5sum* checksum() { return reader->get_checksum(); }
};
#endif
