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
    pthread_mutex_lock(&reader->_mutex);
    w->is_present = false;
    pthread_cond_signal(&reader->space_ready_cond);
    if (reader->all_writers_done()) {
      pthread_cond_signal(&reader->writers_finished_cond);
    }
    pthread_mutex_unlock(&reader->_mutex);
  }

  Reader::TaskHeader* get_task()
  {
    pthread_mutex_lock(&reader->_mutex);
    while (w->is_done) {
      // Wait for the new task become available
      pthread_cond_wait(&reader->operation_ready_cond, &reader->_mutex);
    }
    pthread_mutex_unlock(&reader->_mutex);
    return &reader->operation;
  }

  void submit_task()
  {
    pthread_mutex_lock(&reader->_mutex);
    w->is_done = true;
    if (w->status != STATUS_OK) {
      pthread_cond_signal(&reader->space_ready_cond);
    }
    if (reader->all_writers_done()) {
      pthread_cond_signal(&reader->writers_finished_cond);
    }
    pthread_mutex_unlock(&reader->_mutex);
  }

  int get_data() { return reader->get_data(w); }
  void update_position(int count)
  {
    return reader->update_writer_position(count, w);
  }
  void *pointer() { return reader->wposition(w); }
  // Checksum of the last file received
  MD5sum* checksum() { return &reader->checksum; }
};
#endif
